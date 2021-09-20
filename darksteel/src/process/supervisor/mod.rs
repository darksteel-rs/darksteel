use super::container::ProcessRef;
use super::handler::Handler;
use super::runtime::Runtime;
use super::{
    global_id, ChildRestartPolicy, ExitReason, Process, ProcessConfig, ProcessContext,
    ProcessSignal,
};
use crate::prelude::TaskError;
use crate::process::{send, task::*};
use error::*;
use state::*;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub mod error;
mod state;

#[derive(Copy, Clone, Debug)]
pub enum RestartPolicy {
    /// If a child process terminates, all other child processes are terminated,
    /// and then all child processes, including the terminated one, are
    /// restarted.
    OneForAll,
    /// If a child process terminates, only that process is restarted.
    OneForOne,
    /// If a child process terminates, the rest of the child processes (that is,
    /// the child processes after the terminated process in start order) are
    /// terminated. Then the terminated child process and the rest of the child
    /// processes are restarted.
    RestForOne,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum AutomaticTerminationPolicy {
    /// Automatic termination is disabled.
    ///
    /// In this mode, significant children are not accepted. If the children
    /// created from `build` contains significant children, the supervisor will
    /// refuse to start. Attempts to start significant children dynamically will
    /// be rejected.
    ///
    /// This is the default setting.
    Never,
    /// The supervisor will automatically terminate itself when any significant
    /// child terminates, that is, when a transient significant child terminates
    /// normally or when a temporary significant child terminates normally or
    /// abnormally.
    Any,
    /// The supervisor will automatically terminate itself when all significant
    /// children have terminated, that is, when the last active significant
    /// child terminates. The same rules as for `Any` apply.
    All,
}

pub struct SupervisorBuilder<E>
where
    E: TaskError,
{
    process_config: ProcessConfig,
    supervisor_config: SupervisorConfig,
    child_specs: Vec<Arc<dyn Process<E> + 'static>>,
}

impl<E> SupervisorBuilder<E>
where
    E: TaskError,
{
    pub fn with(
        mut self,
        child: Arc<dyn Process<E> + 'static>,
    ) -> Result<Self, SupervisorBuilderError> {
        if child.config().significant
            && self.supervisor_config.termination_policy == AutomaticTerminationPolicy::Never
        {
            return Err(SupervisorBuilderError::SignificantChild(child.id()));
        }
        if let Some(_) = child.downcast_ref::<Handler<E>>() {
            return Err(SupervisorBuilderError::ChildIsHandler(child.id()));
        }
        for existing_child in &self.child_specs {
            if child.id() == existing_child.id() {
                return Err(SupervisorBuilderError::ChildExists(child.id()));
            }
        }
        self.child_specs.push(child);
        Ok(self)
    }

    pub fn finish(self) -> Arc<Supervisor<E>> {
        Supervisor::new(
            self.process_config,
            self.supervisor_config,
            self.child_specs,
        )
    }
}

#[derive(Clone, Debug)]
pub struct SupervisorConfig {
    pub restart_policy: RestartPolicy,
    pub termination_policy: AutomaticTerminationPolicy,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            restart_policy: RestartPolicy::OneForAll,
            termination_policy: AutomaticTerminationPolicy::Never,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChildOrder(BTreeMap<ProcessId, u16>, u16);

impl ChildOrder {
    /// Insert new pid
    fn insert(&mut self, pid: ProcessId) {
        self.0.insert(pid, self.1);
        self.1 += 1;
    }

    /// Get pids as an ordered `Vec`.
    fn pid_vec(&self) -> Vec<ProcessId> {
        let mut order: Vec<(&ProcessId, &u16)> = self.0.iter().collect();
        order.sort_by(|a, b| a.1.cmp(&b.1));
        order.into_iter().map(|(pid, _)| *pid).collect()
    }

    /// Get pids in a `BTreeSet`.
    fn pid_set(&self) -> BTreeSet<ProcessId> {
        self.0.iter().map(|(pid, _)| *pid).collect()
    }
}

pub struct Supervisor<E>
where
    E: TaskError,
{
    id: ProcessId,
    child_specs: Vec<Arc<dyn Process<E> + 'static>>,
    child_order: Mutex<ChildOrder>,
    child_refs: RwLock<HashMap<ProcessId, ProcessRef<E>>>,
    process_config: ProcessConfig,
    supervisor_config: SupervisorConfig,
}

impl<E> Supervisor<E>
where
    E: TaskError,
{
    fn new(
        process_config: ProcessConfig,
        supervisor_config: SupervisorConfig,
        child_specs: Vec<Arc<dyn Process<E> + 'static>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id: global_id(),
            child_specs,
            child_order: Default::default(),
            child_refs: Default::default(),
            process_config,
            supervisor_config,
        })
    }

    pub fn build() -> SupervisorBuilder<E> {
        SupervisorBuilder {
            process_config: Default::default(),
            supervisor_config: Default::default(),
            child_specs: Default::default(),
        }
    }

    pub fn build_with_config(
        process_config: ProcessConfig,
        supervisor_config: SupervisorConfig,
    ) -> SupervisorBuilder<E> {
        SupervisorBuilder {
            process_config,
            supervisor_config,
            child_specs: Vec::new(),
        }
    }
}

#[crate::async_trait]
impl<E> Process<E> for Supervisor<E>
where
    E: TaskError,
{
    fn id(&self) -> ProcessId {
        self.id
    }

    fn config(&self) -> ProcessConfig {
        self.process_config.clone()
    }

    async fn handle_spawn(&self, pid: ProcessId, runtime: &Arc<Runtime<E>>) {
        for spec in &self.child_specs {
            let child = runtime.spawn_with_parent(spec.clone(), pid).await;
            self.child_order.lock().await.insert(child.pid());
            self.child_refs.write().await.insert(child.pid(), child);
        }
    }

    async fn handle_signals(&self, mut context: ProcessContext<E>) {
        let pid = context.pid();
        let mut manager = StateManager::new(
            self.supervisor_config.clone(),
            self.child_order.lock().await.clone(),
        );
        let parent = context.parent();

        while let Some(signal) = context.recv().await {
            let child_policies = self
                .child_refs
                .read()
                .await
                .iter()
                .map(|(pid, reference)| (*pid, reference.process().config().restart_policy))
                .collect::<HashMap<ProcessId, ChildRestartPolicy>>();

            match manager.next_action(&child_policies, &signal) {
                StateAction::None => (),
                StateAction::Start(pid, finalise) => {
                    if let Some(child) = self.child_refs.read().await.get(&pid) {
                        send(&child.sender(), ProcessSignal::Start);
                    } else {
                        tracing::error!("Could not find Child({pid})", pid = pid);
                    }
                    match finalise {
                        Finalise::None => (),
                        Finalise::Start => send(&parent, ProcessSignal::Active(pid)),
                        Finalise::Terminate => {
                            send(&parent, ProcessSignal::Exit(pid, ExitReason::Terminate))
                        }
                        Finalise::Shutdown => {
                            send(&parent, ProcessSignal::Exit(pid, ExitReason::Shutdown));
                            break;
                        }
                    }
                }
                StateAction::Terminate(pid) => {
                    if let Some(child) = self.child_refs.read().await.get(&pid) {
                        send(&child.sender(), ProcessSignal::Terminate);
                    } else {
                        tracing::error!("Could not find Child({pid})", pid = pid);
                    }
                }
                StateAction::Shutdown { pids_required } => {
                    for pid in pids_required {
                        if let Some(child) = self.child_refs.read().await.get(&pid) {
                            send(&child.sender(), ProcessSignal::Shutdown);
                        }
                    }
                }
                StateAction::Finalise(finalise) => match finalise {
                    Finalise::None => (),
                    Finalise::Start => send(&parent, ProcessSignal::Active(pid)),
                    Finalise::Terminate => {
                        send(&parent, ProcessSignal::Exit(pid, ExitReason::Terminate))
                    }
                    Finalise::Shutdown => {
                        send(&parent, ProcessSignal::Exit(pid, ExitReason::Shutdown));
                        break;
                    }
                },
            }
        }
    }
}
