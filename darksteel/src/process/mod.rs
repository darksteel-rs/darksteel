use self::{runtime::Runtime, task::ProcessId};
use crate::process::task::TaskErrorTrait;
use container::*;
use downcast_rs::{impl_downcast, Downcast};
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};
use tokio::sync::mpsc::UnboundedSender;

/// Types relating to [`Process`] internals.
pub mod container;
/// The underlying runtime for the [`Environment`](crate::environment::Environment).
pub mod runtime;
/// Types for the [`Supervisor`](supervisor::Supervisor) process which handles
/// process restart behaviour within an environment.
pub mod supervisor;
/// Types for the [`Task`](task::Task) process, the building block of the
/// entire system.
pub mod task;

static GLOBAL_PID_COUNT: AtomicU64 = AtomicU64::new(0);
static GLOBAL_ID_COUNT: AtomicU64 = AtomicU64::new(0);

#[derive(Copy, Clone, Debug, PartialEq)]
/// A child termination policy that will determine the conditions for how a
/// process is externally terminated.
pub enum ChildTerminationPolicy {
    /// The task is unconditionally terminated and does not wait for it to
    /// finish.
    Brutal,
    /// Timeout means that the supervisor tells the child process to terminate
    /// and then waits for an exit signal back. If no exit signal is received
    /// within the specified time, the task is unconditionally terminated.
    Timeout(Duration),
    /// The child will not terminate until it has exited normally or abnormally.
    /// This is the default on supervisors.
    Infinity,
}

/// A restart policy that determines what conditions an individual child will
/// restart.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ChildRestartPolicy {
    /// A permanent process is always restarted
    Permanent,
    /// A temporary child process is never restarted (not even when the
    /// supervisor restart strategy is `RestForOne` or `OneForAll` and a sibling
    /// death causes the temporary process to be terminated).
    Temporary,
    /// A transient child process is restarted only if it terminates abnormally.
    Transient,
}

/// A reason for why a process exits. This is useful for controlling how
/// processes restart.
#[derive(Clone, Debug, PartialEq)]
pub enum ExitReason<E>
where
    E: TaskErrorTrait,
{
    /// The process ran to completion and exited normally.
    Normal,
    /// The process failed and returned an error.
    Error(E),
    /// The process panicked mid-execution.
    Panic,
    /// The process had been asked to terminate.
    Terminate,
    /// The task had been asked to, or self initiated a shutdown.
    Shutdown,
}

/// A signal sent from one process to another to trigger an action, or alert of
/// a particular state.
#[derive(Clone, Debug)]
pub enum ProcessSignal<E>
where
    E: TaskErrorTrait,
{
    /// Sending this signal to a process will start it.
    Start,
    /// Sending this signal to a process will terminate it.
    Terminate,
    /// Sending this signal to a process will shut it down and will stop
    /// responding to signals.
    Shutdown,
    /// This signal is sent from a process after it has been started to notify
    /// its parent that it is active.
    Active(ProcessId),
    /// This signal is sent from a process after it has exited and provides a
    /// reason for its exit in [`ExitReason`].
    Exit(ProcessId, ExitReason<E>),
}

/// A configuration of policies on how a process should behave under certain
/// conditions.
#[derive(Clone, Debug)]
pub struct ProcessConfig {
    pub name: Option<String>,
    pub termination_policy: ChildTerminationPolicy,
    pub restart_policy: ChildRestartPolicy,
    pub significant: bool,
}

impl ProcessConfig {
    /// Set a name that will be applied to the process that uses this config.
    pub fn name<S: Into<String>>(&mut self, name: S) {
        self.name = Some(name.into());
    }
}

impl Default for ProcessConfig {
    fn default() -> Self {
        Self {
            name: None,
            termination_policy: ChildTerminationPolicy::Brutal,
            restart_policy: ChildRestartPolicy::Permanent,
            significant: false,
        }
    }
}

mod private {
    use super::{
        supervisor::Supervisor,
        task::{Task, TaskErrorTrait},
    };

    pub trait Sealed {}

    impl<E> Sealed for Task<E> where E: TaskErrorTrait {}
    impl<E> Sealed for Supervisor<E> where E: TaskErrorTrait {}
}

/// A trait shared by all runnable processes within an [`Environment`](crate::environment::Environment).
#[crate::async_trait]
pub trait Process<E>: Downcast + Send + Sync + private::Sealed
where
    E: TaskErrorTrait,
{
    fn id(&self) -> ProcessId;
    fn config(&self) -> ProcessConfig;
    async fn handle_spawn(&self, pid: ProcessId, runtime: &Arc<Runtime<E>>);
    async fn handle_signals(&self, context: ProcessContext<E>);
}

impl_downcast!(Process<E> where E: TaskErrorTrait);

pub(crate) fn global_id() -> ProcessId {
    GLOBAL_ID_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

pub(crate) fn global_pid() -> ProcessId {
    GLOBAL_PID_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

pub(crate) fn send<
    'a,
    S: Into<Option<&'a UnboundedSender<ProcessSignal<E>>>>,
    E: TaskErrorTrait,
>(
    sender: S,
    signal: ProcessSignal<E>,
) {
    let sender = sender.into();
    if let Some(sender) = sender {
        if let Err(error) = sender.send(signal) {
            tracing::error!("Could not send signal: {error}", error = error);
        }
    } else {
        tracing::error!("Could not send signal: None supplied");
    }
}
