use self::{runtime::Runtime, task::ProcessId};
use crate::prelude::TaskError;
use container::*;
use downcast_rs::{impl_downcast, Downcast};
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};
use tokio::sync::mpsc::UnboundedSender;

pub mod container;
pub mod handler;
pub mod runtime;
pub mod supervisor;
pub mod task;

static GLOBAL_PID_COUNT: AtomicU64 = AtomicU64::new(0);
static GLOBAL_ID_COUNT: AtomicU64 = AtomicU64::new(0);

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ChildTerminationPolicy {
    /// The task is unconditionally terminated
    Brutal,
    /// time-out value means that the supervisor tells the child process to
    /// terminate and then waits for an exit signal back. If no exit signal is
    /// received within the specified time, the task is unconditionally
    /// terminated.
    Timeout(Duration),
    /// If the child process is another supervisor, it must be set to infinity
    /// to give the subtree enough time to shut down. It is also allowed to be
    /// set to infinity, if the child process is a worker.
    Infinity,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ChildRestartPolicy {
    /// A permanent task is always restarted
    Permanent,
    /// A temporary child process is never restarted (not even when the
    /// supervisor restart strategy is `RestForOne` or `OneForAll` and a sibling
    /// death causes the temporary process to be terminated).
    Temporary,
    /// A transient child process is restarted only if it terminates abnormally.
    Transient,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExitReason<E>
where
    E: TaskError,
{
    /// The task ran to completion and exited normally.
    Normal,
    /// The task failed and returned an error.
    Error(E),
    /// The task panicked mid-execution.
    Panic,
    /// The task had been asked to terminate.
    Terminate,
    /// The task had been asked to shutdown.
    Shutdown,
}

#[derive(Clone, Debug)]
pub enum ProcessSignal<E>
where
    E: TaskError,
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
    /// reason for its exit in `ExitReason<E>`.
    Exit(ProcessId, ExitReason<E>),
}

#[derive(Clone)]
pub struct ProcessConfig {
    pub name: Option<String>,
    pub termination_policy: ChildTerminationPolicy,
    pub restart_policy: ChildRestartPolicy,
    pub significant: bool,
}

impl ProcessConfig {
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
        handler::Handler,
        supervisor::Supervisor,
        task::{Task, TaskError},
    };

    pub trait Sealed {}

    impl<E> Sealed for Task<E> where E: TaskError {}
    impl<E> Sealed for Supervisor<E> where E: TaskError {}
    impl<E> Sealed for Handler<E> where E: TaskError {}
}

#[crate::async_trait]
pub trait Process<E>: Downcast + Send + Sync + private::Sealed
where
    E: TaskError,
{
    fn id(&self) -> ProcessId;
    fn config(&self) -> ProcessConfig;
    async fn handle_spawn(&self, pid: ProcessId, runtime: &Arc<Runtime<E>>);
    async fn handle_signals(&self, context: ProcessContext<E>);
}

impl_downcast!(Process<E> where E: TaskError);

pub(crate) fn global_id() -> ProcessId {
    GLOBAL_ID_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

pub(crate) fn global_pid() -> ProcessId {
    GLOBAL_PID_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

pub(crate) fn send<'a, S: Into<Option<&'a UnboundedSender<ProcessSignal<E>>>>, E: TaskError>(
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
