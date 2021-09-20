use super::runtime::Runtime;
use super::{global_id, send, ChildTerminationPolicy, Process, ProcessConfig, ProcessContext};
use crate::modules::Modules;
use crate::process::{ExitReason, ProcessSignal};
use dyn_clone::DynClone;
use futures::{
    future::{BoxFuture, Future},
    FutureExt,
};
use std::fmt::{self, Debug, Formatter};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub mod error;

pub use error::TaskError;

pub type ProcessId = u64;
pub type TaskResult<E> = Result<(), E>;

pub trait TaskTrait<E: TaskError>:
    Sync + Fn(Modules) -> BoxFuture<'static, TaskResult<E>> + DynClone + Send + 'static
{
}

impl<F, E> TaskTrait<E> for F
where
    F: Sync + Fn(Modules) -> BoxFuture<'static, TaskResult<E>> + DynClone + Send + 'static,
    E: TaskError,
{
}

dyn_clone::clone_trait_object!(<E> TaskTrait<E> where E: TaskError);

pub struct Task<E>
where
    E: TaskError,
{
    id: ProcessId,
    task: Box<dyn TaskTrait<E>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    active: Arc<AtomicBool>,
    config: ProcessConfig,
}

impl<E> Task<E>
where
    E: TaskError,
{
    pub fn new<F>(task: fn(Modules) -> F) -> Arc<Task<E>>
    where
        F: Future<Output = TaskResult<E>> + Send + 'static,
    {
        Self::new_with_config(Default::default(), task)
    }

    pub fn new_with_config<F>(config: ProcessConfig, task: fn(Modules) -> F) -> Arc<Task<E>>
    where
        F: Future<Output = TaskResult<E>> + Send + 'static,
    {
        Arc::new(Task {
            id: global_id(),
            task: Box::new(move |modules| Box::pin(task(modules))),
            handle: Default::default(),
            active: Arc::new(AtomicBool::new(false)),
            config,
        })
    }
}

impl<E> Debug for Task<E>
where
    E: TaskError,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task").field("id", &self.id).finish()
    }
}

#[crate::async_trait]
impl<E> Process<E> for Task<E>
where
    E: TaskError,
{
    fn id(&self) -> ProcessId {
        self.id
    }

    fn config(&self) -> ProcessConfig {
        self.config.clone()
    }

    async fn handle_spawn(&self, _: ProcessId, _: &Arc<Runtime<E>>) {
        // Do nothing
    }

    async fn handle_signals(&self, mut context: ProcessContext<E>) {
        let pid = context.pid();
        let runtime = Handle::current();
        let parent = context.parent();

        while let Some(signal) = context.recv().await {
            match signal {
                ProcessSignal::Start => {
                    let task = self.task.clone();
                    let modules = context.modules();
                    let active = self.active.clone();

                    if !self.active.load(Ordering::SeqCst) {
                        active.store(true, Ordering::SeqCst);

                        let tx_parent = parent.clone();
                        let handle = runtime.spawn(async move {
                            send(&tx_parent, ProcessSignal::Active(pid));
                            let result = AssertUnwindSafe((task)(modules)).catch_unwind().await;
                            active.store(false, Ordering::SeqCst);
                            match result {
                                Ok(result) => match result {
                                    Ok(()) => send(
                                        &tx_parent,
                                        ProcessSignal::Exit(pid, ExitReason::Normal),
                                    ),
                                    Err(error) => send(
                                        &tx_parent,
                                        ProcessSignal::Exit(pid, ExitReason::Error(error)),
                                    ),
                                },
                                Err(_) => {
                                    send(&tx_parent, ProcessSignal::Exit(pid, ExitReason::Panic))
                                }
                            }
                        });

                        *self.handle.lock().await = Some(handle);
                    } else {
                        tracing::warn!(
                            "Attempted to start Task({pid}) - Already active",
                            pid = pid
                        );
                    }
                }
                ProcessSignal::Terminate => {
                    if self.active.load(Ordering::SeqCst) {
                        match self.config.termination_policy {
                            ChildTerminationPolicy::Brutal => {
                                match &mut *self.handle.lock().await {
                                    Some(handle) => {
                                        let active = self.active.load(Ordering::SeqCst);
                                        handle.abort();
                                        if active {
                                            if let Err(error) = handle.await {
                                                if error.is_cancelled() {
                                                    send(
                                                        &parent,
                                                        ProcessSignal::Exit(
                                                            pid,
                                                            ExitReason::Terminate,
                                                        ),
                                                    )
                                                } else if error.is_panic() {
                                                    send(
                                                        &parent,
                                                        ProcessSignal::Exit(pid, ExitReason::Panic),
                                                    )
                                                }
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                            ChildTerminationPolicy::Timeout(timeout) => {
                                match &mut *self.handle.lock().await {
                                    Some(handle) => {
                                        sleep(timeout).await;
                                        let active = self.active.load(Ordering::SeqCst);
                                        handle.abort();
                                        if active {
                                            if let Err(error) = handle.await {
                                                if error.is_cancelled() {
                                                    send(
                                                        &parent,
                                                        ProcessSignal::Exit(
                                                            pid,
                                                            ExitReason::Terminate,
                                                        ),
                                                    )
                                                } else if error.is_panic() {
                                                    send(
                                                        &parent,
                                                        ProcessSignal::Exit(pid, ExitReason::Panic),
                                                    )
                                                }
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                            ChildTerminationPolicy::Infinity => {
                                match &mut *self.handle.lock().await {
                                    Some(handle) => {
                                        handle
                                            .await
                                            .map_err(|error| {
                                                tracing::error!(
                                                    "Task({pid}) Terminated abnormally: {error:?}",
                                                    pid = pid,
                                                    error = error
                                                )
                                            })
                                            .ok();
                                    }
                                    _ => (),
                                }
                            }
                        }
                        self.active.store(false, Ordering::SeqCst);
                    } else {
                        tracing::warn!(
                            "Attempted to terminate Task({pid}) - Already exited",
                            pid = pid
                        );
                    }
                }
                ProcessSignal::Shutdown => {
                    if self.active.load(Ordering::SeqCst) {
                        match self.config.termination_policy {
                            ChildTerminationPolicy::Brutal => {
                                match &mut *self.handle.lock().await {
                                    Some(handle) => {
                                        handle.abort();
                                    }
                                    _ => (),
                                }
                            }
                            ChildTerminationPolicy::Timeout(timeout) => {
                                match &mut *self.handle.lock().await {
                                    Some(handle) => {
                                        sleep(timeout).await;
                                        handle.abort();
                                    }
                                    _ => (),
                                }
                            }
                            ChildTerminationPolicy::Infinity => {
                                match &mut *self.handle.lock().await {
                                    Some(handle) => {
                                        handle
                                            .await
                                            .map_err(|error| {
                                                tracing::error!(
                                                    "Task({pid}) Task terminated abnormally during shutdown: {error:?}",
                                                    pid = pid,
                                                    error = error
                                                )
                                            })
                                            .ok();
                                    }
                                    _ => (),
                                }
                            }
                        }
                        self.active.store(false, Ordering::SeqCst);
                        send(&parent, ProcessSignal::Exit(pid, ExitReason::Shutdown));
                        break;
                    } else {
                        tracing::info!("Shutdown acknowledged on exited Task({pid})", pid = pid);
                        send(&parent, ProcessSignal::Exit(pid, ExitReason::Shutdown));
                        break;
                    }
                }
                signal => tracing::warn!(
                    "Task({pid}) received signal `{signal:?}` without delegate",
                    pid = pid,
                    signal = signal
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[derive(Clone, Debug)]
    struct MyError;

    impl fmt::Display for MyError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "Error")
        }
    }

    impl Error for MyError {
        fn description(&self) -> &str {
            "Error"
        }
    }

    async fn task(_: Modules) -> TaskResult<MyError> {
        Ok(())
    }

    #[tokio::test]
    // This isn't meant to do anything. It's a compile time check to make
    // sure that the async functions match the trait required for being a
    // task.
    async fn create_task() {
        let _ = Task::new(task);
    }
}
