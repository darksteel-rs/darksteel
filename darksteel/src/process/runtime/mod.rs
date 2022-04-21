use std::{collections::HashMap, sync::Arc};

use super::{
    container::ProcessRef, task::ProcessId, ChildRestartPolicy, Process, ProcessContainer,
    ProcessSignal,
};
use crate::{
    prelude::{Modules, TaskErrorTrait},
    process::{send, ExitReason},
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Mutex, RwLock,
};

/// The underlying runtime of the environment.
pub struct Runtime<E>
where
    E: TaskErrorTrait,
{
    processes: RwLock<HashMap<ProcessId, ProcessContainer<E>>>,
    named: RwLock<HashMap<String, ProcessId>>,
    modules: Modules,
    tx_signal_root: UnboundedSender<ProcessSignal<E>>,
    rx_signal_root: Mutex<UnboundedReceiver<ProcessSignal<E>>>,
}

impl<E> Runtime<E>
where
    E: TaskErrorTrait,
{
    /// Create a new runtime.
    pub(crate) fn new(modules: Modules) -> Arc<Self> {
        let (tx_signal_root, rx_signal_root) = unbounded_channel();

        Arc::new(Self {
            processes: Default::default(),
            named: Default::default(),
            modules,
            tx_signal_root,
            rx_signal_root: Mutex::new(rx_signal_root),
        })
    }
    /// Name a pid currently in the system.
    pub async fn name<S: Into<String>>(&self, pid: ProcessId, name: S) {
        let mut names = self.named.write().await;
        let name = name.into();

        if !names.contains_key(&name) {
            names.insert(name, pid);
        } else {
            tracing::error!(
                "Could not give pid({pid}) name: {name} - Already taken",
                pid = pid,
                name = name
            );
        }
    }
    /// Spawn a process at the root of the runtime.
    pub async fn spawn(self: &Arc<Self>, process: Arc<dyn Process<E>>) -> ProcessRef<E> {
        self.spawn_with_parent(process, None).await
    }
    /// Spawn a process with a given parent pid.
    pub async fn spawn_with_parent<P: Into<Option<ProcessId>>>(
        self: &Arc<Self>,
        process: Arc<dyn Process<E>>,
        parent: P,
    ) -> ProcessRef<E> {
        let parent = if let Some(parent) = parent.into() {
            if let Some(process) = self.processes.read().await.get(&parent) {
                process.sender()
            } else {
                tracing::warn!(
                    "Could not spawn underneath pid `{pid}` - Spawning in root",
                    pid = parent
                );
                self.tx_signal_root.clone()
            }
        } else {
            self.tx_signal_root.clone()
        };
        let container = ProcessContainer::new(
            process.clone(),
            self.modules.clone(),
            parent,
            Arc::downgrade(&self),
        );
        let pid = container.pid();
        let reference = container.get_ref().await;

        if let Some(name) = process.config().name {
            self.name(pid, name).await;
        }

        self.processes.write().await.insert(pid, container);

        process.handle_spawn(pid, self).await;

        if let Some(process) = self.processes.write().await.get_mut(&pid) {
            process.handle_signals().await;
        }

        reference
    }
    /// Get a process's send handle by its pid.
    pub async fn get_sender_by_pid(
        &self,
        pid: ProcessId,
    ) -> Option<UnboundedSender<ProcessSignal<E>>> {
        if let Some(process) = self.processes.read().await.get(&pid) {
            Some(process.sender())
        } else {
            None
        }
    }
    /// Get a process's send handle by its name.
    pub async fn get_sender_by_name<S: Into<String>>(
        &self,
        name: S,
    ) -> Option<UnboundedSender<ProcessSignal<E>>> {
        if let Some(pid) = self.named.read().await.get(&name.into()) {
            if let Some(process) = self.processes.read().await.get(&pid) {
                Some(process.sender())
            } else {
                None
            }
        } else {
            None
        }
    }
    /// Boot the runtime and block until all processes shutdown.
    pub(crate) async fn block_on_all(&self) {
        let mut inbox = self.rx_signal_root.lock().await;

        while let Some(signal) = inbox.recv().await {
            match signal {
                ProcessSignal::Exit(pid, reason) => match reason {
                    ExitReason::Normal => {
                        tracing::info!("Root Process({pid}) exited normally", pid = pid);
                        if let Some(process) = self.processes.read().await.get(&pid) {
                            match process.process().config().restart_policy {
                                ChildRestartPolicy::Permanent => {
                                    send(&process.sender(), ProcessSignal::Start);
                                }
                                ChildRestartPolicy::Temporary => {
                                    send(&process.sender(), ProcessSignal::Shutdown);
                                }
                                ChildRestartPolicy::Transient => {
                                    send(&process.sender(), ProcessSignal::Shutdown);
                                }
                            }
                        } else {
                            tracing::warn!(
                                "Received a signal from a process that can't be sent to."
                            );
                        }
                    }
                    ExitReason::Error(error) => {
                        tracing::info!(
                            "Root Process({pid}) exited with error: {error:?}",
                            pid = pid,
                            error = error
                        );
                        if let Some(process) = self.processes.read().await.get(&pid) {
                            match process.process().config().restart_policy {
                                ChildRestartPolicy::Permanent => {
                                    send(&process.sender(), ProcessSignal::Start);
                                }
                                ChildRestartPolicy::Temporary => {
                                    send(&process.sender(), ProcessSignal::Shutdown);
                                }
                                ChildRestartPolicy::Transient => {
                                    send(&process.sender(), ProcessSignal::Start);
                                }
                            }
                        } else {
                            tracing::warn!(
                                "Received a signal from a process that can't be sent to."
                            );
                        }
                    }
                    ExitReason::Panic => {
                        tracing::info!("Root Process({pid}) panicked", pid = pid);
                        if let Some(process) = self.processes.read().await.get(&pid) {
                            match process.process().config().restart_policy {
                                ChildRestartPolicy::Permanent => {
                                    send(&process.sender(), ProcessSignal::Start);
                                }
                                ChildRestartPolicy::Temporary => {
                                    send(&process.sender(), ProcessSignal::Shutdown);
                                }
                                ChildRestartPolicy::Transient => {
                                    send(&process.sender(), ProcessSignal::Start);
                                }
                            }
                        } else {
                            tracing::warn!(
                                "Received a signal from a process that can't be sent to."
                            );
                        }
                    }
                    ExitReason::Terminate => {
                        tracing::info!("Root Process({pid}) was terminated", pid = pid);
                        if let Some(process) = self.processes.read().await.get(&pid) {
                            match process.process().config().restart_policy {
                                ChildRestartPolicy::Permanent => {
                                    send(&process.sender(), ProcessSignal::Start);
                                }
                                ChildRestartPolicy::Temporary => {
                                    send(&process.sender(), ProcessSignal::Shutdown);
                                }
                                // If a root process is terminated, it is not
                                // considered abnormal.
                                ChildRestartPolicy::Transient => {
                                    send(&process.sender(), ProcessSignal::Shutdown);
                                }
                            }
                        } else {
                            tracing::warn!(
                                "Received a signal from a process that can't be sent to."
                            );
                        }
                    }
                    ExitReason::Shutdown => {
                        tracing::info!("Root Process({pid}) shutdown", pid = pid);
                        let mut processes = self.processes.write().await;

                        processes.remove(&pid);

                        if processes.len() == 0 {
                            break;
                        }
                    }
                },
                _ => (),
            }
        }
    }
}
