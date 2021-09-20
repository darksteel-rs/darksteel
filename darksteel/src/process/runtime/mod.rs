use std::{collections::HashMap, sync::Arc};

use super::{container::ProcessRef, task::ProcessId, Process, ProcessContainer, ProcessSignal};
use crate::{
    prelude::{Modules, TaskError},
    process::{send, ExitReason},
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Mutex, RwLock,
};

pub struct Runtime<E>
where
    E: TaskError,
{
    processes: RwLock<HashMap<ProcessId, ProcessContainer<E>>>,
    named: RwLock<HashMap<String, ProcessId>>,
    modules: Modules,
    tx_signal_root: UnboundedSender<ProcessSignal<E>>,
    rx_signal_root: Mutex<UnboundedReceiver<ProcessSignal<E>>>,
}

impl<E> Runtime<E>
where
    E: TaskError,
{
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

    pub async fn name<S: Into<String>>(&self, pid: ProcessId, name: S) {
        let names = self.named.write().await;
        let name = name.into();

        if !names.contains_key(&name) {
            self.named.write().await.insert(name, pid);
        } else {
            tracing::error!(
                "Could not give pid({pid}) name: {name} - Already taken",
                pid = pid,
                name = name
            );
        }
    }

    pub async fn spawn(self: &Arc<Self>, process: Arc<dyn Process<E>>) -> ProcessRef<E> {
        self.spawn_with_parent(process, None).await
    }

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
        let mut container = ProcessContainer::new(
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

        container.handle_signals().await;
        self.processes.write().await.insert(pid, container);
        process.handle_spawn(pid, self).await;

        reference
    }

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

    pub async fn block_on_all(&self) {
        let mut inbox = self.rx_signal_root.lock().await;

        while let Some(signal) = inbox.recv().await {
            match signal {
                ProcessSignal::Exit(pid, reason) => match reason {
                    ExitReason::Normal => {
                        // TODO: Processes in root should be restarted depending
                        // on their ChildRestartPolicy
                        if let Some(process) = self.processes.read().await.get(&pid) {
                            send(&process.sender(), ProcessSignal::Shutdown)
                        } else {
                            tracing::warn!(
                                "Received a signal from a process that can't be sent to."
                            );
                        }
                    }
                    ExitReason::Shutdown => {
                        let mut processes = self.processes.write().await;

                        processes.remove(&pid);

                        if processes.len() == 0 {
                            break;
                        }
                    }
                    _ => (),
                },
                _ => (),
            }
        }
    }
}
