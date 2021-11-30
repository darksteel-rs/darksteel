use self::{runtime::Runtime, task::ProcessId};
use super::*;
use crate::prelude::{Modules, TaskError};
use std::sync::{Arc, Weak};
use tokio::{
    runtime::Handle,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex, OwnedMutexGuard,
    },
    task::JoinHandle,
};

pub struct ProcessContext<E>
where
    E: TaskError,
{
    pid: ProcessId,
    process: Arc<dyn Process<E>>,
    tx: UnboundedSender<ProcessSignal<E>>,
    tx_parent: UnboundedSender<ProcessSignal<E>>,
    rx: OwnedMutexGuard<UnboundedReceiver<ProcessSignal<E>>>,
    modules: Modules,
    runtime: Weak<Runtime<E>>,
}

impl<E> ProcessContext<E>
where
    E: TaskError,
{
    pub fn pid(&self) -> ProcessId {
        self.pid
    }

    pub fn process(&self) -> &dyn Process<E> {
        &*self.process
    }

    pub fn sender(&self) -> UnboundedSender<ProcessSignal<E>> {
        self.tx.clone()
    }

    pub fn parent(&self) -> UnboundedSender<ProcessSignal<E>> {
        self.tx_parent.clone()
    }

    pub async fn recv(&mut self) -> Option<ProcessSignal<E>> {
        self.rx.recv().await
    }

    pub fn modules(&self) -> Modules {
        self.modules.clone()
    }

    pub fn runtime(&self) -> Option<Arc<Runtime<E>>> {
        self.runtime.upgrade()
    }
}

#[derive(Clone)]
pub struct ProcessRef<E>
where
    E: TaskError,
{
    pid: ProcessId,
    process: Arc<dyn Process<E>>,
    tx: UnboundedSender<ProcessSignal<E>>,
    tx_parent: UnboundedSender<ProcessSignal<E>>,
}

impl<E> std::fmt::Debug for ProcessRef<E>
where
    E: TaskError,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessRef")
            .field("pid", &self.pid)
            .field("tx", &self.tx)
            .field("tx_parent", &self.tx_parent)
            .finish()
    }
}

impl<E> ProcessRef<E>
where
    E: TaskError,
{
    pub fn pid(&self) -> ProcessId {
        self.pid
    }

    pub fn process(&self) -> &dyn Process<E> {
        &*self.process
    }

    pub fn sender(&self) -> UnboundedSender<ProcessSignal<E>> {
        self.tx.clone()
    }

    pub fn parent(&self) -> UnboundedSender<ProcessSignal<E>> {
        self.tx_parent.clone()
    }
}

#[derive(Clone)]
pub struct ProcessContainer<E>
where
    E: TaskError,
{
    pid: ProcessId,
    process: Arc<dyn Process<E>>,
    modules: Modules,
    tx: UnboundedSender<ProcessSignal<E>>,
    tx_parent: UnboundedSender<ProcessSignal<E>>,
    rx: Arc<Mutex<UnboundedReceiver<ProcessSignal<E>>>>,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    runtime: Weak<Runtime<E>>,
}

impl<E> ProcessContainer<E>
where
    E: TaskError,
{
    pub fn new(
        process: Arc<dyn Process<E>>,
        modules: Modules,
        tx_parent: UnboundedSender<ProcessSignal<E>>,
        runtime: Weak<Runtime<E>>,
    ) -> Self {
        let (tx, rx) = unbounded_channel();

        Self {
            pid: global_pid(),
            process,
            modules,
            tx,
            tx_parent,
            rx: Arc::new(Mutex::new(rx)),
            handle: Default::default(),
            runtime,
        }
    }

    pub fn pid(&self) -> ProcessId {
        self.pid
    }

    pub fn process(&self) -> &dyn Process<E> {
        &*self.process
    }

    pub async fn context(&self) -> ProcessContext<E> {
        ProcessContext {
            pid: self.pid,
            process: self.process.clone(),
            tx: self.tx.clone(),
            tx_parent: self.tx_parent.clone(),
            rx: self.rx.clone().lock_owned().await,
            modules: self.modules.clone(),
            runtime: self.runtime.clone(),
        }
    }

    pub async fn get_ref(&self) -> ProcessRef<E> {
        ProcessRef {
            pid: self.pid,
            process: self.process.clone(),
            tx: self.tx.clone(),
            tx_parent: self.tx_parent.clone(),
        }
    }

    pub fn sender(&self) -> UnboundedSender<ProcessSignal<E>> {
        self.tx.clone()
    }

    pub fn parent(&self) -> UnboundedSender<ProcessSignal<E>> {
        self.tx_parent.clone()
    }

    pub fn modules(&self) -> Modules {
        self.modules.clone()
    }

    pub async fn handle_signals(&mut self) {
        let runtime = Handle::current();
        let process = self.process.clone();
        let context = self.context().await;

        *self.handle.lock().await =
            Some(runtime.spawn(async move { process.handle_signals(context).await }));
    }
}
