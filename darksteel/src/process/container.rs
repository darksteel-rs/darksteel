use self::{runtime::Runtime, task::ProcessId};
use super::*;
use crate::prelude::{Modules, TaskErrorTrait};
use std::sync::{Arc, Weak};
use tokio::{
    runtime::Handle,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex, OwnedMutexGuard,
    },
    task::JoinHandle,
};

/// A context derived from process internals.
pub struct ProcessContext<E>
where
    E: TaskErrorTrait,
{
    pid: ProcessId,
    modules: Modules,
    process: Arc<dyn Process<E>>,
    tx: UnboundedSender<ProcessSignal<E>>,
    tx_parent: UnboundedSender<ProcessSignal<E>>,
    rx: OwnedMutexGuard<UnboundedReceiver<ProcessSignal<E>>>,
    runtime: Weak<Runtime<E>>,
}

impl<E> ProcessContext<E>
where
    E: TaskErrorTrait,
{
    pub fn pid(&self) -> ProcessId {
        self.pid
    }
    /// Get a reference to the [`Process`](super::Process).
    pub fn process(&self) -> &dyn Process<E> {
        &*self.process
    }
    /// Derive a reference from the context.
    pub async fn get_ref(&self) -> ProcessRef<E> {
        ProcessRef {
            pid: self.pid,
            process: self.process.clone(),
            tx: self.tx.clone(),
            tx_parent: self.tx_parent.clone(),
        }
    }
    /// Get a sender handle to the process.
    pub fn sender(&self) -> UnboundedSender<ProcessSignal<E>> {
        self.tx.clone()
    }
    /// Get a sender handle to the process's parent.
    pub fn parent(&self) -> UnboundedSender<ProcessSignal<E>> {
        self.tx_parent.clone()
    }
    /// Receive a signal sent to the process.
    #[inline(always)]
    pub async fn recv(&mut self) -> Option<ProcessSignal<E>> {
        self.rx.recv().await
    }
    /// Get a handle to the modules provider.
    pub fn modules(&self) -> Modules {
        self.modules.clone()
    }
    /// Get a handle to the runtime.
    pub fn runtime(&self) -> Weak<Runtime<E>> {
        self.runtime.clone()
    }
}

/// A reference to a process and to its own inbox as well as its parent's.
#[derive(Clone)]
pub struct ProcessRef<E>
where
    E: TaskErrorTrait,
{
    pid: ProcessId,
    process: Arc<dyn Process<E>>,
    tx: UnboundedSender<ProcessSignal<E>>,
    tx_parent: UnboundedSender<ProcessSignal<E>>,
}

impl<E> std::fmt::Debug for ProcessRef<E>
where
    E: TaskErrorTrait,
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
    E: TaskErrorTrait,
{
    /// Get the pid this process refers to.
    pub fn pid(&self) -> ProcessId {
        self.pid
    }
    /// Get a reference to the process.
    pub fn process(&self) -> &dyn Process<E> {
        &*self.process
    }
    /// Get a sender handle to the process.
    pub fn sender(&self) -> UnboundedSender<ProcessSignal<E>> {
        self.tx.clone()
    }
    /// Alert to the parent that you are active
    pub fn alert_active(&self) {
        self.tx_parent.send(ProcessSignal::Active(self.pid)).ok();
    }
}

/// A container for a process within the darksteel [`Environment`](crate::environment::Environment).
#[derive(Clone)]
pub(crate) struct ProcessContainer<E>
where
    E: TaskErrorTrait,
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
    E: TaskErrorTrait,
{
    /// Create a new container with a process and all of the state it requires.
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
    /// Get the pid of the process.
    pub fn pid(&self) -> ProcessId {
        self.pid
    }
    /// Get a reference to the process.
    pub fn process(&self) -> &dyn Process<E> {
        &*self.process
    }
    /// Derive a context from the container.
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
    /// Derive a reference from the container.
    pub async fn get_ref(&self) -> ProcessRef<E> {
        ProcessRef {
            pid: self.pid,
            process: self.process.clone(),
            tx: self.tx.clone(),
            tx_parent: self.tx_parent.clone(),
        }
    }
    /// Get a sender handle to the process.
    pub fn sender(&self) -> UnboundedSender<ProcessSignal<E>> {
        self.tx.clone()
    }
    /// Get a sender handle to the process's parent.
    #[allow(dead_code)]
    pub fn parent(&self) -> UnboundedSender<ProcessSignal<E>> {
        self.tx_parent.clone()
    }
    /// Get a handle for the current modules provider.
    #[allow(dead_code)]
    pub fn modules(&self) -> Modules {
        self.modules.clone()
    }
    /// Activate the signal handler for the process and acquire its handle.
    pub async fn handle_signals(&mut self) {
        let runtime = Handle::current();
        let process = self.process.clone();
        let context = self.context().await;

        *self.handle.lock().await =
            Some(runtime.spawn(async move { process.handle_signals(context).await }));
    }
}
