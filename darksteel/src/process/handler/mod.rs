use super::{
    global_id, send,
    task::{ProcessId, TaskError},
    ExitReason, Process, ProcessConfig, ProcessSignal,
};
use crate::process::container::ProcessContext;
use futures::{future::BoxFuture, Future};
use std::sync::Arc;

pub trait SignalHandler<E: TaskError>:
    Sync + Fn(ProcessContext<E>) -> BoxFuture<'static, ()> + Send + 'static
{
}

impl<F, E> SignalHandler<E> for F
where
    F: Sync + Fn(ProcessContext<E>) -> BoxFuture<'static, ()> + Send + 'static,
    E: TaskError,
{
}

pub struct Handler<E>
where
    E: TaskError,
{
    id: ProcessId,
    signal_handler: Box<dyn SignalHandler<E>>,
    config: ProcessConfig,
}

impl<E> Handler<E>
where
    E: TaskError,
{
    pub fn new<F>(handler: fn(ProcessContext<E>) -> F) -> Arc<Self>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self::new_with_config(Default::default(), handler)
    }

    pub fn new_with_config<F>(
        config: ProcessConfig,
        handler: fn(ProcessContext<E>) -> F,
    ) -> Arc<Self>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Arc::new(Self {
            id: global_id(),
            signal_handler: Box::new(move |context| Box::pin(handler(context))),
            config,
        })
    }
}

#[crate::async_trait]
impl<E> Process<E> for Handler<E>
where
    E: TaskError,
{
    fn id(&self) -> ProcessId {
        self.id
    }

    fn config(&self) -> ProcessConfig {
        self.config.clone()
    }

    async fn handle_signals(&self, context: ProcessContext<E>) {
        let pid = context.pid();
        let parent = context.parent();

        (self.signal_handler)(context).await;
        send(&parent, ProcessSignal::Exit(pid, ExitReason::Normal));
    }

    async fn handle_spawn(&self, _: ProcessId, _: &std::sync::Arc<super::runtime::Runtime<E>>) {
        // Do nothing
    }
}
