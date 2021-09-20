use crate::error::UniversalError;
use crate::modules::Modules;
use crate::process::{runtime::Runtime, task::TaskError};
use crate::process::{send, Process, ProcessSignal};
use std::sync::Arc;

/// The darksteel executor.
pub struct Environment<E = UniversalError>
where
    E: TaskError,
{
    modules: Modules,
    runtime: Arc<Runtime<E>>,
}

impl<E> Environment<E>
where
    E: TaskError,
{
    pub fn new() -> Self {
        let modules = Modules::new();
        Self {
            modules: modules.clone(),
            runtime: Runtime::new(modules),
        }
    }

    pub fn modules(&self) -> Modules {
        self.modules.clone()
    }

    pub async fn start(&self, process: Arc<dyn Process<E> + 'static>) {
        self.start_multiple(&[process]).await;
    }

    pub async fn start_multiple(&self, processes: &[Arc<dyn Process<E>>]) {
        for process in processes {
            let reference = self.runtime.spawn(process.clone()).await;
            send(&reference.sender(), ProcessSignal::Start);
        }
        self.runtime.block_on_all().await;
    }
}
