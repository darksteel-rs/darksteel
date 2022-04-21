use crate::modules::Modules;
use crate::process::{runtime::Runtime, task::TaskErrorTrait};
use crate::process::{send, Process, ProcessSignal};
use std::sync::Arc;

/// The darksteel environment.
pub struct Environment<E>
where
    E: TaskErrorTrait,
{
    modules: Modules,
    runtime: Arc<Runtime<E>>,
}

impl<E> Environment<E>
where
    E: TaskErrorTrait,
{
    /// Create a new [`Environment`] instance.
    pub fn new() -> Self {
        let modules = Modules::new();
        Self {
            modules: modules.clone(),
            runtime: Runtime::new(modules),
        }
    }
    /// Acquire a handle to the current [`Modules`] instance of the environment.
    pub fn modules(&self) -> Modules {
        self.modules.clone()
    }
    /// Start a single task.
    pub async fn start(&self, process: Arc<dyn Process<E> + 'static>) {
        self.start_multiple(&[process]).await;
    }
    /// Start multiple tasks.
    pub async fn start_multiple(&self, processes: &[Arc<dyn Process<E>>]) {
        for process in processes {
            let reference = self.runtime.spawn(process.clone()).await;
            send(&reference.sender(), ProcessSignal::Start);
        }
        self.runtime.block_on_all().await;
    }
}
