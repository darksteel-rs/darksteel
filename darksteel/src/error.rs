use crate::process::task::TaskErrorTrait;

/// An alias for [`anyhow::Error`].
pub type TaskError = anyhow::Error;

impl TaskErrorTrait for TaskError {
    fn internal<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::new(error)
    }
}
