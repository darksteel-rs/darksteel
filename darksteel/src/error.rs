use crate::process::task::TaskErrorTrait;
use std::{error::Error, fmt::Display};
use thiserror::Error;

/// An alias for [`anyhow::Error`].
#[derive(Debug)]
pub struct TaskError(Box<dyn Error + Send + Sync + 'static>);

impl<T> From<T> for TaskError
where
    T: Error + Send + Sync + 'static,
{
    fn from(error: T) -> Self {
        TaskError(Box::new(error))
    }
}

impl TaskErrorTrait for TaskError {
    fn internal<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self(Box::new(error))
    }
}

pub trait UserErrorTrait: Error + Send + Sync + 'static {}
impl<T> UserErrorTrait for T where T: Error + Send + Sync + 'static {}

#[derive(Debug, Error)]
pub struct UserError {
    inner: Box<dyn UserErrorTrait>,
    context: &'static str,
}

impl Display for UserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{context} Error: {error}",
            context = self.context,
            error = self.inner
        )
    }
}

impl UserError {
    pub fn from<T: UserErrorTrait>(context: &'static str, error: T) -> Self {
        Self {
            inner: Box::new(error),
            context,
        }
    }
}
