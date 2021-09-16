use std::{fmt::Debug, sync::Arc};

#[derive(Clone, Debug)]
/// A universal error type. It is encouraged that you swap this out in the
/// `Environment` if you want to see better performance/control.
pub struct UniversalError(Arc<dyn std::error::Error + Send + Sync>);

impl std::fmt::Display for UniversalError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0.to_string())
    }
}

impl<T> From<T> for UniversalError
where
    T: std::error::Error + Send + Sync + 'static,
{
    fn from(error: T) -> Self {
        UniversalError(Arc::new(error))
    }
}
