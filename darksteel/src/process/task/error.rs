use std::error::Error;
use std::fmt::Debug;

/// A trait that must be implemented on the error type used by the darksteel
/// [`Environment`](crate::environment::Environment) and it's tasks.
pub trait TaskErrorTrait: Debug + Send + Sync + 'static {
    fn internal<E: Error + Send + Sync + 'static>(error: E) -> Self;
}
