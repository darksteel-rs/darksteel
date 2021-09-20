use std::fmt::Debug;

pub trait TaskError: Debug + Send + Sync + 'static {}
impl<T> TaskError for T where T: Debug + Send + Sync + 'static {}
