use std::fmt::Debug;

pub trait TaskError: Debug + Send + Sync + Clone + 'static {}
impl<T> TaskError for T where T: Debug + Send + Sync + Clone + 'static {}
