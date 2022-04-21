/// An error message created by one of the messenger modules.
#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    #[error("Inner lock is poisoned")]
    LockPoisoned,
}
