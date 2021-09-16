#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    #[error("Inner lock is poisoned")]
    LockPoisoned,
}
