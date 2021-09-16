#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Inner lock is poisoned")]
    LockPoisoned,
}
