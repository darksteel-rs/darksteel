/// An error returned by the [`Storage`](crate::modules::storage::Storage) module.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Inner lock is poisoned")]
    LockPoisoned,
}
