use super::Module;
use crate::error::UserError;
use error::*;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use tokio::sync::{RwLock, RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard};
use unchecked_unwrap::UncheckedUnwrap;

pub mod error;

pub trait ResourceTrait: Any + Send + Sync + Sized + Default {}
impl<T> ResourceTrait for T where T: Any + Send + Sync + Sized + Default {}

/// An untyped universal resource type.
#[derive(Debug, Clone)]
struct InternalResource(Arc<RwLock<Box<dyn Any + Send + Sync>>>);

impl InternalResource {
    /// Creates a new Resource.
    fn new<T: ResourceTrait + Debug>(data: T) -> Self {
        Self(Arc::new(RwLock::new(Box::new(data))))
    }

    /// Convert into a typed Resource object.
    fn as_typed<T: ResourceTrait>(&self) -> Resource<T> {
        Resource(self.0.clone(), PhantomData::<T>)
    }
}

/// A universal resource type for transmitting data inside darksteel.
#[derive(Debug, Clone)]
pub struct Resource<T>(Arc<RwLock<Box<dyn Any + Send + Sync>>>, PhantomData<T>);

impl<T> Resource<T>
where
    T: ResourceTrait + Debug,
{
    /// Get an immutable reference to the resource.
    #[tracing::instrument(skip_all)]
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        RwLockReadGuard::map(self.0.read().await, |data| {
            // This is completely safe as a resource will never be
            // created with the wrong type.
            unsafe { data.downcast_ref::<T>().unchecked_unwrap() }
        })
    }

    /// Get a mutable reference to the resource.
    #[tracing::instrument(skip_all)]
    pub async fn write(&self) -> RwLockMappedWriteGuard<'_, T> {
        RwLockWriteGuard::map(self.0.write().await, |data| {
            // This is completely safe as a resource will never be
            // created with the wrong type.
            unsafe { data.downcast_mut::<T>().unchecked_unwrap() }
        })
    }
}

/// A global storage container that is passed down to connection tasks.
#[derive(Debug, Clone)]
pub struct Storage {
    map: Arc<Mutex<HashMap<TypeId, InternalResource>>>,
}

impl Storage {
    /// Create a new Storage object.
    pub fn new() -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get a handle on a Resource within storage.
    #[tracing::instrument(skip_all)]
    pub fn handle<T: ResourceTrait + Debug>(&self) -> Result<Resource<T>, StorageError> {
        let mut map = self.map.lock().or(Err(StorageError::LockPoisoned))?;

        if let Some(resource) = map.get(&TypeId::of::<T>()) {
            Ok(resource.as_typed())
        } else {
            let resource = InternalResource::new(T::default());
            map.insert(TypeId::of::<T>(), resource.clone());
            Ok(resource.as_typed())
        }
    }

    /// Clear out a resource from storage.
    #[tracing::instrument(skip_all)]
    pub async fn clear<T: ResourceTrait + Debug>(&self) -> Result<(), StorageError> {
        let map = self.map.lock().or(Err(StorageError::LockPoisoned))?;
        if let Some(resource) = map.get(&TypeId::of::<T>()) {
            let data = resource.as_typed();
            *data.write().await = T::default();
        }
        Ok(())
    }
}

#[crate::async_trait]
impl Module for Storage {
    async fn module(_: &super::Modules) -> Result<Self, UserError> {
        Ok(Self::new())
    }
}
