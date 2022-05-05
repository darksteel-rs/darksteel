use std::{
    any::{Any, TypeId},
    collections::BTreeMap,
    sync::Arc,
};
use tokio::sync::RwLock;

use crate::{
    error::UserError,
    process::task::{Task, TaskErrorTrait},
};

/// Structs relating to creating a node for a distributed system.
pub mod distributed;
/// The messenger structs which include [`Broadcast`](messenger::broadcast::Broadcast)
/// and [`Mpsc`](messenger::mpsc::Mpsc).
pub mod messenger;
/// A [`Storage`](storage::Storage) module for allowing easy shared acces to
/// resources across tasks.
pub mod storage;

/// When implemented on a struct, it can be instantiated from a [`Modules`]
/// struct.
#[crate::async_trait]
pub trait Module: Any + Clone + Send + Sync + 'static {
    async fn module(modules: &Modules) -> Result<Self, UserError>;
}

/// This trait is for a runtime plugin. There is sometimes a need to have a
/// a process running to provide data to a module, and this trait provides an
/// easy interface to orchestrate that.
#[crate::async_trait]
pub trait Plugin<E: TaskErrorTrait>: Module {
    async fn task(&self) -> Result<Arc<Task<E>>, UserError>;
}

/// A struct for instancing modules to be used in [tasks](crate::process::task::Task).
/// A module can be instantiated from the state of other modules lazily.
#[derive(Clone)]
pub struct Modules {
    modules: Arc<RwLock<BTreeMap<TypeId, Box<dyn Any + Send + Sync + 'static>>>>,
}

impl Modules {
    pub fn new() -> Self {
        Self {
            modules: Default::default(),
        }
    }

    pub async fn handle<T: Module + Clone>(&self) -> Result<T, UserError> {
        let modules = self.modules.read().await;

        if let Some(raw_module) = modules.get(&TypeId::of::<T>()) {
            Ok(raw_module.downcast_ref::<T>().unwrap().clone())
        } else {
            drop(modules);

            let module = T::module(self).await?;
            let mut modules = self.modules.write().await;

            // We're going to check again to ensure there is zero possibility
            // of overwriting a module.
            if let Some(raw_module) = modules.get(&TypeId::of::<T>()) {
                Ok(raw_module.downcast_ref::<T>().unwrap().clone())
            } else {
                modules.insert(TypeId::of::<T>(), Box::new(module.clone()));
                Ok(module)
            }
        }
    }
}
