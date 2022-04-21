use std::{
    any::{Any, TypeId},
    collections::BTreeMap,
    sync::Arc,
};
use tokio::sync::RwLock;

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
pub trait IntoModule: Any + Clone + Send + Sync + 'static {
    async fn module(modules: &Modules) -> Self;
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

    pub async fn handle<T: IntoModule + Clone>(&self) -> T {
        let modules = self.modules.read().await;

        if let Some(raw_module) = modules.get(&TypeId::of::<T>()) {
            raw_module.downcast_ref::<T>().unwrap().clone()
        } else {
            drop(modules);

            let module = T::module(self).await;
            let mut modules = self.modules.write().await;

            // We're going to check again to ensure there is zero possibility
            // of overwriting a module.
            if let Some(raw_module) = modules.get(&TypeId::of::<T>()) {
                raw_module.downcast_ref::<T>().unwrap().clone()
            } else {
                modules.insert(TypeId::of::<T>(), Box::new(module.clone()));
                module
            }
        }
    }
}
