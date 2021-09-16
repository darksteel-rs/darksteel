use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};
use tokio::sync::RwLock;

pub mod distributed;
pub mod messenger;
pub mod storage;

#[crate::async_trait]
pub trait IntoModule: Any + Clone + Send + Sync + 'static {
    async fn module(modules: &Modules) -> Self;
}

#[derive(Clone)]
pub struct Modules {
    modules: Arc<RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync + 'static>>>>,
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

            modules.insert(TypeId::of::<T>(), Box::new(module.clone()));
            module
        }
    }
}
