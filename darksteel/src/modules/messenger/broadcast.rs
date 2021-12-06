use super::error::*;
use crate::modules::{IntoModule, Modules};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::broadcast::*;
use unchecked_unwrap::UncheckedUnwrap;

#[derive(Debug, Clone)]
/// A messenger module.
pub struct Broadcast {
    handles: Arc<Mutex<HashMap<(TypeId, usize), Box<dyn Any + Send + Sync + 'static>>>>,
}

impl Broadcast {
    /// Create a new messenger.
    pub fn new() -> Self {
        Self {
            handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get a broadcast channel for a given type
    pub fn channel<T: Any + Send + Sync + Clone + 'static, const CAPACITY: usize>(
        &self,
    ) -> Result<(Sender<T>, Receiver<T>), MessageError> {
        match self.handles.lock() {
            Ok(mut map) => {
                if let Some(handle) = map.get(&(TypeId::of::<T>(), CAPACITY)) {
                    // This is completely safe. We never create an object
                    // without a TypeId as the index.
                    let sender = unsafe { handle.downcast_ref::<Sender<T>>().unchecked_unwrap() };
                    let handle_sender = sender.clone();
                    let handle_receiver = sender.subscribe();

                    Ok((handle_sender, handle_receiver))
                } else {
                    let (sender, _) = channel::<T>(CAPACITY);
                    let handle_sender = sender.clone();
                    let handle_receiver = sender.subscribe();
                    map.insert((TypeId::of::<T>(), CAPACITY), Box::new(sender));

                    Ok((handle_sender, handle_receiver))
                }
            }
            Err(_) => Err(MessageError::LockPoisoned),
        }
    }
}

#[crate::async_trait]
impl IntoModule for Broadcast {
    async fn module(_: &Modules) -> Self {
        Broadcast::new()
    }
}
