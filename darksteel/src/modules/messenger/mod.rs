use super::{IntoModule, Modules};
use error::*;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::broadcast::*;
use unchecked_unwrap::UncheckedUnwrap;

pub mod error;

const MESSAGE_QUEUE_CHANNEL_SIZE: usize = 64;

#[derive(Debug, Clone)]
/// A messenger module.
pub struct Messenger {
    handles: Arc<Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync + 'static>>>>,
    capacity: usize,
}

impl Messenger {
    /// Create a new messenger.
    #[tracing::instrument]
    pub fn new(capacity: usize) -> Self {
        Self {
            handles: Arc::new(Mutex::new(HashMap::new())),
            capacity,
        }
    }

    /// Get a broadcast channel for a given type
    #[tracing::instrument]
    pub fn channel<T: Any + Send + Sync + Clone + 'static>(
        &self,
    ) -> Result<(Sender<T>, Receiver<T>), MessageError> {
        match self.handles.lock() {
            Ok(mut map) => {
                if let Some(handle) = map.get(&TypeId::of::<T>()) {
                    // This is completely safe. We never create an object
                    // without a TypeId as the index.
                    let sender = unsafe { handle.downcast_ref::<Sender<T>>().unchecked_unwrap() };
                    let handle_sender = sender.clone();
                    let handle_receiver = sender.subscribe();

                    Ok((handle_sender, handle_receiver))
                } else {
                    let (sender, _) = channel::<T>(self.capacity);
                    let handle_sender = sender.clone();
                    let handle_receiver = sender.subscribe();
                    map.insert(TypeId::of::<T>(), Box::new(sender));

                    Ok((handle_sender, handle_receiver))
                }
            }
            Err(_) => Err(MessageError::LockPoisoned),
        }
    }
}

#[crate::async_trait]
impl IntoModule for Messenger {
    async fn module(_: &Modules) -> Self {
        Messenger::new(MESSAGE_QUEUE_CHANNEL_SIZE)
    }
}
