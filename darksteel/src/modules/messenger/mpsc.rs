use super::error::*;
use crate::error::UserError;
use crate::modules::{Module, Modules};
use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::{mpsc::*, Mutex as AsyncMutex, MutexGuard};
use unchecked_unwrap::UncheckedUnwrap;

#[derive(Clone)]
pub struct OwnedReceiver<T>
where
    T: Any + Send + Sync + Clone + 'static,
{
    rx: Arc<AsyncMutex<UnboundedReceiver<T>>>,
}

impl<T> OwnedReceiver<T>
where
    T: Any + Send + Sync + Clone + 'static,
{
    /// Create a new receiver.
    fn new(receiver: UnboundedReceiver<T>) -> Self {
        Self {
            rx: Arc::new(AsyncMutex::new(receiver)),
        }
    }

    /// Acquire a handle to the underlying receiver.
    pub async fn handle(&self) -> MutexGuard<'_, UnboundedReceiver<T>> {
        self.rx.lock().await
    }

    /// Drain all the messages in the channel.
    pub async fn drain(&self) {
        let mut receiver = self.rx.lock().await;

        loop {
            if let Err(_) = receiver.try_recv() {
                break;
            }
        }
    }
}

#[derive(Debug, Clone)]
/// An MPSC messenger module.
pub struct Mpsc {
    handles: Arc<Mutex<BTreeMap<TypeId, Box<dyn Any + Send + Sync + 'static>>>>,
}

impl Mpsc {
    /// Create a new messenger.
    pub fn new() -> Self {
        Self {
            handles: Default::default(),
        }
    }

    /// Get a broadcast channel for a given type
    pub fn channel<T: Any + Send + Sync + Clone + 'static>(
        &self,
    ) -> Result<(UnboundedSender<T>, OwnedReceiver<T>), MessageError> {
        match self.handles.lock() {
            Ok(mut map) => {
                if let Some(handle) = map.get(&TypeId::of::<T>()) {
                    // This is completely safe. We never create an object
                    // without a TypeId as the index.
                    let (sender, receiver) = unsafe {
                        handle
                            .downcast_ref::<(UnboundedSender<T>, OwnedReceiver<T>)>()
                            .unchecked_unwrap()
                    };
                    Ok((sender.clone(), receiver.clone()))
                } else {
                    let (sender, receiver) = unbounded_channel::<T>();
                    let handle_receiver = OwnedReceiver::<T>::new(receiver);
                    map.insert(
                        TypeId::of::<T>(),
                        Box::new((sender.clone(), handle_receiver.clone())),
                    );

                    Ok((sender, handle_receiver))
                }
            }
            Err(_) => Err(MessageError::LockPoisoned),
        }
    }
}

#[crate::async_trait]
impl Module for Mpsc {
    async fn module(_: &Modules) -> Result<Self, UserError> {
        Ok(Mpsc::new())
    }
}
