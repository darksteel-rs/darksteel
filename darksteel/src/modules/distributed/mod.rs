use crate::identity::Identity;
use crate::identity::IdentityTrait;
use downcast_rs::impl_downcast;
use downcast_rs::Downcast;
use dyn_clone::DynClone;
use error::*;
use openraft::NodeId;
use openraft::{AppData, AppDataResponse, Raft};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use thiserror::Error;

pub mod discovery;
pub mod error;
pub mod node;
mod router;
mod store;

// Hehehehehe
pub(crate) const GLOBAL_PORT: u16 = 42069;

pub(crate) mod rpc {
    tonic::include_proto!("raft");
}

/// A concrete Raft type.
pub(crate) type RaftNode = Raft<ClientRequest, ClientResponse, router::Router, store::Store>;

/// The application data request type.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    /// The ID of the client which has sent the request.
    pub client: NodeId,
    /// The serial number of this request.
    pub serial: u64,
    pub payload_type: Identity,
    pub payload_data: Vec<u8>,
}

impl AppData for ClientRequest {}

/// The application data response type.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientResponse(Option<Vec<u8>>);

impl AppDataResponse for ClientResponse {}

/// Error used to trigger Raft shutdown from storage.
#[derive(Clone, Debug, Error)]
pub enum ShutdownError {
    #[error("Unsafe storage error.")]
    UnsafeStorageError,
}

pub trait Mutator
where
    Self: Serialize,
{
    fn state_id() -> Identity;
    fn bytes(&self) -> Result<Vec<u8>, MutatorError>;
}

#[typetag::serde]
pub trait DistributedState: Send + Sync + Any + Downcast + DynClone {
    fn mutate(&mut self, data: Vec<u8>) -> Result<Vec<u8>, MutatorError>;
}

impl_downcast!(DistributedState);

pub trait DistributedStateTrait:
    DistributedState + IdentityTrait + Default + Clone + 'static
{
}
impl<T> DistributedStateTrait for T where
    T: DistributedState + IdentityTrait + Default + Clone + 'static
{
}

dyn_clone::clone_trait_object!(DistributedState);
