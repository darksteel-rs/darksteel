use crate::modules::{Plugin, UserError};
use crate::prelude::{Module, Modules};
use crate::process::task::{Task, TaskErrorTrait};

use super::discovery::{Discovery, HostLookup};
use super::router::{RaftRpc, Router};
use super::rpc::{
    raft_actions_server::RaftActionsServer, ClientWriteRequest as ClientWriteRequestRpc,
};
use super::*;
use openraft::raft::ClientWriteResponse;
use openraft::ClientWriteError;
use openraft::{raft::ClientWriteRequest, Config, NodeId};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::{Notify, RwLock};
use tonic::transport::Server;

static GLOBAL_SERIAL_COUNT: AtomicU64 = AtomicU64::new(0);

pub trait NodeContext: Send + Sync + Clone + Default + 'static {
    fn config(&self) -> NodeConfig;
    fn bind(&self, builder: &mut NodeBuilder<Self>);
}

pub struct NodeConfig {
    address: IpAddr,
    cluster_name: String,
    discovery: Box<dyn Discovery>,
}

impl NodeConfig {
    pub fn new<D: Discovery, S: Into<String>>(
        address: IpAddr,
        cluster_name: S,
        discovery: D,
    ) -> NodeConfig {
        NodeConfig {
            address,
            cluster_name: cluster_name.into(),
            discovery: Box::new(discovery),
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        NodeConfig {
            address: "0.0.0.0".parse().unwrap(),
            cluster_name: "darksteel-cluster".into(),
            discovery: Box::new(HostLookup::new("localhost")),
        }
    }
}

pub struct NodeBuilder<T>
where
    T: NodeContext,
{
    context: T,
    initial_state: HashMap<Identity, Box<dyn DistributedState>>,
}

impl<T> NodeBuilder<T>
where
    T: NodeContext,
{
    fn new(context: T) -> Self {
        Self {
            context,
            initial_state: Default::default(),
        }
    }

    pub fn with<S: DistributedStateTrait>(mut self) -> Self {
        self.initial_state.insert(S::ID, Box::new(S::default()));
        self
    }

    pub fn finish(mut self) -> Result<Node<T>, NodeError> {
        let context = self.context.clone();

        context.bind(&mut self);
        Node::new(context, self.initial_state)
    }
}

#[derive(Clone)]
pub struct NodeHandle<T>
where
    T: NodeContext,
{
    node: Node<T>,
}

impl<T> NodeHandle<T> where T: NodeContext {}

#[crate::async_trait]
impl<T> Module for NodeHandle<T>
where
    T: NodeContext,
{
    async fn module(_: &Modules) -> Result<Self, UserError> {
        Ok(Self {
            node: Node::build()
                .finish()
                .map_err(|error| UserError::from("Module", error))?,
        })
    }
}

#[crate::async_trait]
impl<T, E> Plugin<E> for NodeHandle<T>
where
    T: NodeContext,
    E: TaskErrorTrait,
{
    async fn task(&self) -> Result<Arc<Task<E>>, UserError> {
        self.node
            .server_task()
            .await
            .map_err(|error| UserError::from("Plugin", error))
    }
}

#[derive(Clone)]
pub struct Node<T>
where
    T: NodeContext,
{
    context: T,
    pub(crate) router: Arc<router::Router>,
    pub(crate) store: Arc<store::Store>,
    config: Arc<Config>,
    raft: Arc<RwLock<RaftNode>>,
    shutdown: Arc<Notify>,
}

impl<T> Node<T>
where
    T: NodeContext,
{
    pub fn build() -> NodeBuilder<T> {
        NodeBuilder::<T>::new(Default::default())
    }

    pub fn build_with_config(context: T) -> NodeBuilder<T> {
        NodeBuilder::<T>::new(context)
    }

    fn new(
        context: T,
        initial_state: HashMap<Identity, Box<dyn DistributedState>>,
    ) -> Result<Self, NodeError> {
        let config = context.config();
        let mut raft_config = Config::default();

        raft_config.cluster_name = config.cluster_name;

        let raft_config = Arc::new(raft_config);
        let store = store::Store::new(initial_state);
        let router = router::Router::new(store.id(), config.discovery);
        let raft = Arc::new(RwLock::new(RaftNode::new(
            store.id(),
            raft_config.clone(),
            router.clone(),
            store.clone(),
        )));
        let shutdown = Arc::new(Notify::new());

        Ok(Self {
            context,
            router,
            store,
            config: raft_config,
            raft,
            shutdown,
        })
    }

    pub fn id(&self) -> NodeId {
        self.store.id()
    }

    pub async fn peers(&self) -> BTreeSet<NodeId> {
        self.router.peers().await
    }

    pub async fn server_task<E: TaskErrorTrait>(&self) -> Result<Arc<Task<E>>, RouterError> {
        let config = self.context.config();
        let address = SocketAddr::new(config.address, GLOBAL_PORT);
        let raft_config = self.config.clone();
        let router = self.router.clone();
        let store = self.store.clone();
        let raft = self.raft.clone();
        let shutdown = self.shutdown.clone();

        Ok(Task::new(move |_| async move {
            let node = raft.read().await;

            router
                .discover_nodes()
                .await
                .map_err(|error| E::internal(error))?;

            let node_ids = router.peers().await;

            if router.is_cluster_pristine().await {
                if let Err(_) = node.initialize(node_ids).await {
                    tracing::info!("Detected log sync, joining");
                }
            }

            Server::builder()
                .add_service(RaftActionsServer::new(RaftRpc::new(
                    node.clone(),
                    router.clone(),
                    shutdown.clone(),
                )))
                .serve_with_shutdown(address, async { shutdown.notified().await })
                .await
                .map_err(|error| E::internal(error))?;

            node.shutdown().await.ok();
            store.reset().await;
            router.reset().await;

            drop(node);

            *raft.write().await = RaftNode::new(store.id(), raft_config, router, store);

            Ok(())
        }))
    }

    pub async fn state<S>(&self) -> Option<S>
    where
        S: DistributedStateTrait,
    {
        self.store.get_state_machine().await.get_state::<S>()
    }

    pub async fn commit<M: Mutator>(
        &self,
        mutation: M,
    ) -> Result<ClientWriteResponse<ClientResponse>, CommitError> {
        let node = self.raft.read().await;
        let request = ClientRequest {
            client: self.store.id(),
            serial: GLOBAL_SERIAL_COUNT.fetch_add(1, Ordering::SeqCst),
            payload_type: M::state_id(),
            payload_data: mutation.bytes()?,
        };

        client_write(&node, &self.router, &self.shutdown, request).await
    }
}

pub(crate) async fn client_write(
    node: &RaftNode,
    router: &Arc<Router>,
    shutdown: &Arc<Notify>,
    request: ClientRequest,
) -> Result<ClientWriteResponse<ClientResponse>, CommitError> {
    match node
        .client_write(ClientWriteRequest::new(request.clone()))
        .await
    {
        Ok(response) => Ok(response),
        Err(error) => match error {
            ClientWriteError::RaftError(_) => {
                shutdown.notify_waiters();
                Err(error)?
            }
            ClientWriteError::ForwardToLeader(forward) => {
                if let Some(leader_id) = forward.leader_id {
                    if let Some(mut connection) = router.get_node_connection(&leader_id).await {
                        let client = connection.client();

                        let response = client
                            .client_write(ClientWriteRequestRpc {
                                client: request.client,
                                serial: request.serial,
                                payload_type: request.payload_type,
                                payload_data: request.payload_data,
                            })
                            .await?
                            .into_inner();

                        Ok(ClientWriteResponse {
                            log_id: response
                                .log_id
                                .ok_or_else(|| CommitError::MissingResponseData)?
                                .into(),
                            data: ClientResponse(Some(response.data)),
                            membership: if let Some(membership) = response.membership {
                                Some(membership.try_into()?)
                            } else {
                                None
                            },
                        })
                    } else {
                        Err(CommitError::ForwardNoConnection)
                    }
                } else {
                    Err(CommitError::ForwardNoConnection)
                }
            }
            _ => panic!(""),
        },
    }
}
