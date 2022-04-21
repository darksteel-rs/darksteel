use crate::process::task::{Task, TaskErrorTrait};

use super::discovery::{Discovery, HostLookup};
use super::*;
use openraft::RaftMetrics;
use openraft::{raft::ClientWriteRequest, Config, NodeId, State};
use std::collections::BTreeSet;
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::sync::watch::Receiver as WatchReceiver;

const NODE_SIGNAL_CHANNEL_SIZE: usize = 64;
static GLOBAL_SERIAL_COUNT: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub enum NodeSignal {
    StateChanged(State),
    LeaderChanged(Option<u64>),
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

pub struct NodeBuilder {
    config: NodeConfig,
    initial_state: HashMap<Identity, Box<dyn DistributedState>>,
}

impl NodeBuilder {
    fn new(config: NodeConfig) -> Self {
        Self {
            config,
            initial_state: Default::default(),
        }
    }
    pub fn with<S: DistributedStateTrait>(mut self) -> Self {
        self.initial_state.insert(S::ID, Box::new(S::default()));
        self
    }

    pub async fn finish(self) -> Result<Node, NodeError> {
        Node::new(
            self.config.address,
            self.config.discovery,
            self.config.cluster_name,
            self.initial_state,
        )
        .await
    }
}

pub struct Node {
    router: Arc<router::Router>,
    store: Arc<store::Store>,
    node: RaftNode,
    events: Sender<NodeSignal>,
}

impl Node {
    pub fn build() -> NodeBuilder {
        NodeBuilder::new(Default::default())
    }
    pub fn build_with_config(node_config: NodeConfig) -> NodeBuilder {
        NodeBuilder::new(node_config)
    }
    async fn new(
        address: IpAddr,
        discovery: Box<dyn Discovery>,
        cluster_name: String,
        initial_state: HashMap<Identity, Box<dyn DistributedState>>,
    ) -> Result<Self, NodeError> {
        let config = Arc::new(Config::build(&[&cluster_name])?);
        let store = store::Store::new(initial_state);
        let router = router::Router::new(store.id(), address, discovery);
        let node = RaftNode::new(store.id(), config, router.clone(), store.clone());
        let (events, _) = channel(NODE_SIGNAL_CHANNEL_SIZE);

        Ok(Self {
            router,
            store,
            node,
            events,
        })
    }

    pub fn id(&self) -> NodeId {
        self.store.id()
    }

    pub async fn leader(&self) -> Option<NodeId> {
        self.node.current_leader().await
    }

    pub async fn metrics(&self) -> WatchReceiver<RaftMetrics> {
        self.node.metrics()
    }

    pub async fn peers(&self) -> BTreeSet<NodeId> {
        self.router.peers().await
    }

    pub fn subscribe_events(&self) -> Receiver<NodeSignal> {
        self.events.subscribe()
    }

    pub async fn task<E: TaskErrorTrait>(&self) -> Result<Arc<Task<E>>, NodeError> {
        Ok(self.router.server_task(self.node.clone()).await?)
    }

    pub async fn initialise(&self) -> Result<(), NodeError> {
        let node_ids;
        self.router.discover_nodes().await?;
        node_ids = self.router.peers().await;

        if self.router.is_cluster_pristine().await {
            if let Err(_) = self.node.initialize(node_ids).await {
                tracing::info!("Detected log sync, joining");
            }
        }

        Ok(())
    }

    pub async fn state<S>(&self) -> Option<S>
    where
        S: DistributedStateTrait,
    {
        self.store.get_state_machine().await.get_state::<S>()
    }

    pub async fn commit<M: Mutator>(&self, mutation: M) -> Result<(), CommitError> {
        self.node.client_read().await?;
        if let Some(leader_id) = self.node.current_leader().await {
            if leader_id == self.store.id() {
                self.node
                    .client_write(ClientWriteRequest::new(ClientRequest {
                        client: String::new(),
                        serial: GLOBAL_SERIAL_COUNT.fetch_add(1, Ordering::SeqCst),
                        payload_type: M::state_id(),
                        payload_data: mutation.bytes()?,
                    }))
                    .await?;
            } else {
                return Err(CommitError::NotLeader);
            }
        } else {
            return Err(CommitError::NoLeader);
        }

        Ok(())
    }
}
