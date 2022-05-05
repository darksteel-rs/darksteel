use super::{
    discovery::Discovery,
    error::*,
    node::client_write,
    rpc::{
        raft_actions_client::RaftActionsClient, raft_actions_server::RaftActions,
        AppendEntriesRequest as AppendEntriesRequestRpc,
        AppendEntriesResponse as AppendEntriesResponseRpc, ClientWriteRequest, ClientWriteResponse,
        Empty, Entry as EntryRpc, InfoResponse,
        InstallSnapshotRequest as InstallSnapshotRequestRpc,
        InstallSnapshotResponse as InstallSnapshotResponseRpc, LogId as LogIdRpc,
        Membership as MembershipRpc, MembershipConfig, SnapshotMeta as SnapshotMetaRpc,
        VoteRequest as VoteRequestRpc, VoteResponse as VoteResponseRpc,
    },
    ClientRequest, RaftNode, GLOBAL_PORT,
};
use openraft::raft::{InstallSnapshotRequest, InstallSnapshotResponse, Membership};
use openraft::raft::{VoteRequest, VoteResponse};
use openraft::{async_trait::async_trait, LogId};
use openraft::{
    raft::{AppendEntriesRequest, AppendEntriesResponse, Entry},
    SnapshotMeta,
};
use openraft::{NodeId, RaftNetwork};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

pub struct RaftRpc {
    node: RaftNode,
    router: Arc<Router>,
    shutdown: Arc<Notify>,
}

impl RaftRpc {
    pub fn new(node: RaftNode, router: Arc<Router>, shutdown: Arc<Notify>) -> Self {
        Self {
            node,
            router,
            shutdown,
        }
    }
}

impl From<LogIdRpc> for LogId {
    fn from(log_id: LogIdRpc) -> Self {
        Self {
            term: log_id.term,
            index: log_id.index,
        }
    }
}

impl From<LogId> for LogIdRpc {
    fn from(log_id: LogId) -> Self {
        Self {
            term: log_id.term,
            index: log_id.index,
        }
    }
}

impl From<Membership> for MembershipRpc {
    fn from(membership: Membership) -> Self {
        Self {
            configs: membership
                .get_configs()
                .iter()
                .map(|config| MembershipConfig {
                    members: config.into_iter().map(|id| *id).collect(),
                })
                .collect(),
            nodes: Some(MembershipConfig {
                members: membership.all_nodes().into_iter().map(|id| *id).collect(),
            }),
        }
    }
}

impl TryFrom<MembershipRpc> for Membership {
    type Error = Status;
    fn try_from(membership: MembershipRpc) -> Result<Self, Self::Error> {
        Ok(Self::new_multi(
            membership
                .configs
                .into_iter()
                .map(|config| config.members.into_iter().collect())
                .collect(),
        ))
    }
}

impl TryFrom<SnapshotMetaRpc> for SnapshotMeta {
    type Error = Status;

    fn try_from(meta: SnapshotMetaRpc) -> Result<Self, Self::Error> {
        Ok(Self {
            last_log_id: match meta.last_log_id {
                Some(log_id) => log_id.into(),
                None => {
                    return Err(Status::data_loss(
                        "Missing `last_log_id` in `SnapshotMeta` RPC",
                    ))
                }
            },
            snapshot_id: meta.snapshot_id,
        })
    }
}

impl From<SnapshotMeta> for SnapshotMetaRpc {
    fn from(meta: SnapshotMeta) -> Self {
        Self {
            last_log_id: Some(meta.last_log_id.into()),
            snapshot_id: meta.snapshot_id,
        }
    }
}

#[tonic::async_trait]
impl RaftActions for RaftRpc {
    async fn client_write(
        &self,
        request: Request<ClientWriteRequest>,
    ) -> Result<Response<ClientWriteResponse>, Status> {
        let request = request.into_inner();
        let request = ClientRequest {
            client: request.client,
            serial: request.serial,
            payload_type: request.payload_type,
            payload_data: request.payload_data,
        };
        let response = client_write(&self.node, &self.router, &self.shutdown, request)
            .await
            .map_err(|error| Status::aborted(format!("Could not write to client: {:?}", error)))?;

        Ok(Response::new(ClientWriteResponse {
            log_id: Some(response.log_id.into()),
            data: response
                .data
                .0
                .ok_or_else(|| Status::data_loss("Missing `data` in `ClientWrite` RPC"))?,
            membership: if let Some(membership) = response.membership {
                Some(membership.into())
            } else {
                None
            },
        }))
    }
    async fn info(&self, _: Request<Empty>) -> Result<Response<InfoResponse>, Status> {
        use openraft::State;

        let metrics = self.node.metrics().borrow().clone();
        let state = match metrics.state {
            State::Learner => "Learner",
            State::Follower => "Follower",
            State::Candidate => "Candidate",
            State::Leader => "Leader",
            State::Shutdown => "Shutdown",
        };

        Ok(Response::new(InfoResponse {
            id: metrics.id,
            state: state.into(),
            current_term: metrics.current_term,
            last_log_index: metrics.last_log_index,
            last_applied: metrics.last_applied,
            current_leader: metrics.current_leader,
            membership_config: Some(MembershipConfig {
                members: metrics
                    .membership_config
                    .membership
                    .all_nodes()
                    .iter()
                    .map(|member| *member)
                    .collect(),
            }),
        }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequestRpc>,
    ) -> Result<tonic::Response<AppendEntriesResponseRpc>, Status> {
        let request = request.into_inner();
        let response = self
            .node
            .append_entries(AppendEntriesRequest::<ClientRequest> {
                term: request.term,
                leader_id: request.leader_id,
                prev_log_id: LogId {
                    term: request.prev_log_term,
                    index: request.prev_log_index,
                },
                entries: request
                    .entries
                    .into_iter()
                    .map(|entry| Entry::<ClientRequest> {
                        log_id: LogId {
                            term: entry.term,
                            index: entry.index,
                        },
                        payload: bincode::deserialize(entry.payload.as_slice()).unwrap(),
                    })
                    .collect(),
                leader_commit: request
                    .leader_commit
                    .ok_or_else(|| Status::aborted("Missing `leader_commit`"))?
                    .into(),
            })
            .await
            .map_err(|error| {
                self.shutdown.notify_waiters();
                Status::aborted(format!("Could not append entries: {:?}", error))
            })?;

        Ok(Response::new(AppendEntriesResponseRpc {
            term: response.term,
            matched: match response.matched {
                Some(matched) => Some(matched.into()),
                None => None,
            },
            conflict: match response.conflict {
                Some(conflict) => Some(conflict.into()),
                None => None,
            },
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequestRpc>,
    ) -> Result<Response<InstallSnapshotResponseRpc>, Status> {
        let request = request.into_inner();
        let response = self
            .node
            .install_snapshot(InstallSnapshotRequest {
                term: request.term,
                leader_id: request.leader_id,
                meta: request
                    .meta
                    .ok_or_else(|| Status::aborted("Missing `meta`"))?
                    .try_into()?,
                offset: request.offset,
                data: request.data,
                done: request.done,
            })
            .await
            .map_err(|error| {
                self.shutdown.notify_waiters();
                Status::aborted(format!("Could not install snapshot: {:?}", error))
            })?;

        Ok(Response::new(InstallSnapshotResponseRpc {
            term: response.term,
        }))
    }

    async fn vote(
        &self,
        request: Request<VoteRequestRpc>,
    ) -> Result<Response<VoteResponseRpc>, Status> {
        let request = request.into_inner();
        let response = self
            .node
            .vote(VoteRequest {
                term: request.term,
                candidate_id: request.candidate_id,
                last_log_id: request
                    .last_log_id
                    .ok_or_else(|| Status::aborted("Missing `last_log_id`"))?
                    .into(),
            })
            .await
            .map_err(|error| {
                self.shutdown.notify_waiters();
                Status::aborted(format!("Could not vote: {:?}", error))
            })?;

        Ok(Response::new(VoteResponseRpc {
            term: response.term,
            last_log_id: Some(response.last_log_id.into()),
            vote_granted: response.vote_granted,
        }))
    }
}

#[derive(Clone)]
pub struct NodeConnection {
    client: RaftActionsClient<Channel>,
}

impl NodeConnection {
    pub async fn new(ip: String) -> Result<Self, NodeConnectionError> {
        let channel = Endpoint::from_shared(ip.to_owned())?.connect().await?;
        let client = RaftActionsClient::new(channel);

        Ok(Self { client })
    }
    pub fn client(&mut self) -> &mut RaftActionsClient<Channel> {
        &mut self.client
    }
}

pub struct Router {
    id: NodeId,
    /// Discovery method
    discovery: Box<dyn Discovery>,
    /// Discovered nodes
    discovered_nodes: RwLock<BTreeMap<NodeId, NodeConnection>>,
}

impl Router {
    /// Create a new instance.
    pub fn new(id: NodeId, discovery: Box<dyn Discovery>) -> Arc<Self> {
        Arc::new(Self {
            id,
            discovery,
            discovered_nodes: Default::default(),
        })
    }

    pub async fn reset(&self) {
        *self.discovered_nodes.write().await = Default::default();
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub(crate) async fn get_node_connection(&self, id: &NodeId) -> Option<NodeConnection> {
        if let Some(node) = self.discovered_nodes.read().await.get(id) {
            Some(node.clone())
        } else {
            None
        }
    }

    pub async fn peers(&self) -> BTreeSet<NodeId> {
        self.discovered_nodes
            .read()
            .await
            .iter()
            .map(|(id, _)| *id)
            .collect()
    }

    pub async fn discover_nodes(&self) -> Result<(), RouterError> {
        let addresses: _ = self.discovery.discover().await?;
        let mut discovered_nodes = self.discovered_nodes.write().await;

        for address in addresses {
            if let Ok(mut connection) =
                NodeConnection::new(format!("http://{}:{}", address, GLOBAL_PORT)).await
            {
                if let Ok(info) = connection.client().info(Empty {}).await {
                    let id = info.into_inner().id;

                    if id != self.id() {
                        discovered_nodes.insert(id, connection);
                    }
                } else {
                    tracing::warn!("Could not send message to client: {}", address);
                }
            } else {
                tracing::warn!("Could not reach client: {}", address);
            }
        }

        Ok(())
    }

    pub async fn is_cluster_pristine(&self) -> bool {
        let mut pristine = true;

        for (_, connection) in self.discovered_nodes.write().await.iter_mut() {
            if let Ok(info) = connection.client().info(Empty {}).await {
                let info = info.into_inner();

                pristine &= info.state == "NonVoter";
                pristine &= info.last_applied == 0;
                pristine &= info.last_log_index == 0;
                pristine &= info.current_leader == None;
            }
        }

        pristine
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for Router {
    /// Send an AppendEntriesRequest RPC to the target Raft node.
    async fn send_append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> ::anyhow::Result<AppendEntriesResponse> {
        let mut discovered_nodes = self.discovered_nodes.write().await;

        if let Some(connection) = discovered_nodes.get_mut(&target) {
            let response = connection
                .client()
                .append_entries(Request::new(AppendEntriesRequestRpc {
                    term: rpc.term,
                    leader_id: rpc.leader_id,
                    prev_log_index: rpc.prev_log_id.index,
                    prev_log_term: rpc.prev_log_id.term,
                    entries: rpc
                        .entries
                        .into_iter()
                        .map(|entry| EntryRpc {
                            term: entry.log_id.term,
                            index: entry.log_id.index,
                            payload: bincode::serialize(&entry.payload).unwrap(),
                        })
                        .collect(),
                    leader_commit: Some(rpc.leader_commit.into()),
                }))
                .await?
                .into_inner();

            Ok(AppendEntriesResponse {
                term: response.term,
                matched: match response.matched {
                    Some(matched) => Some(matched.into()),
                    None => None,
                },
                conflict: match response.conflict {
                    Some(conflict) => Some(conflict.into()),
                    None => None,
                },
            })
        } else {
            anyhow::bail!("Target node not found in routing table");
        }
    }

    /// Send an InstallSnapshotRequest RPC to the target Raft node.
    async fn send_install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> anyhow::Result<InstallSnapshotResponse> {
        let mut discovered_nodes = self.discovered_nodes.write().await;

        if let Some(connection) = discovered_nodes.get_mut(&target) {
            let response = connection
                .client()
                .install_snapshot(Request::new(InstallSnapshotRequestRpc {
                    term: rpc.term,
                    leader_id: rpc.leader_id,
                    meta: Some(rpc.meta.into()),
                    offset: rpc.offset,
                    data: rpc.data,
                    done: rpc.done,
                }))
                .await?
                .into_inner();

            Ok(InstallSnapshotResponse {
                term: response.term,
            })
        } else {
            anyhow::bail!("Target node not found in routing table");
        }
    }

    /// Send a VoteRequest RPC to the target Raft node.
    async fn send_vote(&self, target: NodeId, rpc: VoteRequest) -> anyhow::Result<VoteResponse> {
        let mut discovered_nodes = self.discovered_nodes.write().await;

        if let Some(connection) = discovered_nodes.get_mut(&target) {
            let response = connection
                .client()
                .vote(Request::new(VoteRequestRpc {
                    term: rpc.term,
                    candidate_id: rpc.candidate_id,
                    last_log_id: Some(rpc.last_log_id.into()),
                }))
                .await?
                .into_inner();

            Ok(VoteResponse {
                term: response.term,
                vote_granted: response.vote_granted,
                last_log_id: response
                    .last_log_id
                    .ok_or_else(|| Status::aborted("Missing `last_log_id`"))?
                    .into(),
            })
        } else {
            anyhow::bail!("Target node not found in routing table");
        }
    }
}
