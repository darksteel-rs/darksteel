use super::{
    discovery::Discovery,
    error::*,
    rpc::{
        raft_actions_client::RaftActionsClient,
        raft_actions_server::{RaftActions, RaftActionsServer},
        AppendEntriesRequest as AppendEntriesRequestRpc,
        AppendEntriesResponse as AppendEntriesResponseRpc, ConflictOpt as ConflictOptRpc, Empty,
        Entry as EntryRpc, InfoResponse, InstallSnapshotRequest as InstallSnapshotRequestRpc,
        InstallSnapshotResponse as InstallSnapshotResponseRpc, MembershipConfig,
        VoteRequest as VoteRequestRpc, VoteResponse as VoteResponseRpc,
    },
    ClientRequest, RaftNode,
};
use async_raft::async_trait::async_trait;
use async_raft::raft::{AppendEntriesRequest, AppendEntriesResponse, ConflictOpt, Entry};
use async_raft::raft::{InstallSnapshotRequest, InstallSnapshotResponse};
use async_raft::raft::{VoteRequest, VoteResponse};
use async_raft::{NodeId, RaftNetwork};
use std::{
    collections::{BTreeMap, HashSet},
    net::IpAddr,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use tonic::{transport::Server, Request, Response, Status};

// Hehehehehe
const GLOBAL_PORT: u16 = 42069;

pub struct RaftRpc {
    node: RaftNode,
}

impl RaftRpc {
    pub fn new(node: RaftNode) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl RaftActions for RaftRpc {
    async fn info(&self, _: Request<Empty>) -> Result<Response<InfoResponse>, Status> {
        use async_raft::State;

        let metrics = self.node.metrics().borrow().clone();
        let state = match metrics.state {
            State::NonVoter => "NonVoter",
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
                    .members
                    .iter()
                    .map(|member| *member)
                    .collect(),
                members_after_consensus: match metrics.membership_config.members_after_consensus {
                    Some(members) => members.iter().map(|member| *member).collect(),
                    None => Vec::new(),
                },
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
                prev_log_index: request.prev_log_index,
                prev_log_term: request.prev_log_term,
                entries: request
                    .entries
                    .into_iter()
                    .map(|entry| Entry::<ClientRequest> {
                        term: entry.term,
                        index: entry.index,
                        payload: bincode::deserialize(entry.payload.as_slice()).unwrap(),
                    })
                    .collect(),
                leader_commit: request.leader_commit,
            })
            .await
            .unwrap();

        Ok(Response::new(AppendEntriesResponseRpc {
            term: response.term,
            success: response.success,
            conflict_opt: match response.conflict_opt {
                Some(conflict_opt) => Some(ConflictOptRpc {
                    term: conflict_opt.term,
                    index: conflict_opt.index,
                }),
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
                last_included_index: request.last_included_index,
                last_included_term: request.last_included_term,
                offset: request.offset,
                data: request.data,
                done: request.done,
            })
            .await
            .unwrap();

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
                last_log_index: request.last_log_index,
                last_log_term: request.last_log_term,
            })
            .await
            .unwrap();

        Ok(Response::new(VoteResponseRpc {
            term: response.term,
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
    address: IpAddr,
    /// Discovery method
    discovery: Box<dyn Discovery>,
    /// Discovered nodes
    discovered_nodes: RwLock<BTreeMap<NodeId, NodeConnection>>,
}

impl Router {
    /// Create a new instance.
    pub fn new(address: IpAddr, discovery: Box<dyn Discovery>) -> Arc<Self> {
        Arc::new(Self {
            address,
            discovery,
            discovered_nodes: Default::default(),
        })
    }

    pub async fn discovered_node_ids(&self) -> HashSet<NodeId> {
        self.discovered_nodes
            .read()
            .await
            .iter()
            .map(|(id, _)| *id)
            .collect()
    }

    pub async fn discover_nodes(&self) -> Result<(), RouterError> {
        let addresses = self.discovery.discover().await?;
        let mut discovered_nodes = self.discovered_nodes.write().await;

        for address in addresses {
            if let Ok(mut connection) =
                NodeConnection::new(format!("http://{}:{}", address, GLOBAL_PORT)).await
            {
                if let Ok(info) = connection.client().info(Empty {}).await {
                    let id = info.into_inner().id;
                    discovered_nodes.insert(id, connection);
                } else {
                    tracing::warn!("Could not reach client: {}", address);
                }
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

    pub async fn server_task(&self, raft_node: RaftNode) -> Result<(), RouterError> {
        let address = SocketAddr::new(self.address, GLOBAL_PORT);

        self.discover_nodes().await?;

        Server::builder()
            .add_service(RaftActionsServer::new(RaftRpc::new(raft_node)))
            .serve(address)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for Router {
    /// Send an AppendEntries RPC to the target Raft node.
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> anyhow::Result<AppendEntriesResponse> {
        let mut discovered_nodes = self.discovered_nodes.write().await;

        if let Some(connection) = discovered_nodes.get_mut(&target) {
            let response = connection
                .client()
                .append_entries(Request::new(AppendEntriesRequestRpc {
                    term: rpc.term,
                    leader_id: rpc.leader_id,
                    prev_log_index: rpc.prev_log_index,
                    prev_log_term: rpc.prev_log_term,
                    entries: rpc
                        .entries
                        .into_iter()
                        .map(|entry| EntryRpc {
                            term: entry.term,
                            index: entry.index,
                            payload: bincode::serialize(&entry.payload).unwrap(),
                        })
                        .collect(),
                    leader_commit: rpc.leader_commit,
                }))
                .await?
                .into_inner();

            Ok(AppendEntriesResponse {
                term: response.term,
                success: response.success,
                conflict_opt: match response.conflict_opt {
                    Some(conflict_opt) => Some(ConflictOpt {
                        term: conflict_opt.term,
                        index: conflict_opt.index,
                    }),
                    None => None,
                },
            })
        } else {
            anyhow::bail!("Target node not found in routing table");
        }
    }

    /// Send an InstallSnapshot RPC to the target Raft node.
    async fn install_snapshot(
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
                    last_included_index: rpc.last_included_index,
                    last_included_term: rpc.last_included_term,
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

    /// Send a RequestVote RPC to the target Raft node.
    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> anyhow::Result<VoteResponse> {
        let mut discovered_nodes = self.discovered_nodes.write().await;

        if let Some(connection) = discovered_nodes.get_mut(&target) {
            let response = connection
                .client()
                .vote(Request::new(VoteRequestRpc {
                    term: rpc.term,
                    candidate_id: rpc.candidate_id,
                    last_log_index: rpc.last_log_index,
                    last_log_term: rpc.last_log_term,
                }))
                .await?
                .into_inner();

            Ok(VoteResponse {
                term: response.term,
                vote_granted: response.vote_granted,
            })
        } else {
            anyhow::bail!("Target node not found in routing table");
        }
    }
}
