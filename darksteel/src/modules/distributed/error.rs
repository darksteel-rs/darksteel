use thiserror::Error;

#[derive(Debug, Error)]
pub enum NodeError {
    #[error("Raft Config Error: {0}")]
    RaftConfig(#[from] async_raft::error::ConfigError),
    #[error("Router Error: {0}")]
    Router(#[from] RouterError),

    #[error("Node Error: {0}")]
    Any(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum NodeConnectionError {
    #[error("Invalid Uri Error: {0}")]
    InvalidUri(#[from] ::http::uri::InvalidUri),
    #[error("Transport Error: {0}")]
    Transport(#[from] tonic::transport::Error),
}

#[derive(Debug, Error)]
pub enum CommitError {
    #[error("Raft Client Read Error: {0}")]
    RaftClientRead(#[from] async_raft::error::ClientReadError),
    #[error("Raft Client Write Error: {0}")]
    RaftClientWrite(#[from] async_raft::error::ClientWriteError<super::ClientRequest>),
    #[error("Mutator Error: ")]
    Mutator(#[from] MutatorError),
    #[error("Could not commit to distributed state, no leader exists")]
    NoLeader,
    #[error("Could not commit to distributed state, not the leader")]
    NotLeader,
}

#[derive(Debug, Error)]
pub enum StateMachineError {
    #[error("The state machine is missing the state type referenced")]
    MissingState,
    #[error("Mutator Error: ")]
    Mutator(#[from] MutatorError),
}

#[derive(Debug, Error)]
pub enum MutatorError {
    #[error("Error with bincode: {0}")]
    Bincode(#[from] bincode::Error),
}

#[derive(Debug, Error)]
pub enum RouterError {
    #[error("Transport Error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("Router Error: {0}")]
    Any(#[from] anyhow::Error),
}
