use super::*;
use openraft::async_trait::async_trait;
use openraft::raft::{Entry, EntryPayload, Membership};
use openraft::storage::{HardState, InitialState, Snapshot, SnapshotMeta};
use openraft::{
    EffectiveMembership, ErrorSubject, ErrorVerb, LogId, NodeId, RaftStorage, StateMachineChanges,
    StorageError, StorageIOError,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreSnapshot {
    /// The last index covered by this snapshot.
    pub(crate) meta: SnapshotMeta,
    /// The data of the state machine at the time of this snapshot.
    pub(crate) data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StateMachine {
    last_applied_log: LogId,
    last_membership: Option<EffectiveMembership>,
    /// A mapping of client IDs to their state info.
    client_serial_responses: HashMap<(String, Identity), (u64, Option<Vec<u8>>)>,
    /// The current status of a client by ID.
    client_status: HashMap<(String, Identity), Vec<u8>>,
    /// The current state of the Raft node
    state: HashMap<Identity, Box<dyn DistributedState>>,
}

impl StateMachine {
    pub fn new(initial_state: HashMap<Identity, Box<dyn DistributedState>>) -> Self {
        Self {
            last_applied_log: Default::default(),
            last_membership: None,
            client_serial_responses: Default::default(),
            client_status: Default::default(),
            state: initial_state,
        }
    }

    pub fn get_state<S>(&self) -> Option<S>
    where
        S: DistributedStateTrait,
    {
        if let Some(boxed_state) = self.state.get(&S::ID) {
            if let Some(state) = boxed_state.downcast_ref::<S>() {
                Some(state.clone())
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn apply_log(
        &mut self,
        id: &Identity,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, StateMachineError> {
        if let Some(state) = self.state.get_mut(id) {
            Ok(state.mutate(data)?)
        } else {
            Err(StateMachineError::MissingState)
        }
    }
}

pub struct Store {
    /// The ID of the Raft node for which this memory storage instances is configured.
    id: NodeId,
    /// The Raft log.
    log: RwLock<BTreeMap<u64, Entry<ClientRequest>>>,
    /// The Raft state machine.
    state_machine: RwLock<StateMachine>,
    /// The current hard state.
    hard_state: RwLock<Option<HardState>>,
    // Current snapshot index
    snapshot_index: Arc<AtomicU64>,
    /// The current snapshot.
    current_snapshot: RwLock<Option<StoreSnapshot>>,
}

impl Store {
    /// Create a new `Store` instance.
    pub fn new(initial_state: HashMap<Identity, Box<dyn DistributedState>>) -> Arc<Self> {
        use std::time::SystemTime;

        let mut hasher = DefaultHasher::new();
        let log = RwLock::new(BTreeMap::new());
        let state_machine = RwLock::new(StateMachine::new(initial_state));
        let hard_state = RwLock::new(None);
        let current_snapshot = RwLock::new(None);
        let snapshot_index = Arc::new(AtomicU64::new(0));

        // There is probably a better way to do this but cryptography is beyond
        // me so I guess this will do.
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();

        hasher.write_u128(time.as_nanos());
        hasher.write_u128(rand::random());

        Arc::new(Self {
            id: hasher.finish(),
            log,
            state_machine,
            hard_state,
            snapshot_index,
            current_snapshot,
        })
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub async fn get_state_machine(&self) -> RwLockReadGuard<'_, StateMachine> {
        self.state_machine.read().await
    }

    fn find_first_membership_log<'a, T, D>(mut it: T) -> Option<EffectiveMembership>
    where
        T: 'a + Iterator<Item = &'a Entry<D>>,
        D: AppData,
    {
        it.find_map(|entry| match &entry.payload {
            EntryPayload::Membership(cfg) => Some(EffectiveMembership {
                log_id: entry.log_id,
                membership: cfg.clone(),
            }),
            _ => None,
        })
    }

    /// Go backwards through the log to find the most recent membership config <= `upto_index`.
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_membership_from_log(
        &self,
        upto_index: Option<u64>,
    ) -> Result<EffectiveMembership, StorageError> {
        let membership_in_log = {
            let log = self.log.read().await;

            let reversed_logs = log.values().rev();
            match upto_index {
                Some(upto) => {
                    let skipped = reversed_logs.skip_while(|entry| entry.log_id.index > upto);
                    Self::find_first_membership_log(skipped)
                }
                None => Self::find_first_membership_log(reversed_logs),
            }
        };

        // Find membership stored in state machine.
        let (_, membership_in_sm) = self.last_applied_state().await?;

        let membership = if membership_in_log.as_ref().map(|x| x.log_id.index)
            > membership_in_sm.as_ref().map(|x| x.log_id.index)
        {
            membership_in_log
        } else {
            membership_in_sm
        };

        // Create a default one if both are None.
        Ok(match membership {
            Some(x) => x,
            None => EffectiveMembership {
                log_id: LogId { term: 0, index: 0 },
                membership: Membership::new_initial(self.id),
            },
        })
    }
}

#[async_trait]
impl RaftStorage<ClientRequest, ClientResponse> for Store {
    type SnapshotData = Cursor<Vec<u8>>;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_membership_config(&self) -> Result<EffectiveMembership, StorageError> {
        self.get_membership_from_log(None).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_initial_state(&self) -> Result<InitialState, StorageError> {
        let membership = self.get_membership_config().await?;
        let mut hard_state = self.hard_state.write().await;
        match &mut *hard_state {
            Some(inner) => {
                // Search for two place and use the max one,
                // because when a state machine is installed there could be logs
                // included in the state machine that are not cleaned:
                // - the last log id
                // - the last_applied log id in state machine.
                let last_in_log = self.last_id_in_log().await?;
                let (last_applied, _) = self.last_applied_state().await?;

                let last_log_id = std::cmp::max(last_in_log, last_applied);

                Ok(InitialState {
                    last_log_id,
                    last_applied,
                    hard_state: inner.clone(),
                    last_membership: membership,
                })
            }
            None => {
                let new = InitialState::new_initial(self.id);
                *hard_state = Some(new.hard_state.clone());
                Ok(new)
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_hard_state(&self, new_hard_state: &HardState) -> Result<(), StorageError> {
        tracing::debug!(?new_hard_state, "save_hard_state");
        let mut hard_state = self.hard_state.write().await;

        *hard_state = Some(new_hard_state.clone());

        Ok(())
    }

    async fn read_hard_state(&self) -> Result<Option<HardState>, StorageError> {
        Ok(self.hard_state.read().await.clone())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_log_entries<RNG: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &self,
        range: RNG,
    ) -> Result<Vec<Entry<ClientRequest>>, StorageError> {
        let res = {
            let log = self.log.read().await;
            log.range(range.clone())
                .map(|(_, val)| val.clone())
                .collect::<Vec<_>>()
        };

        Ok(res)
    }

    async fn try_get_log_entries<RNG: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &self,
        range: RNG,
    ) -> Result<Vec<Entry<ClientRequest>>, StorageError> {
        let res = {
            let log = self.log.read().await;
            log.range(range.clone())
                .map(|(_, val)| val.clone())
                .collect::<Vec<_>>()
        };

        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn try_get_log_entry(
        &self,
        log_index: u64,
    ) -> Result<Option<Entry<ClientRequest>>, StorageError> {
        let log = self.log.read().await;
        Ok(log.get(&log_index).cloned())
    }

    async fn first_id_in_log(&self) -> Result<Option<LogId>, StorageError> {
        let log = self.log.read().await;
        let first = log.iter().next().map(|(_, ent)| ent.log_id);
        Ok(first)
    }

    async fn first_known_log_id(&self) -> Result<LogId, StorageError> {
        let first = self.first_id_in_log().await?;
        let (last_applied, _) = self.last_applied_state().await?;

        if let Some(x) = first {
            return Ok(std::cmp::min(x, last_applied));
        }

        Ok(last_applied)
    }

    async fn last_id_in_log(&self) -> Result<LogId, StorageError> {
        let log = self.log.read().await;
        let last = log
            .iter()
            .last()
            .map(|(_, ent)| ent.log_id)
            .unwrap_or_default();
        Ok(last)
    }

    async fn last_applied_state(
        &self,
    ) -> Result<(LogId, Option<EffectiveMembership>), StorageError> {
        let state_machine = self.state_machine.read().await;

        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, range), fields(range=?range))]
    async fn delete_logs_from<R: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &self,
        range: R,
    ) -> Result<(), StorageError> {
        {
            tracing::debug!("delete_logs_from: {:?}", range);

            let mut log = self.log.write().await;

            let keys = log.range(range).map(|(k, _v)| *k).collect::<Vec<_>>();
            for key in keys {
                log.remove(&key);
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(&self, entries: &[&Entry<ClientRequest>]) -> Result<(), StorageError> {
        let mut log = self.log.write().await;

        for entry in entries {
            log.insert(entry.log_id.index, (*entry).clone());
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &self,
        entries: &[&Entry<ClientRequest>],
    ) -> Result<Vec<ClientResponse>, StorageError> {
        let mut state_machine = self.state_machine.write().await;
        let mut responses = Vec::with_capacity(entries.len());

        for entry in entries {
            tracing::debug!(
                "id: `{}` replicate to state machine index: `{}`",
                self.id,
                entry.log_id.index
            );

            state_machine.last_applied_log = entry.log_id;

            match entry.payload {
                EntryPayload::Blank => responses.push(ClientResponse(None)),
                EntryPayload::Normal(ref data) => {
                    if let Some((serial, response)) = state_machine
                        .client_serial_responses
                        .get(&(data.client.clone(), data.payload_type))
                    {
                        if serial == &data.serial {
                            responses.push(ClientResponse(response.clone()));
                            continue;
                        }
                    }
                    let previous = state_machine.client_status.insert(
                        (data.client.clone(), data.payload_type).clone(),
                        data.payload_data.clone(),
                    );
                    state_machine.client_serial_responses.insert(
                        (data.client.clone(), data.payload_type).clone(),
                        (data.serial, previous.clone()),
                    );
                    state_machine
                        .apply_log(&data.payload_type, data.payload_data.clone())
                        .map_err(|error| {
                            StorageIOError::new(
                                ErrorSubject::StateMachine,
                                ErrorVerb::Write,
                                error.into(),
                            )
                        })?;
                    responses.push(ClientResponse(previous));
                }
                EntryPayload::Membership(ref mem) => {
                    state_machine.last_membership = Some(EffectiveMembership {
                        log_id: entry.log_id,
                        membership: mem.clone(),
                    });
                    responses.push(ClientResponse(None))
                }
            };
        }
        Ok(responses)
    }

    async fn do_log_compaction(&self) -> Result<Snapshot<Self::SnapshotData>, StorageError> {
        let (data, last_applied_log) = {
            // Serialize the data of the state machine.
            let state_machine = self.state_machine.read().await;
            (
                bincode::serialize(&*state_machine).map_err(|e| {
                    StorageIOError::new(ErrorSubject::StateMachine, ErrorVerb::Read, e.into())
                })?,
                state_machine.last_applied_log,
            )
        }; // Release state machine read lock.

        let snapshot_size = data.len();

        let snapshot_index = {
            let l = self.snapshot_index.fetch_add(1, Ordering::SeqCst);
            l + 1
        };

        let meta;
        {
            let mut current_snapshot = self.current_snapshot.write().await;

            let snapshot_id = format!(
                "{}-{}-{}",
                last_applied_log.term, last_applied_log.index, snapshot_index
            );

            meta = SnapshotMeta {
                last_log_id: last_applied_log,
                snapshot_id,
            };

            let snapshot = StoreSnapshot {
                meta: meta.clone(),
                data: data.clone(),
            };

            *current_snapshot = Some(snapshot);
        } // Release log & snapshot write locks.

        tracing::info!({ snapshot_size = snapshot_size }, "Log compaction complete");
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(&self) -> Result<Box<Self::SnapshotData>, StorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn finalize_snapshot_installation(
        &self,
        meta: &SnapshotMeta,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges, StorageError> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "Decoding snapshot for installation"
        );

        let new_snapshot = StoreSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let new_state_machine: StateMachine = bincode::deserialize(&new_snapshot.data)
                .map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.clone()),
                        ErrorVerb::Read,
                        e.into(),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = new_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);

        Ok(StateMachineChanges {
            last_applied: Some(meta.last_log_id),
            is_snapshot: true,
        })
    }

    async fn get_current_snapshot(
        &self,
    ) -> Result<Option<Snapshot<Self::SnapshotData>>, StorageError> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let reader = bincode::serialize(&snapshot).map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(snapshot.meta.clone()),
                        ErrorVerb::Read,
                        e.into(),
                    )
                })?;

                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(reader)),
                }))
            }
            None => Ok(None),
        }
    }
}
