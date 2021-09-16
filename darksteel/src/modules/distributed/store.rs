use super::*;
use anyhow::Result;
use async_raft::async_trait::async_trait;
use async_raft::raft::{Entry, EntryPayload, MembershipConfig};
use async_raft::storage::{CurrentSnapshotData, HardState, InitialState};
use async_raft::{NodeId, RaftStorage};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreSnapshot {
    /// The last index covered by this snapshot.
    pub(crate) index: u64,
    /// The term of the last index covered by this snapshot.
    pub(crate) term: u64,
    /// The last memberhsip config included in this snapshot.
    pub(crate) membership: MembershipConfig,
    /// The data of the state machine at the time of this snapshot.
    pub(crate) data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StateMachine {
    last_applied_log: u64,
    /// A mapping of client IDs to their state info.
    client_serial_responses: HashMap<(String, Identity), (u64, Vec<u8>)>,
    /// The current state of the Raft node
    state: HashMap<Identity, Box<dyn DistributedState>>,
}

impl StateMachine {
    pub fn new(initial_state: HashMap<Identity, Box<dyn DistributedState>>) -> Self {
        Self {
            last_applied_log: Default::default(),
            client_serial_responses: Default::default(),
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
            current_snapshot,
        })
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub async fn get_state_machine(&self) -> RwLockReadGuard<'_, StateMachine> {
        self.state_machine.read().await
    }
}

#[async_trait]
impl RaftStorage<ClientRequest, ClientResponse> for Store {
    type Snapshot = Cursor<Vec<u8>>;
    type ShutdownError = ShutdownError;

    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        let log = self.log.read().await;
        let cfg_opt = log.values().rev().find_map(|entry| match &entry.payload {
            EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
            EntryPayload::SnapshotPointer(snap) => Some(snap.membership.clone()),
            _ => None,
        });
        Ok(match cfg_opt {
            Some(cfg) => cfg,
            None => MembershipConfig::new_initial(self.id),
        })
    }

    async fn get_initial_state(&self) -> Result<InitialState> {
        let membership = self.get_membership_config().await?;
        let mut hs = self.hard_state.write().await;
        let log = self.log.read().await;
        let sm = self.state_machine.read().await;
        match &mut *hs {
            Some(inner) => {
                let (last_log_index, last_log_term) = match log.values().rev().next() {
                    Some(log) => (log.index, log.term),
                    None => (0, 0),
                };
                let last_applied_log = sm.last_applied_log;
                Ok(InitialState {
                    last_log_index,
                    last_log_term,
                    last_applied_log,
                    hard_state: inner.clone(),
                    membership,
                })
            }
            None => {
                let new = InitialState::new_initial(self.id);
                *hs = Some(new.hard_state.clone());
                Ok(new)
            }
        }
    }

    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        *self.hard_state.write().await = Some(hs.clone());
        Ok(())
    }

    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<ClientRequest>>> {
        // Invalid request, return empty vec.
        if start > stop {
            tracing::error!("Invalid request, start > stop");
            return Ok(vec![]);
        }
        let log = self.log.read().await;
        Ok(log.range(start..stop).map(|(_, val)| val.clone()).collect())
    }

    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        if stop.as_ref().map(|stop| &start > stop).unwrap_or(false) {
            tracing::error!("Invalid request, start > stop");
            return Ok(());
        }
        let mut log = self.log.write().await;

        // If a stop point was specified, delete from start until the given stop point.
        if let Some(stop) = stop.as_ref() {
            for key in start..*stop {
                log.remove(&key);
            }
            return Ok(());
        }
        // Else, just split off the remainder.
        log.split_off(&start);
        Ok(())
    }

    async fn append_entry_to_log(&self, entry: &Entry<ClientRequest>) -> Result<()> {
        let mut log = self.log.write().await;
        log.insert(entry.index, entry.clone());
        Ok(())
    }

    async fn replicate_to_log(&self, entries: &[Entry<ClientRequest>]) -> Result<()> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &ClientRequest,
    ) -> Result<ClientResponse> {
        let mut state_machine = self.state_machine.write().await;
        state_machine.last_applied_log = *index;
        if let Some((serial, res)) = state_machine
            .client_serial_responses
            .get(&(data.client.clone(), data.payload_type))
        {
            if serial == &data.serial {
                return Ok(ClientResponse(res.clone()));
            }
        }

        let previous = state_machine.apply_log(&data.payload_type, data.payload_data.clone())?;

        state_machine.client_serial_responses.insert(
            (data.client.clone(), data.payload_type),
            (data.serial, previous.clone()),
        );
        Ok(ClientResponse(previous))
    }

    async fn replicate_to_state_machine(&self, entries: &[(&u64, &ClientRequest)]) -> Result<()> {
        let mut state_machine = self.state_machine.write().await;
        for (index, data) in entries {
            state_machine.last_applied_log = **index;
            if let Some((serial, _)) = state_machine
                .client_serial_responses
                .get(&(data.client.clone(), data.payload_type))
            {
                if serial == &data.serial {
                    continue;
                }
            }

            let previous =
                state_machine.apply_log(&data.payload_type, data.payload_data.clone())?;

            state_machine.client_serial_responses.insert(
                (data.client.clone(), data.payload_type),
                (data.serial, previous.clone()),
            );
        }
        Ok(())
    }

    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        let (data, last_applied_log) = {
            // Serialize the data of the state machine.
            let sm = self.state_machine.read().await;
            (bincode::serialize(&*sm)?, sm.last_applied_log)
        }; // Release state machine read lock.

        let membership_config = {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            self.log
                .read()
                .await
                .values()
                .rev()
                .skip_while(|entry| entry.index > last_applied_log)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id))
        }; // Release log read lock.

        let snapshot_bytes: Vec<u8>;
        let term;
        {
            let mut log = self.log.write().await;
            let mut current_snapshot = self.current_snapshot.write().await;
            term = log
                .get(&last_applied_log)
                .map(|entry| entry.term)
                .ok_or_else(|| anyhow::anyhow!(ERR_INCONSISTENT_LOG))?;
            *log = log.split_off(&last_applied_log);
            log.insert(
                last_applied_log,
                Entry::new_snapshot_pointer(
                    last_applied_log,
                    term,
                    "".into(),
                    membership_config.clone(),
                ),
            );

            let snapshot = StoreSnapshot {
                index: last_applied_log,
                term,
                membership: membership_config.clone(),
                data,
            };
            snapshot_bytes = bincode::serialize(&snapshot)?;
            *current_snapshot = Some(snapshot);
        } // Release log & snapshot write locks.

        tracing::info!(
            { snapshot_size = snapshot_bytes.len() },
            "Log compaction complete"
        );
        Ok(CurrentSnapshotData {
            term,
            index: last_applied_log,
            membership: membership_config.clone(),
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
        })
    }

    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        Ok((String::from(""), Box::new(Cursor::new(Vec::new())))) // Snapshot IDs are insignificant to this storage engine.
    }

    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "Decoding snapshot for installation"
        );
        let new_snapshot: StoreSnapshot = bincode::deserialize(snapshot.get_ref().as_slice())?;
        // Update log.
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let mut log = self.log.write().await;
            let membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > index)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));

            match &delete_through {
                Some(through) => {
                    *log = log.split_off(&(through + 1));
                }
                None => log.clear(),
            }
            log.insert(
                index,
                Entry::new_snapshot_pointer(index, term, id, membership_config),
            );
        }

        // Update the state machine.
        {
            let new_state_machine: StateMachine = bincode::deserialize(&new_snapshot.data)?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = new_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let reader = bincode::serialize(&snapshot)?;
                Ok(Some(CurrentSnapshotData {
                    index: snapshot.index,
                    term: snapshot.term,
                    membership: snapshot.membership.clone(),
                    snapshot: Box::new(Cursor::new(reader)),
                }))
            }
            None => Ok(None),
        }
    }
}
