use super::*;
use crate::prelude::TaskErrorTrait;
use crate::process::ChildRestartPolicy;
use chrono::prelude::*;
use std::collections::{BTreeSet, HashMap, VecDeque};

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum TransitionState {
    Starting,
    Terminating,
}

#[derive(Clone, Debug)]
pub enum StateTransition {
    None,
    Start {
        pids_active: VecDeque<ProcessId>,
        pids_exited: BTreeSet<ProcessId>,
        waiting_on: ProcessId,
    },
    Terminate {
        pids_exit: VecDeque<ProcessId>,
        waiting_on: ProcessId,
    },
    Shutdown {
        pids_required: BTreeSet<ProcessId>,
    },
    RestartAll {
        state: TransitionState,
        pids_exit: VecDeque<ProcessId>,
        pids_active: VecDeque<ProcessId>,
        waiting_on: ProcessId,
    },
    RestartRest {
        state: TransitionState,
        lowest_pid: ProcessId,
        pids_exit: VecDeque<ProcessId>,
        pids_active: VecDeque<ProcessId>,
        waiting_on: ProcessId,
    },
}

pub enum Finalise {
    None,
    Start,
    Terminate,
    Shutdown,
}

pub enum StateAction {
    /// Do not take any action.
    None,
    /// Tell the supervisor it needs to start a process. Sometimes it will need
    /// to finalise a state transition and let its parent know that it has
    /// finished starting by sending a `Signal`.
    Start(ProcessId, Finalise),
    /// Tell the supervisor it needs to start multiple processes. This is only
    /// used in non-ordered restart policies like OneForOne.
    StartMultiple(BTreeSet<ProcessId>, Finalise),
    /// Tell the supervisor it needs to terminate a process.
    Terminate(ProcessId),
    /// Tell the supervisor it needs to shutdown and provides it a list of pids
    /// to shutdown.
    Shutdown {
        pids_required: BTreeSet<ProcessId>,
    },
    Finalise(Finalise),
}

#[derive(Debug)]
pub(crate) struct StateManager {
    config: SupervisorConfig,
    managed_pids: ChildOrder,
    do_not_restart: BTreeSet<ProcessId>,
    state: StateTransition,
    interval_timestamp: DateTime<Utc>,
    interval_restarts: u32,
}

/// The `StateManager` determines how the supervisor shifts between various
/// states eliminating extra work if possible.
impl StateManager {
    pub fn new(config: SupervisorConfig, managed_pids: ChildOrder) -> Self {
        Self {
            config,
            managed_pids,
            do_not_restart: Default::default(),
            state: StateTransition::None,
            interval_timestamp: Utc::now(),
            interval_restarts: 0,
        }
    }

    //
    //  HELPER FUNCTIONS
    //
    fn finish(&mut self, action: impl Into<Option<StateAction>>) -> StateAction {
        self.state = StateTransition::None;

        if let Some(action) = action.into() {
            action
        } else {
            StateAction::None
        }
    }

    fn shutdown(&mut self) -> StateAction {
        self.state = StateTransition::Shutdown {
            pids_required: self.managed_pids.pid_set(),
        };

        StateAction::Shutdown {
            pids_required: self.managed_pids.pid_set(),
        }
    }

    fn do_not_restart<E: TaskErrorTrait>(
        pid: &ProcessId,
        reason: &ExitReason<E>,
        policies: &HashMap<ProcessId, ChildRestartPolicy>,
    ) -> bool {
        if let Some(child) = policies.get(&pid) {
            match child {
                ChildRestartPolicy::Permanent => (),
                ChildRestartPolicy::Temporary => {
                    return true;
                }
                ChildRestartPolicy::Transient => {
                    if let ExitReason::Normal | ExitReason::Terminate = reason {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn exit(pid: ProcessId) -> BTreeSet<ProcessId> {
        let mut exited = BTreeSet::new();
        exited.insert(pid);
        exited
    }

    //
    //  STATE INITIALISERS
    //
    fn start<I: Into<Option<BTreeSet<ProcessId>>>>(&mut self, ignore: I) -> StateAction {
        let mut pids_active = VecDeque::new();
        let ignore = ignore.into();
        self.do_not_restart.clear();

        for pid in &self.managed_pids.pid_vec() {
            if let Some(ignore) = &ignore {
                if !ignore.contains(pid) {
                    pids_active.push_back(*pid);
                }
            } else {
                pids_active.push_back(*pid);
            }
        }

        if let Some(next_pid) = pids_active.pop_front() {
            self.state = StateTransition::Start {
                pids_active,
                pids_exited: Default::default(),
                waiting_on: next_pid,
            };

            StateAction::Start(next_pid, Finalise::None)
        } else {
            self.state = StateTransition::None;
            StateAction::None
        }
    }

    fn terminate<I: Into<Option<BTreeSet<ProcessId>>>>(&mut self, ignore: I) -> StateAction {
        let mut pids_exit = VecDeque::new();
        let ignore = ignore.into();

        for pid in &self.managed_pids.pid_vec() {
            if let Some(ignore) = &ignore {
                if !ignore.contains(pid) {
                    pids_exit.push_front(*pid);
                }
            } else {
                pids_exit.push_front(*pid);
            }
        }

        if let Some(next_pid) = pids_exit.pop_front() {
            self.state = StateTransition::Terminate {
                pids_exit,
                waiting_on: next_pid,
            };

            StateAction::Terminate(next_pid)
        } else {
            self.state = StateTransition::None;
            StateAction::None
        }
    }

    fn restart_one(&mut self, exited: &BTreeSet<ProcessId>, finalise: Finalise) -> StateAction {
        if self.restart_exceeded() {
            return self.terminate(exited.clone());
        }

        self.state = StateTransition::None;
        StateAction::StartMultiple(exited.clone(), finalise)
    }

    fn restart_all(&mut self, exited: &BTreeSet<ProcessId>, finalise: Finalise) -> StateAction {
        if self.restart_exceeded() {
            return self.terminate(exited.clone());
        }

        let mut pids_exit = VecDeque::new();
        let mut pids_active = VecDeque::new();

        for pid in &self.managed_pids.pid_vec() {
            if !exited.contains(pid) {
                // We push the pids we want to terminate in front so the least
                // dependent pids terminate first.
                pids_exit.push_front(*pid);
            }

            if !self.do_not_restart.contains(pid) {
                // We push the pids we want to start to the back to the least
                // dependent start first.
                pids_active.push_back(*pid);
            }
        }

        if let Some(next_pid) = pids_exit.pop_front() {
            self.state = StateTransition::RestartAll {
                state: TransitionState::Terminating,
                pids_exit,
                pids_active,
                waiting_on: next_pid,
            };

            StateAction::Terminate(next_pid)
        } else if let Some(next_pid) = pids_active.pop_front() {
            self.state = StateTransition::RestartAll {
                state: TransitionState::Starting,
                pids_exit,
                pids_active,
                waiting_on: next_pid,
            };

            StateAction::Start(next_pid, finalise)
        } else {
            // At this point we need to check if it is allowed to automatically
            // finalise termination because there are no more pids likely to
            // exit.
            if self.config.termination_policy == AutomaticTerminationPolicy::Any {
                StateAction::Finalise(Finalise::Terminate)
            } else {
                StateAction::None
            }
        }
    }

    fn restart_rest(
        &mut self,
        lowest_pid: &ProcessId,
        exited: &BTreeSet<ProcessId>,
        finalise: Finalise,
    ) -> StateAction {
        if self.restart_exceeded() {
            return self.terminate(exited.clone());
        }

        let mut pids_exit = VecDeque::new();
        let mut pids_active = VecDeque::new();

        for pid in &self.managed_pids.pid_vec() {
            if pid >= lowest_pid {
                if !exited.contains(pid) {
                    // We push the pids we want to terminate in front so the least
                    // dependent pids terminate first.
                    pids_exit.push_front(*pid);
                }

                if !self.do_not_restart.contains(pid) {
                    // We push the pids we want to start to the back to the least
                    // dependent start first.
                    pids_active.push_back(*pid);
                }
            }
        }

        if let Some(next_pid) = pids_exit.pop_front() {
            self.state = StateTransition::RestartRest {
                lowest_pid: *lowest_pid,
                state: TransitionState::Terminating,
                pids_exit,
                pids_active,
                waiting_on: next_pid,
            };

            StateAction::Terminate(next_pid)
        } else if let Some(next_pid) = pids_active.pop_front() {
            self.state = StateTransition::RestartRest {
                lowest_pid: *lowest_pid,
                state: TransitionState::Starting,
                pids_exit,
                pids_active,
                waiting_on: next_pid,
            };

            StateAction::Start(next_pid, finalise)
        } else {
            // At this point we need to check if it is allowed to automatically
            // finalise termination because there are no more pids likely to
            // exit.
            if self.config.termination_policy == AutomaticTerminationPolicy::Any {
                StateAction::Finalise(Finalise::Terminate)
            } else {
                StateAction::None
            }
        }
    }

    /// Check if a restart amount has exceeded the interval and intensity. It is
    /// implied by calling this function that a pid has exited.
    fn restart_exceeded(&mut self) -> bool {
        // If the intensity is set to 0, we can restart an unlimited amount in
        // an interval.
        if self.config.restart_intensity != 0 {
            let now = Utc::now();

            // Check when the last interval was
            if now.signed_duration_since(self.interval_timestamp) > self.config.restart_interval {
                // It's a new interval
                self.interval_timestamp = now;
                self.interval_restarts = 1;
            } else {
                // We are in the current interval
                self.interval_restarts += 1;

                // Have we exceeded our set limit of restarts for the interval?
                if self.interval_restarts > self.config.restart_intensity {
                    return true;
                }
            }
        }

        false
    }

    /// Based on the incoming `Signal`, determine the next action the state
    /// should take.
    pub fn next_action<E: TaskErrorTrait>(
        &mut self,
        policies: &HashMap<ProcessId, ChildRestartPolicy>,
        signal: &ProcessSignal<E>,
    ) -> StateAction {
        // TODO: Handle externally activated shutdowns in children.
        match self.state {
            StateTransition::None => match signal {
                ProcessSignal::Exit(child_pid, reason) => {
                    if Self::do_not_restart(child_pid, reason, policies) {
                        self.do_not_restart.insert(*child_pid);
                    }

                    match self.config.termination_policy {
                        AutomaticTerminationPolicy::Never => (),
                        AutomaticTerminationPolicy::All => {
                            // If our do not restart list has all the pids in
                            // managed pids, we can safely engage finalisation
                            // of termination.
                            if self.do_not_restart == self.managed_pids.pid_set() {
                                return StateAction::Finalise(Finalise::Terminate);
                            }
                        }
                        AutomaticTerminationPolicy::Any => {
                            // If any process terminates, we should begin
                            // automatic termination.
                            return self.terminate(Self::exit(*child_pid));
                        }
                    }

                    match self.config.restart_policy {
                        SupervisorRestartPolicy::OneForOne => {
                            self.restart_one(&Self::exit(*child_pid), Finalise::None)
                        }
                        SupervisorRestartPolicy::OneForAll => {
                            self.restart_all(&Self::exit(*child_pid), Finalise::None)
                        }
                        SupervisorRestartPolicy::RestForOne => {
                            self.restart_rest(child_pid, &Self::exit(*child_pid), Finalise::None)
                        }
                    }
                }
                ProcessSignal::Shutdown => self.shutdown(),
                ProcessSignal::Start => self.start(None),
                _ => StateAction::None,
            },
            StateTransition::Shutdown {
                ref mut pids_required,
            } => match signal {
                ProcessSignal::Exit(child_pid, reason) => match reason {
                    ExitReason::Shutdown => {
                        pids_required.remove(&child_pid);

                        if pids_required.is_empty() {
                            self.state = StateTransition::None;

                            StateAction::Finalise(Finalise::Shutdown)
                        } else {
                            StateAction::None
                        }
                    }
                    _ => StateAction::None,
                },
                _ => StateAction::None,
            },
            StateTransition::RestartAll {
                ref mut state,
                ref mut pids_exit,
                ref mut pids_active,
                ref mut waiting_on,
            } => match state {
                TransitionState::Terminating => match signal {
                    ProcessSignal::Exit(child_pid, reason) => {
                        if Self::do_not_restart(child_pid, reason, policies) {
                            self.do_not_restart.insert(*child_pid);
                            // Remove it from the start list
                            pids_active.retain(|pid| pid != child_pid);
                        }
                        if child_pid == waiting_on {
                            if let Some(next_pid) = pids_exit.pop_front() {
                                *waiting_on = next_pid;
                                StateAction::Terminate(next_pid)
                            } else if let Some(next_pid) = pids_active.pop_front() {
                                *state = TransitionState::Starting;
                                *waiting_on = next_pid;
                                StateAction::Start(next_pid, Finalise::None)
                            } else {
                                self.finish(None)
                            }
                        } else {
                            tracing::info!(
                                "Task({pid}) Exited out of order, removing from list of pids requiring termination",
                                pid = child_pid
                            );
                            // Under this policy we don't really have to deal
                            // with unknown exit pids so this is adequate.
                            pids_exit.retain(|pid| pid != child_pid);

                            StateAction::None
                        }
                    }
                    ProcessSignal::Active(child_pid) => {
                        use sorted_insert::SortedInsert;

                        tracing::info!(
                            "Task({pid}) has become active during `RestartAll`s Termination sequence, adding to list of pids to terminate",
                            pid = child_pid
                        );

                        pids_exit.sorted_insert_desc(*child_pid);

                        StateAction::None
                    }
                    ProcessSignal::Terminate => {
                        let mut ignore = pids_exit
                            .iter()
                            .map(|pid| *pid)
                            .collect::<BTreeSet<ProcessId>>()
                            .difference(&self.managed_pids.pid_set())
                            .cloned()
                            .collect::<BTreeSet<ProcessId>>();

                        ignore.remove(waiting_on);

                        self.terminate(ignore)
                    }
                    ProcessSignal::Shutdown => self.shutdown(),
                    _ => StateAction::None,
                },
                TransitionState::Starting => match signal {
                    ProcessSignal::Exit(child_pid, reason) => {
                        if Self::do_not_restart(child_pid, reason, policies) {
                            self.do_not_restart.insert(*child_pid);
                            // Remove it from the start list
                            pids_active.retain(|pid| pid != child_pid);
                        }
                        let mut exited = pids_active
                            .iter()
                            .map(|pid| *pid)
                            .collect::<BTreeSet<ProcessId>>()
                            .difference(&self.managed_pids.pid_set())
                            .cloned()
                            .collect::<BTreeSet<ProcessId>>();

                        exited.insert(*child_pid);

                        self.restart_all(&exited, Finalise::None)
                    }
                    ProcessSignal::Active(child_pid) => {
                        if *child_pid == *waiting_on {
                            if let Some(next_pid) = pids_active.pop_front() {
                                *waiting_on = next_pid;
                                StateAction::Start(*waiting_on, Finalise::None)
                            } else {
                                self.finish(None)
                            }
                        } else {
                            tracing::info!(
                                "Task({pid}) Started out of order, removing from list of pids requiring starting",
                                pid = child_pid
                            );

                            // We can return nothing because we still have
                            // a pid to wait on if the queue is empty.
                            pids_active.retain(|pid| pid != child_pid);

                            StateAction::None
                        }
                    }
                    ProcessSignal::Terminate => {
                        let mut ignore = pids_active
                            .iter()
                            .map(|pid| *pid)
                            .collect::<BTreeSet<ProcessId>>()
                            .difference(&self.managed_pids.pid_set())
                            .cloned()
                            .collect::<BTreeSet<ProcessId>>();

                        ignore.remove(waiting_on);

                        self.terminate(ignore)
                    }
                    ProcessSignal::Shutdown => self.shutdown(),
                    _ => StateAction::None,
                },
            },
            StateTransition::RestartRest {
                ref lowest_pid,
                ref mut state,
                ref mut pids_exit,
                ref mut pids_active,
                ref mut waiting_on,
            } => match state {
                TransitionState::Terminating => match signal {
                    ProcessSignal::Exit(child_pid, reason) => {
                        if Self::do_not_restart(child_pid, reason, policies) {
                            self.do_not_restart.insert(*child_pid);
                            // Remove it from the start list
                            pids_active.retain(|pid| pid != child_pid);
                        }

                        if child_pid == waiting_on {
                            if let Some(next_pid) = pids_exit.pop_front() {
                                *waiting_on = next_pid;
                                StateAction::Terminate(next_pid)
                            } else if let Some(next_pid) = pids_active.pop_front() {
                                *state = TransitionState::Starting;
                                *waiting_on = next_pid;
                                StateAction::Start(next_pid, Finalise::None)
                            } else {
                                self.finish(None)
                            }
                        } else {
                            // Check to see if the exited pid was started before
                            // the pid that intitiated the `RestartRest` state.
                            if child_pid < lowest_pid {
                                let mut exited = pids_exit
                                    .iter()
                                    .map(|pid| *pid)
                                    .collect::<BTreeSet<ProcessId>>()
                                    .difference(&self.managed_pids.pid_set())
                                    .cloned()
                                    .collect::<BTreeSet<ProcessId>>();

                                exited.insert(*child_pid);

                                self.restart_rest(child_pid, &exited, Finalise::None)
                            } else {
                                tracing::info!(
                                    "Task({pid}) Exited out of order, possibly due to error - removing from list of pids requiring termination",
                                    pid = child_pid
                                );

                                pids_exit.retain(|pid| pid != child_pid);

                                // We can return nothing because we still have
                                // a pid to wait on if the queue is empty.
                                StateAction::None
                            }
                        }
                    }
                    ProcessSignal::Active(child_pid) => {
                        use sorted_insert::SortedInsert;

                        if child_pid < lowest_pid {
                            tracing::info!(
                                "Task({pid}) has become active during `RestartRest`s Termination sequence, adding to list of pids to terminate",
                                pid = child_pid
                            );

                            pids_exit.sorted_insert_desc(*child_pid);
                        }

                        StateAction::None
                    }
                    ProcessSignal::Terminate => {
                        let mut ignore = pids_exit
                            .iter()
                            .map(|pid| *pid)
                            .collect::<BTreeSet<ProcessId>>()
                            .difference(&self.managed_pids.pid_set())
                            .cloned()
                            .collect::<BTreeSet<ProcessId>>();

                        ignore.remove(waiting_on);

                        self.terminate(ignore)
                    }
                    ProcessSignal::Shutdown => self.shutdown(),
                    _ => StateAction::None,
                },
                TransitionState::Starting => match signal {
                    ProcessSignal::Exit(child_pid, reason) => {
                        if Self::do_not_restart(child_pid, reason, policies) {
                            self.do_not_restart.insert(*child_pid);
                            // Remove it from the start list
                            pids_active.retain(|pid| pid != child_pid);
                        }

                        let mut exited = pids_active
                            .iter()
                            .map(|pid| *pid)
                            .collect::<BTreeSet<ProcessId>>()
                            .difference(&self.managed_pids.pid_set())
                            .cloned()
                            .collect::<BTreeSet<ProcessId>>();

                        exited.insert(*child_pid);

                        // Realistically rebooting from the lowest pid will
                        // ensure the most consistent behaviour if it detects a
                        // child exit during the start process.
                        let lowest_pid = std::cmp::min(*child_pid, *lowest_pid);

                        self.restart_rest(&lowest_pid, &exited, Finalise::None)
                    }
                    ProcessSignal::Active(child_pid) => {
                        if child_pid == waiting_on {
                            if let Some(next_pid) = pids_active.pop_front() {
                                *waiting_on = next_pid;
                                StateAction::Start(*waiting_on, Finalise::None)
                            } else {
                                self.finish(None)
                            }
                        } else {
                            tracing::info!(
                                "Task({pid}) Started out of order, removing from list of pids requiring starting",
                                pid = child_pid
                            );

                            pids_active.retain(|pid| pid != child_pid);

                            // We can return nothing because we still have
                            // a pid to wait on if the queue is empty.
                            StateAction::None
                        }
                    }
                    ProcessSignal::Terminate => {
                        let mut ignore = pids_active
                            .iter()
                            .map(|pid| *pid)
                            .collect::<BTreeSet<ProcessId>>()
                            .difference(&self.managed_pids.pid_set())
                            .cloned()
                            .collect::<BTreeSet<ProcessId>>();

                        ignore.remove(waiting_on);

                        self.terminate(ignore)
                    }
                    ProcessSignal::Shutdown => self.shutdown(),
                    _ => StateAction::None,
                },
            },
            StateTransition::Start {
                ref mut pids_active,
                ref mut pids_exited,
                ref mut waiting_on,
            } => match signal {
                ProcessSignal::Active(child_pid) => {
                    if child_pid == waiting_on {
                        if let Some(next_pid) = pids_active.pop_front() {
                            *waiting_on = next_pid;
                            StateAction::Start(*waiting_on, Finalise::None)
                        } else {
                            if pids_exited.is_empty() {
                                self.finish(StateAction::Finalise(Finalise::Start))
                            } else {
                                let pids_exited = pids_exited.clone();

                                match self.config.restart_policy {
                                    SupervisorRestartPolicy::OneForOne => {
                                        self.restart_one(&pids_exited, Finalise::Start)
                                    }
                                    SupervisorRestartPolicy::OneForAll => {
                                        self.restart_all(&pids_exited, Finalise::Start)
                                    }
                                    SupervisorRestartPolicy::RestForOne => {
                                        self.restart_rest(child_pid, &pids_exited, Finalise::Start)
                                    }
                                }
                            }
                        }
                    } else {
                        tracing::info!(
                            "Task({pid}) Started out of order, removing from list of pids requiring starting",
                            pid = child_pid
                        );

                        pids_active.retain(|pid| pid != child_pid);

                        // We can return nothing because we still have
                        // a pid to wait on if the queue is empty.
                        StateAction::None
                    }
                }
                ProcessSignal::Exit(child_pid, reason) => {
                    if Self::do_not_restart(child_pid, reason, policies) {
                        self.do_not_restart.insert(*child_pid);
                        // Remove it from the start list
                        pids_active.retain(|pid| pid != child_pid);
                    }

                    pids_exited.insert(*child_pid);

                    StateAction::None
                }
                _ => StateAction::None,
            },
            StateTransition::Terminate {
                ref mut pids_exit,
                ref mut waiting_on,
            } => match signal {
                ProcessSignal::Exit(child_pid, _) => {
                    if child_pid == waiting_on {
                        tracing::debug!("Received exit from Process({pid})", pid = child_pid);
                        if let Some(next_pid) = pids_exit.pop_front() {
                            *waiting_on = next_pid;
                            StateAction::Terminate(*waiting_on)
                        } else {
                            self.finish(StateAction::Finalise(Finalise::Terminate))
                        }
                    } else {
                        tracing::info!(
                            "Task({pid}) Exited out of order, removing from list of pids requiring termination",
                            pid = child_pid
                        );

                        pids_exit.retain(|pid| pid != child_pid);

                        // We can return nothing because we still have
                        // a pid to wait on if the queue is empty.
                        StateAction::None
                    }
                }
                ProcessSignal::Active(child_pid) => {
                    use sorted_insert::SortedInsert;

                    tracing::info!(
                            "Task({pid}) has become active during Termination, adding to list of pids to terminate",
                            pid = child_pid
                        );

                    pids_exit.sorted_insert_desc(*child_pid);

                    StateAction::None
                }
                _ => StateAction::None,
            },
        }
    }
}
