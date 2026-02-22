use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::time::Instant;

use crate::raft::{LogEntry, raft_config::RaftConfig};

#[derive(Default, PartialEq, Eq, Debug, Clone, Copy)]
pub enum Role {
    #[default]
    FOLLOWER,
    CANDIDATE,
    LEADER,
}

#[derive(Debug)]
pub struct RaftState {
    pub term: u64,
    pub voted_for: Option<u32>,
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub next_indices: Option<HashMap<u32, u64>>,
    pub match_indices: Option<HashMap<u32, u64>>,
    pub role: Role,
    pub last_tick: Instant,

    raft_config: Arc<RaftConfig>,
}

impl RaftState {
    pub fn new(raft_config: Arc<RaftConfig>) -> Self {
        Self {
            term: 0,
            voted_for: None,
            log: vec![LogEntry {
                term: 0,
                command: "".into(),
            }],
            commit_index: 0,
            last_applied: 0,
            next_indices: None,
            match_indices: None,
            role: Role::FOLLOWER,
            last_tick: Instant::now(),
            raft_config,
        }
    }

    pub fn reset_tick(&mut self) {
        self.last_tick = Instant::now();
    }

    pub fn timeout(&self, duration: Duration) -> bool {
        self.last_tick.elapsed() >= duration
    }

    pub fn become_follower(&mut self, term: u64) {
        if term >= self.term {
            self.role = Role::FOLLOWER;
            self.next_indices = None;
            self.match_indices = None;
            if term > self.term {
                self.term = term;
                self.voted_for = None;
            }
            self.reset_tick();
        }
    }

    pub fn become_candidate(&mut self) {
        self.role = Role::CANDIDATE;
        self.term += 1;
        self.voted_for = Some(self.raft_config.me);
        self.match_indices = None;
        self.next_indices = None;
        self.reset_tick();
        tracing::info!(
            "Raft node {}: I started election at term {}",
            self.raft_config.me,
            self.term
        );
    }

    pub fn become_leader(&mut self) {
        self.role = Role::LEADER;
        let cluster_size = self.raft_config.get_number_of_nodes();
        let log_size = self.log.len() as u64;
        let mut next_indices_map = HashMap::with_capacity(cluster_size);
        let mut match_indices_map = HashMap::with_capacity(cluster_size);
        for &id in &self.raft_config.get_peer_ids() {
            next_indices_map.insert(id, log_size);
            match_indices_map.insert(id, 0);
        }
        self.next_indices = Some(next_indices_map);
        self.match_indices = Some(match_indices_map);
        tracing::info!(
            "Raft node {}: I won the election and become leader at term {}",
            self.raft_config.me,
            self.term
        );
    }

    pub fn is_other_node_log_up_to_date(
        &self,
        last_log_term_other_node: u64,
        last_log_index_other_node: u64,
    ) -> bool {
        let Some(last_log) = self.log.last() else {
            panic!(
                "Raft Node {}: I do not have any log which means I am not initialized in a correct way",
                self.raft_config.me,
            );
        };

        last_log_term_other_node > last_log.term
            || (last_log_term_other_node == last_log.term
                && last_log_index_other_node >= (self.log.len() - 1) as u64)
    }

    pub fn contains_prev_log(&self, prev_log_index: u64, prev_log_term: u64) -> bool {
        if let Some(log) = self.log.get(prev_log_index as usize) {
            prev_log_term == log.term
        } else {
            false
        }
    }

    pub fn append_log(&mut self, prev_log_index: u64, log_entries: &[LogEntry]) {
        let start_index = (prev_log_index + 1) as usize;
        let mut index_to_append_from = 0;
        let mut conflict_found = false;
        for (idx, new_entry) in log_entries.iter().enumerate() {
            let target_idx = start_index + idx;
            if target_idx < self.log.len() {
                if self.log[target_idx].term != new_entry.term {
                    self.log.truncate(target_idx);
                    index_to_append_from = idx;
                    conflict_found = true;
                    break;
                }
            } else {
                index_to_append_from = idx;
                conflict_found = true;
                break;
            }
        }
        if conflict_found {
            self.log
                .extend_from_slice(&log_entries[index_to_append_from..]);
        }
    }

    pub fn update_commit(&mut self, leader_commit: u64) {
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min((self.log.len() - 1) as u64);
        }
    }
}
