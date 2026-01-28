use std::{collections::HashMap, time::Duration};

use tokio::time::Instant;

use crate::raft::LogEntry;

#[derive(Default, PartialEq, Eq, Debug, Clone)]
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

    me: u32,
    peer_ids: Vec<u32>,
}

impl RaftState {
    pub fn with_self_and_peer_ids(me: u32, peer_ids: Vec<u32>) -> Self {
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

            me,
            peer_ids,
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
        self.voted_for = Some(self.me);
        self.match_indices = None;
        self.next_indices = None;
        self.reset_tick();
        tracing::info!(
            "Raft node {}: I started election at term {}",
            self.me,
            self.term
        );
    }

    pub fn become_leader(&mut self) {
        self.role = Role::LEADER;
        let cluster_size = self.peer_ids.len() + 1;
        let log_size = self.log.len() as u64;
        let mut next_indices_map = HashMap::with_capacity(cluster_size);
        let mut match_indices_map = HashMap::with_capacity(cluster_size);
        for &id in &self.peer_ids {
            next_indices_map.insert(id, log_size);
            match_indices_map.insert(id, 0);
        }
        self.next_indices = Some(next_indices_map);
        self.match_indices = Some(match_indices_map);
        tracing::info!(
            "Raft node {}: I won the election and become leader at term {}",
            self.me,
            self.term
        );
    }
}
