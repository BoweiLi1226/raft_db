use tokio::time::Instant;

use crate::raft::LogEntry;

#[derive(Default, PartialEq, Debug, Clone)]
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
    pub next_indices: Option<Vec<u64>>,
    pub match_indices: Option<Vec<u64>>,
    pub role: Role,
    pub last_tick: Instant,
}

impl Default for RaftState {
    fn default() -> Self {
        Self {
            term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_indices: None,
            match_indices: None,
            role: Role::FOLLOWER,
            last_tick: Instant::now(),
        }
    }
}

impl RaftState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset_tick(&mut self) {
        self.last_tick = Instant::now();
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
}
