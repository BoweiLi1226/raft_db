use crate::raft::LogEntry;

#[derive(Default, PartialEq, Debug, Clone)]
pub enum Role {
    #[default]
    FOLLOWER,
    CANDIDATE,
    LEADER,
}

#[derive(Default, Debug)]
pub struct RaftState {
    pub term: u64,
    pub voted_for: Option<u32>,
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub next_indices: Option<Vec<u64>>,
    pub match_indices: Option<Vec<u64>>,
    pub role: Role,
}

impl RaftState {
    pub fn new() -> Self {
        Self::default()
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
        }
    }
}
