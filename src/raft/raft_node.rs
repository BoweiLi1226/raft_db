use std::{
    collections::HashMap,
    sync::{Arc, atomic},
    time::Duration,
};

use tokio::{
    sync::{Mutex, Notify},
    task::JoinSet,
    time,
};
use tonic::{Request, transport::Channel};

use crate::raft::{
    AppendEntriesArgs, RequestVoteArgs,
    raft_config::RaftConfig,
    raft_proto::raft_client::RaftClient,
    raft_state::{RaftState, Role},
};

#[derive(Debug)]
pub struct RaftNode {
    pub config: RaftConfig,
    pub state: Mutex<RaftState>,
    peer_clients: HashMap<u32, RaftClient<Channel>>,
}

impl From<RaftConfig> for RaftNode {
    fn from(raft_config: RaftConfig) -> Self {
        let peer_clients = raft_config.make_clients();
        let me = raft_config.me;
        let peer_ids: Vec<u32> = raft_config.nodes.iter().map(|(&id, _)| id).collect();
        Self {
            config: raft_config,
            state: Mutex::new(RaftState::with_self_and_peer_ids(me, peer_ids)),
            peer_clients,
        }
    }
}

impl RaftNode {
    pub fn start_background_tasks(node: Arc<Self>) {
        let raft_node = Arc::clone(&node);
        tokio::spawn(async move { raft_node.election_ticker().await });

        let raft_node = Arc::clone(&node);
        tokio::spawn(async move { raft_node.heartbeat_ticker().await });
    }

    async fn election_ticker(self: Arc<Self>) {
        // TODO: Pending Improvement?
        // Right now if last_tick gets reset right after sleep starts,
        // should_elect would be false.
        // Not sure if this should be the case though.
        loop {
            let timeout = Duration::from_millis(rand::random_range(350..=600));
            time::sleep(timeout).await;
            let should_elect = {
                let state = self.state.lock().await;
                state.role != Role::LEADER && state.timeout(timeout)
            };
            if should_elect {
                Arc::clone(&self).election().await;
            }
        }
    }

    async fn election(self: Arc<Self>) {
        let (args, current_term) = {
            let mut state = self.state.lock().await;
            state.become_candidate();
            let Some(last_log) = state.log.last() else {
                panic!(
                    "Raft node {}: I should have had at least one dummy log entry",
                    self.config.me,
                );
            };
            (
                RequestVoteArgs {
                    term: state.term,
                    candidate_id: self.config.me,
                    last_log_index: (state.log.len() - 1) as u64,
                    last_log_term: last_log.term,
                },
                state.term,
            )
        };

        let notify_on_vote = Arc::new(Notify::new());

        // voted for self
        let vote_obtained = Arc::new(atomic::AtomicU32::new(1));
        let vote_needed = (self.config.nodes.len() / 2) as u32;

        // rpc calls
        let mut tasks = JoinSet::new();
        for client in self.peer_clients.values() {
            let request = Request::new(args);
            let mut client = client.clone();
            let vote_obtained = Arc::clone(&vote_obtained);
            let notify_on_vote = Arc::clone(&notify_on_vote);
            let raft_node = Arc::clone(&self);
            tasks.spawn(async move {
                if let Ok(response) = client.request_vote(request).await {
                    let reply = response.into_inner();
                    if reply.vote_granted {
                        let current_vote = vote_obtained.fetch_add(1, atomic::Ordering::SeqCst) + 1;
                        if current_vote > vote_needed {
                            notify_on_vote.notify_one();
                        }
                    } else {
                        let mut state = raft_node.state.lock().await;
                        if reply.term > state.term {
                            state.become_follower(reply.term);
                        }
                    }
                }
            });
        }

        tokio::select! {
            _ = notify_on_vote.notified() => {
                let mut state = self.state.lock().await;
                if state.term == current_term && state.role == Role::CANDIDATE {
                    state.become_leader();
                } else {
                    return;
                }
            }
            _ = time::sleep(Duration::from_millis(300)) => {}
        }
        tasks.abort_all();
    }

    async fn heartbeat_ticker(self: Arc<Self>) {
        loop {
            time::sleep(Duration::from_millis(100)).await;
            let should_send_heartbeat = {
                let state = self.state.lock().await;
                state.role == Role::LEADER
            };
            if should_send_heartbeat {
                Arc::clone(&self).heartbeat().await;
            }
        }
    }

    async fn heartbeat(self: Arc<Self>) {
        let mut tasks = JoinSet::new();
        for client in self.peer_clients.values() {
            let args = {
                let state = self.state.lock().await;
                AppendEntriesArgs {
                    term: state.term,
                    leader_id: self.config.me,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: Vec::new(),
                    leader_commit: 0,
                }
            };
            let request = Request::new(args);
            let mut client = client.clone();
            let raft_node = Arc::clone(&self);
            tasks.spawn(async move {
                if let Ok(response) = client.append_entries(request).await {
                    let reply = response.into_inner();
                    let mut state = raft_node.state.lock().await;
                    if reply.term > state.term {
                        state.become_follower(reply.term);
                    }
                }
            });
        }
        time::timeout(Duration::from_millis(100), tasks.join_all())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "Raft node {}: Error when waiting for result of heartbeat",
                    self.config.me
                )
            });
    }
}
