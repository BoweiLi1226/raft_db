use core::panic;
use futures::FutureExt;
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
use tonic::{Request, Response, Status, transport::Channel};

use crate::raft::{
    AppendEntriesArgs, AppendEntriesReply, Raft, RequestVoteArgs, RequestVoteReply,
    raft_config::RaftConfig,
    raft_proto::raft_client::RaftClient,
    raft_state::{RaftState, Role},
};

#[derive(Debug)]
pub struct RaftNode {
    config: RaftConfig,
    state: Mutex<RaftState>,
    peer_clients: HashMap<u32, RaftClient<Channel>>,
    notify_election_vote: Arc<Notify>,
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
            notify_election_vote: Arc::new(Notify::new()),
        }
    }
}

impl RaftNode {
    pub fn start_background_tasks(node: &Arc<Self>) {
        let raft_node = Arc::clone(node);
        tokio::spawn(async move { raft_node.election_ticker().await });

        let raft_node = Arc::clone(node);
        tokio::spawn(async move { raft_node.heartbeat_ticker().await });
    }

    async fn election_ticker(&self) {
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
                self.election().await;
            }
        }
    }

    async fn election(&self) {
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

        // consume the notification channel
        self.notify_election_vote.notified().now_or_never();

        // voted for self
        let vote_obtained = Arc::new(atomic::AtomicU32::new(1));
        let vote_needed = (self.config.nodes.len() / 2) as u32;

        // rpc calls
        let mut tasks = JoinSet::new();
        for client in self.peer_clients.values() {
            let request = Request::new(args);
            let mut client = client.clone();
            let vote_obtained = Arc::clone(&vote_obtained);
            let notify_election_vote = Arc::clone(&self.notify_election_vote);
            tasks.spawn(async move {
                if let Ok(response) = client.request_vote(request).await {
                    let reply = response.into_inner();
                    if reply.vote_granted {
                        let current_vote = vote_obtained.fetch_add(1, atomic::Ordering::SeqCst) + 1;
                        if current_vote > vote_needed {
                            notify_election_vote.notify_one();
                        }
                    }
                }
            });
        }

        // TODO: Make sure to handle the case when node receives higher term when
        // collecting votes
        let sleep = time::sleep(Duration::from_millis(300));
        tokio::pin!(sleep);
        tokio::select! {
            _ = self.notify_election_vote.notified() => {
                let mut state = self.state.lock().await;
                if state.term == current_term && state.role == Role::CANDIDATE {
                    state.become_leader();
                } else {
                    return;
                }
            }
            _ = &mut sleep => {}
        }
        tasks.abort_all();
    }

    async fn heartbeat_ticker(&self) {
        loop {
            time::sleep(Duration::from_millis(100)).await;
            let should_send_heartbeat = {
                let state = self.state.lock().await;
                state.role == Role::LEADER
            };
            if should_send_heartbeat {
                self.heartbeat().await;
            }
        }
    }

    async fn heartbeat(&self) {
        todo!("To be implemented")
    }
}

#[tonic::async_trait]
impl Raft for RaftNode {
    async fn request_vote(
        &self,
        args: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let mut reply = RequestVoteReply::default();
        let args = args.into_inner();

        let mut state = self.state.lock().await;
        reply.term = state.term;

        if args.term < state.term {
            reply.vote_granted = false;
            return Ok(Response::new(reply));
        }

        if args.term >= state.term {
            state.become_follower(args.term);
        }

        match state.voted_for {
            None => {
                reply.vote_granted = true;
                state.voted_for = Option::Some(args.candidate_id);
                tracing::info!(
                    "Raft node {}: I voted for {} in term {}.",
                    self.config.me,
                    args.candidate_id,
                    state.term,
                );
            }
            Some(id) if id == args.candidate_id => {
                reply.vote_granted = true;
                tracing::info!(
                    "Raft node {}: I already voted for {} in term {}.",
                    self.config.me,
                    args.candidate_id,
                    state.term,
                );
            }
            Some(already_voted_for) => {
                tracing::info!(
                    "Raft node {}: I refused to vote for node {} in term {} because I already voted for {}.",
                    self.config.me,
                    args.candidate_id,
                    state.term,
                    already_voted_for,
                );
            }
        }

        //TODO: Check Log Completeness

        Ok(Response::new(reply))
    }

    async fn append_entries(
        &self,
        args: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, tonic::Status> {
        let mut reply = AppendEntriesReply::default();
        let args = args.into_inner();

        let mut state = self.state.lock().await;
        reply.term = state.term;

        if (args.term > state.term) || (args.term == state.term && state.role != Role::LEADER) {
            state.become_follower(args.term);
        }

        //TODO: Check Log Completeness

        reply.success = false;

        Ok(Response::new(reply))
    }
}

#[tonic::async_trait]
impl Raft for Arc<RaftNode> {
    async fn request_vote(
        &self,
        args: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        self.as_ref().request_vote(args).await
    }

    async fn append_entries(
        &self,
        args: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, tonic::Status> {
        self.as_ref().append_entries(args).await
    }
}
