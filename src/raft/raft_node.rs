use core::panic;
use std::{
    collections::HashMap,
    sync::{Arc, atomic},
    time::Duration,
};
use tokio::{sync::Mutex, task::JoinSet, time};
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
}

impl RaftNode {
    pub fn from_config(config: RaftConfig) -> Arc<Self> {
        let peer_clients = config.make_clients();
        let me = config.me;
        let peer_ids: Vec<u32> = config.nodes.iter().map(|(&id, _)| id).collect();
        Arc::new(Self {
            config,
            state: Mutex::new(RaftState::with_self_and_peer_ids(me, peer_ids)),
            peer_clients,
        })
    }

    pub fn start_background_tasks(node: &Arc<Self>) {
        let raft_node = node.clone();
        tokio::spawn(async move { raft_node.election_ticker().await });

        let raft_node = node.clone();
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

    async fn heartbeat_ticker(&self) {
        todo!("We can implement using notifier");
    }

    async fn election(&self) {
        // TODO: add election timeout logic
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

        // voted for self
        let vote_obtained = Arc::new(atomic::AtomicU32::new(1));

        // rpc calls
        let mut tasks = JoinSet::new();
        for client in self.peer_clients.values() {
            let request = Request::new(args);
            let mut client = client.clone();
            let vote_obtained_copy = vote_obtained.clone();
            tasks.spawn(async move {
                if let Ok(response) = client.request_vote(request).await {
                    let reply = response.into_inner();
                    if reply.vote_granted {
                        vote_obtained_copy.fetch_add(1, atomic::Ordering::SeqCst);
                    }
                }
            });
        }

        // TODO: I think we could potentially change the logic to notify on votes instead of join
        // tasks
        // TODO: Make sure to handle the case when node receives higher term
        // collect votes
        while tasks.join_next().await.is_some() {
            let vote_obtained = vote_obtained.load(atomic::Ordering::SeqCst);
            if vote_obtained > (self.config.nodes.len() / 2) as u32 {
                tasks.abort_all();
            } else {
                continue;
            }
            {
                let mut state = self.state.lock().await;
                if state.term == current_term && state.role == Role::CANDIDATE {
                    state.become_leader();
                    // TODO: Need to notify heartbeat_ticker
                } else {
                    return;
                }
            }
        }
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

        if state.voted_for.is_none() || state.voted_for == Some(args.candidate_id) {
            reply.vote_granted = true;
            state.voted_for = Option::Some(args.candidate_id);
            tracing::info!(
                "Raft node {}: I voted (or already voted) for {} in term {}.",
                self.config.me,
                args.candidate_id,
                state.term,
            );
        } else {
            tracing::info!(
                "Raft node {}: I refused to vote for node {} in term {} because I already voted for {}.",
                self.config.me,
                args.candidate_id,
                state.term,
                state.voted_for.unwrap(),
            );
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
