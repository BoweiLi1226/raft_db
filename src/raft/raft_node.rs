use std::{sync::Arc, time::Duration};
use tokio::{
    select,
    sync::{Mutex, Notify},
    time,
};
use tonic::{Request, Response, Status};

use crate::raft::{
    AppendEntriesArgs, AppendEntriesReply, Raft, RequestVoteArgs, RequestVoteReply,
    raft_config::RaftConfig,
    raft_state::{RaftState, Role},
};

#[derive(Debug)]
pub struct RaftNode {
    pub config: RaftConfig,
    pub state: Mutex<RaftState>,
    notify_heartbeat: Notify,
}

impl RaftNode {
    pub fn from_config(config: RaftConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            state: Mutex::new(RaftState::new()),
            notify_heartbeat: Notify::new(),
        })
    }

    pub fn run(self: &Arc<Self>) {
        let raft_node = self.clone();
        tokio::spawn(async move { raft_node.election_ticker().await });
    }

    async fn election_ticker(&self) {
        loop {
            let timeout = Duration::from_millis(rand::random_range(150..=300) + 300);
            let sleep = time::sleep(timeout);
            tokio::pin!(sleep);
            select! {
                _ = &mut sleep => {
                    let should_elect = {
                        let state = self.state.lock().await;
                        state.role != Role::LEADER && state.last_tick.elapsed() >= timeout
                    };
                    if should_elect {
                        self.election().await;
                    }
                }
                _ = self.notify_heartbeat.notified() => {}
            }
        }
    }

    async fn election(&self) {
        tracing::info!("Raft node {}: I started election.", self.config.me);
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
            self.notify_heartbeat.notify_one();
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
            self.notify_heartbeat.notify_one();
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
