use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::raft::{
    AppendEntriesArgs, AppendEntriesReply, Raft, RequestVoteArgs, RequestVoteReply,
    raft_state::{RaftState, Role},
};

#[derive(Debug, Clone)]
pub struct RaftNode {
    pub id: u32,
    pub state: Arc<Mutex<RaftState>>,
}

impl RaftNode {
    pub fn with_id(id: u32) -> Self {
        Self {
            id,
            state: Arc::new(Mutex::new(RaftState::new())),
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
        }

        if args.term >= state.term {
            state.become_follower(args.term);
        }

        if state.voted_for.is_none() || state.voted_for == Some(args.candidate_id) {
            reply.vote_granted = true;
            state.voted_for = Option::Some(args.candidate_id);
            tracing::info!(
                "Raft node {}: I voted (or already voted) for {} in term {}.",
                self.id,
                args.candidate_id,
                state.term,
            );
        } else {
            tracing::info!(
                "Raft node {}: I refused to vote for node {} in term {} because I already voted for {}.",
                self.id,
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
