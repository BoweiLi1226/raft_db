use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::raft::{
    AppendEntriesArgs, AppendEntriesReply, Raft, RequestVoteArgs, RequestVoteReply,
    raft_node::RaftNode, raft_state::Role,
};

pub struct RaftService {
    raft_node: Arc<RaftNode>,
}

impl From<Arc<RaftNode>> for RaftService {
    fn from(raft_node: Arc<RaftNode>) -> Self {
        Self { raft_node }
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn request_vote(
        &self,
        args: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let mut reply = RequestVoteReply::default();
        let args = args.into_inner();

        let mut state = self.raft_node.state.lock().await;
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
                    self.raft_node.config.me,
                    args.candidate_id,
                    state.term,
                );
            }
            Some(id) if id == args.candidate_id => {
                reply.vote_granted = true;
                tracing::info!(
                    "Raft node {}: I already voted for {} in term {}.",
                    self.raft_node.config.me,
                    args.candidate_id,
                    state.term,
                );
            }
            Some(already_voted_for) => {
                tracing::info!(
                    "Raft node {}: I refused to vote for node {} in term {} because I already voted for {}.",
                    self.raft_node.config.me,
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

        let mut state = self.raft_node.state.lock().await;
        reply.term = state.term;

        if (args.term > state.term) || (args.term == state.term && state.role != Role::LEADER) {
            state.become_follower(args.term);
        }

        //TODO: Check Log Completeness

        reply.success = false;

        Ok(Response::new(reply))
    }
}
