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
        reply.vote_granted = false;
        // reject vote if received request from node with lower term
        if args.term < state.term {
            return Ok(Response::new(reply));
        } else {
            state.become_follower(args.term);
        }
        // reject if log of the other node is not up to date
        if !state.is_other_node_log_up_to_date(args.last_log_term, args.last_log_index) {
            return Ok(Response::new(reply));
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
        reply.success = false;

        // heartbeat comes from invalid leader
        if args.term < state.term {
            return Ok(Response::new(reply));
        }

        // heartbeat comes from valid leader from now on
        state.reset_tick();
        if (args.term > state.term) || (args.term == state.term && state.role != Role::FOLLOWER) {
            state.become_follower(args.term);
        }

        if !state.contains_prev_log(args.prev_log_index, args.prev_log_term) {
            return Ok(Response::new(reply));
        }
        state.append_log(args.prev_log_index, &args.entries);
        state.update_commit(args.leader_commit);
        reply.success = true;
        Ok(Response::new(reply))
    }
}
