use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::{
    raft::{
        AppendEntriesArgs, AppendEntriesReply, Raft, RequestVoteArgs, RequestVoteReply,
        raft_node::RaftNode,
    },
    state_machine::StateMachine,
};

pub struct RaftService<T: StateMachine> {
    raft_node: Arc<RaftNode<T>>,
}

impl<T: StateMachine> From<Arc<RaftNode<T>>> for RaftService<T> {
    fn from(raft_node: Arc<RaftNode<T>>) -> Self {
        Self { raft_node }
    }
}

#[tonic::async_trait]
impl<T: StateMachine> Raft for RaftService<T> {
    async fn request_vote(
        &self,
        args: Request<RequestVoteArgs>,
    ) -> anyhow::Result<Response<RequestVoteReply>, Status> {
        Ok(Response::new(
            self.raft_node.handle_request_vote(args.into_inner()).await,
        ))
    }

    async fn append_entries(
        &self,
        args: Request<AppendEntriesArgs>,
    ) -> anyhow::Result<Response<AppendEntriesReply>, tonic::Status> {
        Ok(Response::new(
            self.raft_node
                .handle_append_entries(args.into_inner())
                .await,
        ))
    }
}
