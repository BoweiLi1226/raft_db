use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::raft::{
    AppendEntriesArgs, AppendEntriesReply, Raft, RequestVoteArgs, RequestVoteReply,
    raft_node::RaftNode,
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
        Ok(Response::new(
            self.raft_node.handle_request_vote(args.into_inner()).await,
        ))
    }

    async fn append_entries(
        &self,
        args: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, tonic::Status> {
        Ok(Response::new(
            self.raft_node
                .handle_append_entries(args.into_inner())
                .await,
        ))
    }
}

