use tonic::{Request, Response, Status};

use crate::raft::{
    AppendEntriesArgs, AppendEntriesReply, LogEntry, Raft, RequestVoteArgs, RequestVoteReply,
};

#[derive(Default, Debug, Clone)]
pub enum Role {
    #[default]
    FOLLOWER,
    CANDIDATE,
    LEADER,
}

#[derive(Default, Debug, Clone)]
pub struct RaftNode {
    term: u64,
    voted_for: u32,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    next_indices: Vec<u64>,
    match_indices: Vec<u64>,
    role: Role,
}

#[tonic::async_trait]
impl Raft for RaftNode {
    async fn request_vote(
        &self,
        args: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let args = args.into_inner();
        Ok(Response::new(RequestVoteReply::default()))
    }

    async fn append_entries(
        &self,
        args: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, tonic::Status> {
        let args = args.into_inner();
        Ok(Response::new(AppendEntriesReply::default()))
    }
}
