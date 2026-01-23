pub mod raft_proto {
    tonic::include_proto!("raft");
}
pub mod raft_node;

pub use raft_proto::raft_server::Raft;
pub use raft_proto::{
    AppendEntriesArgs, AppendEntriesReply, LogEntry, RequestVoteArgs, RequestVoteReply,
};
