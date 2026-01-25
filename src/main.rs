use std::{collections::HashMap, env};

use raft_db::raft::{
    raft_config::RaftConfig, raft_node::RaftNode, raft_proto::raft_server::RaftServer,
};
use tonic::transport::Server;
use tracing::Level;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let args: Vec<String> = env::args().collect();
    let me = args
        .get(1)
        .expect("Invalid Argument: You should use cargo run -- <ID>")
        .parse::<u32>()?;
    let mut endpoints = HashMap::<u32, String>::with_capacity(3);
    endpoints.insert(1, String::from("127.0.0.1:5001"));
    endpoints.insert(2, String::from("127.0.0.1:5002"));
    endpoints.insert(3, String::from("127.0.0.1:5003"));
    let raft_config = RaftConfig::new(me, endpoints);
    let addr = raft_config.nodes[&me];
    let raft_node = RaftNode::from_config(raft_config);
    RaftNode::spawn(&raft_node);
    let raft_server = RaftServer::new(raft_node);
    tracing::info!("Starting server {} on port {:?}", me, addr);
    Server::builder()
        .add_service(raft_server)
        .serve(addr)
        .await?;
    Ok(())
}
