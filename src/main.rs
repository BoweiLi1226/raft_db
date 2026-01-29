use std::{collections::HashMap, sync::Arc};

use clap::Parser;
use raft_db::raft::{
    raft_config::RaftConfig, raft_node::RaftNode, raft_proto::raft_server::RaftServer,
};
use tonic::transport::Server;
use tracing::Level;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    id: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let args = Args::parse();
    let config = std::fs::read_to_string("config.json")?;
    let endpoints = serde_json::from_str::<HashMap<u32, String>>(&config)?;
    if !endpoints.contains_key(&args.id) {
        return Err(format!("Invalid Raft node id {}", args.id).into());
    }
    let raft_config = RaftConfig::new(args.id, endpoints);
    let addr = raft_config.nodes[&args.id];
    let raft_node = Arc::new(RaftNode::from(raft_config));
    RaftNode::start_background_tasks(&raft_node);
    let raft_server = RaftServer::new(raft_node);
    tracing::info!("Starting server {} on port {:?}", args.id, addr);
    Server::builder()
        .add_service(raft_server)
        .serve(addr)
        .await?;
    Ok(())
}
