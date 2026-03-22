use std::{collections::HashMap, sync::Arc};

use clap::Parser;
use quorum::raft::{
    raft_config::RaftConfig, raft_node::RaftNode, raft_proto::raft_server::RaftServer,
    raft_service::RaftService,
};
use quorum::storage::shared_kv_store::SharedKVStore;
use tonic::transport::Server;
use tracing::Level;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    id: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_ansi(true)
        .init();
    let args = Args::parse();
    let config = std::fs::read_to_string("config.json")?;
    let endpoints = serde_json::from_str::<HashMap<u32, String>>(&config)?;
    if !endpoints.contains_key(&args.id) {
        return Err(anyhow::anyhow!("Invalid Raft node id {}", args.id));
    }
    let raft_config = RaftConfig::new(args.id, endpoints);
    let addr = raft_config.nodes[&args.id];
    let shared_kv_store = SharedKVStore::new();
    let raft_node = RaftNode::<SharedKVStore>::new(Arc::new(raft_config), shared_kv_store);
    let raft_server = RaftServer::new(RaftService::from(raft_node));
    tracing::info!("Starting server {} on port {:?}", args.id, addr);
    Server::builder()
        .add_service(raft_server)
        .serve(addr)
        .await?;
    Ok(())
}
