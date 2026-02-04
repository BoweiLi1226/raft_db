use std::{collections::HashMap, sync::Arc, time::Duration};

use raft_db::raft::{
    raft_config::RaftConfig, raft_node::RaftNode, raft_proto::raft_server::RaftServer,
    raft_service::RaftService, raft_state::Role,
};
use tokio::time;
use tonic::transport::Server;
use tracing::Level;

const BASE_PORT: u32 = 5000;

struct TestRaftCluster {
    raft_nodes: HashMap<u32, Arc<RaftNode>>,
}

impl TestRaftCluster {
    pub fn new(cluster_size: usize) -> Self {
        if cluster_size <= 3 || cluster_size >= 21 {
            panic!("Cluster size needs to be between 3 and 21");
        }
        let mut raw_endpoints: HashMap<u32, String> = HashMap::with_capacity(cluster_size);
        for id in 1..=cluster_size {
            raw_endpoints.insert(id as u32, format!("127.0.0.1:{}", id as u32 + BASE_PORT));
        }
        let mut raft_nodes: HashMap<u32, Arc<RaftNode>> = HashMap::with_capacity(cluster_size);
        for id in 1..=cluster_size {
            let raft_config = RaftConfig::new(id as u32, raw_endpoints.clone());
            let addr = raft_config.nodes[&(id as u32)];
            let raft_node = RaftNode::new(raft_config);
            let node = Arc::clone(&raft_node);
            let raft_server = RaftServer::new(RaftService::from(node));
            tokio::spawn(
                async move { Server::builder().add_service(raft_server).serve(addr).await },
            );
            raft_nodes.insert(id as u32, raft_node);
        }
        Self { raft_nodes }
    }
}

#[tokio::test]
async fn test_initial_election() {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cluster = TestRaftCluster::new(5);

    loop {
        time::sleep(Duration::from_millis(100)).await;
        let mut leader_count = 0;
        let mut leader_id = 0;
        let mut leader_term = 0;
        for node in cluster.raft_nodes.values() {
            let node = Arc::clone(node);
            let (role, id, term) = node.get_state().await;
            if role == Role::LEADER {
                leader_id = id;
                leader_term = term;
                leader_count += 1;
            }
        }
        assert!(leader_count <= 1);
        if leader_count == 1 {
            tracing::info!(
                "Raft node {} became leader at term {}",
                leader_id,
                leader_term
            );
            time::sleep(Duration::from_millis(300)).await;
            let (role, id, term) = cluster.raft_nodes[&leader_id].clone().get_state().await;
            assert!(role == Role::LEADER && id == leader_id && term == leader_term);
            tracing::info!(
                "Raft node {} remains leader at term {}",
                leader_id,
                leader_term
            );
            break;
        }
    }
}
