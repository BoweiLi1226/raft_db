use std::{collections::HashMap, sync::Arc, time::Duration};

use raft_db::raft::{
    raft_config::RaftConfig, raft_node::RaftNode, raft_proto::raft_server::RaftServer,
    raft_service::RaftService, raft_state::Role,
};
use tokio::{sync, time};
use tonic::transport::Server;
use tracing::Level;

const BASE_PORT: u32 = 5000;
const MAX_ATTEMPTS: u32 = 10;

struct TestRaftCluster {
    raft_nodes: HashMap<u32, Arc<RaftNode>>,
    shutdown_signals: HashMap<u32, sync::oneshot::Sender<()>>,
}

impl TestRaftCluster {
    pub fn setup(cluster_size: usize) -> Self {
        let _ = tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .try_init();
        if cluster_size <= 3 || cluster_size >= 21 {
            panic!("Cluster size needs to be between 3 and 21");
        }
        let mut raw_endpoints: HashMap<u32, String> = HashMap::with_capacity(cluster_size);
        for id in 1..=cluster_size {
            raw_endpoints.insert(id as u32, format!("127.0.0.1:{}", id as u32 + BASE_PORT));
        }

        let mut raft_nodes: HashMap<u32, Arc<RaftNode>> = HashMap::with_capacity(cluster_size);
        let mut shutdown_signals: HashMap<u32, sync::oneshot::Sender<()>> =
            HashMap::with_capacity(cluster_size);

        for id in 1..=cluster_size {
            let raft_config = RaftConfig::new(id as u32, raw_endpoints.clone());
            let addr = raft_config.nodes[&(id as u32)];
            let raft_node = RaftNode::new(raft_config);
            let node = Arc::clone(&raft_node);
            let raft_server = RaftServer::new(RaftService::from(node));
            let (tx, rx) = sync::oneshot::channel();
            shutdown_signals.insert(id as u32, tx);

            tokio::spawn(async move {
                let server = Server::builder()
                    .add_service(raft_server)
                    .serve_with_shutdown(addr, async {
                        let _ = rx.await;
                    })
                    .await;
                if server.is_err() {
                    panic!("Raft node {id}: I cannot start server");
                }
            });
            raft_nodes.insert(id as u32, raft_node);
        }
        Self {
            raft_nodes,
            shutdown_signals,
        }
    }

    pub fn shutdown_node(&mut self, id: u32) {
        if let Some(tx) = self.shutdown_signals.remove(&id) {
            let _ = tx.send(());
        }
    }
}

#[tokio::test]
async fn test_initial_election() {
    let cluster = TestRaftCluster::setup(5);

    let Ok((first_leader_id, _)) = wait_for_leader(&cluster).await else {
        panic!("No leader elected for Raft cluster");
    };

    time::sleep(Duration::from_millis(600)).await;

    let Ok((second_leader_id, _)) = wait_for_leader(&cluster).await else {
        panic!("No leader elected for Raft cluster");
    };
    assert_eq!(first_leader_id, second_leader_id);
}

#[tokio::test]
async fn test_election_after_leader_down() {
    let mut cluster = TestRaftCluster::setup(5);

    let Ok((leader_id, _)) = wait_for_leader(&cluster).await else {
        panic!("No leader elected for Raft cluster");
    };

    cluster.shutdown_node(leader_id);

    if let Err(error) = wait_for_leader(&cluster).await {
        panic!("{error}");
    }
}

async fn wait_for_leader(cluster: &TestRaftCluster) -> Result<(u32, u64), &'static str> {
    let mut retry = 0;
    while retry < MAX_ATTEMPTS {
        time::sleep(Duration::from_millis(300)).await;
        let mut leader_count = 0;
        let mut leader_id = 0;
        let mut leader_term = 0;
        for node in cluster.raft_nodes.values() {
            let node = Arc::clone(node);
            let (role, id, term) = get_state(node).await;
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
            return Ok((leader_id, leader_term));
        }
        retry += 1;
    }
    Err("Timeout waiting for a leader")
}

async fn get_state(raft_node: Arc<RaftNode>) -> (Role, u32, u64) {
    let state = raft_node.state.lock().await;
    (state.role, raft_node.config.me, state.term)
}
