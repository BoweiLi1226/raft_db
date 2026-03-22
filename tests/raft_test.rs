use std::{collections::HashMap, sync::Arc, time::Duration};

use quorum::{
    raft::{
        raft_config::RaftConfig, raft_node::RaftNode, raft_proto::raft_server::RaftServer,
        raft_service::RaftService, raft_state::Role,
    },
    storage::{
        shared_kv_store::SharedKVStore,
        utils::{Command, CommandResponse},
    },
};
use tokio::{sync, time};
use tonic::transport::Server;
use tracing::Level;

const MAX_ATTEMPTS: u32 = 10;

struct TestRaftCluster {
    raft_nodes: HashMap<u32, Arc<RaftNode<SharedKVStore>>>,
    shutdown_signals: HashMap<u32, sync::oneshot::Sender<()>>,
}

impl TestRaftCluster {
    pub fn setup(cluster_size: usize, base_port: u32) -> Self {
        let _ = tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .with_thread_names(true)
            .with_ansi(true)
            .try_init();
        if cluster_size <= 3 || cluster_size >= 21 {
            panic!("Cluster size needs to be between 3 and 21");
        }
        let mut raw_endpoints: HashMap<u32, String> = HashMap::with_capacity(cluster_size);
        for id in 1..=cluster_size {
            raw_endpoints.insert(id as u32, format!("127.0.0.1:{}", id as u32 + base_port));
        }

        let mut raft_nodes: HashMap<u32, Arc<RaftNode<SharedKVStore>>> =
            HashMap::with_capacity(cluster_size);
        let mut shutdown_signals: HashMap<u32, sync::oneshot::Sender<()>> =
            HashMap::with_capacity(cluster_size);

        for id in 1..=cluster_size {
            let raft_config = RaftConfig::new(id as u32, raw_endpoints.clone());
            let addr = raft_config.nodes[&(id as u32)];
            let raft_node =
                RaftNode::<SharedKVStore>::new(Arc::new(raft_config), SharedKVStore::new());
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
    let cluster = TestRaftCluster::setup(5, 5000);

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
    let mut cluster = TestRaftCluster::setup(5, 5005);

    let Ok((leader_id, _)) = wait_for_leader(&cluster).await else {
        panic!("No leader elected for Raft cluster");
    };

    cluster.shutdown_node(leader_id);

    if let Err(error) = wait_for_leader(&cluster).await {
        panic!("{error}");
    }
}

#[tokio::test]
async fn test_basic_log_sync() {
    let cluster = TestRaftCluster::setup(5, 5010);

    let Ok((leader_id, _)) = wait_for_leader(&cluster).await else {
        panic!("No leader elected for Raft cluster");
    };

    let put_xxx: Vec<u8> = serde_json::to_vec(&Command::Put {
        key: "xxx".into(),
        value: "xxx".into(),
    })
    .unwrap();

    let put_yyy: Vec<u8> = serde_json::to_vec(&Command::Put {
        key: "yyy".into(),
        value: "yyy".into(),
    })
    .unwrap();

    let put_zzz: Vec<u8> = serde_json::to_vec(&Command::Put {
        key: "zzz".into(),
        value: "zzz".into(),
    })
    .unwrap();

    let delete_xxx: Vec<u8> = serde_json::to_vec(&Command::Delete { key: "xxx".into() }).unwrap();

    let _ = cluster.raft_nodes[&leader_id]
        .start_command(&put_xxx)
        .await
        .unwrap()
        .await;
    let _ = cluster.raft_nodes[&leader_id]
        .start_command(&put_yyy)
        .await
        .unwrap()
        .await;
    let _ = cluster.raft_nodes[&leader_id]
        .start_command(&put_zzz)
        .await
        .unwrap()
        .await;
    let _ = cluster.raft_nodes[&leader_id]
        .start_command(&delete_xxx)
        .await
        .unwrap()
        .await;

    time::sleep(Duration::from_millis(600)).await;

    let get_xxx: Vec<u8> = serde_json::to_vec(&Command::Get { key: "xxx".into() }).unwrap();
    let get_yyy: Vec<u8> = serde_json::to_vec(&Command::Get { key: "yyy".into() }).unwrap();
    let get_zzz: Vec<u8> = serde_json::to_vec(&Command::Get { key: "zzz".into() }).unwrap();

    // let receiver_xxx = cluster.raft_nodes[&leader_id]
    //     .start_command(&get_xxx)
    //     .await
    //     .unwrap();
    let receiver_yyy = cluster.raft_nodes[&leader_id]
        .start_command(&get_yyy)
        .await
        .unwrap();
    let receiver_zzz = cluster.raft_nodes[&leader_id]
        .start_command(&get_zzz)
        .await
        .unwrap();

    // assert_eq!(
    //     receiver_xxx.await.unwrap(),
    //     serde_json::to_vec(&CommandResponse { value: None }).unwrap()
    // );
    assert_eq!(
        receiver_yyy.await.unwrap(),
        serde_json::to_vec(&CommandResponse {
            value: Some("yyy".into())
        })
        .unwrap()
    );
    assert_eq!(
        receiver_zzz.await.unwrap(),
        serde_json::to_vec(&CommandResponse {
            value: Some("zzz".into())
        })
        .unwrap()
    );
}

async fn wait_for_leader(cluster: &TestRaftCluster) -> anyhow::Result<(u32, u64)> {
    let mut retry = 0;
    while retry < MAX_ATTEMPTS {
        time::sleep(Duration::from_millis(300)).await;
        let mut leader_count = 0;
        let mut leader_id = 0;
        let mut leader_term = 0;
        for node in cluster.raft_nodes.values() {
            let node = Arc::clone(node);
            let (role, id, term) = get_state(node).await;
            if role == Role::Leader {
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
    Err(anyhow::anyhow!("Timeout waiting for a leader"))
}

async fn get_state(raft_node: Arc<RaftNode<SharedKVStore>>) -> (Role, u32, u64) {
    let state = raft_node.state.lock().await;
    (state.role, raft_node.config.me, state.term)
}
