use std::{collections::HashMap, net::SocketAddr, time::Duration};

use tonic::transport::Channel;

use crate::raft::raft_proto::raft_client::RaftClient;

#[derive(Debug)]
pub struct RaftConfig {
    pub me: u32,
    pub nodes: HashMap<u32, SocketAddr>,
}

impl RaftConfig {
    pub fn new(me: u32, raw_endpoints: HashMap<u32, String>) -> Self {
        let nodes = raw_endpoints
            .into_iter()
            .map(|(id, addr_str)| {
                let addr = addr_str.parse::<SocketAddr>().unwrap_or_else(|_| {
                    panic!("Raft node {}: I received invalid endpoint {}", id, addr_str);
                });
                (id, addr)
            })
            .collect();
        Self { me, nodes }
    }

    pub fn make_clients(&self) -> HashMap<u32, RaftClient<Channel>> {
        self.nodes
            .iter()
            .filter(|&(&id, _)| id != self.me)
            .map(|(&id, addr)| {
                let endpoint = Channel::from_shared(format!("http://{}", addr))
                    .unwrap_or_else(|_| panic!("Failed to connect to endpoint {}", addr))
                    .connect_timeout(Duration::from_millis(150));
                (id, RaftClient::new(endpoint.connect_lazy()))
            })
            .collect()
    }

    pub fn get_number_of_nodes(&self) -> usize {
        self.nodes.len()
    }

    pub fn get_peer_ids(&self) -> Vec<u32> {
        self.nodes.keys().copied().collect()
    }
}
