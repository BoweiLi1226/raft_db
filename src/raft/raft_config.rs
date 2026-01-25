use std::{collections::HashMap, net::SocketAddr};

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
                    panic!("Raft node {} has invalid endpoint {}", id, addr_str);
                });
                (id, addr)
            })
            .collect();
        Self { me, nodes }
    }
}
