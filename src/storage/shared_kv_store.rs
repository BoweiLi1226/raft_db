use anyhow::Context;
use tokio::sync::RwLock;

use crate::{
    state_machine::StateMachine,
    storage::{
        kv_store::KVStore,
        utils::{Command, CommandResponse},
    },
};

#[derive(Debug, Default)]
pub struct SharedKVStore {
    data: RwLock<KVStore>,
}

impl SharedKVStore {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(KVStore::new()),
        }
    }

    async fn process(&self, command: Command) -> anyhow::Result<CommandResponse> {
        match command {
            Command::PUT { key, value } => self.put(key, value).await,
            Command::GET { key } => self.get(&key).await,
            Command::DELETE { key } => self.delete(&key).await,
        }
    }

    async fn put(&self, key: String, value: String) -> anyhow::Result<CommandResponse> {
        let mut guard = self.data.write().await;
        guard.put(key, value)
    }

    async fn get(&self, key: &str) -> anyhow::Result<CommandResponse> {
        let guard = self.data.read().await;
        guard.get(key)
    }

    async fn delete(&self, key: &str) -> anyhow::Result<CommandResponse> {
        let mut guard = self.data.write().await;
        guard.delete(key)
    }
}

#[async_trait::async_trait]
impl StateMachine for SharedKVStore {
    async fn apply(&mut self, command: &[u8]) -> anyhow::Result<Vec<u8>> {
        let command: Command = serde_json::from_slice(command)?;
        let response = self.process(command).await?;
        serde_json::to_vec(&response).context("Failed to serialize response")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn test_with_single_caller() {
        let k = String::from("test_key");
        let v = String::from("test_value");

        let store = Arc::new(SharedKVStore::new());
        let _ = store.put(k.clone(), v.clone()).await;

        assert_eq!(Some(v), store.get(&k).await.unwrap().value);
    }

    #[tokio::test]
    async fn test_with_multiple_callers() {
        let store = Arc::new(SharedKVStore::new());
        let mut join_set = JoinSet::new();
        for i in 1..100 {
            let store = store.clone();
            join_set.spawn(async move {
                let k = format!("key_{}", i);
                let v = format!("value_{}", i);
                let _ = store.put(k, v).await;
            });
        }

        join_set.join_all().await;

        for i in 1..100 {
            let s = store.get(&format!("key_{}", i)).await.unwrap().value;
            assert_eq!(format!("value_{}", i), s.unwrap());
        }
    }
}
