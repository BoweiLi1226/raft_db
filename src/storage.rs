use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PutArgs {
    pub key: String,
    pub value: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct GetArgs {
    pub key: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Command {
    GET(GetArgs),
    PUT(PutArgs),
}

#[derive(Debug, Clone, Default)]
struct KVStore {
    data: HashMap<String, String>,
}

impl KVStore {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    fn put(&mut self, args: PutArgs) {
        self.data.insert(args.key, args.value);
    }

    fn get(&self, args: GetArgs) -> Option<String> {
        self.data.get(&args.key).cloned()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SharedKVStore {
    data: Arc<RwLock<KVStore>>,
}

impl SharedKVStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(KVStore::new())),
        }
    }

    pub async fn put(&mut self, args: PutArgs) {
        let mut guard = self.data.write().await;
        guard.put(args);
    }

    pub async fn get(&self, args: GetArgs) -> Option<String> {
        let guard = self.data.read().await;
        guard.get(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_with_single_caller() {
        let k = String::from("test_key");
        let v = String::from("test_value");

        let mut store = SharedKVStore::new();
        store
            .put(PutArgs {
                key: k.clone(),
                value: v.clone(),
            })
            .await;

        assert_eq!(Some(v), store.get(GetArgs { key: k }).await);
    }

    #[tokio::test]
    async fn test_with_multiple_callers() {
        let store = SharedKVStore::new();
        let mut handles = vec![];
        for i in 1..20 {
            let mut store = store.clone();
            let handle = tokio::spawn(async move {
                let k = format!("key_{}", i);
                let v = format!("value_{}", i);
                store
                    .put(PutArgs {
                        key: k.clone(),
                        value: v.clone(),
                    })
                    .await;
                let s = store.get(GetArgs { key: k }).await.unwrap();
                assert_eq!(v, s);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        for i in 1..20 {
            let s = store
                .get(GetArgs {
                    key: format!("key_{}", i),
                })
                .await
                .unwrap();
            assert_eq!(format!("value_{}", i), s);
        }
    }
}
