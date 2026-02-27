use std::collections::HashMap;

use crate::storage::utils::CommandResponse;

#[derive(Debug, Default)]
pub struct KVStore {
    data: HashMap<String, String>,
}

impl KVStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn put(&mut self, key: String, value: String) -> anyhow::Result<CommandResponse> {
        self.data.insert(key, value);
        Ok(CommandResponse { value: None })
    }

    pub fn get(&self, key: &str) -> anyhow::Result<CommandResponse> {
        self.data
            .get(key)
            .cloned()
            .map(|value| CommandResponse { value: Some(value) })
            .ok_or(anyhow::anyhow!("Key {key} does not exist"))
    }

    pub fn delete(&mut self, key: &str) -> anyhow::Result<CommandResponse> {
        Ok(CommandResponse {
            value: self.data.remove(key),
        })
    }
}
