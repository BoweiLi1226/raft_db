use std::collections::HashMap;

use crate::storage::utils::{CommandError, CommandResponse};

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

    pub fn put(&mut self, key: String, value: String) -> Result<CommandResponse, CommandError> {
        self.data.insert(key, value);
        Ok(CommandResponse { value: None })
    }

    pub fn get(&self, key: &str) -> Result<CommandResponse, CommandError> {
        self.data
            .get(key)
            .cloned()
            .map(|value| CommandResponse { value: Some(value) })
            .ok_or(CommandError::KeyDoesNotExist)
    }

    pub fn delete(&mut self, key: &str) -> Result<CommandResponse, CommandError> {
        Ok(CommandResponse {
            value: self.data.remove(key),
        })
    }
}
