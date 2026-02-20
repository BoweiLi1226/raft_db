use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Command {
    GET { key: String },
    PUT { key: String, value: String },
    DELETE { key: String },
}

#[derive(Debug)]
pub struct CommandResponse {
    pub value: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CommandError {
    KeyDoesNotExist,
}
