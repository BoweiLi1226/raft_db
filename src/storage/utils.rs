use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Command {
    GET { key: String },
    PUT { key: String, value: String },
    DELETE { key: String },
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CommandResponse {
    pub value: Option<String>,
}
