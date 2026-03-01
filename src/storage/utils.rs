use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Command {
    Get { key: String },
    Put { key: String, value: String },
    Delete { key: String },
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CommandResponse {
    pub value: Option<String>,
}
