use std::io::{self, Write};

use raft_db::storage::{Command, SharedKVStore};
use tracing::Level;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let mut store = SharedKVStore::new();
    println!("Welcome to Raft-KV!");
    loop {
        print!("> ");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            break;
        }
        let input = input.trim();
        if input == "exit" {
            break;
        }
        match serde_json::from_str::<Command>(input) {
            Ok(cmd) => match cmd {
                Command::PUT(args) => {
                    store.put(args).await;
                    tracing::info!("{{\"status\": \"ok\"}}");
                }
                Command::GET(args) => {
                    let value = store.get(args).await;
                    tracing::info!("{{\"value\": \"{:?}\"}}", value);
                }
            },
            Err(e) => {
                tracing::info!("{{\"error\": \"Invalid JSON: {}\"}}", e);
            }
        }
    }
}
