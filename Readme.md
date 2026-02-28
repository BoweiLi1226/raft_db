# Raft DB
## Description
Distributed KV Store Implemented using Rust.

## Run raft node
```
cargo run -- --id <x>
```

## Run 5 nodes on same machine
```
sh ./spawn_5_nodes.sh
```

## Run Tests
It is recommended to serialize the tests 
```
cargo test -- --test-threads=1
```
