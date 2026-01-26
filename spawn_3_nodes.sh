#!/bin/bash
cargo build
cargo run -- --id 1 &
cargo run -- --id 2 &
cargo run -- --id 3 &
trap "kill 0" EXIT
wait
