#!/usr/bin/env bash

# for testing workloads on Robert's MinIO

# tests small matrix multiply
cargo run -p mrl-ctl -- submit --input "s3://robert/input/*" --output "s3://robert/out1" --workload matrix-multiply-1 --address "http://127.0.0.1:8030" --timeout 100
cargo run -p mrl-ctl -- submit --input "s3://robert/out1/*" --output "s3://robert/out2" --workload matrix-multiply-2 --address "http://127.0.0.1:8030" --timeout 100

# tests big matrix multiply
cargo run -p mrl-ctl -- submit --input "s3://robert/biginput/*" --output "s3://robert/out3" --workload matrix-multiply-1 --address "http://127.0.0.1:8030" --timeout 100
cargo run -p mrl-ctl -- submit --input "s3://robert/out3/*" --output "s3://robert/out4" --workload matrix-multiply-2 --address "http://127.0.0.1:8030" --timeout 100

# tests vertex_degree
cargo run -p mrl-ctl -- submit --input "s3://robert/graph-edges/*" --output "s3://robert/out5" --workload vertex-degree --address "http://127.0.0.1:8030" --timeout 100






