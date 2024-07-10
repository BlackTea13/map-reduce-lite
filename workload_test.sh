#!/usr/bin/env bash

# for testing workloads

TIMEOUT=100
COORDINATOR_ADDRESS="http://127.0.0.1:8030"

SMALL_MATRIX_INPUT_1="s3://robert/input/*"
SMALL_MATRIX_OUTPUT_1="s3://robert/out1"

SMALL_MATRIX_INPUT_2=$SMALL_MATRIX_INPUT_1
SMALL_MATRIX_OUTPUT_2="s3://robert/out2"

BIG_MATRIX_INPUT_1="s3://robert/biginput/*"
BIG_MATRIX_OUTPUT_1="s3://robert/out3"

BIG_MATRIX_INPUT_2=$BIG_MATRIX_OUTPUT_1
BIG_MATRIX_OUTPUT_2="s3://robert/out4"

VERTEX_DEGREE_INPUT="s3://robert/graph-edges/*"
VERTEX_DEGREE_OUTPUT="s3://robert/out5"

# tests small matrix multiply
cargo run -p mrl-ctl -- submit --input $SMALL_MATRIX_INPUT_1 --output $SMALL_MATRIX_OUTPUT_1 --workload matrix-multiply-1 --address $COORDINATOR_ADDRESS --timeout $TIMEOUT
cargo run -p mrl-ctl -- submit --input $SMALL_MATRIX_INPUT_2 --output $SMALL_MATRIX_OUTPUT_2 --workload matrix-multiply-2 --address $COORDINATOR_ADDRESS --timeout $TIMEOUT

# tests big matrix multiply
cargo run -p mrl-ctl -- submit --input $BIG_MATRIX_INPUT_1 --output $BIG_MATRIX_OUTPUT_1 --workload matrix-multiply-1 --address $COORDINATOR_ADDRESS --timeout $TIMEOUT
cargo run -p mrl-ctl -- submit --input $BIG_MATRIX_INPUT_2 --output $BIG_MATRIX_OUTPUT_2 --workload matrix-multiply-2 --address $COORDINATOR_ADDRESS --timeout $TIMEOUT

# tests vertex_degree
cargo run -p mrl-ctl -- submit --input $VERTEX_DEGREE_INPUT --output $VERTEX_DEGREE_OUTPUT --workload vertex-degree --address $COORDINATOR_ADDRESS --timeout $TIMEOUT






