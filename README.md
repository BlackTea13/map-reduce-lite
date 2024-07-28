# Map Reduce Lite

Map Reduce is cool. It is even cooler when written in Rust.

# How To Run?

Run compose file in the `mrl-coordinator` directory. It should spin up a MinIO object storage.

Run the coordinator first.
```
cargo run -p mrl-coordinator
```

Run some workers.
```
cargo run -p mrl-worker --release  -- --join "http://127.0.0.1:8030" --minio-url "http://127.0.0.1:9000" --port 50025
```

Submit a job.
```
cargo run -p mrl-ctl -- submit --input "s3://bucket/input/*" --output "s3://bucket/output" --workload wc  --address "http://127.0.0.1:8030" --timeout 10
```

