use std::fs;
use std::path::Path;
use std::ptr::null_mut;

use anyhow::{anyhow, Error};
use aws_sdk_s3 as s3;
use bytes::Bytes;
use dashmap::DashMap;
use glob::glob;
use reqwest::header::TE;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tracing::{error, info};
use url::Url;
use walkdir::WalkDir;

use common::{ihash, KeyValue};
use common::minio::Client;

use crate::core::{CoordinatorClient, MapJobRequest};
use crate::core::coordinator::{AcquireLockRequest, InvalidateLockRequest};

const WORKING_DIR: &str = "/var/tmp/";

type BucketIndex = u32;
type Buckets = DashMap<BucketIndex, Vec<KeyValue>>;

pub async fn upload_objects(bucket: &str, path: &str, buckets: Buckets, client: &Client, coordinator_address: &String) -> Result<(), Error> {
    let mut coordinator_client = CoordinatorClient::connect(coordinator_address.clone()).await?;

    for (index, records) in buckets {
        let records: Vec<String> = records.iter().map(|r| r.to_string()).collect();
        let contents = records.join("\n");
        let out_key = format!("{path}/temp/mr-in-{index}");

        let _ = coordinator_client.acquire_lock(AcquireLockRequest { object_key: out_key.clone() }).await?;

        if client.object_exists(bucket, &out_key).await? {
            client.append_to_s3_object(bucket, path, Bytes::from(contents)).await?;
        } else {
            client.put_object(bucket, &out_key, Bytes::from(contents)).await?;
        }

        let _ = coordinator_client.invalidate_lock(InvalidateLockRequest { object_key: out_key }).await?;
    }

    Ok(())
}

pub async fn perform_map(request: MapJobRequest, worker_id: &u32, num_workers: u32, client: &Client, address: &String) -> Result<(), Error> {
    let bucket_in = request.bucket_in;
    let bucket_out = request.bucket_out;
    let output_key = request.output_key;
    let input_keys = request.input_keys;
    let workload = request.workload;
    let aux = request.aux;

    info!("Received map task with workload `{workload}`");

    let workload = match workload::try_named(&workload) {
        Some(wl) => wl,
        None => return Err(anyhow!("The workload `{}` is not a known workload", workload)),
    };

    let target_dir = format!("{WORKING_DIR}mrl-{worker_id}");
    let target_path = Path::new(&target_dir);
    if !target_path.exists() {
        fs::create_dir_all(target_path)?;
    }

    for key in input_keys {
        client.download_object(&bucket_in, &key, &target_dir).await?;
    }

    let map_fn = workload.map_fn;
    let temp_file_path = format!("{target_dir}/*");
    let input_files = glob(&temp_file_path)?;

    let buckets: Buckets = Buckets::new();
    for pathspec in input_files.flatten() {
        let mut buf = Vec::new();
        {
            let mut file = File::open(&pathspec).await?;
            file.read_to_end(&mut buf).await?;
        }

        let buf = Bytes::from(buf);
        let filename = pathspec.to_str().unwrap_or("unknown").to_string();
        let input_kv = KeyValue {
            key: Bytes::from(filename),
            value: buf,
        };

        let aux_bytes = Bytes::from(aux.clone().join(" "));
        for item in map_fn(input_kv, aux_bytes)? {
            let KeyValue { key, value } = item?;
            let bucket_no = ihash(&key) % num_workers;

            #[allow(clippy::unwrap_or_default)]
            buckets
                .entry(bucket_no)
                .or_insert(Vec::new())
                .push(KeyValue { key, value });
        }
    }

    // cleanup temp files on local
    tokio::task::spawn(async move {
        for entry in WalkDir::new(WORKING_DIR) {
            let entry = entry.unwrap();
            if entry.path().is_dir() && entry.file_name().to_string_lossy().starts_with("mrl") {
                let _ = fs::remove_dir_all(entry.path());
            }
        }
    });

    upload_objects(&bucket_out, &output_key, buckets, client, address).await?;

    Ok(())
}


