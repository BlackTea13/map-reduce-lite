use std::fs;
use std::ops::Deref;
use std::path::Path;

use anyhow::{anyhow, Error};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use glob::glob;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tracing::info;
use walkdir::WalkDir;

use common::minio::Client;
use common::{ihash, KeyValue};

use crate::core::MapJobRequest;
use crate::CoordinatorClient;

const WORKING_DIR: &str = "/var/tmp/";

type BucketIndex = u32;
type Buckets = DashMap<BucketIndex, Vec<KeyValue>>;

pub async fn upload_objects(
    bucket: &str,
    path: &str,
    buckets: Buckets,
    worker_id: &u32,
    client: &Client,
) -> Result<(), Error> {
    for (index, records) in buckets {
        let mut buffer = BytesMut::new();
        for record in records {
            buffer.put_slice(record.key.iter().as_slice());
            buffer.put_slice(b" ");
            buffer.put_slice(record.value.iter().as_slice());
            buffer.put_slice(b"\n");
        }

        let worker_id = worker_id & 0xFFFF;
        let out_key = format!("{path}/temp/mr-in-{index}-{worker_id}");

        client.put_object(bucket, &out_key, buffer.freeze()).await?;
    }

    Ok(())
}

pub async fn perform_map(
    request: MapJobRequest,
    worker_id: &u32,
    num_workers: u32,
    client: &Client,
) -> Result<(), Error> {
    /// TODO: Remove me when straggler is done
    // if *worker_id & 1 == 1 {
    //     tokio::time::sleep(tokio::time::Duration::from_secs(10000)).await;
    // }
    let bucket_in = request.bucket_in;
    let bucket_out = request.bucket_out;
    let output_key = request.output_path;
    let input_keys = request.input_keys;
    let workload = request.workload;
    let aux = request.aux;

    info!("Received map task with workload `{workload}`");

    let workload = match workload::try_named(&workload) {
        Some(wl) => wl,
        None => {
            return Err(anyhow!(
                "The workload `{}` is not a known workload",
                workload
            ))
        }
    };

    let target_dir = format!("{WORKING_DIR}mrl-{}", worker_id & 0xFFFF);
    let target_path = Path::new(&target_dir);
    if !target_path.exists() {
        fs::create_dir_all(target_path)?;
    }

    for key in input_keys {
        client
            .download_object(&bucket_in, &key, &target_dir)
            .await?;
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
            if let Ok(entry) = entry {
                if entry.path().is_dir() && entry.file_name().to_string_lossy().starts_with("mrl") {
                    let _ = fs::remove_dir_all(entry.path());
                }
            }
        }
    });

    upload_objects(&bucket_out, &output_key, buckets, worker_id, client).await?;

    Ok(())
}
