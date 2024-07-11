use std::fs;
use std::path::Path;

use anyhow::{anyhow, Error};
use base64::Engine;
#[allow(unused_imports)]
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use glob::glob;
use rand::Rng;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tracing::{error, info};
use walkdir::WalkDir;

use common::minio::Client;
use common::{hash, KeyValue};

use crate::core::{MapJobRequest, WORKING_DIR_MAP};

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
            let key_encoded = URL_SAFE.encode(record.key.iter().as_slice());
            let value_encoded = URL_SAFE.encode(record.value.iter().as_slice());

            buffer.put_slice(key_encoded.as_bytes());
            buffer.put_slice(b" ");
            buffer.put_slice(value_encoded.as_bytes());
            buffer.put_slice(b"\n");
        }

        let worker_id = worker_id & 0xFFFF;
        let out_key = format!("{path}/temp/mr-in-{index}-{worker_id}");

        match client.put_object(bucket, &out_key, buffer.freeze()).await {
            Ok(_) => {}
            Err(e) => error!("{}", anyhow!(e)),
        }
    }

    Ok(())
}

pub async fn perform_map(
    request: MapJobRequest,
    num_workers: u32,
    client: &Client,
) -> Result<(), Error> {
    let bucket_in = request.bucket_in;
    let bucket_out = request.bucket_out;
    let output_key = request.output_path;
    let input_keys = request.input_keys;
    let workload = request.workload;
    let aux = request.aux;
    let worker_id: u32 = request.worker_id as u32;

    let r: u8 = {
        let mut rng = rand::thread_rng();
        rng.gen()
    };

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

    let target_dir = format!("{WORKING_DIR_MAP}mrl-{}-{r}", worker_id & 0xFFFF);
    let target_path = Path::new(&target_dir);
    if !target_path.exists() {
        fs::create_dir_all(target_path)?;
    }

    for key in input_keys {
        client
            .download_object(&bucket_in, &key, &target_dir)
            .await?;
    }

    info!("Objects downloaded from object storage");

    let map_fn = workload.map_fn;
    let temp_file_path = format!("{target_dir}/*");
    let input_files = glob(&temp_file_path)?;

    info!("Creating buckets...");
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
            let bucket_no = hash(&key) % num_workers;

            #[allow(clippy::unwrap_or_default)]
            buckets
                .entry(bucket_no)
                .or_insert(Vec::new())
                .push(KeyValue { key, value });
        }
    }
    info!("Map function completed on buckets");
    info!("Cleaning up files on local filesystem...");
    // cleanup temp files on local
    for entry in WalkDir::new(WORKING_DIR_MAP).into_iter().flatten() {
        if entry.path().is_dir()
            && entry
                .file_name()
                .to_string_lossy()
                .starts_with(&format!("mrl-{}-{r}", worker_id & 0xFFFF))
        {
            let _ = fs::remove_dir_all(entry.path());
        }
    }

    info!("Uploading buckets to object storage...");
    upload_objects(&bucket_out, &output_key, buckets, &worker_id, client).await?;
    info!("Upload complete");

    Ok(())
}
