use anyhow::{anyhow, Error};
use bytes::Bytes;
use reqwest::header::TE;
use tracing::{error, info};
use common::KeyValue;
use url::Url;
use common::minio::Client;
use crate::core::MapJobRequest;
use aws_sdk_s3 as s3;

const TEMP_DIR: &str = "/var/tmp/";

pub async fn perform_map(request: MapJobRequest, client: &Client) -> Result<(), Error> {
    info!("Starting map task");

    let input_path = request.input_files;
    let workload = request.workload;
    let aux = request.aux;

    info!("Received map task with workload `{workload}` and input path `{input_path}, with workload {workload}`");

    let workload = match workload::try_named(workload.as_str()) {
        Some(wl) => wl,
        None => return Err(anyhow!("The workload `{}` is not a known workload", workload)),
    };
    
    let s3_url = match Url::parse(&input_path) {
        Ok(url) => url,
        Err(e) => {
            error!("failed parsing input path {input_path} : {}", e);
            return Err(anyhow!(e))
        }
    };
    
    let bucket = s3_url.domain().unwrap();
    let key = &s3_url.path()[1..]; // we don't want the first '/' character
    
    client.list_objects(bucket).await?;
    
    
    // put stuff in temp folder in Unix filesystems
    if let Err(e) = client.download_object(bucket, key, TEMP_DIR).await {
        error!("failed to download object in bucket `{bucket}` with key `{key}`: {e}");
        return Err(e);
    }
    
    let map_fn = workload.map_fn;

    Ok(())
}
