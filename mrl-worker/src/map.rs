use anyhow::{anyhow, Error};
use aws_sdk_s3 as s3;
use bytes::Bytes;
use reqwest::header::TE;
use tracing::{error, info};
use url::Url;

use common::KeyValue;
use common::minio::Client;

use crate::core::MapJobRequest;

const TEMP_DIR: &str = "/var/tmp";

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
            return Err(anyhow!(e));
        }
    };

    let bucket = s3_url.domain().unwrap();
    let path = &s3_url.path()[1..]; // we don't want the first '/' character

    // put stuff in temp folder in Unix filesystems
    if let Err(e) = client.glob_download(bucket, path, TEMP_DIR).await {
        error!("failed to download objects in bucket `{bucket}`: {e}");
        return Err(e);
    }

    let map_fn = workload.map_fn;

    Ok(())
}
