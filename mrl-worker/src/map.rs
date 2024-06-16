use reqwest;
use anyhow::{anyhow, Error};
use bytes::Bytes;
use tracing::debug;
use common::KeyValue;
use crate::core::MapJobRequest;

pub async fn perform_map(request: MapJobRequest) -> Result<(), Error> {
    debug!("Starting map task");

    let input_path = request.input_files;
    let workload = request.workload;
    let url = request.presigned_url;
    let aux = request.aux;

    let workload = match workload::try_named(workload.as_str()) {
        Some(wl) => wl,
        None => return Err(anyhow!("The workload `{}` is not a known workload", workload)),
    };

    let response = reqwest::get(url).await?;
    let body = response.text().await?;

    let map_fn = workload.map_fn;

    println!("body: {}", body);
    Ok(())
}
