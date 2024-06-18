use anyhow::{anyhow, Error};
use bytes::Bytes;
use tracing::info;
use common::KeyValue;
use url::Url;
use crate::core::MapJobRequest;

pub async fn perform_map(request: MapJobRequest) -> Result<(), Error> {
    info!("Starting map task");

    let input_path = request.input_files;
    let workload = request.workload;
    let aux = request.aux;

    info!("Received map task with workload `{workload}` and input path `{input_path}`");

    let workload = match workload::try_named(workload.as_str()) {
        Some(wl) => wl,
        None => return Err(anyhow!("The workload `{}` is not a known workload", workload)),
    };

    let s3_url = Url::parse(&input_path).expect("input path is invalid");
    let bucket = s3_url.domain().expect("input path has invalid bucket");
    let key = s3_url.path();
    
    info!("Input files at bucket: {bucket} and path: {key}");
    
    let map_fn = workload.map_fn;

    Ok(())
}
