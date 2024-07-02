use std::borrow::Cow;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::{self, prelude::*};
use std::path::Path;

use anyhow::{anyhow, Error};
use bytes::Bytes;
use bytesize::MB;
use dashmap::DashMap;
use ext_sort::{buffer::LimitedBufferBuilder, ExternalSorter, ExternalSorterBuilder};
use glob::glob;
use tracing::error;
use walkdir::WalkDir;

use common::minio::Client;
use common::{ihash, KeyValue};

use crate::core::worker::ReduceJobRequest;
use crate::info;
use crate::CoordinatorClient;

// use tokio::fs::File;
// use tokio::io::AsyncReadExt;

const WORKING_DIR: &str = "/var/tmp/";

pub fn external_sort(filename: &str) -> String {
    let input_reader = io::BufReader::new(fs::File::open(filename).unwrap());
    let output_file_name = format!("{}-sorted", &filename);
    let mut output_writer = io::BufWriter::new(fs::File::create(&output_file_name).unwrap());

    let sorter: ExternalSorter<String, io::Error, LimitedBufferBuilder> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(Path::new("./"))
            .with_buffer(LimitedBufferBuilder::new((100 * MB) as usize, false))
            .build()
            .unwrap();

    let sorted = sorter.sort(input_reader.lines()).unwrap();

    for item in sorted.map(Result::unwrap) {
        output_writer
            .write_all(format!("{}\n", item).as_bytes())
            .unwrap();
    }
    output_writer.flush().unwrap();

    output_file_name
}

pub async fn perform_reduce(
    request: ReduceJobRequest,
    worker_id: &u32,
    client: &Client,
) -> Result<(), Error> {
    let aux = request.aux;
    let bucket = request.bucket;
    let inputs = request.inputs;
    let output_path = request.output;
    let reduce_id = request.reduce_id;
    let workload = request.workload;

    info!("Received reduce task with workload `{workload}`");

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

    for key in inputs {
        client.download_object(&bucket, &key, &target_dir).await?;
    }

    let reduce_func = workload.reduce_fn;
    let temp_file_path = format!("{target_dir}/*");
    let input_files = glob(&temp_file_path)?;

    let input_file_names: Vec<String> = input_files
        .flatten()
        .map(|file| file.to_str().unwrap().to_string())
        .collect();

    let combined_output_location = format!("{target_dir}/combined");
    let mut output = File::create(&combined_output_location)?;
    for input_file_name in input_file_names {
        let mut input = File::open(input_file_name)?;
        io::copy(&mut input, &mut output)?;
    }

    let sorted_output_location = external_sort(&combined_output_location);

    let out_pathspec = format!("{target_dir}/part");
    let mut out_file = File::create(&out_pathspec)?;

    let file = File::open(sorted_output_location)?;
    let reader = BufReader::new(file);

    let mut previous_key = String::new();
    let mut values: Vec<Bytes> = vec![];
    for line in reader.lines() {
        if let Ok(line) = line {
            let (key, value) = line.split_once(' ').unwrap();
            let (key, value) = (key.to_string(), value.to_string());

            if previous_key == "" {
                previous_key = key;
                values.push(Bytes::from(value));
            } else if previous_key != key {
                let aux_bytes = Bytes::from(aux.clone().join(" "));

                let out = reduce_func(
                    Bytes::from(previous_key.clone()),
                    Box::new(values.clone().into_iter()),
                    aux_bytes,
                )?;

                out_file.write_all(&out)?;

                values.clear();
                values.push(Bytes::from(value));

                previous_key = key;
            } else {
                values.push(Bytes::from(value));
            }
        }
    }

    // write the last group to the output
    let aux_bytes = Bytes::from(aux.clone().join(" "));
    let out = reduce_func(
        Bytes::from(previous_key.clone()),
        Box::new(values.clone().into_iter()),
        aux_bytes,
    )?;
    out_file.write_all(&out)?;

    let output_key = format!("{output_path}/mr-out-{}", worker_id & 0xFFFF);
    client
        .upload_file(&bucket, &output_key, out_pathspec)
        .await?;

    // cleanup temp files on local
    tokio::task::spawn(async move {
        for entry in WalkDir::new(WORKING_DIR) {
            if let Ok(entry) = entry {
                if entry.path().is_dir()
                    && entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(&format!("mrl-{}", reduce_id))
                {
                    let _ = fs::remove_dir_all(entry.path());
                }
            }
        }
    });

    // cleanup temp files in S3
    let temp_path = match output_path.as_str() {
        "" => "temp",
        _ => &format!("{}/temp", output_path),
    };

    client.delete_path(&bucket, temp_path).await?;

    Ok(())
}
