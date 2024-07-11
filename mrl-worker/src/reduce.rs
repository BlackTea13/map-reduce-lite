use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::{self, prelude::*};
use std::path::Path;

use anyhow::{anyhow, Error};
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use bytes::Bytes;
use bytesize::MB;
use ext_sort::{buffer::LimitedBufferBuilder, ExternalSorter, ExternalSorterBuilder};
use glob::glob;
use rand::Rng;
use tracing::error;
use walkdir::WalkDir;

use common::minio::Client;

use crate::core::worker::ReduceJobRequest;
use crate::core::WORKING_DIR_REDUCE;
use crate::info;

// use tokio::fs::File;
// use tokio::io::AsyncReadExt;

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
    client: &Client,
    id: u32,
) -> Result<(), Error> {
    let request_clone = request.clone();
    let reduce_ids = request_clone.reduce_ids;
    let workload = request_clone.workload;

    info!("Received reduce task with workload `{workload}`");

    for reduce_id in &reduce_ids {
        let r: u8 = {
            let mut rng = rand::thread_rng();
            rng.gen()
        };

        info!("Starting reduce on reduce id: {reduce_id}");
        perform_reduce_per_id(request.clone(), client, *reduce_id, r, id).await?;
    }

    for _reduce_id in reduce_ids.clone() {
        let _ = tokio::task::spawn(async move {
            for entry in WalkDir::new(WORKING_DIR_REDUCE).into_iter().flatten() {
                if entry.path().is_dir()
                    && entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(&format!("mrl-{}", id & 0xFFFF))
                {
                    let _ = fs::remove_dir_all(entry.path());
                }
            }
        })
        .await;
    }

    Ok(())
}

pub async fn perform_reduce_per_id(
    request: ReduceJobRequest,
    client: &Client,
    reduce_id: u32,
    rand: u8,
    id: u32,
) -> Result<(), Error> {
    let aux = request.aux;
    let bucket = request.bucket;
    let inputs = request.inputs;
    let output_path = request.output;
    let workload = request.workload;

    let inputs: Vec<&String> = inputs
        .iter()
        .filter(|key| key.contains(&format!("mr-in-{}", reduce_id)))
        .collect();

    let workload = match workload::try_named(&workload) {
        Some(wl) => wl,
        None => {
            return Err(anyhow!(
                "The workload `{}` is not a known workload",
                workload
            ))
        }
    };

    let target_dir = format!(
        "{WORKING_DIR_REDUCE}mrl-{}-{}-{rand}",
        id & 0xFFFF,
        reduce_id
    );
    let target_path = Path::new(&target_dir);
    if !target_path.exists() {
        fs::create_dir_all(target_path)?;
    }

    info!("Downloading intermediate objects from object storage...");
    for key in &inputs {
        let res = client.download_object(&bucket, key, &target_dir).await;
        match res {
            Ok(_) => {}
            Err(e) => info!("error: {}", e),
        }
    }

    let reduce_func = workload.reduce_fn;
    let temp_file_path = format!("{target_dir}/*");
    let input_files = glob(&temp_file_path)?;

    let input_file_names: Vec<String> = input_files
        .flatten()
        .map(|file| file.to_str().unwrap().to_string())
        .collect();

    info!("Combining intermediate objects...");
    let combined_output_location = format!("{target_dir}/combined");
    let mut output = File::create(&combined_output_location)?;
    for input_file_name in input_file_names {
        let mut input = File::open(input_file_name)?;
        io::copy(&mut input, &mut output)?;
    }

    info!("Running external sort on combined keys");
    let sorted_output_location = external_sort(&combined_output_location);

    let out_pathspec = format!("{target_dir}/part");
    let mut out_file = File::create(&out_pathspec)?;

    let file = File::open(sorted_output_location)?;
    let reader = BufReader::new(file);

    let mut previous_key = String::new();
    let mut values: Vec<Bytes> = vec![];

    info!("Grouping keys...");
    for line in reader.lines().map_while(Result::ok) {
        if line.is_empty() {
            continue;
        }

        let (key, value) = line.split_once(' ').unwrap();
        let (key, value) = (key.to_string(), value.to_string());

        if URL_SAFE.decode(&value).is_err() {
            error!("failed decode value: {}", &value);
        }
        if String::from_utf8(URL_SAFE.decode(&value)?).is_err() {
            error!("failed utf8 value: {}", &value);
        }

        if URL_SAFE.decode(&key).is_err() {
            error!("failed decode key: {}", &key);
        }
        if String::from_utf8(URL_SAFE.decode(&key)?).is_err() {
            error!("failed utf8 key: {}", &key);
        }

        let (key, value) = (
            String::from_utf8(URL_SAFE.decode(key)?)?,
            String::from_utf8(URL_SAFE.decode(value)?)?,
        );

        if previous_key.is_empty() {
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
    info!("Grouping keys completed");

    // write the last group to the output
    info!("Running reduce function on groups...");
    let aux_bytes = Bytes::from(aux.clone().join(" "));
    let out = reduce_func(
        Bytes::from(previous_key.clone()),
        Box::new(values.clone().into_iter()),
        aux_bytes,
    )?;

    info!("Writing output to file...");
    out_file.write_all(&out)?;

    let output_key = format!("{output_path}/mr-out-{}", reduce_id);

    info!("Uploading file to object storage...");
    client
        .upload_file(&bucket, &output_key, out_pathspec)
        .await?;
    info!("Upload complete");

    Ok(())
}
