use std::path::Path;

use anyhow::{anyhow, Error};
/// Helper functions and structures for dealing with minio.
use aws_sdk_s3 as s3;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_types::byte_stream::Length;
use bytes::Bytes;
use tokio::fs::File;
use tokio::io::copy;
use tracing::{debug, error, info};

const CHUNK_SIZE: u64 = 1024 * 1024 * 5;

pub struct ClientConfig {
    /// id
    pub access_key_id: String,

    /// password
    pub secret_access_key: String,

    /// object store region
    pub region: String,

    /// minio url
    pub url: String,
}

#[derive(Debug)]
pub struct Client {
    pub client: s3::Client,
}

impl Client {
    pub fn from_conf(cfg: ClientConfig) -> Self {
        let cred = s3::config::Credentials::new(
            cfg.access_key_id,
            cfg.secret_access_key,
            None,
            None,
            "some provider",
        );
        let region = s3::config::Region::new(cfg.region);
        let conf_builder = s3::config::Builder::new()
            .credentials_provider(cred)
            .region(region)
            .endpoint_url(cfg.url)
            .behavior_version_latest();
        let conf = conf_builder.build();

        Self {
            client: s3::Client::from_conf(conf),
        }
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<(), Error> {
        self.client.create_bucket().bucket(bucket).send().await?;
        Ok(())
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Bytes, Error> {
        let data = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?
            .body
            .collect()
            .await?
            .into_bytes();
        Ok(data)
    }

    pub async fn put_object(&self, bucket: &str, key: &str, data: Bytes) -> Result<(), Error> {
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(data))
            .send()
            .await?;
        Ok(())
    }

    pub async fn list_objects(&self, bucket: &str) -> Result<(), Error> {
        let mut response = self.client
            .list_objects_v2()
            .bucket(bucket.to_owned())
            .max_keys(50) // In this example, go 10 at a time.
            .into_paginator()
            .send();

        while let Some(result) = response.next().await {
            match result {
                Ok(output) => {
                    for object in output.contents() {
                        info!(" - {}", object.key().unwrap_or("Unknown"));
                    }
                }
                Err(err) => {
                    error!("{err:?}")
                }
            }
        }

        Ok(())
    }


    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), Error> {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        Ok(())
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        self.client.delete_bucket().bucket(bucket).send().await?;
        Ok(())
    }

    pub async fn download_object(&self, bucket: &str, key: &str, dir: &str) -> Result<(), Error> {
        info!("Preparing to download object `{key}` from bucket `{bucket}` to directory `{dir}`");
        let file_name = format!("{dir}/mr-in-{}", key.to_string());
        debug!("Downloading file from S3.");

        // Define the part size (e.g., 5 MB)
        let part_size = 1 * 1024 * 1024;
        let mut start_byte = 0;
        let mut end_byte = part_size - 1;

        // Open the local file for writing
        let mut file = File::create(&file_name).await?;

        loop {
            // Define the byte range for the part
            let range = format!("bytes={}-{}", start_byte, end_byte);

            // Download the part
            let get_object_request = self
                .client
                .get_object()
                .bucket(bucket)
                .key(key)
                .range(range);

            let get_object_output = get_object_request.send().await?;
            let body = get_object_output.body; // .unwrap();
            let mut body_stream = body.into_async_read();

            // Write the part to the local file
            copy(&mut body_stream, &mut file).await?;

            // Update the byte range for the next part
            start_byte = end_byte + 1;
            end_byte += part_size;

            // Check if we have reached the end of the file
            if start_byte >= get_object_output.content_length.unwrap() as u64 {
                break;
            }
        }

        Ok(())
    }

    pub async fn upload_file(&self, bucket: &str, key: &str, path: String) -> Result<(), Error> {
        info!("Preparing to upload file `{path}` to bucket `{bucket}` with key `{key}`");
        let multipart_upload = self.client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = multipart_upload.upload_id.unwrap();

        let file_path = Path::new(&path);
        let file_size = tokio::fs::metadata(file_path)
            .await
            .expect("the file path/metadata does not exist")
            .len();

        let mut chunk_count = (file_size / CHUNK_SIZE) + 1;
        let mut size_of_last_chunk = file_size % CHUNK_SIZE;
        if size_of_last_chunk == 0 {
            size_of_last_chunk = CHUNK_SIZE;
            chunk_count -= 1;
        }

        if file_size == 0 {
            return Err(anyhow!("bad file size"));
        }

        let mut upload_parts = Vec::new();

        for chunk_index in 0..chunk_count {
            let this_chunk = if chunk_count - 1 == chunk_index {
                size_of_last_chunk
            } else {
                CHUNK_SIZE
            };
            let stream = ByteStream::read_from()
                .path(file_path)
                .offset(chunk_index * CHUNK_SIZE)
                .length(Length::Exact(this_chunk))
                .build()
                .await
                .unwrap();

            //Chunk index needs to start at 0, but part numbers start at 1.
            let part_number = (chunk_index as i32) + 1;
            let upload_part_res = self.client
                .upload_part()
                .key(key)
                .bucket(bucket)
                .upload_id(&upload_id)
                .body(stream)
                .part_number(part_number)
                .send()
                .await?;

            upload_parts.push(
                CompletedPart::builder()
                    .e_tag(upload_part_res.e_tag.unwrap_or_default())
                    .part_number(part_number)
                    .build(),
            );
        }

        let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
            .set_parts(Some(upload_parts))
            .build();

        let _complete_multipart_upload_res = self.client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .multipart_upload(completed_multipart_upload)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();

        Ok(())
    }
}
