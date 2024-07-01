use std::path::Path;

use anyhow::{anyhow, Error};
/// Helper functions and structures for dealing with minio.
use aws_sdk_s3 as s3;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_types::byte_stream::Length;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use bytes::Bytes;
use globset::Glob;
use tokio::fs::File;
use tokio::io::copy;
use tokio::io::AsyncReadExt;
use tracing::{debug, error, info};
use url::Url;

// Added line to import StreamExt

const CHUNK_SIZE: u64 = 1024 * 1024 * 5;

#[derive(Debug)]
pub struct BucketKey {
    pub bucket: String,
    pub key: String,
}

/// retrieves a bucket and key for a given path, the path should contain the s3 protocol
pub fn path_to_bucket_key(path: &str) -> Result<BucketKey, Error> {
    let mut s3_url = Url::parse(path).map_err(|e| anyhow!("Could not parse input given: {}", e))?;

    if s3_url.scheme() != "s3" {
        return Err(anyhow!("protocol of path is not S3"));
    }

    while s3_url.path().ends_with('*') || s3_url.path().ends_with('/') {
        if let Ok(mut segments) = s3_url.path_segments_mut() {
            segments.pop_if_empty();
            segments.pop();
        }
    }

    let bucket = s3_url
        .domain()
        .ok_or(anyhow!("something went wrong trying to retrieve bucket"))?;

    let mut key = "";
    if !s3_url.path().is_empty() {
        key = &s3_url.path()[1..]; // we slice out the first `/` character
    }

    Ok(BucketKey {
        bucket: bucket.to_string(),
        key: key.to_string(),
    })
}

#[derive(Clone)]
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

#[derive(Debug, Clone)]
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

    pub async fn move_objects(
        &self,
        bucket: &str,
        source_path: &str,
        destination_path: &str,
    ) -> Result<(), Error> {
        let source_objects = self.list_objects_in_dir(bucket, source_path).await?;

        for source_object in source_objects {
            let destination_key = format!(
                "{}/{}",
                destination_path,
                source_object
                    .trim_start_matches(source_path)
                    .trim_start_matches('/')
            );

            self.copy_object(bucket, &source_object, &destination_key)
                .await?;

            self.delete_object(bucket, &source_object).await?;
        }

        Ok(())
    }

    pub async fn copy_object(
        &self,
        bucket: &str,
        source_key: &str,
        destination_key: &str,
    ) -> Result<(), Error> {
        let copy_source = format!("{}/{}", bucket, source_key);

        self.client
            .copy_object()
            .bucket(bucket)
            .copy_source(copy_source)
            .key(destination_key)
            .send()
            .await?;

        Ok(())
    }

    pub async fn rename_object(
        &self,
        bucket: &str,
        old_key: &str,
        new_key: &str,
    ) -> Result<(), Error> {
        self.delete_object(bucket, old_key).await?;

        self.copy_object(bucket, old_key, new_key).await?;

        Ok(())
    }

    pub async fn list_objects(&self, bucket: &str) -> Result<Vec<String>, Error> {
        self.list_objects_in_dir(bucket, "").await
    }

    /// Lists all objects found in the specified folder in S3.
    /// the folder path argument expects a URL string, it will remove wildcard operators '*'
    pub async fn list_objects_in_dir(&self, bucket: &str, key: &str) -> Result<Vec<String>, Error> {
        let mut response = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(key)
            .max_keys(50) // In this example, go 10 at a time.
            .into_paginator()
            .send();

        let mut objects = vec![];
        while let Some(result) = response.next().await {
            match result {
                Ok(output) => {
                    for object in output.contents() {
                        objects.push(object.key.clone().unwrap_or(String::from("Unknown")));
                    }
                }
                Err(err) => {
                    error!("{err:?}")
                }
            }
        }

        Ok(objects)
    }

    pub async fn object_exists(
        &self,
        bucket: &str,
        key: &str, // Path to the object
    ) -> Result<bool, Error> {
        let object_request = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;

        match object_request {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.into_service_error().code() == Some("NotFound") {
                    return Ok(false);
                }
                Err(anyhow!("something went wrong"))
            }
        }
    }

    pub async fn append_to_s3_object(
        &self,
        bucket_name: &str,
        object_key: &str,
        data_to_append: Bytes,
    ) -> Result<(), Error> {
        let get_object_response = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(object_key)
            .send()
            .await?;

        let mut existing_data = Vec::new();

        let _ = get_object_response
            .body
            .into_async_read()
            .read_to_end(&mut existing_data)
            .await?;

        existing_data.extend_from_slice(&data_to_append);

        self.client
            .put_object()
            .bucket(bucket_name)
            .key(object_key)
            .body(ByteStream::from(existing_data))
            .send()
            .await?;

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

    pub async fn glob_download(
        &self,
        bucket: &str,
        glob_pattern: &str,
        dir: &str,
    ) -> Result<(), Error> {
        let glob = Glob::new(glob_pattern).expect("invalid glob pattern");
        let glob_matcher = glob.compile_matcher();
        let objects = self.client.list_objects_v2().bucket(bucket).send().await?;

        for object in objects.contents.unwrap() {
            let key = object.key.as_ref().unwrap();

            if glob_matcher.is_match(key) {
                self.download_object(bucket, key, dir).await?;
            }
        }

        Ok(())
    }

    pub async fn download_object(&self, bucket: &str, key: &str, dir: &str) -> Result<(), Error> {
        debug!("Downloading object `{key}` from bucket `{bucket}` to directory `{dir}`");
        let file_name = format!(
            "{dir}/mr-in-{}",
            key.split('/').collect::<Vec<_>>().last().unwrap()
        );

        // Define the part size (e.g., 5 MB)
        let part_size = 1 * 1024 * 1024;
        let mut start_byte = 0;
        let mut end_byte = part_size - 1;

        // Open the local file for writing
        let mut file = File::create(file_name).await?; // FAILs

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
        let multipart_upload = self
            .client
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
            let upload_part_res = self
                .client
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

        let completed_multipart_upload: CompletedMultipartUpload =
            CompletedMultipartUpload::builder()
                .set_parts(Some(upload_parts))
                .build();

        let _complete_multipart_upload_res = self
            .client
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
