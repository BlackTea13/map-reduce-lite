/// Helper functions and structures for dealing with minio.
use aws_sdk_s3 as s3;
use aws_sdk_s3::primitives::ByteStream;
use anyhow::Error;
use bytes::Bytes;

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

    pub async fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        self.client.delete_bucket().bucket(bucket).send().await?;
        Ok(())
    }
}
