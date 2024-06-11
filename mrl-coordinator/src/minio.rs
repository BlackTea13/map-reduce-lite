/// Helper functions and structures for dealing with minio.
use aws_sdk_s3 as s3;

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
}

