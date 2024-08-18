use std::{collections::HashMap, str::FromStr};

use arweave_rs::{crypto::base64::Base64, transaction::tags::Tag, transaction::Tx};
use reqwest::{StatusCode, Url};

pub struct Client {
    base_url: Url,
    http_client: reqwest::Client,
}

impl Client {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            base_url: Url::from_str(arweave_rs::consts::ARWEAVE_BASE_URL)?,
            http_client: reqwest::ClientBuilder::new().build()?,
        })
    }

    async fn fetch_data<D>(&self, url: Url) -> anyhow::Result<D>
    where
        D: FromStr,
        D::Err: std::error::Error + Send + Sync + 'static,
    {
        let res = self.http_client.get(url).send().await?.error_for_status()?;

        if res.status() == StatusCode::ACCEPTED {
            return Err(anyhow::anyhow!("Pending"));
        }

        let val = D::from_str(&res.text().await?)?;
        Ok(val)
    }

    pub async fn fetch_transaction(&self, id: &Base64) -> anyhow::Result<TxMetadata> {
        let tx: Tx = self
            .fetch_data(self.base_url.join(&format!("tx/{id}"))?)
            .await?;

        let tags = tx
            .tags
            .iter()
            .map(
                |Tag::<Base64> {
                     ref name,
                     ref value,
                 }| {
                    name.to_utf8_string()
                        .and_then(|n| value.to_utf8_string().map(|v| (n, v)))
                },
            )
            .collect::<Result<Vec<_>, _>>()?;

        let tag_map = HashMap::from_iter(tags);
        Ok(TxMetadata { tag_map })
    }

    pub async fn fetch_transaction_data(&self, id: &Base64) -> anyhow::Result<Base64> {
        self.fetch_data(self.base_url.join(&format!("tx/{id}/data"))?)
            .await
    }
}

#[derive(Debug)]
pub struct TxMetadata {
    tag_map: HashMap<String, String>,
}

impl TxMetadata {
    pub fn get_tag(&self, name: &str) -> Option<&str> {
        self.tag_map.get(name).map(|s| s.as_str())
    }

    pub fn required_tag(&self, name: &str) -> anyhow::Result<&str> {
        self.get_tag(name)
            .ok_or_else(|| anyhow::anyhow!("tag {name} not found!"))
    }

    pub fn is_bundle(&self) -> anyhow::Result<()> {
        let bundle_format = self.required_tag("Bundle-Format")?;
        let bundle_version = self.required_tag("Bundle-Version")?;

        match (bundle_format, bundle_version) {
            ("binary", "2.0.0") => Ok(()),
            _ => Err(anyhow::anyhow!(
                "incorrect bundle format {bundle_format} or version {bundle_version}"
            )),
        }
    }
}
