use std::{collections::HashMap, str::FromStr};

use arweave_rs::{
    crypto::base64::Base64,
    transaction::{tags::Tag, Tx},
};
use async_stream::try_stream;
use futures_core::Stream;
use reqwest::{StatusCode, Url};
use serde::Deserialize;
use serde_aux::prelude::*;
use tokio_util::bytes::Bytes;

#[derive(Debug)]
pub struct TxMetadata {
    tag_map: HashMap<String, String>,
}

impl TxMetadata {
    pub fn get_tag(&self, name: &str) -> Option<&str> {
        self.tag_map.get(name).map(|s| s.as_str())
    }

    pub fn is_bundle(&self) -> bool {
        let is_bundle_format = self
            .get_tag("Bundle-Format")
            .filter(|v| *v == "binary")
            .is_some();

        let is_correct_bundle_version = self
            .get_tag("Bundle-Version")
            .filter(|v| *v == "2.0.0")
            .is_some();

        is_bundle_format && is_correct_bundle_version
    }
}

#[derive(Debug, Deserialize)]
pub struct TransactionOffset {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub size: usize,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub offset: usize,
}

#[derive(Debug, Deserialize)]
pub struct TransactionChunk {
    pub chunk: Base64,
}

pub struct Client {
    base_url: Url,
    http_client: reqwest::Client,
}

impl Client {
    pub fn new(api_url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            base_url: Url::from_str(api_url)?,
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

    pub async fn fetch_transaction_offset(&self, id: &Base64) -> anyhow::Result<TransactionOffset> {
        let resp = self
            .http_client
            .get(self.base_url.join(&format!("tx/{id}/offset"))?)
            .send()
            .await?
            .error_for_status()?;

        Ok(resp.json().await?)
    }

    pub async fn fetch_chunk_data(&self, offset: usize) -> anyhow::Result<TransactionChunk> {
        let resp = self
            .http_client
            .get(self.base_url.join(&format!("chunk/{offset}"))?)
            .send()
            .await?
            .error_for_status()?;

        Ok(resp.json().await?)
    }

    pub fn transaction_data_chunk_stream<'a>(
        &'a self,
        id: &'a Base64,
    ) -> impl Stream<Item = anyhow::Result<Bytes>> + 'a {
        try_stream! {
            // inspired by <https://github.com/everFinance/goar/blob/main/client.go#L612>
            let tx_offset_data = self.fetch_transaction_offset(id).await?;
            let mut chunk_offset = tx_offset_data.offset - tx_offset_data.size + 1;
            while chunk_offset < tx_offset_data.offset {
                let data = self.fetch_chunk_data(chunk_offset).await?.chunk;
                chunk_offset+= data.0.len();
                yield Bytes::from(data.0);
            }

        }
    }
}
