use anyhow::Context;
use arweave_rs::crypto::{base64::Base64, hash::sha256};
use async_stream::try_stream;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::avro::{self, BundleTag};

#[derive(Debug, Serialize, Deserialize)]
pub struct DataItem {
    pub signature_name: String,
    pub signature: Base64,
    pub bundle_id: Base64,
    pub owner_public_key: Base64,
    pub target: Option<Base64>,
    pub anchor: Option<Base64>,
    pub tags: Vec<BundleTag>,
    pub data: Base64,
}

pub async fn read_data_item<R>(mut reader: R) -> anyhow::Result<DataItem>
where
    R: AsyncRead + Unpin,
{
    let signature_type = reader.read_u16_le().await.context("signature type")?;
    let (signature_name, sig_length, pub_key_length) = match signature_type {
        1 => ("arweave", 512, 512),
        2 => ("ed25519", 64, 32),
        3 => ("ethereum", 65, 65),
        4 => ("solana", 64, 32),
        v => return Err(anyhow::anyhow!("Unsupported signature type: {v}")),
    };
    // signature type 1 has 512 bytes signature
    let signature = read_buffer_as_base64(&mut reader, sig_length)
        .await
        .context("signature")?;

    let bundle_id = Base64::from(&sha256(&signature.0)[..]);

    let owner_public_key = read_buffer_as_base64(&mut reader, pub_key_length)
        .await
        .context("owner public key")?;

    let target = read_optional_field_as_base64(&mut reader, 32)
        .await
        .context("target")?;

    let anchor = read_optional_field_as_base64(&mut reader, 32)
        .await
        .context("anchor")?;

    let tag_count = reader.read_u64_le().await.context("tag count")?;

    let tags_size = reader.read_u64_le().await.context("tags_size")?;

    let tags = if tags_size > 0 {
        let mut tag_data = vec![0; tags_size as usize];
        reader
            .read_exact(tag_data.as_mut_slice())
            .await
            .context("tag data")?;

        avro::parse_tag_list(tag_data.as_slice()).context("Avro tags parse")?
    } else {
        vec![]
    };

    assert_eq!(tag_count as usize, tags.len());

    let mut data = Vec::with_capacity(1024); // allocate 1kbytes initially
    let _ = reader.read_to_end(&mut data).await.context("data field")?;

    Ok(DataItem {
        signature_name: signature_name.to_string(),
        signature,
        bundle_id,
        owner_public_key,
        target,
        anchor,
        tags,
        data: Base64(data),
    })
}

pub fn ans104_bundle_data_item_stream<R>(
    mut reader: R,
) -> impl Stream<Item = anyhow::Result<DataItem>>
where
    R: AsyncRead + Unpin,
{
    try_stream! {
        let total_items = read_u256_as_u128(&mut reader).await.context("total DataItems read")?;
        let data_items_table = read_data_item_and_entry_id_table(&mut reader, total_items).await.context("DataItems table read")?;
        let total = data_items_table.len();

        for (idx, (data_item_size, _)) in data_items_table.into_iter().enumerate() {
            let mut data_item_reader = (&mut reader).take(data_item_size as u64);
            let data_item = read_data_item(&mut data_item_reader).await.context(format!("DataItem {idx} of {total}  (size: {data_item_size}) read"))?;
            yield data_item
        }

    }
}

// a little helper to read u256 (32bytes size) integers as u128 (ignoring upper half)
// because: u128 max value in bytes is theoretical maximum volume size of the ZFS filesystem
// u256 max value in bits ( u253 in bytes!) is information content of a one-solar-mass black hole.
// we are safe
async fn read_u256_as_u128<R>(mut reader: R) -> anyhow::Result<u128>
where
    R: AsyncRead + Unpin,
{
    let num = reader.read_u128_le().await?;
    let upper_half = reader.read_u128_le().await?;
    // make sure that upper half is zero - otherwise we are dealing with integers bigger than u128
    debug_assert!(upper_half == 0);
    Ok(num)
}

async fn read_buffer_as_base64<R>(mut reader: R, size: usize) -> anyhow::Result<Base64>
where
    R: AsyncRead + Unpin,
{
    let mut vec = vec![0; size];
    reader.read_exact(vec.as_mut_slice()).await?;
    Ok(Base64(vec))
}

async fn read_optional_field_as_base64<R>(
    mut reader: R,
    size: usize,
) -> anyhow::Result<Option<Base64>>
where
    R: AsyncRead + Unpin,
{
    let is_present = reader.read_u8().await?;
    assert!(is_present < 2); // either 0 or 1 is allowed
    Ok(if is_present == 1 {
        Some(read_buffer_as_base64(reader, size).await?)
    } else {
        None
    })
}

async fn read_data_item_and_entry_id_table<R>(
    mut reader: R,
    total_items: u128,
) -> anyhow::Result<Vec<(u128, Base64)>>
where
    R: AsyncRead + Unpin,
{
    let mut res = vec![];
    for _ in 0..total_items {
        let size = read_u256_as_u128(&mut reader).await?;
        let entry_id = read_buffer_as_base64(&mut reader, 32).await?;

        res.push((size, entry_id));
    }
    Ok(res)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;
    use arweave_rs::crypto::hash::sha256;
    use futures_util::stream::TryStreamExt;

    #[tokio::test]
    async fn parse_sample_tx_data_bundle() {
        let hex_str = include_str!("../res/uYpAeGCj8Xe_J0sKiZ_aJ4Zl1zQLgDH5ia-pqtNLJEA_data.hex");
        let data = hex::decode(hex_str).expect("should parse");

        let data_items = ans104_bundle_data_item_stream(data.as_slice())
            .try_collect::<Vec<DataItem>>()
            .await
            .expect("should work");

        assert_eq!(data_items.len(), 4);
    }

    #[tokio::test]
    async fn parse_first_item_in_tx_data_bundle() {
        let hex_str = include_str!("../res/first_item.hex");
        let data = hex::decode(hex_str).expect("should parse");

        let data_item = read_data_item(data.as_slice()).await.expect("should work");
        // some sanity checks
        assert_eq!(data_item.tags.len(), 18);
        assert_eq!(data_item.data.0.len(), 11904);
    }

    #[tokio::test]
    async fn test_read_to_the_end() {
        let mut data: &[u8] = b"12345";
        let mut buff = Vec::with_capacity(1000);
        data.read_to_end(&mut buff).await.expect("should not fail");

        assert_eq!(&buff, b"12345")
    }

    #[tokio::test]
    async fn test_read_exact() {
        let mut data: &[u8] = b"12345";
        let mut buff = vec![0u8; 5];
        data.read_exact(&mut buff).await.expect("should not fail");

        assert_eq!(&buff, b"12345")
    }

    #[test]
    fn test_signature_to_id() {
        let expected_id =
            Base64::from_str("Q2K1PXNWhFyPhqBXnZbonljqJQ_QeMStIVtmAluydSE").expect("should parse");
        let sig = Base64::from_str("wIzrHPqxNFBJsjxBkTjYzL7J82err-wMhpvR8ryYMuPc2txKaQJjQ2jrMxvVmRHdgR-Vw3HzblWKw_WcYNF7e9J2f_JVBedwr9laDHqozJXiKrAfauLhP2Pjz23Ggp87f7iLDvDsG2-0ZVpCgxXkvqyKk_9vgTb2brdhj0oVSHJFKOL-q-OrrnfWchfAJiyAxS4jee3uadXNO2ENh8VeQegVbTB4G7WjqKwxPmgXpQwNucrkZOovWmbMNaFuC9BILo9WI_aOn_xvtqWI8zJBWr5JDqz_ph8O_AFaFp66ULph9ryvgYvr_WXFDfk8OvHOo-IKtwnttinugjPElGZ7u3xjahMmKRR-1k14uT8gTwdE40vyv1H3kS8yNvhqZWC2EKnoJvH3Bak0KPhhRMQ9BOJFoSvpzpauD_jQTJwyBhBt38TNUXW5cmGLB2y0ksyStA87wwaTV4E9iLzlOB-oO0m3SxWc-HVXAhucUYewGU3UZpLQypCry7EM1J7mvcroyOweKS2FAtVeHdAwKGIkgWzZeencgSyiOVhP1tLk55oOlDGX31sP3lhbLWN0I_TsxgmAs2GOnmIOSg784D0EEeX2zAYXW-s_Zs-h_lzvvyyeq9GDJqthFp0DdCY1yIrwDx-GOSf3e4h31mXjpfD_JH0P3bD30M-caUUMLb7BFYQ").expect("should parse");

        let id = sha256(&sig.0);
        let id = Base64::from(&id[..]);

        assert_eq!(id, expected_id);
    }
}
