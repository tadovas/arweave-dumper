use arweave_rs::crypto::base64::Base64;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::avro::{self, BundleTag};

#[derive(Debug, Serialize, Deserialize)]
pub struct DataItem {
    pub signature_type: u16,
    pub signature: Base64,
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
    let signature_type = reader.read_u16_le().await?;
    debug_assert_eq!(
        signature_type, 1,
        "unexpected signature type: {signature_type}"
    );
    // signature type 1 has 512 bytes signature
    let signature = read_buffer_as_base64(&mut reader, 512).await?;

    let owner_public_key = read_buffer_as_base64(&mut reader, 512).await?;

    let target = read_optional_field_as_base64(&mut reader, 32).await?;

    let anchor = read_optional_field_as_base64(&mut reader, 32).await?;

    let tag_count = reader.read_u64_le().await?;

    let tags_size = reader.read_u64_le().await?;

    let mut tag_data = vec![0; tags_size as usize];
    reader.read_exact(tag_data.as_mut_slice()).await?;

    let tags = avro::parse_tag_list(tag_data.as_slice())?;
    debug_assert_eq!(tag_count as usize, tags.len());

    let mut data = vec![0; 1024];
    let _ = reader.read_to_end(&mut data).await?;

    Ok(DataItem {
        signature_type,
        signature,
        owner_public_key,
        target,
        anchor,
        tags,
        data: Base64(data),
    })
}

/// Reads a list of ASN-104 Data items from given byte reader
pub async fn read_ans104_bundle<R>(mut reader: R) -> anyhow::Result<Vec<DataItem>>
where
    R: AsyncRead + Unpin,
{
    let total_items = read_u256_as_u128(&mut reader).await?;
    let data_items_table = read_data_item_and_entry_id_table(&mut reader, total_items).await?;

    //TODO (convert into stream of data items?)
    let mut res = vec![];
    for (data_item_size, _) in data_items_table {
        let mut data_item_reader = (&mut reader).take(data_item_size as u64);
        let data_item = read_data_item(&mut data_item_reader).await?;
        res.push(data_item);
    }
    Ok(res)
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
    debug_assert!(is_present < 2); // either 0 or 1 is allowed
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
    use super::*;

    #[tokio::test]
    async fn parse_sample_tx_data_bundle() {
        let hex_str = include_str!("../res/uYpAeGCj8Xe_J0sKiZ_aJ4Zl1zQLgDH5ia-pqtNLJEA_data.hex");
        let data = hex::decode(hex_str).expect("should parse");

        let data_items = read_ans104_bundle(data.as_slice())
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
        assert_eq!(data_item.data.0.len(), 12928);
    }
}
