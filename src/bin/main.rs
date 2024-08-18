use arweave_dumper::{arweave, bundle};
use arweave_rs::crypto::base64::Base64;
use clap::{command, Parser};
use futures_util::{pin_mut, TryStreamExt as _};
use tokio::io::AsyncWriteExt;

/// Transaction bundle dumper from Arweave network
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    /// Transaction ID to fetch
    #[arg(short, long)]
    transaction_id: Base64,

    /// JSON output file name. Default name: <transaction_ID>.json
    #[arg(long, short)]
    output_file: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args {
        transaction_id,
        output_file,
    } = Args::try_parse()?;

    let arweave_client = arweave::Client::new()?;

    let tx = arweave_client.fetch_transaction(&transaction_id).await?;

    if !tx.is_bundle() {
        return Err(anyhow::anyhow!(
            "Given transacion by ID is not ANS-104 bundle"
        ));
    }

    //TODO: instead of reading whole body - make a stream consumable by async read
    let data = arweave_client
        .fetch_transaction_data(&transaction_id)
        .await?;

    let data_item_stream = bundle::ans104_bundle_data_item_stream(data.0.as_slice());
    pin_mut!(data_item_stream);

    let filename = output_file.unwrap_or_else(|| format! {"{transaction_id}.json"});
    let writer = tokio::fs::File::create(&filename).await?;
    let mut buf_writer = tokio::io::BufWriter::new(writer);

    while let Some(data_item) = data_item_stream.try_next().await? {
        // TODO - we need async json writer
        buf_writer
            .write_all(&serde_json::to_vec_pretty(&data_item)?)
            .await?;
    }

    buf_writer.flush().await?;
    println!("Bundle data stored in: {filename}");
    Ok(())
}
