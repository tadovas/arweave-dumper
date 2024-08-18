use arweave_dumper::{arweave, bundle};
use arweave_rs::crypto::base64::Base64;
use clap::{command, Parser};

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
    //TODO: instead returning a list of DataItems return a stream of data items
    let data_items = bundle::read_ans104_bundle(data.0.as_slice()).await?;

    let filename = output_file.unwrap_or_else(|| format! {"{transaction_id}.json"});
    //TODO: explore option to sink stream of data items into file - effectively making pull based parsing
    tokio::fs::write(&filename, serde_json::to_string_pretty(&data_items)?).await?;

    println!("Bundle data stored in: {filename}");
    Ok(())
}
