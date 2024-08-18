# Arweave transaction bundle parser
Fetches given bundle transaction from arweave network and stores  [ANS-104 spec](https://github.com/ArweaveTeam/arweave-standards/blob/master/ans/ANS-104.md) DataItems into json file. Checks if given transaction is really a bundle.
## Usage:
```
cargo run -- --help
Transaction bundle dumper from Arweave network

Usage: main [OPTIONS] --transaction-id <TRANSACTION_ID>

Options:
  -t, --transaction-id <TRANSACTION_ID>  Transaction ID to fetch
      --base-url <BASE_URL>              Arweave API base url [default: https://arweave.net/]
  -o, --output-file <OUTPUT_FILE>        JSON output file name. Default name: <transaction_ID>.json
  -h, --help                             Print help                            Print help

```
## Example:
```
cargo run -- -t o0le1MwgKBVIrh3fqJnWCGNa4N0rDd2WDm15jjGIvBo
Bundle data stored in: o0le1MwgKBVIrh3fqJnWCGNa4N0rDd2WDm15jjGIvBo.json
```
JSON file should contain 0 to N DataItems. Regarding VERY large bundles - the only objects which are fully materialized in memory are data chunks theirselves (transaction data is downloaded by chunks) - one at the time, and DataItem itself (also fully serialized into JSON object). JSON array itself is written asynchronously to underlying file.

## Known issues:
Fails with transaction `VTzmxeLxtJnxuTJCkm9WmEgnL1ZTlL6KBRuhub83QXE`
```
Parsing data
Error: DataItem read

Caused by:
    0: Avro tags parse
    1: Failed to read bytes for decoding variable length integer
    2: failed to fill whole buffer
```
Need to further dig in through Apache AVRO decoder.
