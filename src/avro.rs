use std::sync::LazyLock;

use apache_avro::Schema;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct BundleTag {
    pub name: String,
    pub value: String,
}

const SCHEMA_STR: &str = r#"
{
  "type": "array",
  "items": {
    "type": "record",
    "name": "Tag",
    "fields": [
      { "name": "name", "type": "string" },
      { "name": "value", "type": "string" }
    ]
  }
}
"#;

static SCHEMA_INSTANCE: LazyLock<Schema> =
    LazyLock::new(|| apache_avro::Schema::parse_str(SCHEMA_STR).expect("should parse"));

pub fn parse_tag_list<R>(mut reader: R) -> anyhow::Result<Vec<BundleTag>>
where
    R: std::io::Read,
{
    let value =
        apache_avro::from_avro_datum(&SCHEMA_INSTANCE, &mut reader, Some(&SCHEMA_INSTANCE))?;
    let tags = apache_avro::from_value(&value)?;
    Ok(tags)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_serialized_tags_read() {
        let hex_str = include_str!("../res/first_item_tags.hex");
        let data = hex::decode(hex_str).expect("should parse");

        let tags = parse_tag_list(data.as_slice()).expect("should parse");
        assert_eq!(tags.len(), 18);
    }
}
