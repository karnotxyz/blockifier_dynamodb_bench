use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoDbClient};
use starknet_api::core::{ClassHash, CompiledClassHash};
use starknet_types_core::felt::Felt;
use std::str::FromStr;
use std::sync::Arc;

use super::{table::{DynamoTable, TableSchema}, ToDDBString};

pub struct CompiledClassTable;

impl DynamoTable for CompiledClassTable {
    fn schema() -> TableSchema {
        TableSchema {
            name: "starknet_compiled_classes".to_string(),
            hash_key: "class_hash".to_string(),
            sort_key: None,
        }
    }
}

impl CompiledClassTable {
    pub async fn get_compiled_class_hash_value(
        client: Arc<DynamoDbClient>,
        class_hash: &ClassHash,
    ) -> anyhow::Result<Option<CompiledClassHash>> {
        let result = client
            .get_item()
            .table_name(Self::schema().name)
            .key("class_hash", AttributeValue::S(class_hash.to_ddb_string()))
            .send()
            .await?;

        if let Some(item) = result.item {
            if let Some(value) = item.get("compiled_class_hash") {
                if let Ok(hash_str) = value.as_s() {
                    return Ok(Some(CompiledClassHash(Felt::from_str(hash_str)?)));
                }
            }
        }

        Ok(None)
    }
}
