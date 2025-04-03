use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoDbClient};
use starknet_api::core::ContractAddress;
use starknet_api::state::StorageKey;
use starknet_types_core::felt::Felt;
use std::str::FromStr;
use std::sync::Arc;

use super::{
    table::{DynamoTable, TableSchema},
    ToDDBString,
};

pub struct StorageTable;

impl DynamoTable for StorageTable {
    fn schema() -> TableSchema {
        TableSchema {
            name: "starknet_storage".to_string(),
            hash_key: "contract_address".to_string(),
            sort_key: Some("storage_key".to_string()),
        }
    }
}

impl StorageTable {
    pub async fn get_storage_value(
        client: Arc<DynamoDbClient>,
        contract_address: &ContractAddress,
        key: &StorageKey,
    ) -> anyhow::Result<Option<Felt>> {
        let result = client
            .get_item()
            .table_name(Self::schema().name)
            .key(
                "contract_address",
                AttributeValue::S(contract_address.to_ddb_string()),
            )
            .key(
                "storage_key",
                AttributeValue::S(key.0.key().to_ddb_string()),
            )
            .send()
            .await?;

        if let Some(item) = result.item {
            if let Some(value) = item.get("value") {
                if let Ok(felt_str) = value.as_s() {
                    return Ok(Some(Felt::from_str(felt_str)?));
                }
            }
        }

        Ok(None)
    }
}
