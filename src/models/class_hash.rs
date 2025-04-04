use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoDbClient};
use starknet_api::core::{ClassHash, ContractAddress};
use starknet_types_core::felt::Felt;
use std::str::FromStr;
use std::sync::Arc;

use super::{
    table::{DynamoTable, TableSchema},
    ToDDBString,
};

pub struct ClassHashTable;

impl DynamoTable for ClassHashTable {
    fn schema() -> TableSchema {
        TableSchema {
            name: "blockifier_ddb_starknet_class_hashes".to_string(),
            hash_key: "contract_address".to_string(),
            sort_key: None,
        }
    }
}

impl ClassHashTable {
    pub async fn get_class_hash_value(
        client: Arc<DynamoDbClient>,
        contract_address: &ContractAddress,
    ) -> anyhow::Result<Option<ClassHash>> {
        let result = client
            .get_item()
            .table_name(Self::schema().name)
            .key(
                "contract_address",
                AttributeValue::S(contract_address.to_ddb_string()),
            )
            .send()
            .await?;

        if let Some(item) = result.item {
            if let Some(value) = item.get("class_hash") {
                if let Ok(hash_str) = value.as_s() {
                    return Ok(Some(ClassHash(Felt::from_str(hash_str)?)));
                }
            }
        }

        Ok(None)
    }
}
