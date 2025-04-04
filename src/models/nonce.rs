use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoDbClient};
use log::info;
use starknet_api::core::{ContractAddress, Nonce};
use starknet_types_core::felt::Felt;
use std::str::FromStr;
use std::sync::Arc;

use super::{
    table::{DynamoTable, TableSchema},
    ToDDBString,
};

pub struct NonceTable;

impl DynamoTable for NonceTable {
    fn schema() -> TableSchema {
        TableSchema {
            name: "blockifier_ddb_starknet_nonces".to_string(),
            hash_key: "contract_address".to_string(),
            sort_key: None,
        }
    }
}

impl NonceTable {
    pub async fn get_nonce_value(
        client: Arc<DynamoDbClient>,
        contract_address: &ContractAddress,
    ) -> anyhow::Result<Option<Nonce>> {
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
            if let Some(value) = item.get("nonce") {
                if let Ok(nonce_str) = value.as_s() {
                    return Ok(Some(Nonce(Felt::from_str(nonce_str)?)));
                }
            }
        }

        Ok(None)
    }
}
