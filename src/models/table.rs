use aws_sdk_dynamodb::{
    error::SdkError,
    types::{AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType},
    Client as DynamoDbClient,
};
use log::info;
use starknet_api::{
    core::{ClassHash, CompiledClassHash, ContractAddress, Nonce},
    state::StorageKey,
};
use starknet_types_core::felt::Felt;
use std::sync::Arc;

#[derive(Clone)]
pub struct TableSchema {
    pub name: String,
    pub hash_key: String,
    pub sort_key: Option<String>,
}

pub trait DynamoTable {
    fn schema() -> TableSchema;

    async fn deploy(client: Arc<DynamoDbClient>) -> Result<(), Box<dyn std::error::Error>> {
        let schema = Self::schema();
        let mut key_schema = vec![KeySchemaElement::builder()
            .attribute_name(&schema.hash_key)
            .key_type(KeyType::Hash)
            .build()?];

        let mut attribute_definitions = vec![AttributeDefinition::builder()
            .attribute_name(&schema.hash_key)
            .attribute_type(ScalarAttributeType::S)
            .build()?];

        if let Some(sort_key) = &schema.sort_key {
            key_schema.push(
                KeySchemaElement::builder()
                    .attribute_name(sort_key)
                    .key_type(KeyType::Range)
                    .build()?,
            );
            let attribute_type = if sort_key == "timestamp" {
                ScalarAttributeType::N
            } else {
                ScalarAttributeType::S
            };
            attribute_definitions.push(
                AttributeDefinition::builder()
                    .attribute_name(sort_key)
                    .attribute_type(attribute_type)
                    .build()?,
            );
        }

        client
            .create_table()
            .table_name(&schema.name)
            .set_key_schema(Some(key_schema))
            .set_attribute_definitions(Some(attribute_definitions))
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await?;

        info!("Created table: {}", schema.name);
        Ok(())
    }

    async fn delete(client: Arc<DynamoDbClient>) -> Result<(), Box<dyn std::error::Error>> {
        let schema = Self::schema();
        let name = schema.name.clone();
        match client.delete_table().table_name(&name).send().await {
            Ok(_) => (),
            Err(e) => match e {
                SdkError::ServiceError(err) => {
                    if err.err().is_resource_not_found_exception() {
                        info!("Table {} does not exist", name);
                        return Ok(());
                    }
                    return Err(Box::new(err.into_err()));
                }
                _ => return Err(Box::new(e)),
            },
        }

        info!("Deleted table: {}", name);
        Ok(())
    }
}

pub trait ToDDBString {
    fn to_ddb_string(&self) -> String;
}

impl ToDDBString for Felt {
    fn to_ddb_string(&self) -> String {
        format!("0x{:x}", self)
    }
}

impl ToDDBString for StorageKey {
    fn to_ddb_string(&self) -> String {
        format!("0x{:x}", self.0.key())
    }
}

impl ToDDBString for ClassHash {
    fn to_ddb_string(&self) -> String {
        format!("0x{:x}", self.0)
    }
}

impl ToDDBString for ContractAddress {
    fn to_ddb_string(&self) -> String {
        format!("0x{:x}", self.0.key())
    }
}

impl ToDDBString for Nonce {
    fn to_ddb_string(&self) -> String {
        format!("0x{:x}", self.0)
    }
}

impl ToDDBString for CompiledClassHash {
    fn to_ddb_string(&self) -> String {
        format!("0x{:x}", self.0)
    }
}
