use log::info;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::{client, Client as DynamoDbClient};
use blockifier::state::global_cache::CachedClass;
use cairo_lang_starknet_classes::casm_contract_class::CasmContractClass;
use starknet_api::contract_class::SierraVersion;
use starknet_api::core::{ClassHash, CompiledClassHash, ContractAddress, Nonce};
use starknet_api::state::{SierraContractClass, StateDiff, StorageKey};
use starknet_types_core::felt::Felt;

use blockifier::execution::contract_class::{CompiledClassV1, RunnableCompiledClass};
use blockifier::state::cached_state::{CommitmentStateDiff, StateMaps, StorageEntry};
use blockifier::state::errors::StateError;
use blockifier::state::state_api::{StateReader, StateResult};
use tokio::runtime::Runtime;

use crate::models::{
    ClassHashTable, CompiledClassTable, DynamoTable, NonceTable, StorageTable, ToDDBString,
};

const TABLE_NAME_COMPILED_CLASSES: &str = "starknet_compiled_classes";

/// A DynamoDB-based implementation of `StateReader`.
#[derive(Debug, Clone)]
pub struct DynamoDbStateReader {
    client: Arc<DynamoDbClient>,
    class_hash_to_class: HashMap<ClassHash, RunnableCompiledClass>,
}

impl DynamoDbStateReader {
    pub fn new(
        client: Arc<DynamoDbClient>,
        class_hash_to_class: HashMap<ClassHash, RunnableCompiledClass>,
    ) -> Self {
        Self {
            client,
            class_hash_to_class,
        }
    }

    // Storing classes in memory for now. The actual DynamoDB implementation would cache class
    // anyways so this should not affect the benchmark a lot.
    fn get_compiled_class(&self, class_hash: ClassHash) -> StateResult<RunnableCompiledClass> {
        let contract_class = self.class_hash_to_class.get(&class_hash).cloned();
        match contract_class {
            Some(contract_class) => Ok(contract_class),
            _ => Err(StateError::UndeclaredClassHash(class_hash)),
        }
    }
}

impl StateReader for DynamoDbStateReader {
    fn get_storage_at(
        &self,
        contract_address: ContractAddress,
        key: StorageKey,
    ) -> StateResult<Felt> {
        println!(
            "Reading storage at contract {} with key {}",
            contract_address,
            key.0.key(),
        );
        let client = self.client.clone();
        let contract_address = contract_address.clone();
        let key = key.clone();

        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let value = StorageTable::get_storage_value(client, &contract_address, &key)
                    .await
                    .map_err(|e| {
                        StateError::StateReadError(format!("Failed to read storage: {}", e))
                    })?
                    .unwrap_or_default();
                println!("Retrieved storage value: {}", value);
                Ok(value)
            })
        })
        .join()
        .expect("Thread panicked")
    }

    fn get_nonce_at(&self, contract_address: ContractAddress) -> StateResult<Nonce> {
        println!("Reading nonce at contract {}", contract_address);
        let client = self.client.clone();
        let contract_address = contract_address.clone();

        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let nonce = NonceTable::get_nonce_value(client, &contract_address)
                    .await
                    .map_err(|e| {
                        StateError::StateReadError(format!("Failed to read nonce: {}", e))
                    })?
                    .unwrap_or_default();
                println!("Retrieved nonce: {}", nonce);
                Ok(nonce)
            })
        })
        .join()
        .expect("Thread panicked")
    }

    fn get_class_hash_at(&self, contract_address: ContractAddress) -> StateResult<ClassHash> {
        println!("Reading class hash at contract {}", contract_address);
        let client = self.client.clone();
        let contract_address = contract_address.clone();

        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let class_hash = ClassHashTable::get_class_hash_value(client, &contract_address)
                    .await
                    .map_err(|e| {
                        StateError::StateReadError(format!("Failed to read class hash: {}", e))
                    })?
                    .unwrap_or_default();
                println!("Retrieved class hash: {}", class_hash);
                Ok(class_hash)
            })
        })
        .join()
        .expect("Thread panicked")
    }

    fn get_compiled_class_hash(&self, class_hash: ClassHash) -> StateResult<CompiledClassHash> {
        println!("Reading compiled class hash for class hash {}", class_hash);
        let client = self.client.clone();
        let class_hash = class_hash.clone();

        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let compiled_hash =
                    CompiledClassTable::get_compiled_class_hash_value(client, &class_hash)
                        .await
                        .map_err(|e| {
                            StateError::StateReadError(format!(
                                "Failed to read compiled class hash: {}",
                                e
                            ))
                        })?
                        .unwrap_or_default();
                println!("Retrieved compiled class hash: {}", compiled_hash);
                Ok(compiled_hash)
            })
        })
        .join()
        .expect("Thread panicked")
    }

    fn get_compiled_class(&self, class_hash: ClassHash) -> StateResult<RunnableCompiledClass> {
        println!("Reading compiled class for class hash {}", class_hash);
        let result = self.get_compiled_class(class_hash);
        println!(
            "Retrieved compiled class: {}",
            if result.is_ok() {
                "Success"
            } else {
                "Not found"
            }
        );
        result
    }
}

pub async fn update_state_diff(
    client: Arc<DynamoDbClient>,
    state_diff: CommitmentStateDiff,
) -> anyhow::Result<()> {
    // Collect all write requests for the transaction
    let mut write_requests = Vec::new();

    // Handle storage updates
    for (contract_address, storage_updates) in state_diff.storage_updates {
        for (key, value) in storage_updates {
            println!(
                "Updating storage for contract {} with key DDB {} and key {:?} and value {}",
                contract_address,
                key.to_ddb_string(),
                key,
                value.to_ddb_string()
            );
            write_requests.push(
                aws_sdk_dynamodb::types::TransactWriteItem::builder()
                    .put(
                        aws_sdk_dynamodb::types::Put::builder()
                            .table_name(StorageTable::schema().name)
                            .item(
                                "contract_address",
                                AttributeValue::S(contract_address.to_ddb_string()),
                            )
                            .item("storage_key", AttributeValue::S(key.to_ddb_string()))
                            .item("value", AttributeValue::S(value.to_ddb_string()))
                            .build()?,
                    )
                    .build(),
            );
        }
    }

    // Handle nonce updates
    for (contract_address, nonce) in state_diff.address_to_nonce {
        write_requests.push(
            aws_sdk_dynamodb::types::TransactWriteItem::builder()
                .put(
                    aws_sdk_dynamodb::types::Put::builder()
                        .table_name(NonceTable::schema().name)
                        .item(
                            "contract_address",
                            AttributeValue::S(contract_address.to_ddb_string()),
                        )
                        .item("nonce", AttributeValue::S(nonce.0.to_ddb_string()))
                        .build()?,
                )
                .build(),
        );
    }

    // Handle class hash updates
    for (contract_address, class_hash) in state_diff.address_to_class_hash {
        write_requests.push(
            aws_sdk_dynamodb::types::TransactWriteItem::builder()
                .put(
                    aws_sdk_dynamodb::types::Put::builder()
                        .table_name(ClassHashTable::schema().name)
                        .item(
                            "contract_address",
                            AttributeValue::S(contract_address.to_ddb_string()),
                        )
                        .item("class_hash", AttributeValue::S(class_hash.to_ddb_string()))
                        .build()?,
                )
                .build(),
        );
    }

    // Handle compiled class hash updates
    for (class_hash, compiled_class_hash) in state_diff.class_hash_to_compiled_class_hash {
        write_requests.push(
            aws_sdk_dynamodb::types::TransactWriteItem::builder()
                .put(
                    aws_sdk_dynamodb::types::Put::builder()
                        .table_name(CompiledClassTable::schema().name)
                        .item("class_hash", AttributeValue::S(class_hash.to_ddb_string()))
                        .item(
                            "compiled_class_hash",
                            AttributeValue::S(compiled_class_hash.to_ddb_string()),
                        )
                        .build()?,
                )
                .build(),
        );
    }

    // Check if we have more than 100 items
    if write_requests.len() > 100 {
        return Err(anyhow::anyhow!(
            "Too many items to update in one transaction: {}. DynamoDB limit is 25",
            write_requests.len()
        ));
    }

    // Execute the transaction if we have any updates
    if !write_requests.is_empty() {
        client
            .transact_write_items()
            .set_transact_items(Some(write_requests))
            .send()
            .await?;
    }

    Ok(())
}
