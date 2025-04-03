use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use log::{debug, info};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

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

use crate::metrics::StateMetrics;
use crate::models::{
    ClassHashTable, CompiledClassTable, DynamoTable, NonceTable, StorageTable, ToDDBString,
};
use crate::read_tracker::{ReadTracker, ReadValue};
/// A DynamoDB-based implementation of `StateReader`.
#[derive(Debug, Clone)]
pub struct DynamoDbStateReader {
    client: Arc<DynamoDbClient>,
    class_hash_to_class: HashMap<ClassHash, RunnableCompiledClass>,
    pub metrics: Arc<StateMetrics>,
    pub read_tracker: Arc<ReadTracker>,
}

impl DynamoDbStateReader {
    pub fn new(
        client: Arc<DynamoDbClient>,
        class_hash_to_class: HashMap<ClassHash, RunnableCompiledClass>,
        metrics: Arc<StateMetrics>,
        read_tracker: Arc<ReadTracker>,
    ) -> Self {
        Self {
            client,
            class_hash_to_class,
            metrics,
            read_tracker,
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

/// Helper function to create a condition check for a DynamoDB item
fn create_condition_check(
    table_name: String,
    key_names: &[&str],
    key_values: &[String],
    value_field: &str,
    read_value: &ReadValue<impl ToDDBString>,
) -> anyhow::Result<aws_sdk_dynamodb::types::TransactWriteItem> {
    assert_eq!(
        key_names.len(),
        key_values.len(),
        "Number of key names must match number of key values"
    );

    let mut builder = aws_sdk_dynamodb::types::ConditionCheck::builder().table_name(table_name);

    // Add all key attributes
    for (name, value) in key_names.iter().zip(key_values.iter()) {
        builder = builder.key(*name, AttributeValue::S(value.clone()));
    }

    match read_value {
        ReadValue::NotPresent => {
            builder = builder
                .condition_expression(format!("attribute_not_exists(#attr_name)"))
                .expression_attribute_names("#attr_name", value_field);
        }
        ReadValue::Present(value) => {
            builder = builder
                .condition_expression("#attr_name = :val")
                .expression_attribute_names("#attr_name", value_field)
                .expression_attribute_values(":val", AttributeValue::S(value.to_ddb_string()));
        }
    }

    Ok(aws_sdk_dynamodb::types::TransactWriteItem::builder()
        .condition_check(builder.build()?)
        .build())
}

/// Helper function to create a put request for a DynamoDB item, optionally with a condition check
fn create_put_request(
    table_name: String,
    key_names: &[&str],
    key_values: &[String],
    value_field: &str,
    value: impl ToDDBString,
    read_value: Option<&ReadValue<impl ToDDBString>>,
) -> anyhow::Result<aws_sdk_dynamodb::types::TransactWriteItem> {
    assert_eq!(
        key_names.len(),
        key_values.len(),
        "Number of key names must match number of key values"
    );

    let mut builder = aws_sdk_dynamodb::types::Put::builder().table_name(table_name);

    // Add all key attributes
    for (name, value) in key_names.iter().zip(key_values.iter()) {
        builder = builder.item(*name, AttributeValue::S(value.clone()));
    }

    // Add the value field
    builder = builder.item(value_field, AttributeValue::S(value.to_ddb_string()));

    // Add condition check if read_value is provided
    if let Some(read_value) = read_value {
        match read_value {
            ReadValue::NotPresent => {
                builder = builder
                    .condition_expression("attribute_not_exists(#attr_name)")
                    .expression_attribute_names("#attr_name", value_field);
            }
            ReadValue::Present(expected_value) => {
                builder = builder
                    .condition_expression("#attr_name = :val")
                    .expression_attribute_names("#attr_name", value_field)
                    .expression_attribute_values(
                        ":val",
                        AttributeValue::S(expected_value.to_ddb_string()),
                    );
            }
        }
    }

    Ok(aws_sdk_dynamodb::types::TransactWriteItem::builder()
        .put(builder.build()?)
        .build())
}

impl StateReader for DynamoDbStateReader {
    fn get_storage_at(
        &self,
        contract_address: ContractAddress,
        key: StorageKey,
    ) -> StateResult<Felt> {
        let client = self.client.clone();
        let contract_address = contract_address.clone();
        let key = key.clone();
        let metrics = self.metrics.clone();
        let read_tracker = self.read_tracker.clone();

        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let start = Instant::now();
                let result = StorageTable::get_storage_value(client, &contract_address, &key)
                    .await
                    .map_err(|e| {
                        StateError::StateReadError(format!("Failed to read storage: {}", e))
                    })?;

                // Track the read
                read_tracker.track_storage_read(contract_address, key, result);

                metrics
                    .storage_reads
                    .lock()
                    .unwrap()
                    .add_duration(start.elapsed());
                Ok(result.unwrap_or_default())
            })
        })
        .join()
        .expect("Thread panicked")
    }

    fn get_nonce_at(&self, contract_address: ContractAddress) -> StateResult<Nonce> {
        let client = self.client.clone();
        let contract_address = contract_address.clone();
        let metrics = self.metrics.clone();
        let read_tracker = self.read_tracker.clone();

        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let start = Instant::now();
                let result = NonceTable::get_nonce_value(client, &contract_address)
                    .await
                    .map_err(|e| {
                        StateError::StateReadError(format!("Failed to read nonce: {}", e))
                    })?;

                // Track the read
                read_tracker.track_nonce_read(contract_address, result);

                metrics
                    .nonce_reads
                    .lock()
                    .unwrap()
                    .add_duration(start.elapsed());
                Ok(result.unwrap_or_default())
            })
        })
        .join()
        .expect("Thread panicked")
    }

    fn get_class_hash_at(&self, contract_address: ContractAddress) -> StateResult<ClassHash> {
        let client = self.client.clone();
        let contract_address = contract_address.clone();
        let metrics = self.metrics.clone();
        let read_tracker = self.read_tracker.clone();

        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let start = Instant::now();
                let result = ClassHashTable::get_class_hash_value(client, &contract_address)
                    .await
                    .map_err(|e| {
                        StateError::StateReadError(format!("Failed to read class hash: {}", e))
                    })?;

                // Track the read
                read_tracker.track_class_hash_read(contract_address, result);

                metrics
                    .class_hash_reads
                    .lock()
                    .unwrap()
                    .add_duration(start.elapsed());
                Ok(result.unwrap_or_default())
            })
        })
        .join()
        .expect("Thread panicked")
    }

    fn get_compiled_class_hash(&self, class_hash: ClassHash) -> StateResult<CompiledClassHash> {
        let client = self.client.clone();
        let class_hash = class_hash.clone();
        let metrics = self.metrics.clone();
        let read_tracker = self.read_tracker.clone();

        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let start = Instant::now();
                let result = CompiledClassTable::get_compiled_class_hash_value(client, &class_hash)
                    .await
                    .map_err(|e| {
                        StateError::StateReadError(format!(
                            "Failed to read compiled class hash: {}",
                            e
                        ))
                    })?;

                // Track the read
                read_tracker.track_compiled_class_hash_read(class_hash, result);

                metrics
                    .compiled_class_hash_reads
                    .lock()
                    .unwrap()
                    .add_duration(start.elapsed());
                Ok(result.unwrap_or_default())
            })
        })
        .join()
        .expect("Thread panicked")
    }

    fn get_compiled_class(&self, class_hash: ClassHash) -> StateResult<RunnableCompiledClass> {
        let result = self.get_compiled_class(class_hash);
        result
    }
}

pub async fn update_state_diff(
    client: Arc<DynamoDbClient>,
    state_diff: CommitmentStateDiff,
    metrics: Arc<StateMetrics>,
    read_tracker: Arc<ReadTracker>,
    allow_chunking: bool,
) -> anyhow::Result<()> {
    let start = Instant::now();
    debug!("Starting state diff update...");

    // Collect all write requests for the transaction
    let mut all_requests = Vec::new();

    // Handle storage updates
    debug!("Processing storage updates...");
    for (contract_address, storage_updates) in state_diff.storage_updates {
        if contract_address == ContractAddress::from(2_u8) {
            // 0x2 is a special address that stores compressed state diffs
            // Skipping this for now as it hurts parallelisation and can be optimised later
            continue;
        }
        for (key, value) in storage_updates {
            let key_values = vec![contract_address.to_ddb_string(), key.to_ddb_string()];
            debug!(
                "Storage update - Contract: {}, Key: {:?}, Value: {}",
                contract_address, key, value
            );

            // First check if there's a read value
            let maybe_read = if read_tracker.has_storage_read(&contract_address, &key).await {
                debug!(
                    "Found existing storage read for contract {} and key {:?}",
                    contract_address, key
                );
                read_tracker
                    .remove_storage_read(&contract_address, &key)
                    .await
            } else {
                None
            };

            all_requests.push(create_put_request(
                StorageTable::schema().name,
                &["contract_address", "storage_key"],
                &key_values,
                "value",
                value,
                maybe_read.as_ref(),
            )?);
        }
    }

    // Handle nonce updates
    debug!("Processing nonce updates...");
    for (contract_address, nonce) in state_diff.address_to_nonce {
        let key_values = vec![contract_address.to_ddb_string()];
        debug!(
            "Nonce update - Contract: {}, New nonce: {}",
            contract_address, nonce
        );

        // First check if there's a read value
        let maybe_read = if read_tracker.has_nonce_read(&contract_address).await {
            debug!(
                "Found existing nonce read for contract {}",
                contract_address
            );
            read_tracker.remove_nonce_read(&contract_address).await
        } else {
            None
        };

        all_requests.push(create_put_request(
            NonceTable::schema().name,
            &["contract_address"],
            &key_values,
            "nonce",
            nonce,
            maybe_read.as_ref(),
        )?);
    }

    // Handle class hash updates
    debug!("Processing class hash updates...");
    for (contract_address, class_hash) in state_diff.address_to_class_hash {
        let key_values = vec![contract_address.to_ddb_string()];
        debug!(
            "Class hash update - Contract: {}, New hash: {}",
            contract_address, class_hash
        );

        // First check if there's a read value
        let maybe_read = if read_tracker.has_class_hash_read(&contract_address).await {
            debug!(
                "Found existing class hash read for contract {}",
                contract_address
            );
            read_tracker.remove_class_hash_read(&contract_address).await
        } else {
            None
        };

        all_requests.push(create_put_request(
            ClassHashTable::schema().name,
            &["contract_address"],
            &key_values,
            "class_hash",
            class_hash,
            maybe_read.as_ref(),
        )?);
    }

    // Handle compiled class hash updates
    debug!("Processing compiled class hash updates...");
    for (class_hash, compiled_class_hash) in state_diff.class_hash_to_compiled_class_hash {
        let key_values = vec![class_hash.to_ddb_string()];
        debug!(
            "Compiled class hash update - Class hash: {}, Compiled hash: {}",
            class_hash, compiled_class_hash
        );

        // First check if there's a read value
        let maybe_read = if read_tracker.has_compiled_class_hash_read(&class_hash).await {
            debug!(
                "Found existing compiled class hash read for class hash {}",
                class_hash
            );
            read_tracker
                .remove_compiled_class_hash_read(&class_hash)
                .await
        } else {
            None
        };

        all_requests.push(create_put_request(
            CompiledClassTable::schema().name,
            &["class_hash"],
            &key_values,
            "compiled_class_hash",
            compiled_class_hash,
            maybe_read.as_ref(),
        )?);
    }

    // Process any remaining reads that weren't handled during puts
    debug!("Processing remaining reads...");

    // Storage reads
    let mut storage_reads = read_tracker.storage_reads.lock().await;
    debug!("Processing {} remaining storage reads", storage_reads.len());
    for (contract_address, storage_map) in storage_reads.drain() {
        for (key, read_value) in storage_map {
            debug!(
                "Remaining storage read - Contract: {}, Key: {:?}, Value: {:?}",
                contract_address, key, read_value
            );
            let key_values = vec![contract_address.to_ddb_string(), key.to_ddb_string()];
            all_requests.push(create_condition_check(
                StorageTable::schema().name,
                &["contract_address", "storage_key"],
                &key_values,
                "value",
                &read_value,
            )?);
        }
    }
    drop(storage_reads);

    // Nonce reads
    let mut nonce_reads = read_tracker.nonce_reads.lock().await;
    debug!("Processing {} remaining nonce reads", nonce_reads.len());
    for (contract_address, read_value) in nonce_reads.drain() {
        debug!(
            "Remaining nonce read - Contract: {}, Value: {:?}",
            contract_address, read_value
        );
        let key_values = vec![contract_address.to_ddb_string()];
        all_requests.push(create_condition_check(
            NonceTable::schema().name,
            &["contract_address"],
            &key_values,
            "nonce",
            &read_value,
        )?);
    }
    drop(nonce_reads);

    // Class hash reads
    let mut class_hash_reads = read_tracker.class_hash_reads.lock().await;
    debug!(
        "Processing {} remaining class hash reads",
        class_hash_reads.len()
    );
    for (contract_address, read_value) in class_hash_reads.drain() {
        debug!(
            "Remaining class hash read - Contract: {}, Value: {:?}",
            contract_address, read_value
        );
        let key_values = vec![contract_address.to_ddb_string()];
        all_requests.push(create_condition_check(
            ClassHashTable::schema().name,
            &["contract_address"],
            &key_values,
            "class_hash",
            &read_value,
        )?);
    }
    drop(class_hash_reads);

    // Compiled class hash reads
    let mut compiled_class_hash_reads = read_tracker.compiled_class_hash_reads.lock().await;
    debug!(
        "Processing {} remaining compiled class hash reads",
        compiled_class_hash_reads.len()
    );
    for (class_hash, read_value) in compiled_class_hash_reads.drain() {
        debug!(
            "Remaining compiled class hash read - Class hash: {}, Value: {:?}",
            class_hash, read_value
        );
        let key_values = vec![class_hash.to_ddb_string()];
        all_requests.push(create_condition_check(
            CompiledClassTable::schema().name,
            &["class_hash"],
            &key_values,
            "compiled_class_hash",
            &read_value,
        )?);
    }
    drop(compiled_class_hash_reads);

    // Check if we have more than 100 items
    debug!("Total number of requests: {}", all_requests.len());
    if all_requests.len() > 100 && !allow_chunking {
        return Err(anyhow::anyhow!(
            "Too many items to update in one transaction: {}. DynamoDB limit is 100",
            all_requests.len()
        ));
    }

    // Execute the transaction if we have any updates
    if !all_requests.is_empty() {
        info!(
            "Executing DynamoDB transaction with {} requests",
            all_requests.len()
        );
        for chunk in all_requests.chunks(100) {
            client
                .transact_write_items()
                .set_transact_items(Some(chunk.to_vec()))
                .send()
                .await?;
        }
        debug!("DynamoDB transaction completed successfully");
    } else {
        debug!("No requests to process, skipping DynamoDB transaction");
    }

    // Assert that all reads have been processed
    if !read_tracker.is_empty().await {
        panic!(
            "Read tracker not empty after processing all reads: {:?}",
            read_tracker
        );
    }

    debug!("Read tracker is empty");

    metrics
        .state_updates
        .lock()
        .unwrap()
        .add_duration(start.elapsed());
    debug!("State diff update completed in {:?}", start.elapsed());
    Ok(())
}
