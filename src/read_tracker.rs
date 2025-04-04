use log::debug;
use starknet_api::core::{ClassHash, CompiledClassHash, ContractAddress, Nonce};
use starknet_api::state::StorageKey;
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub enum ReadValue<T> {
    NotPresent,
    Present(T),
}

#[derive(Debug, Default)]
pub struct ReadTracker {
    // Maps contract_address -> storage_key -> value
    pub storage_reads: Mutex<HashMap<ContractAddress, HashMap<StorageKey, ReadValue<Felt>>>>,
    // Maps contract_address -> nonce
    pub nonce_reads: Mutex<HashMap<ContractAddress, ReadValue<Nonce>>>,
    // Maps contract_address -> class_hash
    pub class_hash_reads: Mutex<HashMap<ContractAddress, ReadValue<ClassHash>>>,
    // Maps class_hash -> compiled_class_hash
    pub compiled_class_hash_reads: Mutex<HashMap<ClassHash, ReadValue<CompiledClassHash>>>,
}

impl ReadTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn track_storage_read(
        &self,
        contract_address: ContractAddress,
        key: StorageKey,
        value: Option<Felt>,
    ) {
        let mut storage_reads = self.storage_reads.lock().await;
        let read_value = match value {
            Some(v) => ReadValue::Present(v),
            None => ReadValue::NotPresent,
        };
        storage_reads
            .entry(contract_address)
            .or_default()
            .insert(key, read_value);
    }

    pub async fn track_nonce_read(&self, contract_address: ContractAddress, value: Option<Nonce>) {
        let mut nonce_reads = self.nonce_reads.lock().await;
        let read_value = match value {
            Some(v) => ReadValue::Present(v),
            None => ReadValue::NotPresent,
        };
        nonce_reads.insert(contract_address, read_value);
    }

    pub async fn track_class_hash_read(
        &self,
        contract_address: ContractAddress,
        value: Option<ClassHash>,
    ) {
        let mut class_hash_reads = self.class_hash_reads.lock().await;
        let read_value = match value {
            Some(v) => ReadValue::Present(v),
            None => ReadValue::NotPresent,
        };
        class_hash_reads.insert(contract_address, read_value);
    }

    pub async fn track_compiled_class_hash_read(
        &self,
        class_hash: ClassHash,
        value: Option<CompiledClassHash>,
    ) {
        let mut compiled_class_hash_reads = self.compiled_class_hash_reads.lock().await;
        let read_value = match value {
            Some(v) => ReadValue::Present(v),
            None => ReadValue::NotPresent,
        };
        compiled_class_hash_reads.insert(class_hash, read_value);
    }

    pub async fn remove_storage_read(
        &self,
        contract_address: &ContractAddress,
        key: &StorageKey,
    ) -> Option<ReadValue<Felt>> {
        let mut storage_reads = self.storage_reads.lock().await;
        if let Some(contract_reads) = storage_reads.get_mut(contract_address) {
            let value = contract_reads.remove(key);
            if contract_reads.is_empty() {
                storage_reads.remove(contract_address);
            }
            value
        } else {
            None
        }
    }

    pub async fn remove_nonce_read(
        &self,
        contract_address: &ContractAddress,
    ) -> Option<ReadValue<Nonce>> {
        let mut nonce_reads = self.nonce_reads.lock().await;
        nonce_reads.remove(contract_address)
    }

    pub async fn remove_class_hash_read(
        &self,
        contract_address: &ContractAddress,
    ) -> Option<ReadValue<ClassHash>> {
        let mut class_hash_reads = self.class_hash_reads.lock().await;
        class_hash_reads.remove(contract_address)
    }

    pub async fn remove_compiled_class_hash_read(
        &self,
        class_hash: &ClassHash,
    ) -> Option<ReadValue<CompiledClassHash>> {
        let mut compiled_class_hash_reads = self.compiled_class_hash_reads.lock().await;
        compiled_class_hash_reads.remove(class_hash)
    }

    pub async fn has_storage_read(&self, contract_address: &ContractAddress, key: &StorageKey) -> bool {
        let storage_reads = self.storage_reads.lock().await;
        storage_reads
            .get(contract_address)
            .map_or(false, |reads| reads.contains_key(key))
    }

    pub async fn has_nonce_read(&self, contract_address: &ContractAddress) -> bool {
        let nonce_reads = self.nonce_reads.lock().await;
        nonce_reads.contains_key(contract_address)
    }

    pub async fn has_class_hash_read(&self, contract_address: &ContractAddress) -> bool {
        let class_hash_reads = self.class_hash_reads.lock().await;
        class_hash_reads.contains_key(contract_address)
    }

    pub async fn has_compiled_class_hash_read(&self, class_hash: &ClassHash) -> bool {
        let compiled_class_hash_reads = self.compiled_class_hash_reads.lock().await;
        compiled_class_hash_reads.contains_key(class_hash)
    }

    pub async fn is_empty(&self) -> bool {
        debug!("Checking if read tracker is empty");
        let storage_reads = self.storage_reads.lock().await;
        debug!("Acquired storage reads lock");
        let nonce_reads = self.nonce_reads.lock().await;
        debug!("Acquired nonce reads lock");
        let class_hash_reads = self.class_hash_reads.lock().await;
        debug!("Acquired class hash reads lock");
        let compiled_class_hash_reads = self.compiled_class_hash_reads.lock().await;
        debug!("Acquired compiled class hash reads lock");

        storage_reads.is_empty()
            && nonce_reads.is_empty()
            && class_hash_reads.is_empty()
            && compiled_class_hash_reads.is_empty()
    }

    pub async fn len_map(&self) -> HashMap<String, usize> {
        let mut len_map = HashMap::new();
        let storage_reads = self.storage_reads.lock().await;
        len_map.insert("storage_reads".to_string(), storage_reads.len());
        let nonce_reads = self.nonce_reads.lock().await;
        len_map.insert("nonce_reads".to_string(), nonce_reads.len());
        let class_hash_reads = self.class_hash_reads.lock().await;
        len_map.insert("class_hash_reads".to_string(), class_hash_reads.len());
        let compiled_class_hash_reads = self.compiled_class_hash_reads.lock().await;
        len_map.insert(
            "compiled_class_hash_reads".to_string(),
            compiled_class_hash_reads.len(),
        );
        len_map
    }
}
