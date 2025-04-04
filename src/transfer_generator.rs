// Taken from https://github.com/starkware-libs/sequencer

use std::collections::HashMap;
use std::sync::Arc;
use std::{thread, u16};

use aws_config::{BehaviorVersion, Region};
use blockifier::execution::contract_class::RunnableCompiledClass;
use blockifier::state;
use blockifier::state::cached_state::{CachedState, CommitmentStateDiff, StateMaps};
use blockifier::test_utils::contracts::{FeatureContractData, FeatureContractTrait};
use blockifier_test_utils::cairo_versions::CairoVersion;
use blockifier_test_utils::contracts::FeatureContract;
use indexmap::IndexMap;
use log::info;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use starknet_api::abi::abi_utils::{get_fee_token_var_address, selector_from_name};
use starknet_api::block::FeeType;
use starknet_api::core::{ClassHash, ContractAddress};
use starknet_api::executable_transaction::AccountTransaction as ApiExecutableTransaction;
use starknet_api::test_utils::invoke::executable_invoke_tx;
use starknet_api::test_utils::NonceManager;
use starknet_api::transaction::constants::TRANSFER_ENTRY_POINT_NAME;
use starknet_api::transaction::fields::Fee;
use starknet_api::transaction::{TransactionHash, TransactionVersion};
use starknet_api::{calldata, felt, invoke_tx_args};
use starknet_types_core::felt::Felt;

use aws_sdk_dynamodb::Client as DynamoDbClient;
use blockifier::blockifier::config::{ConcurrencyConfig, TransactionExecutorConfig};
use blockifier::blockifier::transaction_executor::{TransactionExecutor, DEFAULT_STACK_SIZE};
use blockifier::context::{BlockContext, ChainInfo};
use blockifier::test_utils::BALANCE;
use blockifier::transaction::account_transaction::AccountTransaction;
use blockifier::transaction::transaction_execution::Transaction;
use blockifier_test_utils::cairo_versions::RunnableCairo1;
use starknet_api::test_utils::MAX_FEE;
use strum::IntoEnumIterator;
use tokio::runtime::Runtime;

use crate::dynamodb_state_reader::{update_state_diff, DynamoDbStateReader};
use crate::metrics::StateMetrics;
use crate::read_tracker::ReadTracker;
const N_ACCOUNTS: u16 = 10000;
const N_TXS: usize = 10;
const RANDOMIZATION_SEED: u64 = 0;
const CAIRO_VERSION: CairoVersion = CairoVersion::Cairo1(RunnableCairo1::Casm);
const TRANSACTION_VERSION: TransactionVersion = TransactionVersion(Felt::THREE);
const RECIPIENT_GENERATOR_TYPE: RecipientGeneratorType = RecipientGeneratorType::RoundRobin;

#[derive(Clone)]
pub struct TransfersGeneratorConfig {
    pub n_accounts: u16,
    pub balance: Fee,
    pub max_fee: Fee,
    pub n_txs: usize,
    pub randomization_seed: u64,
    pub cairo_version: CairoVersion,
    pub tx_version: TransactionVersion,
    pub recipient_generator_type: RecipientGeneratorType,
    pub concurrency_config: ConcurrencyConfig,
    pub stack_size: usize,
    pub dynamo_db_client: Arc<DynamoDbClient>,
    pub metrics: Arc<StateMetrics>,
    pub read_tracker: Arc<ReadTracker>,
    pub account_seed: u16,
    pub chunk_size: usize,
}

impl Default for TransfersGeneratorConfig {
    fn default() -> Self {
        // disabling concurrency, we will spawn multiple pods for concurrency
        let concurrency_enabled = false;

        // Create AWS config in a separate thread to avoid runtime issues
        let client = thread::spawn(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let config = aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new(
                        std::env::var("AWS_DEFAULT_REGION").unwrap_or("ap-south-1".to_string()),
                    ))
                    .load()
                    .await;
                Arc::new(DynamoDbClient::new(&config))
            })
        })
        .join()
        .expect("Thread panicked");

        Self {
            n_accounts: N_ACCOUNTS,
            balance: Fee(BALANCE.0 * 1000),
            max_fee: MAX_FEE,
            n_txs: N_TXS,
            randomization_seed: RANDOMIZATION_SEED,
            cairo_version: CAIRO_VERSION,
            tx_version: TRANSACTION_VERSION,
            recipient_generator_type: RECIPIENT_GENERATOR_TYPE,
            concurrency_config: ConcurrencyConfig::create_for_testing(concurrency_enabled),
            stack_size: DEFAULT_STACK_SIZE,
            dynamo_db_client: client,
            metrics: Arc::new(StateMetrics::new()),
            read_tracker: Arc::new(ReadTracker::new()),
            account_seed: 0,
            chunk_size: 1000,
        }
    }
}

#[derive(Clone)]
pub enum RecipientGeneratorType {
    Random,
    RoundRobin,
    DisjointFromSenders,
}

pub struct TransfersGenerator {
    account_addresses: Vec<ContractAddress>,
    chain_info: ChainInfo,
    pub executor: TransactionExecutor<DynamoDbStateReader>,
    nonce_manager: NonceManager,
    sender_index: usize,
    random_recipient_generator: Option<StdRng>,
    recipient_addresses: Option<Vec<ContractAddress>>,
    config: TransfersGeneratorConfig,
    dynamo_db_client: Arc<DynamoDbClient>,
    pub metrics: Arc<StateMetrics>,
    pub read_tracker: Arc<ReadTracker>,
    account_seed: u16,
    class_hash_to_class: HashMap<ClassHash, RunnableCompiledClass>,
    chunk_size: usize,
}

impl TransfersGenerator {
    pub async fn new(config: TransfersGeneratorConfig) -> Self {
        let account_contract = FeatureContract::AccountWithoutValidations(config.cairo_version);
        let block_context = BlockContext::create_for_account_testing();
        let chain_info = block_context.chain_info().clone();

        let start_instance_id = config.account_seed * config.n_accounts;
        info!(
            "Seeds used for sender address generation: {} -> {}",
            start_instance_id,
            start_instance_id + config.n_accounts
        );
        let account_addresses = (start_instance_id..start_instance_id + config.n_accounts)
            .map(|instance_id| account_contract.get_instance_address(instance_id as u16))
            .collect::<Vec<_>>();

        let (state, class_hash_to_class) = test_state(
            &chain_info,
            config.balance,
            &[(account_contract, account_addresses.clone())],
            config.dynamo_db_client.clone(),
            config.metrics.clone(),
            config.read_tracker.clone(),
        )
        .await;
        let executor_config = TransactionExecutorConfig {
            concurrency_config: config.concurrency_config.clone(),
            stack_size: config.stack_size,
        };
        let executor = TransactionExecutor::new(state, block_context, executor_config);

        let nonce_manager = NonceManager::default();
        let mut recipient_addresses = None;
        let mut random_recipient_generator = None;
        match config.recipient_generator_type {
            RecipientGeneratorType::Random => {
                random_recipient_generator = Some(StdRng::seed_from_u64(config.randomization_seed));
            }
            RecipientGeneratorType::RoundRobin => {}
            RecipientGeneratorType::DisjointFromSenders => {
                let end_instance_id = u16::MAX - config.n_accounts * config.account_seed;
                info!(
                    "Seeds used for recipient address generation: {} -> {}",
                    end_instance_id - config.n_accounts,
                    end_instance_id
                );
                recipient_addresses = Some(
                    (end_instance_id - config.n_accounts..end_instance_id)
                        .map(|instance_id| account_contract.get_instance_address(instance_id))
                        .collect::<Vec<_>>(),
                );
            }
        };
        Self {
            account_addresses,
            chain_info,
            executor,
            nonce_manager,
            sender_index: 0,
            random_recipient_generator,
            recipient_addresses,
            config: config.clone(),
            dynamo_db_client: config.dynamo_db_client.clone(),
            metrics: config.metrics.clone(),
            read_tracker: config.read_tracker.clone(),
            account_seed: config.account_seed,
            class_hash_to_class,
            chunk_size: config.chunk_size,
        }
    }

    pub fn get_next_recipient(&mut self) -> ContractAddress {
        match self.config.recipient_generator_type {
            RecipientGeneratorType::Random => {
                let random_recipient_generator = self.random_recipient_generator.as_mut().unwrap();
                let recipient_index =
                    random_recipient_generator.gen_range(0..self.account_addresses.len());
                self.account_addresses[recipient_index]
            }
            RecipientGeneratorType::RoundRobin => {
                let recipient_index = (self.sender_index + 1) % self.account_addresses.len();
                self.account_addresses[recipient_index]
            }
            RecipientGeneratorType::DisjointFromSenders => {
                self.recipient_addresses.as_ref().unwrap()[self.sender_index]
            }
        }
    }

    pub fn reload_executor(&mut self) {
        let state = CachedState::new(DynamoDbStateReader::new(
            self.dynamo_db_client.clone(),
            self.class_hash_to_class.clone(),
            self.metrics.clone(),
            self.read_tracker.clone(),
        ));
        let block_context = BlockContext::create_for_account_testing();
        let executor_config = TransactionExecutorConfig {
            concurrency_config: self.config.concurrency_config.clone(),
            stack_size: self.config.stack_size,
        };
        self.executor = TransactionExecutor::new(state, block_context, executor_config);
    }

    pub async fn execute_transfers(&mut self) {
        let mut txs: Vec<Transaction> = Vec::with_capacity(self.config.n_txs);
        let mut tx_hashes = Vec::with_capacity(self.config.n_txs);
        let start_time = std::time::Instant::now();
        for _ in 0..self.config.n_txs {
            let sender_address = self.account_addresses[self.sender_index];
            let recipient_address = self.get_next_recipient();
            self.sender_index = (self.sender_index + 1) % self.account_addresses.len();

            let tx = self.generate_transfer(sender_address, recipient_address);
            let account_tx = AccountTransaction::new_for_sequencing(tx);
            tx_hashes.push(account_tx.tx_hash());
            txs.push(Transaction::Account(account_tx));
        }

        // Process transactions in chunks of 20
        for (chunk, hash_chunk) in txs
            .chunks(self.config.chunk_size)
            .zip(tx_hashes.chunks(self.config.chunk_size))
        {
            let results = self.executor.execute_txs(chunk);
            for result in results {
                assert!(!result.unwrap().0.is_reverted());
            }
            let state_diff = self.executor.finalize().unwrap().state_diff;
            update_state_diff(
                self.dynamo_db_client.clone(),
                state_diff.clone(),
                self.metrics.clone(),
                self.read_tracker.clone(),
                hash_chunk.to_vec(),
                false,
            )
            .await
            .expect(format!("Failed to update state diff for diff {:?}", state_diff).as_str());

            // This is done to reset to state diffs
            self.reload_executor();
        }

        let elapsed = start_time.elapsed();
        info!("{} transfers took {:?}", self.config.n_txs, elapsed);
    }

    pub fn generate_transfer(
        &mut self,
        sender_address: ContractAddress,
        recipient_address: ContractAddress,
    ) -> ApiExecutableTransaction {
        let nonce = self.nonce_manager.next(sender_address);

        let entry_point_selector = selector_from_name(TRANSFER_ENTRY_POINT_NAME);
        let contract_address = if self.config.tx_version == TransactionVersion::ONE {
            *self
                .chain_info
                .fee_token_addresses
                .eth_fee_token_address
                .0
                .key()
        } else if self.config.tx_version == TransactionVersion::THREE {
            *self
                .chain_info
                .fee_token_addresses
                .strk_fee_token_address
                .0
                .key()
        } else {
            panic!(
                "Unsupported transaction version: {:?}",
                self.config.tx_version
            )
        };

        let execute_calldata = calldata![
            contract_address,           // Contract address.
            entry_point_selector.0,     // EP selector.
            felt!(3_u8),                // Calldata length.
            *recipient_address.0.key(), // Calldata: recipient.
            felt!(1_u8),                // Calldata: lsb amount.
            felt!(0_u8)                 // Calldata: msb amount.
        ];

        executable_invoke_tx(invoke_tx_args! {
            max_fee: self.config.max_fee,
            sender_address,
            calldata: execute_calldata,
            version: self.config.tx_version,
            nonce,
            tx_hash: TransactionHash(Felt::from(rand::random::<u128>())),
        })
    }
}

pub async fn test_state(
    chain_info: &ChainInfo,
    initial_balances: Fee,
    contract_instances: &[(FeatureContract, Vec<ContractAddress>)],
    dynamo_db_client: Arc<DynamoDbClient>,
    metrics: Arc<StateMetrics>,
    read_tracker: Arc<ReadTracker>,
) -> (
    CachedState<DynamoDbStateReader>,
    HashMap<ClassHash, RunnableCompiledClass>,
) {
    let contract_instances_vec: Vec<(FeatureContractData, Vec<ContractAddress>)> =
        contract_instances
            .iter()
            .map(|(feature_contract, instance_addresses)| {
                ((*feature_contract).into(), instance_addresses.clone())
            })
            .collect();
    test_state_ex(
        chain_info,
        initial_balances,
        &contract_instances_vec[..],
        dynamo_db_client,
        metrics,
        read_tracker,
    )
    .await
}

pub async fn test_state_ex(
    chain_info: &ChainInfo,
    initial_balances: Fee,
    contract_instances: &[(FeatureContractData, Vec<ContractAddress>)],
    dynamo_db_client: Arc<DynamoDbClient>,
    metrics: Arc<StateMetrics>,
    read_tracker: Arc<ReadTracker>,
) -> (
    CachedState<DynamoDbStateReader>,
    HashMap<ClassHash, RunnableCompiledClass>,
) {
    test_state_inner(
        chain_info,
        initial_balances,
        contract_instances,
        CairoVersion::Cairo0,
        dynamo_db_client,
        metrics,
        read_tracker,
    )
    .await
}

/// Initializes a state reader for testing:
/// * "Declares" a Cairo0 account and a Cairo0 ERC20 contract (class hash => class mapping set).
/// * "Deploys" two ERC20 contracts (address => class hash mapping set) at the fee token addresses
///   on the input block context.
/// * Makes the Cairo0 account privileged (minter on both tokens, funded in both tokens).
/// * "Declares" the input list of contracts.
/// * "Deploys" the requested number of instances of each input contract.
/// * Makes each input account contract privileged.
pub async fn test_state_inner(
    chain_info: &ChainInfo,
    initial_balances: Fee,
    contract_instances: &[(FeatureContractData, Vec<ContractAddress>)],
    erc20_contract_version: CairoVersion,
    dynamo_db_client: Arc<DynamoDbClient>,
    metrics: Arc<StateMetrics>,
    read_tracker: Arc<ReadTracker>,
) -> (
    CachedState<DynamoDbStateReader>,
    HashMap<ClassHash, RunnableCompiledClass>,
) {
    let mut class_hash_to_class = HashMap::new();
    let mut address_to_class_hash = HashMap::new();

    // Declare and deploy account and ERC20 contracts.
    let erc20 = FeatureContract::ERC20(erc20_contract_version);
    class_hash_to_class.insert(erc20.get_class_hash(), erc20.get_runnable_class());
    address_to_class_hash.insert(
        chain_info.fee_token_address(&FeeType::Eth),
        erc20.get_class_hash(),
    );
    address_to_class_hash.insert(
        chain_info.fee_token_address(&FeeType::Strk),
        erc20.get_class_hash(),
    );

    // Set up the rest of the requested contracts.
    for (contract, instance_addresses) in contract_instances.iter() {
        let class_hash = contract.class_hash;
        class_hash_to_class.insert(class_hash, contract.runnable_class.clone());
        for instance_address in instance_addresses.iter() {
            address_to_class_hash.insert(*instance_address, class_hash);
        }
    }

    let mut state_diff = CommitmentStateDiff::default();
    state_diff.address_to_class_hash = IndexMap::from_iter(address_to_class_hash);
    let state_reader = DynamoDbStateReader::new(
        dynamo_db_client.clone(),
        class_hash_to_class.clone(),
        metrics.clone(),
        read_tracker.clone(),
    );

    // fund the accounts.
    for (contract, instance_addresses) in contract_instances.iter() {
        for instance_address in instance_addresses.iter() {
            if contract.require_funding {
                fund_account(
                    chain_info,
                    *instance_address,
                    initial_balances,
                    &mut state_diff,
                )
                .await;
            }
        }
    }

    info!("Updating test state diff...");
    update_state_diff(
        dynamo_db_client.clone(),
        state_diff,
        metrics,
        read_tracker.clone(),
        vec![],
        true,
    )
    .await
    .unwrap();

    (CachedState::from(state_reader), class_hash_to_class)
}

/// Utility to fund an account.
pub async fn fund_account(
    chain_info: &ChainInfo,
    account_address: ContractAddress,
    initial_balance: Fee,
    state_diff: &mut CommitmentStateDiff,
) {
    let balance_key = get_fee_token_var_address(account_address);
    for fee_type in FeeType::iter() {
        let fee_token_address = chain_info.fee_token_address(&fee_type);
        state_diff
            .storage_updates
            .entry(fee_token_address)
            .or_insert_with(IndexMap::new)
            .insert(balance_key, felt!(initial_balance.0));
    }
}
