// Taken from https://github.com/starkware-libs/sequencer

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use aws_config::{BehaviorVersion, Region};
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
use starknet_api::core::ContractAddress;
use starknet_api::executable_transaction::AccountTransaction as ApiExecutableTransaction;
use starknet_api::test_utils::invoke::executable_invoke_tx;
use starknet_api::test_utils::NonceManager;
use starknet_api::transaction::constants::TRANSFER_ENTRY_POINT_NAME;
use starknet_api::transaction::fields::Fee;
use starknet_api::transaction::TransactionVersion;
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
}

impl Default for TransfersGeneratorConfig {
    fn default() -> Self {
        let concurrency_enabled = true;

        // Create AWS config in a separate thread to avoid runtime issues
        let client = thread::spawn(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let config = aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new("ap-south-1"))
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
}

impl TransfersGenerator {
    pub async fn new(config: TransfersGeneratorConfig) -> Self {
        let account_contract = FeatureContract::AccountWithoutValidations(config.cairo_version);
        let block_context = BlockContext::create_for_account_testing();
        let chain_info = block_context.chain_info().clone();
        let state = test_state(
            &chain_info,
            config.balance,
            &[(account_contract, config.n_accounts)],
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
        let account_addresses = (0..config.n_accounts)
            .map(|instance_id| account_contract.get_instance_address(instance_id))
            .collect::<Vec<_>>();
        let nonce_manager = NonceManager::default();
        let mut recipient_addresses = None;
        let mut random_recipient_generator = None;
        match config.recipient_generator_type {
            RecipientGeneratorType::Random => {
                random_recipient_generator = Some(StdRng::seed_from_u64(config.randomization_seed));
            }
            RecipientGeneratorType::RoundRobin => {}
            RecipientGeneratorType::DisjointFromSenders => {
                recipient_addresses = Some(
                    (config.n_accounts..2 * config.n_accounts)
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

    pub async fn execute_transfers(&mut self) {
        for i in 0..self.config.n_txs {
            let start_time = std::time::Instant::now();

            let sender_address = self.account_addresses[self.sender_index];
            let recipient_address = self.get_next_recipient();
            self.sender_index = (self.sender_index + 1) % self.account_addresses.len();

            let tx = self.generate_transfer(sender_address, recipient_address);
            let account_tx = AccountTransaction::new_for_sequencing(tx);
            let results = self
                .executor
                .execute_txs(&[Transaction::Account(account_tx)]);
            for result in results {
                assert!(!result.unwrap().0.is_reverted());
            }
            let state_diff = self.executor.finalize().unwrap().state_diff;
            update_state_diff(
                self.dynamo_db_client.clone(),
                state_diff,
                self.metrics.clone(),
                self.read_tracker.clone(),
            )
            .await
            .unwrap();

            let elapsed = start_time.elapsed();
            info!("Transfer {} took {:?}", i + 1, elapsed);
        }
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
        })
    }
}

pub async fn test_state(
    chain_info: &ChainInfo,
    initial_balances: Fee,
    contract_instances: &[(FeatureContract, u16)],
    dynamo_db_client: Arc<DynamoDbClient>,
    metrics: Arc<StateMetrics>,
    read_tracker: Arc<ReadTracker>,
) -> CachedState<DynamoDbStateReader> {
    let contract_instances_vec: Vec<(FeatureContractData, u16)> = contract_instances
        .iter()
        .map(|(feature_contract, i)| ((*feature_contract).into(), *i))
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
    contract_instances: &[(FeatureContractData, u16)],
    dynamo_db_client: Arc<DynamoDbClient>,
    metrics: Arc<StateMetrics>,
    read_tracker: Arc<ReadTracker>,
) -> CachedState<DynamoDbStateReader> {
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
    contract_instances: &[(FeatureContractData, u16)],
    erc20_contract_version: CairoVersion,
    dynamo_db_client: Arc<DynamoDbClient>,
    metrics: Arc<StateMetrics>,
    read_tracker: Arc<ReadTracker>,
) -> CachedState<DynamoDbStateReader> {
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
    for (contract, n_instances) in contract_instances.iter() {
        let class_hash = contract.class_hash;
        class_hash_to_class.insert(class_hash, contract.runnable_class.clone());
        for instance in 0..*n_instances {
            let instance_address = contract.get_instance_address(instance);
            address_to_class_hash.insert(instance_address, class_hash);
        }
    }

    let mut state_diff = CommitmentStateDiff::default();
    state_diff.address_to_class_hash = IndexMap::from_iter(address_to_class_hash);
    let state_reader = DynamoDbStateReader::new(
        dynamo_db_client.clone(),
        class_hash_to_class,
        metrics.clone(),
        read_tracker.clone(),
    );

    // fund the accounts.
    for (contract, n_instances) in contract_instances.iter() {
        for instance in 0..*n_instances {
            let instance_address = contract.get_instance_address(instance);
            if contract.require_funding {
                fund_account(
                    chain_info,
                    instance_address,
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
    )
    .await
    .unwrap();

    CachedState::from(state_reader)
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
