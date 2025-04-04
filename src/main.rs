use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::Client as DynamoDbClient;
use clap::{Parser, Subcommand};
use log::info;
use metrics::StateMetrics;
use std::sync::Arc;
use std::time::Duration;
use transfer_generator::{RecipientGeneratorType, TransfersGenerator, TransfersGeneratorConfig};

mod dynamodb_state_reader;
mod metrics;
mod models;
mod read_tracker;
mod transfer_generator;

use models::{
    ClassHashTable, ClassTable, CompiledClassTable, CounterTable, DynamoTable, NonceTable,
    StorageTable, TxLogTable,
};

fn format_duration(duration: Duration) -> String {
    if duration.as_secs() > 0 {
        format!("{:.2}s", duration.as_secs_f64())
    } else {
        format!("{}ms", duration.as_millis())
    }
}

fn log_metrics(metric_name: &str, metrics: &metrics::OperationMetrics) {
    if let Some(avg) = metrics.average_duration() {
        info!("{} average time: {}", metric_name, format_duration(avg));
    }
    if let Some(median) = metrics.median_duration() {
        info!("{} median time: {}", metric_name, format_duration(median));
    }
    info!(
        "{} total operations: {}",
        metric_name,
        metrics.durations.len()
    );
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create DynamoDB tables for the benchmark
    Migrate,
    /// Delete DynamoDB tables used in the benchmark
    Cleanup,
    /// Run the benchmark
    Bench {
        #[arg(long, default_value_t = 1)]
        n_accounts: u16,
        #[arg(long, default_value_t = 10)]
        n_txs: u16,
        #[arg(long, default_value_t = 1000)]
        chunk_size: u16,
        #[arg(long, default_value_t = false)]
        cleanup_and_migrate: bool,
    },
    /// Track transactions per second
    TrackTps {
        #[arg(long, default_value_t = 1)]
        time_window: u64,
    },
}

async fn cleanup(client: Arc<DynamoDbClient>) -> Result<(), Box<dyn std::error::Error>> {
    StorageTable::delete(client.clone()).await?;
    NonceTable::delete(client.clone()).await?;
    ClassHashTable::delete(client.clone()).await?;
    ClassTable::delete(client.clone()).await?;
    CompiledClassTable::delete(client.clone()).await?;
    CounterTable::delete(client.clone()).await?;
    TxLogTable::delete(client.clone()).await?;
    Ok(())
}

async fn migrate(client: Arc<DynamoDbClient>) -> Result<(), Box<dyn std::error::Error>> {
    StorageTable::deploy(client.clone()).await?;
    NonceTable::deploy(client.clone()).await?;
    ClassHashTable::deploy(client.clone()).await?;
    ClassTable::deploy(client.clone()).await?;
    CompiledClassTable::deploy(client.clone()).await?;
    CounterTable::deploy(client.clone()).await?;
    TxLogTable::deploy(client.clone()).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    env_logger::builder()
        .filter_level(
            std::env::var("RUST_LOG")
                .map(|level| level.parse().unwrap_or(log::LevelFilter::Info))
                .unwrap_or(log::LevelFilter::Info),
        )
        .init();
    info!("Starting benchmark...");

    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("ap-south-1"))
        .load()
        .await;
    let client = Arc::new(DynamoDbClient::new(&config));

    match cli.command {
        Commands::Migrate => {
            migrate(client).await?;
        }
        Commands::Cleanup => {
            cleanup(client).await?;
        }
        Commands::TrackTps { time_window } => {
            info!("Starting TPS tracking with {}s window...", time_window);
            loop {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let window_start = now - (time_window as u128 * 1_000_000_000);

                let txs =
                    TxLogTable::query_tx_logs_by_time_range(client.clone(), window_start, now)
                        .await?;

                let tps = txs.len() as f64 / time_window as f64;
                info!(
                    "Current TPS: {:.2} (transactions in last {}s: {})",
                    tps,
                    time_window,
                    txs.len()
                );

                // Sleep for 1 second before next measurement
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
        Commands::Bench {
            n_accounts,
            n_txs,
            chunk_size,
            cleanup_and_migrate,
        } => {
            info!("Running benchmark with {} accounts...", n_accounts);
            // First cleanup and then migrate to ensure a fresh state
            if cleanup_and_migrate {
                cleanup(client.clone()).await?;
                info!("Tables deleted successfully!");
                info!("Waiting 10s for DynamoDB APIs to refresh...");
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                info!("Done waiting!");
                migrate(client.clone()).await?;
                info!("Tables created successfully!");
                info!("Waiting 10s for DynamoDB APIs to refresh...");
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                info!("Done waiting!");
            }

            // Get a unique counter value for this bench run
            // This ensures multiple instances of the bench can run in parallel
            // Each instance will use this unique counter to ensure no collisions
            // across accounts generated
            let counter = CounterTable::get_and_increment(client.clone()).await?;
            info!("Starting bench run #{}", counter);
            let metrics = Arc::new(StateMetrics::new());

            let transfers_generator_config = TransfersGeneratorConfig {
                recipient_generator_type: RecipientGeneratorType::DisjointFromSenders,
                n_accounts,
                dynamo_db_client: client.clone(),
                metrics: metrics.clone(),
                n_txs: n_txs as usize,
                chunk_size: chunk_size as usize,
                account_seed: counter,
                ..Default::default()
            };
            let mut transfers_generator = TransfersGenerator::new(transfers_generator_config).await;
            info!("Starting transfers...");
            transfers_generator.execute_transfers().await;

            // Log metrics
            info!("\n=== Performance Metrics ===");
            {
                let storage_reads = metrics.storage_reads.lock().unwrap();
                log_metrics("Storage reads", &storage_reads);
            }
            {
                let nonce_reads = metrics.nonce_reads.lock().unwrap();
                log_metrics("Nonce reads", &nonce_reads);
            }
            {
                let class_hash_reads = metrics.class_hash_reads.lock().unwrap();
                log_metrics("Class hash reads", &class_hash_reads);
            }
            {
                let compiled_class_hash_reads = metrics.compiled_class_hash_reads.lock().unwrap();
                log_metrics("Compiled class hash reads", &compiled_class_hash_reads);
            }
            {
                let state_updates = metrics.state_updates.lock().unwrap();
                log_metrics("State updates", &state_updates);
            }
            info!("=== End of Metrics ===\n");

            info!("Benchmark completed!");
        }
    }

    Ok(())
}
