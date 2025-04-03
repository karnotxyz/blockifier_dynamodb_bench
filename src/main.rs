use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::Client as DynamoDbClient;
use clap::{Parser, Subcommand};
use log::info;
use std::sync::Arc;
use std::time::Duration;
use transfer_generator::{RecipientGeneratorType, TransfersGenerator, TransfersGeneratorConfig};

mod dynamodb_state_reader;
mod metrics;
mod models;
mod transfer_generator;

use models::{
    ClassHashTable, ClassTable, CompiledClassTable, DynamoTable, NonceTable, StorageTable,
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
        #[arg(short, long, default_value_t = 1)]
        n_accounts: u16,
    },
}

async fn cleanup(client: Arc<DynamoDbClient>) -> Result<(), Box<dyn std::error::Error>> {
    StorageTable::delete(client.clone()).await?;
    NonceTable::delete(client.clone()).await?;
    ClassHashTable::delete(client.clone()).await?;
    ClassTable::delete(client.clone()).await?;
    CompiledClassTable::delete(client.clone()).await?;
    Ok(())
}

async fn migrate(client: Arc<DynamoDbClient>) -> Result<(), Box<dyn std::error::Error>> {
    StorageTable::deploy(client.clone()).await?;
    NonceTable::deploy(client.clone()).await?;
    ClassHashTable::deploy(client.clone()).await?;
    ClassTable::deploy(client.clone()).await?;
    CompiledClassTable::deploy(client.clone()).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    env_logger::builder()
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "[{} {}] {}",
                buf.timestamp_millis(),
                record.level(),
                record.args()
            )
        })
        .filter_level(log::LevelFilter::Info)
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
        Commands::Bench { n_accounts } => {
            info!("Running benchmark with {} accounts...", n_accounts);
            // First cleanup and then migrate to ensure a fresh state
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

            let transfers_generator_config = TransfersGeneratorConfig {
                recipient_generator_type: RecipientGeneratorType::DisjointFromSenders,
                n_accounts,
                dynamo_db_client: client.clone(),
                ..Default::default()
            };
            let mut transfers_generator = TransfersGenerator::new(transfers_generator_config).await;
            transfers_generator.execute_transfers().await;

            // Log metrics
            info!("\n=== Performance Metrics ===");
            let metrics = transfers_generator.metrics.clone();
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
