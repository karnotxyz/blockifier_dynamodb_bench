use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::Client as DynamoDbClient;
use clap::{Parser, Subcommand};
use std::sync::Arc;
use transfer_generator::{RecipientGeneratorType, TransfersGenerator, TransfersGeneratorConfig};

mod dynamodb_state_reader;
mod models;
mod transfer_generator;

use models::{
    ClassHashTable, ClassTable, CompiledClassTable, DynamoTable, NonceTable, StorageTable,
};

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
    env_logger::init();

    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
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
            println!("Running benchmark with {} accounts...", n_accounts);
            // First cleanup and then migrate to ensure a fresh state
            cleanup(client.clone()).await?;
            println!("Tables deleted successfully!");
            println!("Waiting 10s for DynamoDB APIs to refresh...");
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            println!("Done waiting!");
            migrate(client.clone()).await?;
            println!("Tables created successfully!");
            println!("Waiting 10s for DynamoDB APIs to refresh...");
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            println!("Done waiting!");

            let transfers_generator_config = TransfersGeneratorConfig {
                recipient_generator_type: RecipientGeneratorType::DisjointFromSenders,
                n_accounts,
                dynamo_db_client: client.clone(),
                ..Default::default()
            };
            let mut transfers_generator = TransfersGenerator::new(transfers_generator_config).await;
            transfers_generator.execute_transfers().await;
            println!("Benchmark completed!");
        }
    }

    Ok(())
}
