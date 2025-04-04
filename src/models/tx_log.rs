use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoDbClient};
use starknet_api::transaction::TransactionHash;
use std::sync::Arc;
use std::time::SystemTime;

use super::{
    table::{DynamoTable, TableSchema},
    ToDDBString,
};

pub struct TxLogTable;

pub const ALL_TXS_PARTITION_KEY: &str = "ALL_TXS";

impl DynamoTable for TxLogTable {
    fn schema() -> TableSchema {
        TableSchema {
            name: "blockifier_ddb_starknet_tx_logs".to_string(),
            hash_key: "partition_key".to_string(),
            sort_key: Some("timestamp".to_string()),
        }
    }
}

impl TxLogTable {
    pub async fn query_tx_logs_by_time_range(
        client: Arc<DynamoDbClient>,
        start_time: u128,
        end_time: u128,
    ) -> anyhow::Result<Vec<()>> {
        let result = client
            .query()
            .table_name(Self::schema().name)
            .key_condition_expression("#pk = :pk AND #ts BETWEEN :start AND :end")
            .expression_attribute_names("#pk", "partition_key")
            .expression_attribute_names("#ts", "timestamp")
            .expression_attribute_values(
                ":pk",
                AttributeValue::S(ALL_TXS_PARTITION_KEY.to_string()),
            )
            .expression_attribute_values(":start", AttributeValue::N(start_time.to_string()))
            .expression_attribute_values(":end", AttributeValue::N(end_time.to_string()))
            .send()
            .await?;

        Ok(result
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|_| ())
            .collect())
    }
}
