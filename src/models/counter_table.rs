use aws_sdk_dynamodb::{types::AttributeValue, Client as DynamoDbClient};
use std::sync::Arc;

use super::table::{DynamoTable, TableSchema};

pub struct CounterTable;

impl DynamoTable for CounterTable {
    fn schema() -> TableSchema {
        TableSchema {
            name: "blockifier_ddb_counter".to_string(),
            hash_key: "counter_id".to_string(),
            sort_key: None,
        }
    }
}

impl CounterTable {
    pub async fn get_and_increment(
        client: Arc<DynamoDbClient>,
    ) -> Result<u16, Box<dyn std::error::Error>> {
        let schema = Self::schema();

        // Use update_item with atomic counter
        let result = client
            .update_item()
            .table_name(&schema.name)
            .key("counter_id", AttributeValue::S("global".to_string()))
            .update_expression("ADD #value :incr")
            .expression_attribute_names("#value", "value")
            .expression_attribute_values(":incr", AttributeValue::N("1".to_string()))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::UpdatedNew)
            .send()
            .await?;

        // Parse the returned value
        let value = result
            .attributes()
            .ok_or("No attributes returned")?
            .get("value")
            .ok_or("No value attribute")?
            .as_n()
            .map_err(|_| "Value is not a number")?
            .parse::<u16>()?;

        Ok(value)
    }
}
