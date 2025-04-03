use super::table::{DynamoTable, TableSchema};

pub struct ClassTable;

impl DynamoTable for ClassTable {
    fn schema() -> TableSchema {
        TableSchema {
            name: "starknet_classes".to_string(),
            hash_key: "class_hash".to_string(),
            sort_key: None,
        }
    }
}
