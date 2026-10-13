use serde::Deserialize;

use crate::storage::transactions::entity::TransactionType;

#[derive(Debug, Deserialize)]
pub struct TransactionCsv {
    pub r#type: TransactionType,
    pub client: u16,
    pub tx: u32,
    pub amount: Option<String>,
}
