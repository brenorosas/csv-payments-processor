use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TransactionCsv {
    pub r#type: String,
    pub client: u16,
    pub tx: u32,
    pub amount: Option<String>,
}
