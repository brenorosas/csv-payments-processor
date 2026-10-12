use rusqlite::Row;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ValueRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl FromSql for TransactionType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_str()? {
            "deposit" => Ok(Self::Deposit),
            "withdrawal" => Ok(Self::Withdrawal),
            "dispute" => Ok(Self::Dispute),
            "resolve" => Ok(Self::Resolve),
            "chargeback" => Ok(Self::Chargeback),
            other => Err(FromSqlError::Other(
                format!("unknown transaction type `{other}`").into(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Transaction {
    pub r#type: TransactionType,
    pub client_id: u16,
    pub id: u32,
    pub version: u32,
    pub amount: Option<i64>,
    pub created_at: String,
}

impl TryFrom<&Row<'_>> for Transaction {
    type Error = rusqlite::Error;

    fn try_from(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            r#type: row.get("type")?,
            client_id: row.get("client_id")?,
            id: row.get("id")?,
            version: row.get("version")?,
            amount: row.get("amount")?,
            created_at: row.get("created_at")?,
        })
    }
}
