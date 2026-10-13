use rusqlite::Row;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl ToSql for TransactionType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            Self::Deposit => "deposit",
            Self::Withdrawal => "withdrawal",
            Self::Dispute => "dispute",
            Self::Resolve => "resolve",
            Self::Chargeback => "chargeback",
        }
        .into())
    }
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

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NewTransaction {
    pub r#type: TransactionType,
    pub client_id: u16,
    pub id: u32,
    pub version: u32,
    pub amount: Option<i64>,
}

impl NewTransaction {
    #[cfg(feature = "test-utils")]
    pub fn new_for_tests(
        r#type: TransactionType,
        client_id: u16,
        id: u32,
        version: u32,
        amount: Option<i64>,
    ) -> Self {
        Self {
            r#type,
            client_id,
            id,
            version,
            amount,
        }
    }
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
