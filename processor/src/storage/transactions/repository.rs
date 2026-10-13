use std::collections::HashMap;

use async_trait::async_trait;
use tokio_rusqlite::Connection;

use super::entity::{NewTransaction, Transaction};
use super::error::TransactionRepositoryError;
use crate::storage::error::RepositoryError;

const INSERT: &str = include_str!("sql/insert.sql");
const GET_LAST_VERSIONS: &str = include_str!("sql/get_last_versions.sql");

#[async_trait]
pub trait TransactionRepository {
    async fn insert(
        &self,
        transactions: &[NewTransaction],
    ) -> Result<(), TransactionRepositoryError>;
    async fn get_last_versions(
        &self,
        transaction_ids: &[u32],
    ) -> Result<HashMap<u32, Transaction>, TransactionRepositoryError>;
}

pub struct SqliteTransactionRepository {
    conn: Connection,
}

impl SqliteTransactionRepository {
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }
}

#[async_trait]
impl TransactionRepository for SqliteTransactionRepository {
    async fn insert(
        &self,
        transactions: &[NewTransaction],
    ) -> Result<(), TransactionRepositoryError> {
        let transactions_json =
            serde_json::to_string(transactions).map_err(RepositoryError::from)?;

        self.conn
            .call(move |conn| -> Result<usize, rusqlite::Error> {
                conn.execute(INSERT, [transactions_json])
            })
            .await
            .map_err(RepositoryError::from)?;

        Ok(())
    }

    async fn get_last_versions(
        &self,
        transaction_ids: &[u32],
    ) -> Result<HashMap<u32, Transaction>, TransactionRepositoryError> {
        let ids = transaction_ids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let ids_json = format!("[{ids}]");

        let transactions = self
            .conn
            .call(move |conn| -> Result<HashMap<u32, Transaction>, rusqlite::Error> {
                let mut stmt = conn.prepare_cached(GET_LAST_VERSIONS)?;
                let rows = stmt.query_map([ids_json], |row| Transaction::try_from(row))?;

                let mut transactions = HashMap::new();
                for transaction in rows {
                    let transaction = transaction?;
                    transactions.insert(transaction.id, transaction);
                }
                Ok(transactions)
            })
            .await
            .map_err(RepositoryError::from)?;

        Ok(transactions)
    }
}
