use std::collections::HashMap;

use rusqlite::Connection;

use super::entity::{NewTransaction, Transaction};
use super::error::TransactionRepositoryError;
use crate::storage::error::RepositoryError;

const INSERT: &str = include_str!("sql/insert.sql");
const GET_LAST_VERSIONS: &str = include_str!("sql/get_last_versions.sql");

#[cfg_attr(feature = "test-utils", mockall::automock)]
pub trait TransactionRepository: Send + Sync {
    fn insert(
        &self,
        conn: &Connection,
        transactions: &[NewTransaction],
    ) -> Result<(), TransactionRepositoryError>;
    fn get_last_versions(
        &self,
        conn: &Connection,
        transaction_ids: &[u32],
    ) -> Result<HashMap<u32, Transaction>, TransactionRepositoryError>;
}

pub struct SqliteTransactionRepository;

impl TransactionRepository for SqliteTransactionRepository {
    fn insert(
        &self,
        conn: &Connection,
        transactions: &[NewTransaction],
    ) -> Result<(), TransactionRepositoryError> {
        let transactions_json =
            serde_json::to_string(transactions).map_err(RepositoryError::from)?;
        conn.execute(INSERT, [transactions_json])
            .map_err(RepositoryError::from)?;
        Ok(())
    }

    fn get_last_versions(
        &self,
        conn: &Connection,
        transaction_ids: &[u32],
    ) -> Result<HashMap<u32, Transaction>, TransactionRepositoryError> {
        let ids = transaction_ids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",");

        let mut stmt = conn
            .prepare_cached(GET_LAST_VERSIONS)
            .map_err(RepositoryError::from)?;
        let rows = stmt
            .query_map([format!("[{ids}]")], |row| Transaction::try_from(row))
            .map_err(RepositoryError::from)?;

        let mut transactions = HashMap::new();
        for transaction in rows {
            let transaction = transaction.map_err(RepositoryError::from)?;
            transactions.insert(transaction.id, transaction);
        }
        Ok(transactions)
    }
}
