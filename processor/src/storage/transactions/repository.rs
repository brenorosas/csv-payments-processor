use std::collections::HashMap;

use rusqlite::Connection;

use super::entity::{NewTransaction, Transaction};

const INSERT: &str = include_str!("sql/insert.sql");
const GET_LAST_VERSIONS: &str = include_str!("sql/get_last_versions.sql");

pub trait TransactionRepository {
    fn insert(&self, transactions: &[NewTransaction]) -> anyhow::Result<()>;
    fn get_last_versions(&self, transaction_ids: &[u32]) -> anyhow::Result<HashMap<u32, Transaction>>;
}

pub struct SqliteTransactionRepository<'a> {
    conn: &'a Connection,
}

impl<'a> SqliteTransactionRepository<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }
}

impl TransactionRepository for SqliteTransactionRepository<'_> {
    fn insert(&self, transactions: &[NewTransaction]) -> anyhow::Result<()> {
        self.conn
            .execute(INSERT, [serde_json::to_string(transactions)?])?;
        Ok(())
    }

    fn get_last_versions(&self, transaction_ids: &[u32]) -> anyhow::Result<HashMap<u32, Transaction>> {
        // The ids are passed as a JSON array and expanded in SQL with json_each.
        let ids = transaction_ids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",");

        let mut stmt = self.conn.prepare_cached(GET_LAST_VERSIONS)?;
        let rows = stmt.query_map([format!("[{ids}]")], |row| Transaction::try_from(row))?;

        let mut transactions = HashMap::new();
        for transaction in rows {
            let transaction = transaction?;
            transactions.insert(transaction.id, transaction);
        }
        Ok(transactions)
    }
}
