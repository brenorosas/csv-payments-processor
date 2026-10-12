use rusqlite::{Connection, params};

use super::entity::Transaction;

const INSERT: &str = include_str!("sql/insert.sql");

pub trait TransactionRepository {
    fn insert(&self, transaction: &Transaction) -> anyhow::Result<()>;
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
    fn insert(&self, transaction: &Transaction) -> anyhow::Result<()> {
        self.conn.execute(
            INSERT,
            params![
                transaction.r#type,
                transaction.client_id,
                transaction.id,
                transaction.version,
                transaction.amount
            ],
        )?;
        Ok(())
    }
}
