use rusqlite::{Connection, OptionalExtension, params};

use super::entity::Transaction;

const INSERT: &str = include_str!("sql/insert.sql");
const GET_LAST_VERSION: &str = include_str!("sql/get_last_version.sql");

pub trait TransactionRepository {
    fn insert(&self, transaction: &Transaction) -> anyhow::Result<()>;
    fn get_last_version(&self, transaction_id: u32) -> anyhow::Result<Option<Transaction>>;
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

    fn get_last_version(&self, transaction_id: u32) -> anyhow::Result<Option<Transaction>> {
        let transaction = self
            .conn
            .query_row(GET_LAST_VERSION, [transaction_id], |row| {
                Transaction::try_from(row)
            })
            .optional()?;
        Ok(transaction)
    }
}
