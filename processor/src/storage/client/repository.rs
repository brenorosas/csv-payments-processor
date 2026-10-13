use rusqlite::Connection;

use super::entity::NewClient;

const INSERT: &str = include_str!("sql/insert.sql");

pub trait ClientRepository {
    fn insert(&self, clients: &[NewClient]) -> anyhow::Result<()>;
}

pub struct SqliteClientRepository<'a> {
    conn: &'a Connection,
}

impl<'a> SqliteClientRepository<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }
}

impl ClientRepository for SqliteClientRepository<'_> {
    fn insert(&self, clients: &[NewClient]) -> anyhow::Result<()> {
        self.conn.execute(INSERT, [serde_json::to_string(clients)?])?;
        Ok(())
    }
}
