use rusqlite::{Connection, params};

use super::entity::Client;

const INSERT: &str = include_str!("sql/insert.sql");

pub trait ClientRepository {
    fn insert(&self, client: &Client) -> anyhow::Result<()>;
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
    fn insert(&self, client: &Client) -> anyhow::Result<()> {
        self.conn.execute(
            INSERT,
            params![
                client.id,
                client.available_amount,
                client.held_amount,
                client.total_amount,
                client.locked
            ],
        )?;
        Ok(())
    }
}
