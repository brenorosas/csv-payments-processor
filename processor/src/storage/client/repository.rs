use rusqlite::Connection;

use super::entity::NewClient;
use super::error::ClientRepositoryError;
use crate::storage::error::RepositoryError;

const UPSERT: &str = include_str!("sql/upsert.sql");

pub trait ClientRepository {
    fn upsert(&self, clients: &[NewClient]) -> Result<(), ClientRepositoryError>;
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
    fn upsert(&self, clients: &[NewClient]) -> Result<(), ClientRepositoryError> {
        let tx = self.conn.unchecked_transaction().map_err(RepositoryError::from)?;
        let clients_json = serde_json::to_string(clients).map_err(RepositoryError::from)?;
        let changed = tx
            .execute(UPSERT, [clients_json])
            .map_err(RepositoryError::from)?;

        if changed != clients.len() {
            return Err(ClientRepositoryError::VersionConflict {
                written: changed,
                expected: clients.len(),
            });
        }

        tx.commit().map_err(RepositoryError::from)?;
        Ok(())
    }
}
