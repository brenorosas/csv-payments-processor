use std::collections::HashMap;

use async_trait::async_trait;
use tokio_rusqlite::Connection;

use super::entity::{Client, NewClient};
use super::error::ClientRepositoryError;
use crate::storage::error::RepositoryError;

const UPSERT: &str = include_str!("sql/upsert.sql");
const GET_CLIENTS: &str = include_str!("sql/get_clients.sql");

#[async_trait]
pub trait ClientRepository {
    async fn upsert(&self, clients: &[NewClient]) -> Result<(), ClientRepositoryError>;
    async fn get_clients(
        &self,
        client_ids: &[u16],
    ) -> Result<HashMap<u16, Client>, ClientRepositoryError>;
}

pub struct SqliteClientRepository {
    conn: Connection,
}

impl SqliteClientRepository {
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }
}

#[async_trait]
impl ClientRepository for SqliteClientRepository {
    async fn upsert(&self, clients: &[NewClient]) -> Result<(), ClientRepositoryError> {
        let clients_json = serde_json::to_string(clients).map_err(RepositoryError::from)?;
        let expected = clients.len();

        let changed = self
            .conn
            .call(move |conn| -> Result<usize, rusqlite::Error> {
                let tx = conn.transaction()?;
                let changed = tx.execute(UPSERT, [clients_json])?;
                if changed == expected {
                    tx.commit()?;
                }
                Ok(changed)
            })
            .await
            .map_err(RepositoryError::from)?;

        if changed != expected {
            return Err(ClientRepositoryError::VersionConflict {
                written: changed,
                expected,
            });
        }

        Ok(())
    }

    async fn get_clients(
        &self,
        client_ids: &[u16],
    ) -> Result<HashMap<u16, Client>, ClientRepositoryError> {
        let ids = client_ids
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let ids_json = format!("[{ids}]");

        let clients = self
            .conn
            .call(move |conn| -> Result<HashMap<u16, Client>, rusqlite::Error> {
                let mut stmt = conn.prepare_cached(GET_CLIENTS)?;
                let rows = stmt.query_map([ids_json], |row| Client::try_from(row))?;

                let mut clients = HashMap::new();
                for client in rows {
                    let client = client?;
                    clients.insert(client.id, client);
                }
                Ok(clients)
            })
            .await
            .map_err(RepositoryError::from)?;

        Ok(clients)
    }
}
