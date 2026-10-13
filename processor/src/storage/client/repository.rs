use std::collections::HashMap;

use rusqlite::Connection;

use super::entity::{Client, NewClient};
use super::error::ClientRepositoryError;
use crate::storage::error::RepositoryError;

const UPSERT: &str = include_str!("sql/upsert.sql");
const GET_CLIENTS: &str = include_str!("sql/get_clients.sql");
const GET_CLIENTS_PAGE: &str = include_str!("sql/get_clients_page.sql");

#[cfg_attr(feature = "test-utils", mockall::automock)]
pub trait ClientRepository: Send + Sync {
    fn upsert(&self, conn: &Connection, clients: &[NewClient])
    -> Result<(), ClientRepositoryError>;
    fn get_clients(
        &self,
        conn: &Connection,
        client_ids: &[u16],
    ) -> Result<HashMap<u16, Client>, ClientRepositoryError>;
    fn get_clients_page(
        &self,
        conn: &Connection,
        after_id: Option<u16>,
        limit: usize,
    ) -> Result<Vec<Client>, ClientRepositoryError>;
}

pub struct SqliteClientRepository;

impl ClientRepository for SqliteClientRepository {
    fn upsert(
        &self,
        conn: &Connection,
        clients: &[NewClient],
    ) -> Result<(), ClientRepositoryError> {
        let clients_json = serde_json::to_string(clients).map_err(RepositoryError::from)?;
        let changed = conn
            .execute(UPSERT, [clients_json])
            .map_err(RepositoryError::from)?;

        if changed != clients.len() {
            return Err(ClientRepositoryError::VersionConflict {
                written: changed,
                expected: clients.len(),
            });
        }

        Ok(())
    }

    fn get_clients(
        &self,
        conn: &Connection,
        client_ids: &[u16],
    ) -> Result<HashMap<u16, Client>, ClientRepositoryError> {
        let ids = client_ids
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(",");

        let mut stmt = conn
            .prepare_cached(GET_CLIENTS)
            .map_err(RepositoryError::from)?;
        let rows = stmt
            .query_map([format!("[{ids}]")], |row| Client::try_from(row))
            .map_err(RepositoryError::from)?;

        let mut clients = HashMap::new();
        for client in rows {
            let client = client.map_err(RepositoryError::from)?;
            clients.insert(client.id, client);
        }
        Ok(clients)
    }

    fn get_clients_page(
        &self,
        conn: &Connection,
        after_id: Option<u16>,
        limit: usize,
    ) -> Result<Vec<Client>, ClientRepositoryError> {
        let mut stmt = conn
            .prepare_cached(GET_CLIENTS_PAGE)
            .map_err(RepositoryError::from)?;
        let clients = stmt
            .query_map(rusqlite::params![after_id, limit as i64], |row| {
                Client::try_from(row)
            })
            .map_err(RepositoryError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RepositoryError::from)?;
        Ok(clients)
    }
}
