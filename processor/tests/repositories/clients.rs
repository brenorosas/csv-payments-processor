use std::collections::HashMap;

use processor::storage::client::entity::{Client, NewClient};
use processor::storage::client::error::ClientRepositoryError;
use processor::storage::client::repository::{ClientRepository, SqliteClientRepository};
use processor::storage::error::RepositoryError;
use tokio_rusqlite::Connection;

use super::setup;

async fn read_clients(conn: &Connection) -> Vec<Client> {
    conn.call(|conn| -> Result<Vec<Client>, rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT * FROM clients ORDER BY id")?;
        stmt.query_map([], |row| Client::try_from(row))?.collect()
    })
    .await
    .unwrap()
}

async fn upsert(
    conn: &Connection,
    clients: Vec<NewClient>,
) -> Result<(), tokio_rusqlite::Error<ClientRepositoryError>> {
    conn.call(move |conn| {
        let tx = conn.transaction().map_err(RepositoryError::from)?;
        SqliteClientRepository.upsert(&tx, &clients)?;
        tx.commit().map_err(RepositoryError::from)?;
        Ok::<_, ClientRepositoryError>(())
    })
    .await
}

async fn get_clients(conn: &Connection, client_ids: Vec<u16>) -> HashMap<u16, Client> {
    conn.call(move |conn| SqliteClientRepository.get_clients(conn, &client_ids))
        .await
        .unwrap()
}

fn assert_stored(client: &Client, new_client: &NewClient, expected_version: u32) {
    assert_eq!(client.id, new_client.id);
    assert_eq!(client.available_amount, new_client.available_amount);
    assert_eq!(client.held_amount, new_client.held_amount);
    assert_eq!(client.total_amount, new_client.total_amount);
    assert_eq!(client.locked, new_client.locked);
    assert_eq!(client.version, expected_version);
    assert!(!client.created_at.is_empty());
    assert!(!client.updated_at.is_empty());
}

#[tokio::test]
async fn upsert_inserts_new_clients_and_updates_existing_ones() {
    let conn = setup().await;

    let first = vec![
        NewClient::new_for_tests(1, 10_000, 2_500, 12_500, false, 1),
        NewClient::new_for_tests(2, 25_000, 0, 25_000, true, 1),
    ];
    upsert(&conn, first.clone()).await.unwrap();

    let stored = read_clients(&conn).await;
    assert_eq!(stored.len(), first.len());
    for (client, new_client) in stored.iter().zip(&first) {
        assert_stored(client, new_client, 1);
    }

    let second = vec![
        NewClient::new_for_tests(1, 4_000, 6_000, 10_000, true, 1),
        NewClient::new_for_tests(3, 3_000, 0, 3_000, false, 1),
    ];
    upsert(&conn, second.clone()).await.unwrap();

    let stored_after = read_clients(&conn).await;
    assert_eq!(stored_after.len(), 3);
    assert_stored(&stored_after[0], &second[0], 2);
    assert_eq!(stored_after[0].created_at, stored[0].created_at);
    assert_eq!(stored_after[1], stored[1]);
    assert_stored(&stored_after[2], &second[1], 1);
}

#[tokio::test]
async fn upsert_fails_when_the_version_is_stale() {
    let conn = setup().await;

    upsert(
        &conn,
        vec![
            NewClient::new_for_tests(1, 1_000, 0, 1_000, false, 1),
            NewClient::new_for_tests(2, 2_000, 0, 2_000, false, 1),
        ],
    )
    .await
    .unwrap();
    upsert(
        &conn,
        vec![NewClient::new_for_tests(1, 900, 0, 900, false, 1)],
    )
    .await
    .unwrap();
    let before = read_clients(&conn).await;

    let result = upsert(
        &conn,
        vec![
            NewClient::new_for_tests(2, 1_500, 500, 2_000, true, 1),
            NewClient::new_for_tests(3, 3_000, 0, 3_000, false, 1),
            NewClient::new_for_tests(1, 500, 0, 500, true, 1),
        ],
    )
    .await;

    assert!(matches!(
        result,
        Err(tokio_rusqlite::Error::Error(
            ClientRepositoryError::VersionConflict {
                written: 2,
                expected: 3
            }
        ))
    ));
    assert_eq!(read_clients(&conn).await, before);
}

#[tokio::test]
async fn get_clients_returns_only_the_requested_existing_clients() {
    let conn = setup().await;

    let new_clients = vec![
        NewClient::new_for_tests(1, 10_000, 2_500, 12_500, false, 1),
        NewClient::new_for_tests(2, 25_000, 0, 25_000, true, 1),
        NewClient::new_for_tests(3, 3_000, 500, 3_500, false, 1),
    ];
    upsert(&conn, new_clients.clone()).await.unwrap();

    let clients = get_clients(&conn, vec![1, 3, 99]).await;

    assert_eq!(clients.len(), 2);
    assert_stored(&clients[&1], &new_clients[0], 1);
    assert_stored(&clients[&3], &new_clients[2], 1);
}
