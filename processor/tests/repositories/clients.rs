use processor::storage::client::entity::{Client, NewClient};
use processor::storage::client::error::ClientRepositoryError;
use processor::storage::client::repository::{ClientRepository, SqliteClientRepository};

use super::setup;

fn read_clients(conn: &rusqlite::Connection) -> Vec<Client> {
    let mut stmt = conn.prepare("SELECT * FROM clients ORDER BY id").unwrap();
    stmt.query_map([], |row| Client::try_from(row))
        .unwrap()
        .collect::<Result<_, _>>()
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

#[test]
fn upsert_inserts_new_clients_and_updates_existing_ones() {
    let conn = setup();
    let repository = SqliteClientRepository::new(&conn);

    let first = [
        NewClient::new_for_tests(1, 10_000, 2_500, 12_500, false, 1),
        NewClient::new_for_tests(2, 25_000, 0, 25_000, true, 1),
    ];
    repository.upsert(&first).unwrap();

    let stored = read_clients(&conn);
    assert_eq!(stored.len(), first.len());
    for (client, new_client) in stored.iter().zip(&first) {
        assert_stored(client, new_client, 1);
    }

    let second = [
        NewClient::new_for_tests(1, 4_000, 6_000, 10_000, true, 1),
        NewClient::new_for_tests(3, 3_000, 0, 3_000, false, 1),
    ];
    repository.upsert(&second).unwrap();

    let stored_after = read_clients(&conn);
    assert_eq!(stored_after.len(), 3);
    assert_stored(&stored_after[0], &second[0], 2);
    assert_eq!(stored_after[0].created_at, stored[0].created_at);
    assert_eq!(stored_after[1], stored[1]);
    assert_stored(&stored_after[2], &second[1], 1);
}

#[test]
fn upsert_fails_when_the_version_is_stale() {
    let conn = setup();
    let repository = SqliteClientRepository::new(&conn);

    repository
        .upsert(&[
            NewClient::new_for_tests(1, 1_000, 0, 1_000, false, 1),
            NewClient::new_for_tests(2, 2_000, 0, 2_000, false, 1),
        ])
        .unwrap();
    repository
        .upsert(&[NewClient::new_for_tests(1, 900, 0, 900, false, 1)])
        .unwrap();
    let before = read_clients(&conn);

    let result = repository.upsert(&[
        NewClient::new_for_tests(2, 1_500, 500, 2_000, true, 1),
        NewClient::new_for_tests(3, 3_000, 0, 3_000, false, 1),
        NewClient::new_for_tests(1, 500, 0, 500, true, 1),
    ]);

    assert!(matches!(
        result,
        Err(ClientRepositoryError::VersionConflict {
            written: 2,
            expected: 3
        })
    ));
    assert_eq!(read_clients(&conn), before);
}
