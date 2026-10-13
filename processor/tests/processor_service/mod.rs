mod process_transactions;

use std::collections::HashMap;
use std::sync::Arc;

use processor::processor_service::service::ProcessorService;
use processor::storage::client::entity::Client;
use processor::storage::client::repository::{ClientRepository, SqliteClientRepository};
use processor::storage::db::run_migrations;
use processor::storage::transactions::entity::Transaction;
use processor::storage::transactions::repository::{
    SqliteTransactionRepository, TransactionRepository,
};
use tokio_rusqlite::Connection;

async fn setup() -> (ProcessorService, Connection) {
    let conn = Connection::open_in_memory().await.unwrap();
    conn.call(|conn| Ok::<_, rusqlite::Error>(run_migrations(conn)))
        .await
        .unwrap()
        .unwrap();

    let service = ProcessorService::new(
        conn.clone(),
        Arc::new(SqliteClientRepository),
        Arc::new(SqliteTransactionRepository),
    );

    (service, conn)
}

async fn get_clients(conn: &Connection, ids: Vec<u16>) -> HashMap<u16, Client> {
    conn.call(move |conn| SqliteClientRepository.get_clients(conn, &ids))
        .await
        .unwrap()
}

async fn get_last_transactions(conn: &Connection, ids: Vec<u32>) -> HashMap<u32, Transaction> {
    conn.call(move |conn| SqliteTransactionRepository.get_last_versions(conn, &ids))
        .await
        .unwrap()
}
