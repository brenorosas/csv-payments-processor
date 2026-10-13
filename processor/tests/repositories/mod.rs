mod clients;
mod transactions;

use processor::storage::db::run_migrations;
use tokio_rusqlite::Connection;

async fn setup() -> Connection {
    let conn = Connection::open_in_memory().await.unwrap();
    run_migrations(&conn).await.unwrap();
    conn
}
