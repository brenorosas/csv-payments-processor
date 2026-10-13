mod clients;
mod transactions;

use processor::storage::db::run_migrations;
use rusqlite::Connection;

fn setup() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    run_migrations(&conn).unwrap();
    conn
}
