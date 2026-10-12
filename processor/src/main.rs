mod storage;

use rusqlite::Connection;
use storage::client::repository::SqliteClientRepository;
use storage::transactions::repository::SqliteTransactionRepository;
use tempfile::NamedTempFile;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file = NamedTempFile::new()?;
    let conn = Connection::open(file.path())?;
    storage::db::run_migrations(&conn)?;

    let client_repository = SqliteClientRepository::new(&conn);
    let transaction_repository = SqliteTransactionRepository::new(&conn);

    println!("database ready at {}", file.path().display());
    Ok(())
}
