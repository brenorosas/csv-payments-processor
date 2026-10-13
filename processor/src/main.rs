mod storage;
mod transaction_csv;

use rusqlite::Connection;
use storage::client::repository::SqliteClientRepository;
use storage::transactions::repository::SqliteTransactionRepository;
use tempfile::NamedTempFile;
use transaction_csv::TransactionCsv;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file = NamedTempFile::new()?;
    let conn = Connection::open(file.path())?;
    storage::db::run_migrations(&conn)?;

    let client_repository = SqliteClientRepository::new(&conn);
    let transaction_repository = SqliteTransactionRepository::new(&conn);

    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: cargo run -- transactions.csv"))?;
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)?;

    for record in reader.deserialize::<TransactionCsv>() {
        let transaction = record?;
        println!("{transaction:?}");
    }

    println!("database ready at {}", file.path().display());
    Ok(())
}
