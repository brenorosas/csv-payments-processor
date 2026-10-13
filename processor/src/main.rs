use std::sync::Arc;

use processor::processor_service::service::ProcessorService;
use processor::storage;
use processor::transaction_csv::TransactionCsv;
use storage::client::repository::SqliteClientRepository;
use storage::transactions::repository::SqliteTransactionRepository;
use tempfile::NamedTempFile;
use tokio_rusqlite::Connection;

const BATCH_SIZE: usize = 1000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let file = NamedTempFile::new()?;
    storage::db::run_migrations(&rusqlite::Connection::open(file.path())?)?;
    let conn = Connection::open(file.path()).await?;

    let processor_service = ProcessorService::new(
        conn,
        Arc::new(SqliteClientRepository),
        Arc::new(SqliteTransactionRepository),
    );

    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: cargo run -- transactions.csv"))?;
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)?;

    let mut batch = Vec::with_capacity(BATCH_SIZE);

    for record in reader.deserialize::<TransactionCsv>() {
        let transaction = record?;
        batch.push(transaction);
        if batch.len() >= BATCH_SIZE {
            processor_service.process_transactions(&batch).await?;
            batch.clear();
        }
    }

    println!("database ready at {}", file.path().display());
    Ok(())
}
