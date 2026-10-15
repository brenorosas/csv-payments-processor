use std::sync::Arc;

use processor::client_csv::ClientCsv;
use processor::processor_service::service::ProcessorService;
use processor::storage;
use processor::transaction_csv::TransactionCsv;
use storage::client::repository::{ClientRepository, SqliteClientRepository};
use storage::transactions::repository::SqliteTransactionRepository;
use tokio_rusqlite::Connection;

const BATCH_SIZE: usize = 1000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (path, conn, processor_service) = setup().await?;

    process_csv_in_batches(&processor_service, &path).await?;
    print_clients(&conn).await?;

    Ok(())
}

async fn setup() -> anyhow::Result<(String, Connection, ProcessorService)> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: cargo run -- transactions.csv"))?;

    let conn = Connection::open_in_memory().await?;
    conn.call(|conn| Ok::<_, rusqlite::Error>(storage::db::run_migrations(conn)))
        .await??;

    let processor_service = ProcessorService::new(
        conn.clone(),
        Arc::new(SqliteClientRepository),
        Arc::new(SqliteTransactionRepository),
    );

    Ok((path, conn, processor_service))
}

async fn process_csv_in_batches(
    processor_service: &ProcessorService,
    path: &str,
) -> anyhow::Result<()> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .flexible(true)
        .from_path(path)?;

    let mut batch = Vec::with_capacity(BATCH_SIZE);

    for record in reader.deserialize::<TransactionCsv>() {
        let transaction = match record {
            Ok(transaction) => transaction,
            Err(error) if matches!(error.kind(), csv::ErrorKind::Io(_)) => return Err(error.into()),
            Err(error) => {
                tracing::warn!(%error, "row ignored: could not be parsed");
                continue;
            }
        };
        batch.push(transaction);
        if batch.len() >= BATCH_SIZE {
            processor_service.process_transactions(&batch).await?;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        processor_service.process_transactions(&batch).await?;
    }

    Ok(())
}

async fn print_clients(conn: &Connection) -> anyhow::Result<()> {
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(std::io::stdout());
    writer.write_record(["client", "available", "held", "total", "locked"])?;
    let mut after_id = None;
    loop {
        let clients = conn
            .call(move |conn| SqliteClientRepository.get_clients_page(conn, after_id, BATCH_SIZE))
            .await?;
        for client in &clients {
            writer.serialize(ClientCsv::from(client))?;
        }
        if clients.len() < BATCH_SIZE {
            break;
        }
        after_id = clients.last().map(|client| client.id);
    }
    writer.flush()?;

    Ok(())
}
