mod storage;

use rusqlite::Connection;
use tempfile::NamedTempFile;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file = NamedTempFile::new()?;
    let conn = Connection::open(file.path())?;
    storage::db::run_migrations(&conn)?;

    println!("database ready at {}", file.path().display());
    Ok(())
}
