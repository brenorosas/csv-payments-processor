use std::fs;
use std::path::Path;

use tokio_rusqlite::Connection;

pub async fn run_migrations(conn: &Connection) -> anyhow::Result<()> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");

    let mut files: Vec<_> = fs::read_dir(dir)?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<_, _>>()?;

    files.sort();

    let migrations = files
        .into_iter()
        .map(fs::read_to_string)
        .collect::<Result<Vec<_>, _>>()?;

    conn.call(move |conn| -> Result<(), rusqlite::Error> {
        for sql in &migrations {
            conn.execute_batch(sql)?;
        }
        Ok(())
    })
    .await?;

    Ok(())
}
