use std::fs;
use std::path::Path;

use rusqlite::Connection;

pub fn run_migrations(conn: &Connection) -> anyhow::Result<()> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");

    let mut files: Vec<_> = fs::read_dir(dir)?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<_, _>>()?;

    files.sort();

    for path in files {
        conn.execute_batch(&fs::read_to_string(path)?)?;
    }

    Ok(())
}
