use rusqlite::Connection;

use super::migrations::MIGRATIONS;

pub fn run_migrations(conn: &Connection) -> anyhow::Result<()> {
    for sql in MIGRATIONS {
        conn.execute_batch(sql)?;
    }

    Ok(())
}
