use rusqlite::Row;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq)]
pub struct Client {
    pub id: u16,
    pub available_amount: i64,
    pub held_amount: i64,
    pub total_amount: i64,
    pub locked: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NewClient {
    pub id: u16,
    pub available_amount: i64,
    pub held_amount: i64,
    pub total_amount: i64,
    pub locked: bool,
}

impl TryFrom<&Row<'_>> for Client {
    type Error = rusqlite::Error;

    fn try_from(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get("id")?,
            available_amount: row.get("available_amount")?,
            held_amount: row.get("held_amount")?,
            total_amount: row.get("total_amount")?,
            locked: row.get("locked")?,
            created_at: row.get("created_at")?,
            updated_at: row.get("updated_at")?,
        })
    }
}
