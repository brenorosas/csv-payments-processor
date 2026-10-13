use rusqlite::Row;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq)]
pub struct Client {
    pub id: u16,
    pub available_amount: i64,
    pub held_amount: i64,
    pub total_amount: i64,
    pub locked: bool,
    pub version: u32,
    pub created_at: String,
    pub updated_at: String,
}

impl Client {
    pub fn new(id: u16) -> Self {
        Self {
            id,
            available_amount: 0,
            held_amount: 0,
            total_amount: 0,
            locked: false,
            version: 1,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }
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
            version: row.get("version")?,
            created_at: row.get("created_at")?,
            updated_at: row.get("updated_at")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NewClient {
    pub id: u16,
    pub available_amount: i64,
    pub held_amount: i64,
    pub total_amount: i64,
    pub locked: bool,
    pub version: u32,
}

impl NewClient {
    #[cfg(feature = "test-utils")]
    pub fn new_for_tests(
        id: u16,
        available_amount: i64,
        held_amount: i64,
        total_amount: i64,
        locked: bool,
        version: u32,
    ) -> Self {
        Self {
            id,
            available_amount,
            held_amount,
            total_amount,
            locked,
            version,
        }
    }
}
