use thiserror::Error;

/// Errors common to every repository.
#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("could not serialize the batch: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("connection error: {0}")]
    Connection(tokio_rusqlite::Error),
}

impl From<tokio_rusqlite::Error> for RepositoryError {
    fn from(error: tokio_rusqlite::Error) -> Self {
        match error {
            tokio_rusqlite::Error::Error(error) => Self::Database(error),
            other => Self::Connection(other),
        }
    }
}
