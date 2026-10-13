use thiserror::Error;

/// Errors common to every repository.
#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("could not serialize the batch: {0}")]
    Serialization(#[from] serde_json::Error),
}
