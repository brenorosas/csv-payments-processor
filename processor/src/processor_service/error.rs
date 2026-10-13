use thiserror::Error;

use crate::storage::client::error::ClientRepositoryError;
use crate::storage::transactions::error::TransactionRepositoryError;

#[derive(Debug, Error)]
pub enum ProcessorServiceError {
    #[error(transparent)]
    Client(#[from] ClientRepositoryError),

    #[error(transparent)]
    Transaction(#[from] TransactionRepositoryError),

    #[error(transparent)]
    Database(#[from] rusqlite::Error),

    #[error("connection error: {0}")]
    Connection(String),
}

impl From<tokio_rusqlite::Error<ProcessorServiceError>> for ProcessorServiceError {
    fn from(error: tokio_rusqlite::Error<ProcessorServiceError>) -> Self {
        match error {
            tokio_rusqlite::Error::Error(error) => error,
            other => Self::Connection(other.to_string()),
        }
    }
}
