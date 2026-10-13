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

    #[error("deposit with transaction id {0}, which already exists")]
    DepositWithExistingTransactionId(u32),

    #[error("withdrawal with transaction id {0}, which already exists")]
    WithdrawalWithExistingTransactionId(u32),

    #[error("withdrawal {tx} exceeds the available amount of client {client}")]
    WithdrawalWithInsufficientFunds { tx: u32, client: u16 },

    #[error("transaction {0} has no amount")]
    MissingAmount(u32),

    #[error("transaction {tx} has an invalid amount `{amount}`")]
    InvalidAmount { tx: u32, amount: String },
}

impl From<tokio_rusqlite::Error<ProcessorServiceError>> for ProcessorServiceError {
    fn from(error: tokio_rusqlite::Error<ProcessorServiceError>) -> Self {
        match error {
            tokio_rusqlite::Error::Error(error) => error,
            other => Self::Connection(other.to_string()),
        }
    }
}
