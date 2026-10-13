use thiserror::Error;

use crate::storage::error::RepositoryError;

#[derive(Debug, Error)]
pub enum TransactionRepositoryError {
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}
