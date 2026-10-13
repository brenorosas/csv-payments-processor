use thiserror::Error;

use crate::storage::error::RepositoryError;

#[derive(Debug, Error)]
pub enum ClientRepositoryError {
    #[error(transparent)]
    Repository(#[from] RepositoryError),

    #[error("version conflict: only {written} of {expected} clients were written")]
    VersionConflict { written: usize, expected: usize },
}
