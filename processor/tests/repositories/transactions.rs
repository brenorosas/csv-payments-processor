use std::collections::HashMap;

use processor::storage::error::RepositoryError;
use processor::storage::transactions::entity::{NewTransaction, Transaction, TransactionType};
use processor::storage::transactions::error::TransactionRepositoryError;
use processor::storage::transactions::repository::{
    SqliteTransactionRepository, TransactionRepository,
};
use tokio_rusqlite::Connection;

use super::setup;

async fn insert(
    conn: &Connection,
    transactions: Vec<NewTransaction>,
) -> Result<(), tokio_rusqlite::Error<TransactionRepositoryError>> {
    conn.call(move |conn| SqliteTransactionRepository.insert(conn, &transactions))
        .await
}

async fn get_last_versions(
    conn: &Connection,
    transaction_ids: Vec<u32>,
) -> HashMap<u32, Transaction> {
    conn.call(move |conn| SqliteTransactionRepository.get_last_versions(conn, &transaction_ids))
        .await
        .unwrap()
}

fn assert_stored(transaction: &Transaction, new_transaction: &NewTransaction) {
    assert_eq!(transaction.r#type, new_transaction.r#type);
    assert_eq!(transaction.client_id, new_transaction.client_id);
    assert_eq!(transaction.id, new_transaction.id);
    assert_eq!(transaction.version, new_transaction.version);
    assert_eq!(transaction.amount, new_transaction.amount);
    assert!(!transaction.created_at.is_empty());
}

#[tokio::test]
async fn insert_then_get_last_versions_returns_the_last_version_of_the_requested_ids() {
    let conn = setup().await;

    let new_transactions = vec![
        NewTransaction::new_for_tests(TransactionType::Deposit, 1, 10, 1, Some(15_000)),
        NewTransaction::new_for_tests(TransactionType::Dispute, 1, 10, 2, None),
        NewTransaction::new_for_tests(TransactionType::Resolve, 1, 10, 3, None),
        NewTransaction::new_for_tests(TransactionType::Withdrawal, 2, 20, 1, Some(5_000)),
        NewTransaction::new_for_tests(TransactionType::Deposit, 3, 30, 1, Some(7_500)),
    ];
    insert(&conn, new_transactions.clone()).await.unwrap();

    let transactions = get_last_versions(&conn, vec![10, 20, 99]).await;

    assert_eq!(transactions.len(), 2);
    assert_stored(&transactions[&10], &new_transactions[2]);
    assert_stored(&transactions[&20], &new_transactions[3]);
}

#[tokio::test]
async fn insert_rejects_duplicated_id_and_version() {
    let conn = setup().await;
    let transaction = NewTransaction::new_for_tests(TransactionType::Deposit, 1, 1, 1, Some(100));

    insert(&conn, vec![transaction.clone()]).await.unwrap();

    assert!(matches!(
        insert(&conn, vec![transaction]).await,
        Err(tokio_rusqlite::Error::Error(
            TransactionRepositoryError::Repository(RepositoryError::Database(_))
        ))
    ));
}
