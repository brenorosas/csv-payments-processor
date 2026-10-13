use processor::storage::error::RepositoryError;
use processor::storage::transactions::entity::{NewTransaction, Transaction, TransactionType};
use processor::storage::transactions::error::TransactionRepositoryError;
use processor::storage::transactions::repository::{
    SqliteTransactionRepository, TransactionRepository,
};

use super::setup;

fn assert_stored(transaction: &Transaction, new_transaction: &NewTransaction) {
    assert_eq!(transaction.r#type, new_transaction.r#type);
    assert_eq!(transaction.client_id, new_transaction.client_id);
    assert_eq!(transaction.id, new_transaction.id);
    assert_eq!(transaction.version, new_transaction.version);
    assert_eq!(transaction.amount, new_transaction.amount);
    assert!(!transaction.created_at.is_empty());
}

#[test]
fn insert_then_get_last_versions_returns_the_last_version_of_the_requested_ids() {
    let conn = setup();
    let repository = SqliteTransactionRepository::new(&conn);

    let new_transactions = [
        NewTransaction::new_for_tests(TransactionType::Deposit, 1, 10, 1, Some(15_000)),
        NewTransaction::new_for_tests(TransactionType::Dispute, 1, 10, 2, None),
        NewTransaction::new_for_tests(TransactionType::Resolve, 1, 10, 3, None),
        NewTransaction::new_for_tests(TransactionType::Withdrawal, 2, 20, 1, Some(5_000)),
        NewTransaction::new_for_tests(TransactionType::Deposit, 3, 30, 1, Some(7_500)),
    ];
    repository.insert(&new_transactions).unwrap();

    let transactions = repository.get_last_versions(&[10, 20, 99]).unwrap();

    assert_eq!(transactions.len(), 2);
    assert_stored(&transactions[&10], &new_transactions[2]);
    assert_stored(&transactions[&20], &new_transactions[3]);
}

#[test]
fn insert_rejects_duplicated_id_and_version() {
    let conn = setup();
    let repository = SqliteTransactionRepository::new(&conn);
    let transaction = NewTransaction::new_for_tests(TransactionType::Deposit, 1, 1, 1, Some(100));

    repository
        .insert(std::slice::from_ref(&transaction))
        .unwrap();

    assert!(matches!(
        repository.insert(&[transaction]),
        Err(TransactionRepositoryError::Repository(
            RepositoryError::Database(_)
        ))
    ));
}
