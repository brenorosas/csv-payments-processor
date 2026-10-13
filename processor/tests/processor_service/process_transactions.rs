use std::sync::Arc;

use processor::processor_service::error::ProcessorServiceError;
use processor::processor_service::service::ProcessorService;
use processor::storage::client::error::ClientRepositoryError;
use processor::storage::client::repository::{
    ClientRepository, MockClientRepository, SqliteClientRepository,
};
use processor::storage::error::RepositoryError;
use processor::storage::transactions::entity::TransactionType;
use processor::storage::transactions::error::TransactionRepositoryError;
use processor::storage::transactions::repository::{
    MockTransactionRepository, SqliteTransactionRepository, TransactionRepository,
};
use processor::transaction_csv::TransactionCsv;

use super::{get_clients, get_last_transactions, setup};

fn csv(r#type: TransactionType, client: u16, tx: u32, amount: Option<&str>) -> TransactionCsv {
    TransactionCsv {
        r#type,
        client,
        tx,
        amount: amount.map(String::from),
    }
}

#[tokio::test]
async fn deposits_and_withdrawals_are_saved_and_later_batches_see_them() {
    let (service, conn) = setup().await;

    service
        .process_transactions(&[
            csv(TransactionType::Deposit, 1, 1, Some("1.0")),
            csv(TransactionType::Deposit, 2, 2, Some("2.0")),
            csv(TransactionType::Deposit, 1, 3, Some("2.0")),
            csv(TransactionType::Withdrawal, 1, 4, Some("1.5")),
            csv(TransactionType::Withdrawal, 2, 5, Some("3.0")),
        ])
        .await
        .unwrap();

    let clients = get_clients(&conn, vec![1, 2]).await;
    assert_eq!(clients.len(), 2);
    assert_eq!(clients[&1].available_amount, 15_000);
    assert_eq!(clients[&1].held_amount, 0);
    assert_eq!(clients[&1].total_amount, 15_000);
    assert_eq!(clients[&2].available_amount, 20_000);
    assert_eq!(clients[&2].total_amount, 20_000);

    let transactions = get_last_transactions(&conn, vec![1, 2, 3, 4, 5]).await;
    assert_eq!(transactions.len(), 4);
    assert_eq!(transactions[&4].r#type, TransactionType::Withdrawal);
    assert_eq!(transactions[&4].client_id, 1);
    assert_eq!(transactions[&4].amount, Some(15_000));

    service
        .process_transactions(&[
            csv(TransactionType::Deposit, 1, 1, Some("9.0")),
            csv(TransactionType::Deposit, 3, 6, Some("0.0001")),
        ])
        .await
        .unwrap();

    let clients = get_clients(&conn, vec![1, 3]).await;
    assert_eq!(clients[&1].available_amount, 15_000);
    assert_eq!(clients[&3].available_amount, 1);

    let transactions = get_last_transactions(&conn, vec![1, 6]).await;
    assert_eq!(transactions[&1].amount, Some(10_000));
    assert_eq!(transactions[&6].client_id, 3);
}

#[tokio::test]
async fn dispute_resolve_and_chargeback_move_the_funds_across_batches() {
    let (service, conn) = setup().await;

    service
        .process_transactions(&[
            csv(TransactionType::Deposit, 1, 1, Some("2")),
            csv(TransactionType::Deposit, 1, 2, Some("1")),
            csv(TransactionType::Dispute, 1, 1, None),
        ])
        .await
        .unwrap();

    let client = &get_clients(&conn, vec![1]).await[&1];
    assert_eq!(client.available_amount, 10_000);
    assert_eq!(client.held_amount, 20_000);
    assert_eq!(client.total_amount, 30_000);
    assert!(!client.locked);
    assert_eq!(client.version, 1);
    let transactions = get_last_transactions(&conn, vec![1, 2]).await;
    assert_eq!(transactions[&1].r#type, TransactionType::Dispute);
    assert_eq!(transactions[&1].version, 2);
    assert_eq!(transactions[&1].amount, Some(20_000));
    assert_eq!(transactions[&2].r#type, TransactionType::Deposit);

    service
        .process_transactions(&[csv(TransactionType::Resolve, 1, 1, None)])
        .await
        .unwrap();

    let client = &get_clients(&conn, vec![1]).await[&1];
    assert_eq!(client.available_amount, 30_000);
    assert_eq!(client.held_amount, 0);
    assert_eq!(client.total_amount, 30_000);
    assert!(!client.locked);
    assert_eq!(client.version, 2);
    let transactions = get_last_transactions(&conn, vec![1]).await;
    assert_eq!(transactions[&1].r#type, TransactionType::Resolve);
    assert_eq!(transactions[&1].version, 3);

    service
        .process_transactions(&[
            csv(TransactionType::Dispute, 1, 2, None),
            csv(TransactionType::Chargeback, 1, 2, None),
        ])
        .await
        .unwrap();

    let client = &get_clients(&conn, vec![1]).await[&1];
    assert_eq!(client.available_amount, 20_000);
    assert_eq!(client.held_amount, 0);
    assert_eq!(client.total_amount, 20_000);
    assert!(client.locked);
    assert_eq!(client.version, 3);
    let transactions = get_last_transactions(&conn, vec![2]).await;
    assert_eq!(transactions[&2].r#type, TransactionType::Chargeback);
    assert_eq!(transactions[&2].version, 3);
}

#[tokio::test]
async fn invalid_transactions_are_ignored_and_every_client_is_still_created() {
    let (service, conn) = setup().await;

    service
        .process_transactions(&[
            csv(TransactionType::Deposit, 7, 1, Some("2")),
            csv(TransactionType::Deposit, 7, 1, Some("9")),
            csv(TransactionType::Withdrawal, 60, 2, Some("1")),
            csv(TransactionType::Deposit, 3, 3, Some("abc")),
            csv(TransactionType::Deposit, 3, 4, None),
            csv(TransactionType::Dispute, 5, 99, None),
            csv(TransactionType::Resolve, 7, 1, None),
            csv(TransactionType::Chargeback, 7, 1, None),
            csv(TransactionType::Deposit, 7, 5, Some("1")),
        ])
        .await
        .unwrap();

    let clients = get_clients(&conn, vec![3, 5, 7, 60]).await;
    assert_eq!(clients.len(), 4);
    assert_eq!(clients[&7].available_amount, 30_000);
    assert_eq!(clients[&7].total_amount, 30_000);
    for id in [3, 5, 60] {
        assert_eq!(clients[&id].available_amount, 0);
        assert_eq!(clients[&id].held_amount, 0);
        assert_eq!(clients[&id].total_amount, 0);
        assert!(!clients[&id].locked);
    }
    assert!(!clients[&7].locked);

    let transactions = get_last_transactions(&conn, vec![1, 2, 3, 4, 5, 99]).await;
    let mut saved: Vec<u32> = transactions.keys().copied().collect();
    saved.sort();
    assert_eq!(saved, vec![1, 5]);
    assert_eq!(transactions[&1].amount, Some(20_000));
}

#[tokio::test]
async fn a_failing_client_upsert_saves_nothing_and_skips_the_transaction_insert() {
    let (service, conn) = setup().await;

    let mut client_repository = MockClientRepository::new();
    client_repository
        .expect_get_clients()
        .returning(|conn, ids| SqliteClientRepository.get_clients(conn, ids));
    client_repository
        .expect_upsert()
        .times(1)
        .returning(|_, _| {
            Err(ClientRepositoryError::VersionConflict {
                written: 0,
                expected: 1,
            })
        });

    let mut transaction_repository = MockTransactionRepository::new();
    transaction_repository
        .expect_get_last_versions()
        .returning(|conn, ids| SqliteTransactionRepository.get_last_versions(conn, ids));
    transaction_repository.expect_insert().never();

    let mocked_service = ProcessorService::new(
        conn.clone(),
        Arc::new(client_repository),
        Arc::new(transaction_repository),
    );

    service
        .process_transactions(&[csv(TransactionType::Deposit, 1, 1, Some("1"))])
        .await
        .unwrap();

    let result = mocked_service
        .process_transactions(&[
            csv(TransactionType::Deposit, 1, 2, Some("1")),
            csv(TransactionType::Deposit, 2, 3, Some("5")),
        ])
        .await;

    assert!(matches!(
        result,
        Err(ProcessorServiceError::Client(
            ClientRepositoryError::VersionConflict {
                written: 0,
                expected: 1
            }
        ))
    ));

    let clients = get_clients(&conn, vec![1, 2]).await;
    assert_eq!(clients.len(), 1);
    assert_eq!(clients[&1].available_amount, 10_000);
    assert_eq!(clients[&1].version, 1);
    let transactions = get_last_transactions(&conn, vec![1, 2, 3]).await;
    assert_eq!(transactions.len(), 1);
    assert!(transactions.contains_key(&1));
}

#[tokio::test]
async fn a_failing_transaction_insert_rolls_back_the_client_upsert() {
    let (service, conn) = setup().await;

    let mut client_repository = MockClientRepository::new();
    client_repository
        .expect_get_clients()
        .returning(|conn, ids| SqliteClientRepository.get_clients(conn, ids));
    client_repository
        .expect_upsert()
        .withf(|_, clients| clients.len() == 2)
        .times(1)
        .returning(|conn, clients| SqliteClientRepository.upsert(conn, clients));

    let mut transaction_repository = MockTransactionRepository::new();
    transaction_repository
        .expect_get_last_versions()
        .returning(|conn, ids| SqliteTransactionRepository.get_last_versions(conn, ids));
    transaction_repository
        .expect_insert()
        .withf(|_, transactions| transactions.len() == 2)
        .times(1)
        .returning(|_, _| Err(RepositoryError::Database(rusqlite::Error::InvalidQuery).into()));

    let mocked_service = ProcessorService::new(
        conn.clone(),
        Arc::new(client_repository),
        Arc::new(transaction_repository),
    );

    service
        .process_transactions(&[csv(TransactionType::Deposit, 1, 1, Some("1"))])
        .await
        .unwrap();

    let result = mocked_service
        .process_transactions(&[
            csv(TransactionType::Deposit, 1, 2, Some("1")),
            csv(TransactionType::Deposit, 2, 3, Some("5")),
        ])
        .await;

    assert!(matches!(
        result,
        Err(ProcessorServiceError::Transaction(
            TransactionRepositoryError::Repository(RepositoryError::Database(_))
        ))
    ));

    let clients = get_clients(&conn, vec![1, 2]).await;
    assert_eq!(clients.len(), 1);
    assert_eq!(clients[&1].available_amount, 10_000);
    assert_eq!(clients[&1].total_amount, 10_000);
    assert_eq!(clients[&1].version, 1);
    let transactions = get_last_transactions(&conn, vec![1, 2, 3]).await;
    assert_eq!(transactions.len(), 1);
    assert!(transactions.contains_key(&1));

    service
        .process_transactions(&[csv(TransactionType::Deposit, 1, 2, Some("1"))])
        .await
        .unwrap();

    let clients = get_clients(&conn, vec![1]).await;
    assert_eq!(clients[&1].available_amount, 20_000);
    assert_eq!(clients[&1].version, 2);
    assert_eq!(get_last_transactions(&conn, vec![2]).await.len(), 1);
}
