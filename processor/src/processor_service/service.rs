use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio_rusqlite::Connection;

use super::error::ProcessorServiceError;
use crate::storage::client::entity::{Client, NewClient};
use crate::storage::client::repository::ClientRepository;
use crate::storage::transactions::entity::{NewTransaction, Transaction, TransactionType};
use crate::storage::transactions::repository::TransactionRepository;
use crate::transaction_csv::TransactionCsv;

pub struct ProcessorService {
    conn: Connection,
    client_repository: Arc<dyn ClientRepository>,
    transaction_repository: Arc<dyn TransactionRepository>,
}

impl ProcessorService {
    pub fn new(
        conn: Connection,
        client_repository: Arc<dyn ClientRepository>,
        transaction_repository: Arc<dyn TransactionRepository>,
    ) -> Self {
        Self {
            conn,
            client_repository,
            transaction_repository,
        }
    }

    pub async fn process_transactions(
        &self,
        transactions: &[TransactionCsv],
    ) -> Result<(), ProcessorServiceError> {
        let client_ids: Vec<u16> = transactions
            .iter()
            .map(|transaction| transaction.client)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let transaction_ids: Vec<u32> = transactions
            .iter()
            .map(|transaction| transaction.tx)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let client_repository = self.client_repository.clone();
        let transaction_repository = self.transaction_repository.clone();
        let (mut clients, mut last_transactions) = self
            .conn
            .call(move |conn| {
                let clients = client_repository.get_clients(conn, &client_ids)?;
                let last_transactions =
                    transaction_repository.get_last_versions(conn, &transaction_ids)?;
                Ok::<_, ProcessorServiceError>((clients, last_transactions))
            })
            .await?;

        let mut transactions_to_insert: Vec<NewTransaction> = Vec::new();

        for transaction in transactions {
            match transaction.r#type {
                TransactionType::Deposit => self.process_deposit(
                    transaction,
                    &mut clients,
                    &mut last_transactions,
                    &mut transactions_to_insert,
                )?,
                TransactionType::Withdrawal => self.process_withdrawal(
                    transaction,
                    &mut clients,
                    &mut last_transactions,
                    &mut transactions_to_insert,
                ),
                TransactionType::Dispute => self.process_dispute(
                    transaction,
                    &mut clients,
                    &mut last_transactions,
                    &mut transactions_to_insert,
                ),
                TransactionType::Resolve => self.process_resolve(
                    transaction,
                    &mut clients,
                    &mut last_transactions,
                    &mut transactions_to_insert,
                ),
                TransactionType::Chargeback => self.process_chargeback(
                    transaction,
                    &mut clients,
                    &mut last_transactions,
                    &mut transactions_to_insert,
                ),
            }
        }

        let clients_to_upsert: Vec<NewClient> = clients
            .values()
            .map(|client| NewClient {
                id: client.id,
                available_amount: client.available_amount,
                held_amount: client.held_amount,
                total_amount: client.total_amount,
                locked: client.locked,
                version: client.version,
            })
            .collect();

        let client_repository = self.client_repository.clone();
        let transaction_repository = self.transaction_repository.clone();
        self.conn
            .call(move |conn| {
                let tx = conn.transaction()?;
                client_repository.upsert(&tx, &clients_to_upsert)?;
                transaction_repository.insert(&tx, &transactions_to_insert)?;
                tx.commit()?;
                Ok::<_, ProcessorServiceError>(())
            })
            .await?;

        Ok(())
    }

    fn process_deposit(
        &self,
        transaction: &TransactionCsv,
        clients: &mut HashMap<u16, Client>,
        last_transactions: &mut HashMap<u32, Transaction>,
        transactions_to_insert: &mut Vec<NewTransaction>,
    ) -> Result<(), ProcessorServiceError> {
        if last_transactions.contains_key(&transaction.tx) {
            return Err(ProcessorServiceError::DepositWithExistingTransactionId(
                transaction.tx,
            ));
        }

        let amount = Self::parse_amount(transaction)?;

        let client = clients.entry(transaction.client).or_insert_with(|| Client {
            id: transaction.client,
            available_amount: 0,
            held_amount: 0,
            total_amount: 0,
            locked: false,
            version: 1,
            created_at: String::new(),
            updated_at: String::new(),
        });

        client.available_amount += amount;
        client.total_amount += amount;

        transactions_to_insert.push(NewTransaction {
            r#type: transaction.r#type,
            client_id: transaction.client,
            id: transaction.tx,
            version: 1,
            amount: Some(amount),
        });
        last_transactions.insert(
            transaction.tx,
            Transaction {
                r#type: transaction.r#type,
                client_id: transaction.client,
                id: transaction.tx,
                version: 1,
                amount: Some(amount),
                created_at: String::new(),
            },
        );

        Ok(())
    }

    fn process_withdrawal(
        &self,
        transaction: &TransactionCsv,
        clients: &mut HashMap<u16, Client>,
        last_transactions: &mut HashMap<u32, Transaction>,
        transactions_to_insert: &mut Vec<NewTransaction>,
    ) {
    }

    fn process_dispute(
        &self,
        transaction: &TransactionCsv,
        clients: &mut HashMap<u16, Client>,
        last_transactions: &mut HashMap<u32, Transaction>,
        transactions_to_insert: &mut Vec<NewTransaction>,
    ) {
    }

    fn process_resolve(
        &self,
        transaction: &TransactionCsv,
        clients: &mut HashMap<u16, Client>,
        last_transactions: &mut HashMap<u32, Transaction>,
        transactions_to_insert: &mut Vec<NewTransaction>,
    ) {
    }

    fn process_chargeback(
        &self,
        transaction: &TransactionCsv,
        clients: &mut HashMap<u16, Client>,
        last_transactions: &mut HashMap<u32, Transaction>,
        transactions_to_insert: &mut Vec<NewTransaction>,
    ) {
    }

    const AMOUNT_SCALE: i64 = 10_000;

    fn parse_amount(transaction: &TransactionCsv) -> Result<i64, ProcessorServiceError> {
        let amount = transaction
            .amount
            .as_deref()
            .ok_or(ProcessorServiceError::MissingAmount(transaction.tx))?;
        let invalid = || ProcessorServiceError::InvalidAmount {
            tx: transaction.tx,
            amount: amount.to_string(),
        };

        let decimal = Decimal::from_str(amount.trim()).map_err(|_| invalid())?;
        (decimal * Decimal::from(Self::AMOUNT_SCALE))
            .to_i64()
            .ok_or_else(invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::client::repository::SqliteClientRepository;
    use crate::storage::transactions::repository::SqliteTransactionRepository;

    fn transaction_with_amount(amount: Option<&str>) -> TransactionCsv {
        TransactionCsv {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 42,
            amount: amount.map(String::from),
        }
    }

    fn parse(amount: &str) -> Result<i64, ProcessorServiceError> {
        ProcessorService::parse_amount(&transaction_with_amount(Some(amount)))
    }

    #[test]
    fn parse_amount() {
        assert_eq!(parse("1").unwrap(), 10_000);
        assert_eq!(parse("250").unwrap(), 2_500_000);
        assert_eq!(parse("0").unwrap(), 0);

        assert_eq!(parse("1.5").unwrap(), 15_000);
        assert_eq!(parse("1.23").unwrap(), 12_300);
        assert_eq!(parse("1.234").unwrap(), 12_340);
        assert_eq!(parse("1.2345").unwrap(), 12_345);
        assert_eq!(parse("0.0001").unwrap(), 1);

        assert_eq!(parse("500000.0001").unwrap(), 5_000_000_001);

        assert_eq!(parse("  1.5").unwrap(), 15_000);
        assert_eq!(parse("1.5  ").unwrap(), 15_000);
        assert_eq!(parse("   1.2345   ").unwrap(), 12_345);

        let missing = ProcessorService::parse_amount(&transaction_with_amount(None));
        assert!(matches!(
            missing,
            Err(ProcessorServiceError::MissingAmount(42))
        ));

        for amount in ["", "   ", "abc", "1,5", "1.2.3", "nan", "1 000"] {
            let result = parse(amount);
            assert!(
                matches!(
                    &result,
                    Err(ProcessorServiceError::InvalidAmount { tx: 42, amount: invalid }) if invalid == amount
                ),
                "{amount:?} should be invalid, got {result:?}"
            );
        }
    }

    async fn service() -> ProcessorService {
        ProcessorService::new(
            Connection::open_in_memory().await.unwrap(),
            Arc::new(SqliteClientRepository),
            Arc::new(SqliteTransactionRepository),
        )
    }

    fn deposit(tx: u32, client: u16, amount: &str) -> TransactionCsv {
        TransactionCsv {
            r#type: TransactionType::Deposit,
            client,
            tx,
            amount: Some(amount.to_string()),
        }
    }

    #[tokio::test]
    async fn process_deposit_updates_the_client_and_registers_the_transaction() {
        let service = service().await;
        let mut clients = HashMap::new();
        let mut last_transactions = HashMap::new();
        let mut transactions_to_insert = Vec::new();

        service
            .process_deposit(
                &deposit(1, 7, "1.5"),
                &mut clients,
                &mut last_transactions,
                &mut transactions_to_insert,
            )
            .unwrap();

        let client = &clients[&7];
        assert_eq!(client.id, 7);
        assert_eq!(client.available_amount, 15_000);
        assert_eq!(client.held_amount, 0);
        assert_eq!(client.total_amount, 15_000);
        assert!(!client.locked);
        assert_eq!(client.version, 1);
        assert_eq!(
            transactions_to_insert,
            vec![NewTransaction {
                r#type: TransactionType::Deposit,
                client_id: 7,
                id: 1,
                version: 1,
                amount: Some(15_000),
            }]
        );
        assert_eq!(last_transactions[&1].client_id, 7);
        assert_eq!(last_transactions[&1].amount, Some(15_000));

        service
            .process_deposit(
                &deposit(2, 7, "2.0001"),
                &mut clients,
                &mut last_transactions,
                &mut transactions_to_insert,
            )
            .unwrap();

        assert_eq!(clients[&7].available_amount, 35_001);
        assert_eq!(clients[&7].total_amount, 35_001);
        assert_eq!(transactions_to_insert.len(), 2);
        assert_eq!(transactions_to_insert[1].id, 2);

        clients.insert(
            8,
            Client {
                id: 8,
                available_amount: 1_000,
                held_amount: 500,
                total_amount: 1_500,
                locked: false,
                version: 3,
                created_at: "2024-01-01 00:00:00".to_string(),
                updated_at: "2024-01-02 00:00:00".to_string(),
            },
        );

        service
            .process_deposit(
                &deposit(3, 8, "1"),
                &mut clients,
                &mut last_transactions,
                &mut transactions_to_insert,
            )
            .unwrap();

        let existing = &clients[&8];
        assert_eq!(existing.available_amount, 11_000);
        assert_eq!(existing.held_amount, 500);
        assert_eq!(existing.total_amount, 11_500);
        assert_eq!(existing.version, 3);
        assert_eq!(existing.created_at, "2024-01-01 00:00:00");
        assert_eq!(transactions_to_insert.len(), 3);
        assert_eq!(clients.len(), 2);
    }

    #[tokio::test]
    async fn process_deposit_fails_without_changing_anything() {
        let service = service().await;
        let mut clients = HashMap::new();
        let mut last_transactions = HashMap::new();
        let mut transactions_to_insert = Vec::new();

        last_transactions.insert(
            50,
            Transaction {
                r#type: TransactionType::Deposit,
                client_id: 7,
                id: 50,
                version: 1,
                amount: Some(10_000),
                created_at: "2024-01-01 00:00:00".to_string(),
            },
        );
        service
            .process_deposit(
                &deposit(1, 7, "1"),
                &mut clients,
                &mut last_transactions,
                &mut transactions_to_insert,
            )
            .unwrap();

        let clients_before = clients.clone();
        let last_transactions_before = last_transactions.clone();
        let transactions_to_insert_before = transactions_to_insert.clone();

        let missing_amount = TransactionCsv {
            amount: None,
            ..deposit(60, 7, "1")
        };
        let results = [
            (deposit(50, 7, "1"), "already in the database"),
            (deposit(1, 7, "1"), "already in the same batch"),
            (missing_amount, "missing amount"),
            (deposit(61, 8, "abc"), "invalid amount"),
        ];

        for (transaction, case) in results {
            let result = service.process_deposit(
                &transaction,
                &mut clients,
                &mut last_transactions,
                &mut transactions_to_insert,
            );

            match (case, result) {
                (
                    "already in the database",
                    Err(ProcessorServiceError::DepositWithExistingTransactionId(50)),
                )
                | (
                    "already in the same batch",
                    Err(ProcessorServiceError::DepositWithExistingTransactionId(1)),
                )
                | ("missing amount", Err(ProcessorServiceError::MissingAmount(60)))
                | ("invalid amount", Err(ProcessorServiceError::InvalidAmount { tx: 61, .. })) => {}
                (case, result) => panic!("{case}: unexpected result {result:?}"),
            }
        }

        assert_eq!(clients, clients_before);
        assert_eq!(last_transactions, last_transactions_before);
        assert_eq!(transactions_to_insert, transactions_to_insert_before);
    }
}
