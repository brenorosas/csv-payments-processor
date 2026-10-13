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
                TransactionType::Deposit => {
                    self.process_deposit(
                        transaction,
                        &mut clients,
                        &mut last_transactions,
                        &mut transactions_to_insert,
                    )
                    .await?
                }
                TransactionType::Withdrawal => {
                    self.process_withdrawal(
                        transaction,
                        &mut clients,
                        &mut last_transactions,
                        &mut transactions_to_insert,
                    )
                    .await
                }
                TransactionType::Dispute => {
                    self.process_dispute(
                        transaction,
                        &mut clients,
                        &mut last_transactions,
                        &mut transactions_to_insert,
                    )
                    .await
                }
                TransactionType::Resolve => {
                    self.process_resolve(
                        transaction,
                        &mut clients,
                        &mut last_transactions,
                        &mut transactions_to_insert,
                    )
                    .await
                }
                TransactionType::Chargeback => {
                    self.process_chargeback(
                        transaction,
                        &mut clients,
                        &mut last_transactions,
                        &mut transactions_to_insert,
                    )
                    .await
                }
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

    async fn process_deposit(
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

    async fn process_withdrawal(
        &self,
        transaction: &TransactionCsv,
        clients: &mut HashMap<u16, Client>,
        last_transactions: &mut HashMap<u32, Transaction>,
        transactions_to_insert: &mut Vec<NewTransaction>,
    ) {
    }

    async fn process_dispute(
        &self,
        transaction: &TransactionCsv,
        clients: &mut HashMap<u16, Client>,
        last_transactions: &mut HashMap<u32, Transaction>,
        transactions_to_insert: &mut Vec<NewTransaction>,
    ) {
    }

    async fn process_resolve(
        &self,
        transaction: &TransactionCsv,
        clients: &mut HashMap<u16, Client>,
        last_transactions: &mut HashMap<u32, Transaction>,
        transactions_to_insert: &mut Vec<NewTransaction>,
    ) {
    }

    async fn process_chargeback(
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
}
