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
        let (mut clients, mut last_transactions) =
            self.get_clients_and_last_transactions(transactions).await?;

        let mut transactions_to_insert: Vec<NewTransaction> = Vec::new();

        for transaction in transactions {
            match transaction.r#type {
                TransactionType::Deposit => self.process_deposit(
                    transaction,
                    &mut clients,
                    &mut last_transactions,
                    &mut transactions_to_insert,
                ),
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

    async fn get_clients_and_last_transactions(
        &self,
        transactions: &[TransactionCsv],
    ) -> Result<(HashMap<u16, Client>, HashMap<u32, Transaction>), ProcessorServiceError> {
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
        let (mut clients, last_transactions) = self
            .conn
            .call(move |conn| {
                let clients = client_repository.get_clients(conn, &client_ids)?;
                let last_transactions =
                    transaction_repository.get_last_versions(conn, &transaction_ids)?;
                Ok::<_, ProcessorServiceError>((clients, last_transactions))
            })
            .await?;

        for transaction in transactions {
            clients
                .entry(transaction.client)
                .or_insert_with(|| Client::new(transaction.client));
        }

        Ok((clients, last_transactions))
    }

    fn process_deposit(
        &self,
        transaction: &TransactionCsv,
        clients: &mut HashMap<u16, Client>,
        last_transactions: &mut HashMap<u32, Transaction>,
        transactions_to_insert: &mut Vec<NewTransaction>,
    ) {
        if last_transactions.contains_key(&transaction.tx) {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "deposit ignored: transaction id already exists"
            );
            return;
        }

        let Some(amount) = Self::parse_amount(transaction) else {
            return;
        };

        let client = clients
            .entry(transaction.client)
            .or_insert_with(|| Client::new(transaction.client));

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
    }

    fn process_withdrawal(
        &self,
        transaction: &TransactionCsv,
        clients: &mut HashMap<u16, Client>,
        last_transactions: &mut HashMap<u32, Transaction>,
        transactions_to_insert: &mut Vec<NewTransaction>,
    ) {
        if last_transactions.contains_key(&transaction.tx) {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "withdrawal ignored: transaction id already exists"
            );
            return;
        }

        let Some(amount) = Self::parse_amount(transaction) else {
            return;
        };

        let Some(client) = clients
            .get_mut(&transaction.client)
            .filter(|client| client.available_amount >= amount)
        else {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "withdrawal ignored: insufficient available funds"
            );
            return;
        };

        client.available_amount -= amount;
        client.total_amount -= amount;

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
    }

    fn process_dispute(
        &self,
        transaction: &TransactionCsv,
        clients: &mut HashMap<u16, Client>,
        last_transactions: &mut HashMap<u32, Transaction>,
        transactions_to_insert: &mut Vec<NewTransaction>,
    ) {
        let Some(disputed) = last_transactions.get(&transaction.tx) else {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "dispute ignored: transaction not found"
            );
            return;
        };

        if disputed.client_id != transaction.client {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "dispute ignored: transaction belongs to another client"
            );
            return;
        }

        if disputed.r#type != TransactionType::Deposit {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "dispute ignored: transaction is not a deposit"
            );
            return;
        }

        let Some(amount) = disputed.amount else {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "dispute ignored: transaction has no amount"
            );
            return;
        };
        let version = disputed.version + 1;

        let client = clients
            .entry(transaction.client)
            .or_insert_with(|| Client::new(transaction.client));

        client.available_amount -= amount;
        client.held_amount += amount;

        transactions_to_insert.push(NewTransaction {
            r#type: transaction.r#type,
            client_id: transaction.client,
            id: transaction.tx,
            version: version,
            amount: Some(amount),
        });
        last_transactions.insert(
            transaction.tx,
            Transaction {
                r#type: transaction.r#type,
                client_id: transaction.client,
                id: transaction.tx,
                version: version,
                amount: Some(amount),
                created_at: String::new(),
            },
        );
    }

    fn process_resolve(
        &self,
        transaction: &TransactionCsv,
        clients: &mut HashMap<u16, Client>,
        last_transactions: &mut HashMap<u32, Transaction>,
        transactions_to_insert: &mut Vec<NewTransaction>,
    ) {
        let Some(disputed) = last_transactions.get(&transaction.tx) else {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "resolve ignored: transaction not found"
            );
            return;
        };

        if disputed.client_id != transaction.client {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "resolve ignored: transaction belongs to another client"
            );
            return;
        }

        if disputed.r#type != TransactionType::Dispute {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "resolve ignored: transaction is not under dispute"
            );
            return;
        }

        let Some(amount) = disputed.amount else {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "resolve ignored: transaction has no amount"
            );
            return;
        };
        let version = disputed.version + 1;

        let client = clients
            .entry(transaction.client)
            .or_insert_with(|| Client::new(transaction.client));

        client.available_amount += amount;
        client.held_amount -= amount;

        transactions_to_insert.push(NewTransaction {
            r#type: transaction.r#type,
            client_id: transaction.client,
            id: transaction.tx,
            version: version,
            amount: Some(amount),
        });
        last_transactions.insert(
            transaction.tx,
            Transaction {
                r#type: transaction.r#type,
                client_id: transaction.client,
                id: transaction.tx,
                version: version,
                amount: Some(amount),
                created_at: String::new(),
            },
        );
    }

    fn process_chargeback(
        &self,
        transaction: &TransactionCsv,
        clients: &mut HashMap<u16, Client>,
        last_transactions: &mut HashMap<u32, Transaction>,
        transactions_to_insert: &mut Vec<NewTransaction>,
    ) {
        let Some(disputed) = last_transactions.get(&transaction.tx) else {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "chargeback ignored: transaction not found"
            );
            return;
        };

        if disputed.client_id != transaction.client {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "chargeback ignored: transaction belongs to another client"
            );
            return;
        }

        if disputed.r#type != TransactionType::Dispute {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "chargeback ignored: transaction is not under dispute"
            );
            return;
        }

        let Some(amount) = disputed.amount else {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "chargeback ignored: transaction has no amount"
            );
            return;
        };
        let version = disputed.version + 1;

        let client = clients
            .entry(transaction.client)
            .or_insert_with(|| Client::new(transaction.client));

        client.held_amount -= amount;
        client.total_amount -= amount;
        client.locked = true;

        transactions_to_insert.push(NewTransaction {
            r#type: transaction.r#type,
            client_id: transaction.client,
            id: transaction.tx,
            version: version,
            amount: Some(amount),
        });
        last_transactions.insert(
            transaction.tx,
            Transaction {
                r#type: transaction.r#type,
                client_id: transaction.client,
                id: transaction.tx,
                version: version,
                amount: Some(amount),
                created_at: String::new(),
            },
        );
    }

    const AMOUNT_SCALE: i64 = 10_000;

    fn parse_amount(transaction: &TransactionCsv) -> Option<i64> {
        let Some(amount) = transaction.amount.as_deref() else {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                "transaction ignored: missing amount"
            );
            return None;
        };

        let parsed = Decimal::from_str(amount.trim())
            .ok()
            .and_then(|decimal| (decimal * Decimal::from(Self::AMOUNT_SCALE)).to_i64())
            .filter(|amount| *amount > 0);

        if parsed.is_none() {
            tracing::warn!(
                tx = transaction.tx,
                client = transaction.client,
                amount,
                "transaction ignored: invalid amount"
            );
        }

        parsed
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
