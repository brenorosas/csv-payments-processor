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
            .and_then(|decimal| (decimal * Decimal::from(Self::AMOUNT_SCALE)).to_i64());

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

    fn parse(amount: &str) -> Option<i64> {
        ProcessorService::parse_amount(&transaction_with_amount(Some(amount)))
    }

    #[test]
    fn parse_amount() {
        assert_eq!(parse("1"), Some(10_000));
        assert_eq!(parse("250"), Some(2_500_000));
        assert_eq!(parse("0"), Some(0));

        assert_eq!(parse("1.5"), Some(15_000));
        assert_eq!(parse("1.23"), Some(12_300));
        assert_eq!(parse("1.234"), Some(12_340));
        assert_eq!(parse("1.2345"), Some(12_345));
        assert_eq!(parse("0.0001"), Some(1));

        assert_eq!(parse("500000.0001"), Some(5_000_000_001));

        assert_eq!(parse("  1.5"), Some(15_000));
        assert_eq!(parse("1.5  "), Some(15_000));
        assert_eq!(parse("   1.2345   "), Some(12_345));

        assert_eq!(
            ProcessorService::parse_amount(&transaction_with_amount(None)),
            None
        );

        for amount in ["", "   ", "abc", "1,5", "1.2.3", "nan", "1 000"] {
            assert_eq!(parse(amount), None, "{amount:?} should be invalid");
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

    fn withdrawal(tx: u32, client: u16, amount: &str) -> TransactionCsv {
        TransactionCsv {
            r#type: TransactionType::Withdrawal,
            client,
            tx,
            amount: Some(amount.to_string()),
        }
    }

    fn dispute(tx: u32, client: u16) -> TransactionCsv {
        TransactionCsv {
            r#type: TransactionType::Dispute,
            client,
            tx,
            amount: None,
        }
    }

    fn resolve(tx: u32, client: u16) -> TransactionCsv {
        TransactionCsv {
            r#type: TransactionType::Resolve,
            client,
            tx,
            amount: None,
        }
    }

    fn chargeback(tx: u32, client: u16) -> TransactionCsv {
        TransactionCsv {
            r#type: TransactionType::Chargeback,
            client,
            tx,
            amount: None,
        }
    }

    fn client_with_funds(id: u16, available: i64, held: i64) -> Client {
        Client {
            available_amount: available,
            held_amount: held,
            total_amount: available + held,
            version: 3,
            created_at: "2024-01-01 00:00:00".to_string(),
            updated_at: "2024-01-02 00:00:00".to_string(),
            ..Client::new(id)
        }
    }

    #[tokio::test]
    async fn process_deposit_updates_the_client_and_registers_the_transaction() {
        let service = service().await;
        let mut clients = HashMap::new();
        let mut last_transactions = HashMap::new();
        let mut transactions_to_insert = Vec::new();

        service.process_deposit(
            &deposit(1, 7, "1.5"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

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

        service.process_deposit(
            &deposit(2, 7, "2.0001"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

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

        service.process_deposit(
            &deposit(3, 8, "1"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

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
        service.process_deposit(
            &deposit(1, 7, "1"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

        let clients_before = clients.clone();
        let last_transactions_before = last_transactions.clone();
        let transactions_to_insert_before = transactions_to_insert.clone();

        let missing_amount = TransactionCsv {
            amount: None,
            ..deposit(60, 7, "1")
        };

        for transaction in [
            deposit(50, 7, "1"),
            deposit(1, 7, "1"),
            missing_amount,
            deposit(61, 8, "abc"),
        ] {
            service.process_deposit(
                &transaction,
                &mut clients,
                &mut last_transactions,
                &mut transactions_to_insert,
            );
        }

        assert_eq!(clients, clients_before);
        assert_eq!(last_transactions, last_transactions_before);
        assert_eq!(transactions_to_insert, transactions_to_insert_before);
    }

    #[tokio::test]
    async fn process_withdrawal_updates_the_client_and_registers_the_transaction() {
        let service = service().await;
        let mut clients = HashMap::from([(8, client_with_funds(8, 10_000, 5_000))]);
        let mut last_transactions = HashMap::new();
        let mut transactions_to_insert = Vec::new();

        service.process_withdrawal(
            &withdrawal(1, 8, "0.4"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

        let client = &clients[&8];
        assert_eq!(client.available_amount, 6_000);
        assert_eq!(client.held_amount, 5_000);
        assert_eq!(client.total_amount, 11_000);
        assert_eq!(client.version, 3);
        assert_eq!(client.created_at, "2024-01-01 00:00:00");
        assert_eq!(
            transactions_to_insert,
            vec![NewTransaction {
                r#type: TransactionType::Withdrawal,
                client_id: 8,
                id: 1,
                version: 1,
                amount: Some(4_000),
            }]
        );
        assert_eq!(last_transactions[&1].r#type, TransactionType::Withdrawal);
        assert_eq!(last_transactions[&1].client_id, 8);
        assert_eq!(last_transactions[&1].amount, Some(4_000));

        service.process_withdrawal(
            &withdrawal(2, 8, "0.6"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

        assert_eq!(clients[&8].available_amount, 0);
        assert_eq!(clients[&8].held_amount, 5_000);
        assert_eq!(clients[&8].total_amount, 5_000);
        assert_eq!(transactions_to_insert.len(), 2);
        assert_eq!(transactions_to_insert[1].id, 2);
        assert_eq!(clients.len(), 1);
    }

    #[tokio::test]
    async fn process_withdrawal_fails_without_changing_anything() {
        let service = service().await;
        let mut clients =
            HashMap::from([(8, client_with_funds(8, 1_000, 5_000)), (9, Client::new(9))]);
        let mut last_transactions = HashMap::from([(
            50,
            Transaction {
                r#type: TransactionType::Deposit,
                client_id: 8,
                id: 50,
                version: 1,
                amount: Some(10_000),
                created_at: "2024-01-01 00:00:00".to_string(),
            },
        )]);
        let mut transactions_to_insert = Vec::new();

        service.process_withdrawal(
            &withdrawal(1, 8, "0.05"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

        let clients_before = clients.clone();
        let last_transactions_before = last_transactions.clone();
        let transactions_to_insert_before = transactions_to_insert.clone();

        let mut run = |transaction: TransactionCsv| {
            service.process_withdrawal(
                &transaction,
                &mut clients,
                &mut last_transactions,
                &mut transactions_to_insert,
            )
        };

        run(withdrawal(50, 8, "0.01"));
        run(withdrawal(1, 8, "0.01"));
        run(withdrawal(2, 8, "0.0951"));
        run(withdrawal(3, 9, "0.0001"));
        run(withdrawal(4, 99, "1"));
        run(TransactionCsv {
            amount: None,
            ..withdrawal(5, 8, "1")
        });
        run(withdrawal(6, 8, "abc"));

        assert_eq!(clients, clients_before);
        assert_eq!(last_transactions, last_transactions_before);
        assert_eq!(transactions_to_insert, transactions_to_insert_before);
    }

    #[tokio::test]
    async fn process_dispute_holds_the_funds_of_the_disputed_deposit() {
        let service = service().await;
        let mut clients = HashMap::from([(8, client_with_funds(8, 10_000, 500))]);
        let mut last_transactions = HashMap::from([(
            60,
            Transaction {
                r#type: TransactionType::Deposit,
                client_id: 8,
                id: 60,
                version: 1,
                amount: Some(4_000),
                created_at: "2024-01-01 00:00:00".to_string(),
            },
        )]);
        let mut transactions_to_insert = Vec::new();

        service.process_dispute(
            &dispute(60, 8),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

        let client = &clients[&8];
        assert_eq!(client.available_amount, 6_000);
        assert_eq!(client.held_amount, 4_500);
        assert_eq!(client.total_amount, 10_500);
        assert_eq!(client.version, 3);
        assert_eq!(
            transactions_to_insert,
            vec![NewTransaction {
                r#type: TransactionType::Dispute,
                client_id: 8,
                id: 60,
                version: 2,
                amount: Some(4_000),
            }]
        );
        assert_eq!(last_transactions[&60].r#type, TransactionType::Dispute);
        assert_eq!(last_transactions[&60].version, 2);
        assert_eq!(last_transactions[&60].amount, Some(4_000));

        service.process_deposit(
            &deposit(61, 8, "1"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );
        service.process_dispute(
            &dispute(61, 8),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

        assert_eq!(clients[&8].available_amount, 6_000);
        assert_eq!(clients[&8].held_amount, 14_500);
        assert_eq!(clients[&8].total_amount, 20_500);
        assert_eq!(transactions_to_insert.len(), 3);
        assert_eq!(transactions_to_insert[2].version, 2);
        assert_eq!(last_transactions[&60].version, 2);
    }

    #[tokio::test]
    async fn process_dispute_does_not_change_anything_when_it_cannot_apply() {
        let service = service().await;
        let mut clients = HashMap::new();
        let mut last_transactions = HashMap::new();
        let mut transactions_to_insert = Vec::new();

        service.process_deposit(
            &deposit(1, 8, "1"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );
        service.process_withdrawal(
            &withdrawal(2, 8, "0.3"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );
        service.process_dispute(
            &dispute(1, 8),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

        let clients_before = clients.clone();
        let last_transactions_before = last_transactions.clone();
        let transactions_to_insert_before = transactions_to_insert.clone();

        let mut run = |transaction: TransactionCsv| {
            service.process_dispute(
                &transaction,
                &mut clients,
                &mut last_transactions,
                &mut transactions_to_insert,
            )
        };

        run(dispute(99, 8));
        run(dispute(2, 8));
        run(dispute(1, 8));

        assert_eq!(clients, clients_before);
        assert_eq!(last_transactions, last_transactions_before);
        assert_eq!(transactions_to_insert, transactions_to_insert_before);
    }

    #[tokio::test]
    async fn process_resolve_releases_the_held_funds_of_the_disputed_deposit() {
        let service = service().await;
        let mut clients = HashMap::from([(8, client_with_funds(8, 6_000, 4_500))]);
        let mut last_transactions = HashMap::from([(
            60,
            Transaction {
                r#type: TransactionType::Dispute,
                client_id: 8,
                id: 60,
                version: 2,
                amount: Some(4_000),
                created_at: "2024-01-01 00:00:00".to_string(),
            },
        )]);
        let mut transactions_to_insert = Vec::new();

        service.process_resolve(
            &resolve(60, 8),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

        let client = &clients[&8];
        assert_eq!(client.available_amount, 10_000);
        assert_eq!(client.held_amount, 500);
        assert_eq!(client.total_amount, 10_500);
        assert_eq!(client.version, 3);
        assert_eq!(
            transactions_to_insert,
            vec![NewTransaction {
                r#type: TransactionType::Resolve,
                client_id: 8,
                id: 60,
                version: 3,
                amount: Some(4_000),
            }]
        );
        assert_eq!(last_transactions[&60].r#type, TransactionType::Resolve);
        assert_eq!(last_transactions[&60].version, 3);
        assert_eq!(last_transactions[&60].amount, Some(4_000));

        let before = clients[&8].clone();
        service.process_deposit(
            &deposit(61, 8, "1"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );
        let after_deposit = clients[&8].clone();
        service.process_dispute(
            &dispute(61, 8),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );
        service.process_resolve(
            &resolve(61, 8),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

        assert_eq!(clients[&8], after_deposit);
        assert_eq!(
            after_deposit.available_amount,
            before.available_amount + 10_000
        );
        assert_eq!(transactions_to_insert.len(), 4);
        assert_eq!(transactions_to_insert[2].r#type, TransactionType::Dispute);
        assert_eq!(transactions_to_insert[2].version, 2);
        assert_eq!(transactions_to_insert[3].r#type, TransactionType::Resolve);
        assert_eq!(transactions_to_insert[3].version, 3);
    }

    #[tokio::test]
    async fn process_resolve_does_not_change_anything_when_it_cannot_apply() {
        let service = service().await;
        let mut clients = HashMap::new();
        let mut last_transactions = HashMap::new();
        let mut transactions_to_insert = Vec::new();

        for transaction in [
            deposit(1, 8, "1"),
            deposit(3, 8, "2"),
            withdrawal(2, 8, "0.3"),
        ] {
            match transaction.r#type {
                TransactionType::Deposit => service.process_deposit(
                    &transaction,
                    &mut clients,
                    &mut last_transactions,
                    &mut transactions_to_insert,
                ),
                _ => service.process_withdrawal(
                    &transaction,
                    &mut clients,
                    &mut last_transactions,
                    &mut transactions_to_insert,
                ),
            }
        }
        service.process_dispute(
            &dispute(1, 8),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );
        service.process_resolve(
            &resolve(1, 8),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

        let clients_before = clients.clone();
        let last_transactions_before = last_transactions.clone();
        let transactions_to_insert_before = transactions_to_insert.clone();

        let mut run = |transaction: TransactionCsv| {
            service.process_resolve(
                &transaction,
                &mut clients,
                &mut last_transactions,
                &mut transactions_to_insert,
            )
        };

        run(resolve(99, 8));
        run(resolve(3, 8));
        run(resolve(2, 8));
        run(resolve(1, 8));

        assert_eq!(clients, clients_before);
        assert_eq!(last_transactions, last_transactions_before);
        assert_eq!(transactions_to_insert, transactions_to_insert_before);
    }

    #[tokio::test]
    async fn process_chargeback_removes_the_held_funds_of_the_disputed_deposit() {
        let service = service().await;
        let mut clients = HashMap::from([(8, client_with_funds(8, 6_000, 4_500))]);
        let mut last_transactions = HashMap::from([(
            60,
            Transaction {
                r#type: TransactionType::Dispute,
                client_id: 8,
                id: 60,
                version: 2,
                amount: Some(4_000),
                created_at: "2024-01-01 00:00:00".to_string(),
            },
        )]);
        let mut transactions_to_insert = Vec::new();

        service.process_chargeback(
            &chargeback(60, 8),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

        let client = &clients[&8];
        assert_eq!(client.available_amount, 6_000);
        assert_eq!(client.held_amount, 500);
        assert_eq!(client.total_amount, 6_500);
        assert!(client.locked);
        assert_eq!(client.version, 3);
        assert_eq!(
            transactions_to_insert,
            vec![NewTransaction {
                r#type: TransactionType::Chargeback,
                client_id: 8,
                id: 60,
                version: 3,
                amount: Some(4_000),
            }]
        );
        assert_eq!(last_transactions[&60].r#type, TransactionType::Chargeback);
        assert_eq!(last_transactions[&60].version, 3);
        assert_eq!(last_transactions[&60].amount, Some(4_000));

        service.process_deposit(
            &deposit(61, 8, "1"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );
        service.process_dispute(
            &dispute(61, 8),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );
        service.process_chargeback(
            &chargeback(61, 8),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

        assert_eq!(clients[&8].available_amount, 6_000);
        assert_eq!(clients[&8].held_amount, 500);
        assert_eq!(clients[&8].total_amount, 6_500);
        assert_eq!(transactions_to_insert.len(), 4);
        assert_eq!(transactions_to_insert[2].r#type, TransactionType::Dispute);
        assert_eq!(transactions_to_insert[2].version, 2);
        assert_eq!(
            transactions_to_insert[3].r#type,
            TransactionType::Chargeback
        );
        assert_eq!(transactions_to_insert[3].version, 3);
    }

    #[tokio::test]
    async fn process_chargeback_does_not_change_anything_when_it_cannot_apply() {
        let service = service().await;
        let mut clients = HashMap::new();
        let mut last_transactions = HashMap::new();
        let mut transactions_to_insert = Vec::new();

        service.process_deposit(
            &deposit(1, 8, "1"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );
        service.process_deposit(
            &deposit(3, 8, "2"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );
        service.process_withdrawal(
            &withdrawal(2, 8, "0.3"),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );
        service.process_dispute(
            &dispute(1, 8),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );
        service.process_chargeback(
            &chargeback(1, 8),
            &mut clients,
            &mut last_transactions,
            &mut transactions_to_insert,
        );

        let clients_before = clients.clone();
        let last_transactions_before = last_transactions.clone();
        let transactions_to_insert_before = transactions_to_insert.clone();

        let mut run = |transaction: TransactionCsv| {
            service.process_chargeback(
                &transaction,
                &mut clients,
                &mut last_transactions,
                &mut transactions_to_insert,
            )
        };

        run(chargeback(99, 8));
        run(chargeback(3, 8));
        run(chargeback(2, 8));
        run(chargeback(1, 8));

        assert_eq!(clients, clients_before);
        assert_eq!(last_transactions, last_transactions_before);
        assert_eq!(transactions_to_insert, transactions_to_insert_before);
    }
}
