# Payments Processor

Reads a CSV of transactions (deposit, withdrawal, dispute, resolve, chargeback), applies them in order to each client's account and prints the final accounts as a CSV.

Run it inside the `processor` folder:

```bash
cargo run -- transactions.csv > accounts.csv
```

```text
input (transactions.csv)          output (accounts.csv)
type, client, tx, amount          client,available,held,total,locked
deposit, 1, 1, 1.0                1,1.5,0,1.5,false
deposit, 2, 2, 2.0                2,2,0,2,false
deposit, 1, 3, 2.0
withdrawal, 1, 4, 1.5
withdrawal, 2, 5, 3.0
```

Rows that cannot be applied are ignored and logged as warnings on stderr, so stdout only has the CSV.

**Stack:** Rust 1.95.0. SQLite is used as temporary storage: the migrations run at start and the database file is deleted at the end.

## Tests

Unit tests live together with the business logic: in the same file, or in a file in the same folder when it gets too big (for example `src/processor_service/tests.rs`). Integration tests are in `tests/` and end-to-end tests in `tests/e2e` (one CSV of transactions and one CSV of expected output per case, corner cases included). Manual tests were also run. Rust types add safety: `u16` clients, `u32` transaction ids, an `enum` for the transaction type and integer amounts instead of floats.

## Discussion

**Memory.** SQLite is used so the CSV can be processed without keeping everything in memory: the file is read row by row and the transactions and accounts live in the database. `BATCH_SIZE` in `src/main.rs` sets how many transactions and accounts are in memory at once. It can be increased safely: bigger batches mean fewer SQLite reads and writes and lower processing time, at the cost of more memory.

**Dangerous part.** Amounts are converted to integers with four decimals (`1.5` is stored as `15000`) in a 64-bit integer, so a balance above `922,337,203,685,477.5807` overflows and can break the account.

**Using it in a server.** If many CSVs came at the same time, would the order between the CSVs matter? If it does, each CSV would need some information like a timestamp, and we would have to decide what happens when a CSV with an earlier timestamp arrives after a later one: should the history be recomputed, for example?

After deciding in which order the CSVs are processed, each one can be handled like this: go through all its transactions and send them to a topic (Kafka, as an example) using `client_id` as the partition key. Transactions of different clients can be processed in parallel, because they do not depend on each other. Transactions of the same client must be processed in order, and since the same client always goes to the same partition, that order is kept. Each consumer then reads batches from its partition exactly like this code does from the CSV, and partitions and consumers can grow as necessary.

## Assumptions

**Disputes only apply to deposits.** The requirements only describe a dispute as lowering `available` and raising `held`, and that makes no sense for a withdrawal, whose money already left the account. To implement it for withdrawals, a dispute would raise `held` and `total` instead (a provisional credit), and the transactions repository would need a new function to get a transaction's first version, to know if it was a deposit or a withdrawal when handling resolve and chargeback.

## AI notes

The whole project was built with Claude in a single session, always with Sonnet 5.5 (`claude-sonnet-5-5`). The transcript is in [ai/transcript.jsonl](ai/transcript.jsonl).
