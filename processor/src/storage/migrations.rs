pub const MIGRATIONS: &[&str] = &[
    include_str!("../../migrations/001_create_transactions.sql"),
    include_str!("../../migrations/002_create_clients.sql"),
];
