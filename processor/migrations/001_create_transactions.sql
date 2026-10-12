CREATE TABLE transactions (
    type TEXT NOT NULL CHECK (type IN ('deposit', 'withdrawal', 'dispute', 'resolve', 'chargeback')),
    client_id INTEGER NOT NULL,
    id INTEGER NOT NULL,
    version INTEGER NOT NULL,
    amount INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, version)
);
