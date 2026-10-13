INSERT INTO clients (id, available_amount, held_amount, total_amount, locked, version)
SELECT
    value ->> 'id',
    value ->> 'available_amount',
    value ->> 'held_amount',
    value ->> 'total_amount',
    value ->> 'locked',
    value ->> 'version'
FROM json_each(?1)
WHERE true
ON CONFLICT (id) DO UPDATE SET
    available_amount = excluded.available_amount,
    held_amount = excluded.held_amount,
    total_amount = excluded.total_amount,
    locked = excluded.locked,
    version = clients.version + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE clients.version = excluded.version
