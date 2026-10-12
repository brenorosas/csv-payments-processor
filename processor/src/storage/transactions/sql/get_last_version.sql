SELECT type, client_id, id, version, amount, created_at
FROM transactions
WHERE id = ?1
ORDER BY version DESC
LIMIT 1
