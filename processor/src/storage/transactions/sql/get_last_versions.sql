SELECT type, client_id, id, version, amount, created_at
FROM transactions t
WHERE id IN (SELECT value FROM json_each(?1))
  AND version = (SELECT MAX(version) FROM transactions WHERE id = t.id)
