SELECT *
FROM clients
WHERE id IN (SELECT value FROM json_each(?1))
