INSERT INTO transactions (type, client_id, id, version, amount)
SELECT
    value ->> 'type',
    value ->> 'client_id',
    value ->> 'id',
    value ->> 'version',
    value ->> 'amount'
FROM json_each(?1)
