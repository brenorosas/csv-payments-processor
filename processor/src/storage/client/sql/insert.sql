INSERT INTO clients (id, available_amount, held_amount, total_amount, locked)
SELECT
    value ->> 'id',
    value ->> 'available_amount',
    value ->> 'held_amount',
    value ->> 'total_amount',
    value ->> 'locked'
FROM json_each(?1)
