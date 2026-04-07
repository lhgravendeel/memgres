DROP SCHEMA IF EXISTS test_840 CASCADE;
CREATE SCHEMA test_840;
SET search_path TO test_840;

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE bookings (
    booking_id integer PRIMARY KEY,
    room_id integer NOT NULL,
    period tsrange NOT NULL
);

-- Skip exclusion constraint — requires btree_gist extension with GiST index support

INSERT INTO bookings VALUES
(1, 10, tsrange('2024-01-01 10:00', '2024-01-01 11:00')),
(2, 10, tsrange('2024-01-01 11:00', '2024-01-01 12:00')),
(3, 20, tsrange('2024-01-01 10:30', '2024-01-01 11:30'));

-- begin-expected
-- columns: booking_id,room_id
-- row: 1|10
-- row: 3|20
-- end-expected
SELECT booking_id, room_id
FROM bookings
WHERE period && tsrange('2024-01-01 10:15', '2024-01-01 10:45')
ORDER BY booking_id;

-- begin-expected
-- columns: room_id,period_contains
-- row: 10|t
-- row: 20|t
-- end-expected
SELECT room_id, period @> timestamp '2024-01-01 10:45' AS period_contains
FROM bookings
WHERE booking_id IN (1, 3)
ORDER BY room_id;
