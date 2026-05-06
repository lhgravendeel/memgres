DROP SCHEMA IF EXISTS test_730 CASCADE;
CREATE SCHEMA test_730;
SET search_path TO test_730;

CREATE TABLE items (
    item_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    item_name text NOT NULL
);

INSERT INTO items(item_name) VALUES ('a'), ('b'), ('c');

-- begin-expected
-- columns: sequence_name
-- row: items_item_id_seq
-- end-expected
SELECT pg_get_serial_sequence('test_730.items', 'item_id')::regclass::text AS sequence_name;

-- begin-expected
-- columns: max_item_id
-- row: 3
-- end-expected
SELECT MAX(item_id) AS max_item_id
FROM items;

-- begin-expected
-- columns: table_size_is_positive
-- row: t
-- end-expected
SELECT pg_relation_size('test_730.items'::regclass) > 0 AS table_size_is_positive;

-- begin-expected
-- columns: total_size_ge_table_size
-- row: t
-- end-expected
SELECT pg_total_relation_size('test_730.items'::regclass) >= pg_relation_size('test_730.items'::regclass) AS total_size_ge_table_size;

