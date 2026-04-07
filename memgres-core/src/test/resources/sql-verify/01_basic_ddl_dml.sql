-- ============================================================
-- 01: Basic DDL, DML, and SELECT
-- ============================================================

-- === Table creation with various types ===
CREATE TABLE items (id serial PRIMARY KEY, name text NOT NULL, price numeric(10,2), active boolean DEFAULT true, created_at timestamp DEFAULT now());
CREATE TABLE tags (id serial PRIMARY KEY, label text UNIQUE NOT NULL);
CREATE TABLE item_tags (item_id int REFERENCES items(id) ON DELETE CASCADE, tag_id int REFERENCES tags(id) ON DELETE CASCADE, PRIMARY KEY(item_id, tag_id));
CREATE TABLE configs (key text PRIMARY KEY, value text, updated_at timestamptz DEFAULT now());
CREATE TABLE measurements (id bigserial PRIMARY KEY, sensor_id int NOT NULL, reading double precision, recorded_at timestamp NOT NULL DEFAULT now());
CREATE TABLE blobs (id serial PRIMARY KEY, data bytea, mime text);
CREATE TABLE docs (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), title text NOT NULL, body text, metadata jsonb DEFAULT '{}');
CREATE TABLE arrays_test (id serial PRIMARY KEY, int_arr int[], text_arr text[], nested int[][]);

-- === Basic INSERTs ===
INSERT INTO items (name, price) VALUES ('Widget', 9.99);
INSERT INTO items (name, price) VALUES ('Gadget', 19.99);
INSERT INTO items (name, price, active) VALUES ('Doohickey', 4.50, false);
INSERT INTO items (name, price) VALUES ('Thingamajig', 149.99);
INSERT INTO items (name, price) VALUES ('Whatchamacallit', 0.99);
INSERT INTO tags (label) VALUES ('electronics');
INSERT INTO tags (label) VALUES ('hardware');
INSERT INTO tags (label) VALUES ('sale');
INSERT INTO item_tags VALUES (1, 1);
INSERT INTO item_tags VALUES (1, 2);
INSERT INTO item_tags VALUES (2, 1);
INSERT INTO item_tags VALUES (3, 3);
INSERT INTO configs (key, value) VALUES ('theme', 'dark');
INSERT INTO configs (key, value) VALUES ('lang', 'en');
INSERT INTO configs (key, value) VALUES ('page_size', '25');
INSERT INTO measurements (sensor_id, reading) VALUES (1, 23.5);
INSERT INTO measurements (sensor_id, reading) VALUES (1, 24.1);
INSERT INTO measurements (sensor_id, reading) VALUES (2, 18.7);
INSERT INTO measurements (sensor_id, reading, recorded_at) VALUES (2, 19.3, '2024-06-15 10:30:00');
INSERT INTO docs (title, body, metadata) VALUES ('First Doc', 'Hello world', '{"author": "alice", "tags": ["intro"]}');
INSERT INTO docs (title, body) VALUES ('Second Doc', 'Goodbye world');
INSERT INTO arrays_test (int_arr, text_arr) VALUES ('{1,2,3}', '{"a","b","c"}');
INSERT INTO arrays_test (int_arr, text_arr) VALUES (ARRAY[4,5,6], ARRAY['d','e','f']);

-- === INSERT with DEFAULT VALUES ===
INSERT INTO items (name, price) VALUES ('Default Item', 0) RETURNING id;
INSERT INTO docs DEFAULT VALUES;

-- === INSERT with RETURNING ===
INSERT INTO items (name, price) VALUES ('Returned', 7.77) RETURNING id, name, price;
INSERT INTO configs (key, value) VALUES ('new_key', 'new_val') RETURNING *;

-- === Basic SELECTs ===
SELECT * FROM items;
SELECT id, name, price FROM items WHERE active = true;
SELECT name, price FROM items WHERE price > 10 ORDER BY price DESC;
SELECT name FROM items ORDER BY name ASC;
SELECT * FROM items LIMIT 2;
SELECT * FROM items LIMIT 2 OFFSET 2;
SELECT * FROM items WHERE name LIKE '%get%';
SELECT * FROM items WHERE name ILIKE '%WIDGET%';
SELECT * FROM items WHERE price BETWEEN 5 AND 50;
SELECT * FROM items WHERE id IN (1, 3, 5);
SELECT * FROM items WHERE active IS TRUE;
SELECT * FROM items WHERE active IS NOT FALSE;
SELECT COUNT(*) FROM items;
SELECT COUNT(*) AS total, SUM(price) AS revenue FROM items WHERE active = true;
SELECT DISTINCT active FROM items;
SELECT active, COUNT(*) FROM items GROUP BY active;
SELECT active, COUNT(*) FROM items GROUP BY active HAVING COUNT(*) > 1;

-- === UPDATE ===
UPDATE items SET price = price * 1.1 WHERE active = true;
UPDATE items SET active = true WHERE id = 3 RETURNING *;
UPDATE configs SET value = '50', updated_at = now() WHERE key = 'page_size' RETURNING key, value;

-- === DELETE ===
DELETE FROM item_tags WHERE item_id = 5;
DELETE FROM items WHERE id = 5 RETURNING name;
DELETE FROM configs WHERE key = 'new_key' RETURNING *;

-- === Verify state ===
SELECT COUNT(*) FROM items;
SELECT * FROM items ORDER BY id;
SELECT * FROM configs ORDER BY key;

-- === DROP ===
DROP TABLE IF EXISTS nonexistent_table;
CREATE TABLE temp_drop (id int);
DROP TABLE temp_drop;

-- === CREATE TABLE IF NOT EXISTS ===
CREATE TABLE IF NOT EXISTS items (id serial PRIMARY KEY, name text);
CREATE TABLE IF NOT EXISTS new_table (id serial PRIMARY KEY, val text);
DROP TABLE new_table;

-- === TRUNCATE ===
CREATE TABLE trunc_test (id serial PRIMARY KEY, val text);
INSERT INTO trunc_test (val) VALUES ('a'), ('b'), ('c');
SELECT COUNT(*) FROM trunc_test;
TRUNCATE trunc_test;
SELECT COUNT(*) FROM trunc_test;
DROP TABLE trunc_test;

-- ============================================================
-- INVALID QUERIES
-- ============================================================

-- Duplicate primary key
INSERT INTO items (id, name, price) VALUES (1, 'Duplicate', 0);

-- NOT NULL violation
INSERT INTO items (name, price) VALUES (NULL, 0);

-- UNIQUE violation
INSERT INTO tags (label) VALUES ('electronics');

-- FK violation
INSERT INTO item_tags VALUES (999, 1);
INSERT INTO item_tags VALUES (1, 999);

-- Table doesn't exist
SELECT * FROM nonexistent_table;
INSERT INTO nonexistent_table VALUES (1);
UPDATE nonexistent_table SET id = 1;
DELETE FROM nonexistent_table;

-- Column doesn't exist
SELECT nonexistent_column FROM items;
INSERT INTO items (nonexistent_column) VALUES (1);
UPDATE items SET nonexistent_column = 1;

-- Type mismatch
INSERT INTO items (name, price) VALUES ('bad', 'not_a_number');
INSERT INTO measurements (sensor_id, reading) VALUES ('not_int', 1.0);

-- DROP without IF EXISTS
DROP TABLE nonexistent_table;
