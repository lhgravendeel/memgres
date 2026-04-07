-- ============================================================
-- 08: Constraints, Transactions, and Error Handling
-- ============================================================

-- === Setup with various constraints ===
CREATE TABLE parent (id serial PRIMARY KEY, name text UNIQUE NOT NULL);
CREATE TABLE child (id serial PRIMARY KEY, parent_id int NOT NULL REFERENCES parent(id) ON DELETE CASCADE, data text);
CREATE TABLE checked (id serial PRIMARY KEY, val int CHECK (val > 0 AND val < 1000), label text CHECK (length(label) <= 50));
CREATE TABLE multi_unique (a int, b int, c text, UNIQUE(a, b));

INSERT INTO parent (name) VALUES ('alpha'), ('beta'), ('gamma');
INSERT INTO child (parent_id, data) VALUES (1, 'child1'), (1, 'child2'), (2, 'child3');

-- === PK violations ===
INSERT INTO parent (id, name) VALUES (1, 'duplicate_pk');

-- === UNIQUE violations ===
INSERT INTO parent (name) VALUES ('alpha');
INSERT INTO multi_unique (a, b, c) VALUES (1, 2, 'first');
INSERT INTO multi_unique (a, b, c) VALUES (1, 2, 'duplicate');
INSERT INTO multi_unique (a, b, c) VALUES (1, 3, 'different_b_ok');

-- === NOT NULL violations ===
INSERT INTO parent (name) VALUES (NULL);
INSERT INTO child (parent_id, data) VALUES (NULL, 'no_parent');

-- === CHECK violations ===
INSERT INTO checked (val) VALUES (0);
INSERT INTO checked (val) VALUES (1000);
INSERT INTO checked (val) VALUES (-5);
INSERT INTO checked (val, label) VALUES (50, REPEAT('x', 51));
INSERT INTO checked (val) VALUES (500);
SELECT * FROM checked;

-- === FK violations ===
INSERT INTO child (parent_id, data) VALUES (999, 'bad_fk');
UPDATE child SET parent_id = 999 WHERE id = 1;
DELETE FROM parent WHERE id = 1;

-- Wait — ON DELETE CASCADE should work
SELECT COUNT(*) FROM child WHERE parent_id = 1;
DELETE FROM parent WHERE id = 1;
SELECT COUNT(*) FROM child WHERE parent_id = 1;
SELECT * FROM child ORDER BY id;

-- === ON DELETE SET NULL ===
CREATE TABLE set_null_parent (id serial PRIMARY KEY, name text);
CREATE TABLE set_null_child (id serial PRIMARY KEY, parent_id int REFERENCES set_null_parent(id) ON DELETE SET NULL, info text);
INSERT INTO set_null_parent (name) VALUES ('parent1');
INSERT INTO set_null_child (parent_id, info) VALUES (1, 'will_be_nulled');
DELETE FROM set_null_parent WHERE id = 1;
SELECT parent_id, info FROM set_null_child;
DROP TABLE set_null_child;
DROP TABLE set_null_parent;

-- === ON UPDATE CASCADE ===
CREATE TABLE cascade_parent (id int PRIMARY KEY, label text);
CREATE TABLE cascade_child (id serial PRIMARY KEY, parent_id int REFERENCES cascade_parent(id) ON UPDATE CASCADE, data text);
INSERT INTO cascade_parent VALUES (100, 'original');
INSERT INTO cascade_child (parent_id, data) VALUES (100, 'tracks_parent');
UPDATE cascade_parent SET id = 200 WHERE id = 100;
SELECT parent_id FROM cascade_child;
DROP TABLE cascade_child;
DROP TABLE cascade_parent;

-- === Transactions: basic ===
CREATE TABLE txn_test (id serial PRIMARY KEY, val text);
BEGIN;
INSERT INTO txn_test (val) VALUES ('committed');
COMMIT;
SELECT * FROM txn_test;

BEGIN;
INSERT INTO txn_test (val) VALUES ('rolled_back');
ROLLBACK;
SELECT * FROM txn_test;
SELECT COUNT(*) FROM txn_test;

-- === Transactions: error and rollback ===
BEGIN;
INSERT INTO txn_test (val) VALUES ('before_error');
INSERT INTO parent (name) VALUES ('alpha');
SELECT COUNT(*) FROM txn_test;
ROLLBACK;
SELECT COUNT(*) FROM txn_test;

-- === Savepoints ===
BEGIN;
INSERT INTO txn_test (val) VALUES ('base');
SAVEPOINT sp1;
INSERT INTO txn_test (val) VALUES ('after_sp1');
SAVEPOINT sp2;
INSERT INTO txn_test (val) VALUES ('after_sp2');
ROLLBACK TO sp2;
INSERT INTO txn_test (val) VALUES ('after_rollback_to_sp2');
COMMIT;
SELECT val FROM txn_test ORDER BY id;

-- === Autocommit behavior ===
INSERT INTO txn_test (val) VALUES ('autocommit1');
INSERT INTO txn_test (val) VALUES ('autocommit2');
SELECT COUNT(*) FROM txn_test;

-- === Deferred constraints ===
CREATE TABLE def_parent (id int PRIMARY KEY);
CREATE TABLE def_child (id int PRIMARY KEY, pid int, CONSTRAINT fk_deferred FOREIGN KEY (pid) REFERENCES def_parent(id) DEFERRABLE INITIALLY DEFERRED);
BEGIN;
INSERT INTO def_child (id, pid) VALUES (1, 100);
INSERT INTO def_parent (id) VALUES (100);
COMMIT;
SELECT * FROM def_child;
SELECT * FROM def_parent;
DROP TABLE def_child;
DROP TABLE def_parent;

-- === ALTER TABLE ADD/DROP CONSTRAINT ===
CREATE TABLE alter_con (id serial PRIMARY KEY, val int, label text);
ALTER TABLE alter_con ADD CONSTRAINT chk_val CHECK (val >= 0);
INSERT INTO alter_con (val) VALUES (-1);
INSERT INTO alter_con (val) VALUES (5);
ALTER TABLE alter_con DROP CONSTRAINT chk_val;
INSERT INTO alter_con (val) VALUES (-1);
SELECT val FROM alter_con ORDER BY val;
DROP TABLE alter_con;

-- Cleanup
DROP TABLE txn_test;
DROP TABLE checked;
DROP TABLE multi_unique;
DROP TABLE child;
DROP TABLE parent;
