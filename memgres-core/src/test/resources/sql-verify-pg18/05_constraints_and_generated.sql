\echo '=== 05_constraints_and_generated.sql ==='
\set VERBOSITY verbose
\set SHOW_CONTEXT never
\set ON_ERROR_STOP off
SET search_path = pg_catalog, public;
DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;

CREATE TABLE parent(
    id int PRIMARY KEY,
    code text UNIQUE,
    qty int NOT NULL CHECK (qty >= 0),
    category text,
    category_norm text GENERATED ALWAYS AS (upper(category)) STORED
);

CREATE TABLE child(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    parent_id int REFERENCES parent(id),
    qty int CHECK (qty > 0),
    note text NOT NULL,
    uq1 int,
    uq2 int,
    UNIQUE(uq1, uq2)
);

INSERT INTO parent(id, code, qty, category) VALUES
    (1, 'A', 10, 'foo'),
    (2, 'B', 0, 'bar');

INSERT INTO child(parent_id, qty, note, uq1, uq2) VALUES
    (1, 1, 'ok1', 1, 1),
    (1, 2, 'ok2', 1, 2),
    (2, 5, 'ok3', 2, 1);

SELECT id, code, qty, category, category_norm FROM parent ORDER BY id;
SELECT id, parent_id, qty, note, uq1, uq2 FROM child ORDER BY id;

-- constraint violations
INSERT INTO parent(id, code, qty, category) VALUES (1, 'C', 5, 'baz');
INSERT INTO parent(id, code, qty, category) VALUES (3, 'A', 5, 'baz');
INSERT INTO parent(id, code, qty, category) VALUES (4, 'D', -1, 'baz');
INSERT INTO parent(id, code, qty, category) VALUES (5, 'E', NULL, 'baz');
INSERT INTO child(parent_id, qty, note, uq1, uq2) VALUES (999, 1, 'bad-fk', 9, 9);
INSERT INTO child(parent_id, qty, note, uq1, uq2) VALUES (1, 0, 'bad-check', 8, 8);
INSERT INTO child(parent_id, qty, note, uq1, uq2) VALUES (1, 1, NULL, 7, 7);
INSERT INTO child(parent_id, qty, note, uq1, uq2) VALUES (1, 1, 'dup-unique', 1, 1);
INSERT INTO child(id, parent_id, qty, note, uq1, uq2) VALUES (1, 1, 1, 'manual-id', 6, 6);

-- alter-table constraint cases
ALTER TABLE child ADD CONSTRAINT child_qty_positive CHECK (qty > 0);
ALTER TABLE child ADD CONSTRAINT child_qty_positive CHECK (qty > 0);
ALTER TABLE child ADD CONSTRAINT bad_fk2 FOREIGN KEY (note) REFERENCES parent(code);
ALTER TABLE child ADD CONSTRAINT bad_fk3 FOREIGN KEY (parent_id) REFERENCES parent(no_such_col);
ALTER TABLE child ALTER COLUMN note DROP NOT NULL;
ALTER TABLE child ADD COLUMN derived int GENERATED ALWAYS AS (qty * 2) STORED;
SELECT id, qty, derived FROM child ORDER BY id;

-- generated column restrictions / errors
CREATE TABLE gen_bad_01(a int, b timestamp GENERATED ALWAYS AS (clock_timestamp()) STORED);
CREATE TABLE gen_bad_02(a int, b int GENERATED ALWAYS AS (c) STORED, c int);
CREATE TABLE gen_bad_03(a int, b int GENERATED ALWAYS AS ((SELECT 1)) STORED);

DROP SCHEMA compat CASCADE;
