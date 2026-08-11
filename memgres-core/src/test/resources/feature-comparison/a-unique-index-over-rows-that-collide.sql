-- ============================================================================
-- A unique index over rows that already collide
--
-- Nothing is written, so what is reported is the state the table was found
-- holding: "could not create unique index", naming the index -- including the
-- name an unnamed index derived -- and nothing is left behind. A null in the
-- key stops nothing unless the index says nulls are not distinct. Every value
-- here was read off PostgreSQL 18.
-- ============================================================================

CREATE TABLE zw5x_a (a int, b text, c text, d int);
INSERT INTO zw5x_a VALUES (1,'p q','x,y',NULL),(1,'p q','x,y',NULL),(2,'z','w',5);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zw5x_a_u0"
-- end-expected-error
CREATE UNIQUE INDEX zw5x_a_u0 ON zw5x_a (a);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zw5x_a_u1"
-- end-expected-error
CREATE UNIQUE INDEX zw5x_a_u1 ON zw5x_a (a, b);

-- An expression key, a partial index and an index carrying INCLUDE columns,
-- a COLLATE clause or a sort direction are all judged over the same rows.
-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zw5x_a_u3"
-- end-expected-error
CREATE UNIQUE INDEX zw5x_a_u3 ON zw5x_a ((a+1), upper(b));

-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zw5x_a_u5"
-- end-expected-error
CREATE UNIQUE INDEX zw5x_a_u5 ON zw5x_a (lower(c));

-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zw5x_a_u6"
-- end-expected-error
CREATE UNIQUE INDEX zw5x_a_u6 ON zw5x_a (a, b) INCLUDE (c);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zw5x_a_u7"
-- end-expected-error
CREATE UNIQUE INDEX zw5x_a_u7 ON zw5x_a (b COLLATE "C");

-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zw5x_a_u8"
-- end-expected-error
CREATE UNIQUE INDEX zw5x_a_u8 ON zw5x_a (a DESC, b);

-- A partial index judges only the rows its predicate lets through -- and both
-- of the colliding rows are among them.
-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zw5x_a_u9"
-- end-expected-error
CREATE UNIQUE INDEX zw5x_a_u9 ON zw5x_a (a) WHERE d IS NULL;

-- One whose predicate lets only one of them through is built.
CREATE UNIQUE INDEX zw5x_a_ok0 ON zw5x_a (a) WHERE d IS NOT NULL;

-- An index nobody named reports the name it derived.
-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zw5x_a_a_b_idx"
-- end-expected-error
CREATE UNIQUE INDEX ON zw5x_a (a, b);

-- Two rows hold d = NULL, and nulls are distinct unless the index says they
-- are not.
CREATE UNIQUE INDEX zw5x_a_ok1 ON zw5x_a (d);
CREATE UNIQUE INDEX zw5x_a_ok2 ON zw5x_a (a, d);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zw5x_a_u10"
-- end-expected-error
CREATE UNIQUE INDEX zw5x_a_u10 ON zw5x_a (a, d) NULLS NOT DISTINCT;

-- Nothing the refused statements asked for was created.
-- begin-expected
-- columns: idx
-- row: zw5x_a_ok0,zw5x_a_ok1,zw5x_a_ok2
-- end-expected
SELECT string_agg(indexname, ',' ORDER BY indexname) AS idx FROM pg_indexes WHERE tablename = 'zw5x_a';

DROP TABLE zw5x_a;

-- ----------------------------------------------------------------------------
-- A key that needs quoting keeps its quotes, and a constraint is not recorded
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_q ("A b" int, x int);
INSERT INTO zw5x_q VALUES (1,1),(1,2);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zw5x_q_c"
-- end-expected-error
ALTER TABLE zw5x_q ADD CONSTRAINT zw5x_q_c UNIQUE ("A b");

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_constraint WHERE conname = 'zw5x_q_c';

DELETE FROM zw5x_q WHERE x = 2;
ALTER TABLE zw5x_q ADD CONSTRAINT zw5x_q_c UNIQUE ("A b");

-- begin-expected
-- columns: def
-- row: UNIQUE ("A b")
-- end-expected
SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conname = 'zw5x_q_c';

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zw5x_q_c"
-- end-expected-error
INSERT INTO zw5x_q VALUES (1, 9);

DROP TABLE zw5x_q;

-- ----------------------------------------------------------------------------
-- A retype that would break a key is refused by the index it would rebuild
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_rt ("A b" text, x int);
CREATE UNIQUE INDEX zw5x_rt_u ON zw5x_rt ("A b");
INSERT INTO zw5x_rt VALUES ('1', 1), ('01', 2);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zw5x_rt_u"
-- end-expected-error
ALTER TABLE zw5x_rt ALTER COLUMN "A b" TYPE int USING "A b"::int;

-- The refusal left the column and its rows as they were.
-- begin-expected
-- columns: t
-- row: text
-- end-expected
SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute WHERE attrelid = 'zw5x_rt'::regclass AND attname = 'A b';

-- begin-expected
-- columns: A b | x
-- row: 1 | 1
-- row: 01 | 2
-- end-expected
SELECT "A b", x FROM zw5x_rt ORDER BY x;

DROP TABLE zw5x_rt;

-- ----------------------------------------------------------------------------
-- A write refused by an expression index names the index it collided with
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_b (a int, b text, c int);
CREATE UNIQUE INDEX zw5x_b_u1 ON zw5x_b (a);
CREATE UNIQUE INDEX zw5x_b_u2 ON zw5x_b (lower(b));
CREATE UNIQUE INDEX zw5x_b_u3 ON zw5x_b ((a+c));
INSERT INTO zw5x_b VALUES (1,'x',5);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zw5x_b_u1"
-- end-expected-error
INSERT INTO zw5x_b VALUES (1,'y',9);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zw5x_b_u2"
-- end-expected-error
INSERT INTO zw5x_b VALUES (2,'X',9);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zw5x_b_u3"
-- end-expected-error
INSERT INTO zw5x_b VALUES (3,'z',3);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM zw5x_b;

DROP TABLE zw5x_b;
