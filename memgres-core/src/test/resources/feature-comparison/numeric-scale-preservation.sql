-- NUMERIC(p,s): declared scale must be preserved on output, including for columns whose
-- type was set via ALTER TABLE ... ALTER COLUMN ... TYPE numeric(p, s).
--
-- Regression coverage for mtask-6 Bug 9: PostgreSQL always emits a NUMERIC(p,s) value at the
-- column's declared scale (INSERT 10 into NUMERIC(10,2) -> selects back as 10.00). memgres's
-- storage coercion (TypeCoercion.applyPrecision) already handled columns declared with a typmod
-- in CREATE TABLE, but ALTER COLUMN TYPE stripped the typmod and kept the old column's
-- (usually null) precision/scale — the exact path by which installations.capacity got its
-- NUMERIC(10,2) in the V18 app migration — so values round-tripped at their incidental scale
-- (10.0 instead of 10.00).

-- setup
CREATE TABLE nsp_t (id int PRIMARY KEY, capacity numeric(10, 2));
INSERT INTO nsp_t VALUES (1, 10), (2, 10.5), (3, 10.25);

-- stmt 1: values inserted at various scales all emit at the declared scale 2
-- begin-expected
-- columns: capacity
-- row: 10.00
-- row: 10.50
-- row: 10.25
-- end-expected
SELECT capacity FROM nsp_t ORDER BY id;

-- cleanup
DROP TABLE nsp_t;

-- setup: same, but the typmod arrives via ALTER COLUMN TYPE (the app-migration shape)
CREATE TABLE nsp_alter_t (id int PRIMARY KEY, capacity numeric);
ALTER TABLE nsp_alter_t ALTER COLUMN capacity TYPE numeric(10, 2);
INSERT INTO nsp_alter_t VALUES (1, 10.0);

-- stmt 2: the altered column enforces its new declared scale
-- begin-expected
-- columns: capacity
-- row: 10.00
-- end-expected
SELECT capacity FROM nsp_alter_t WHERE id = 1;

-- stmt 3: excess input scale rounds (half away from zero) to the declared scale
INSERT INTO nsp_alter_t VALUES (2, 10.255);

-- begin-expected
-- columns: capacity
-- row: 10.26
-- end-expected
SELECT capacity FROM nsp_alter_t WHERE id = 2;

-- cleanup
DROP TABLE nsp_alter_t;

-- setup: ALTER COLUMN TYPE numeric (no typmod) removes a previously declared scale
CREATE TABLE nsp_clear_t (id int PRIMARY KEY, v numeric(10, 2));
ALTER TABLE nsp_clear_t ALTER COLUMN v TYPE numeric;
INSERT INTO nsp_clear_t VALUES (1, 10.005);

-- stmt 4: unconstrained numeric keeps the value's own scale, no rounding to the old scale
-- begin-expected
-- columns: v
-- row: 10.005
-- end-expected
SELECT v FROM nsp_clear_t WHERE id = 1;

-- cleanup
DROP TABLE nsp_clear_t;
