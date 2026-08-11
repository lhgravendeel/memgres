-- ============================================================================
-- A rule's WHERE is read as SQL, so its own string literals are left alone
-- ============================================================================

CREATE TABLE ruleq_lit (a text);
CREATE TABLE ruleq_lit_log (m text);
CREATE RULE ruleq_lit_r AS ON INSERT TO ruleq_lit
  WHERE NEW.a <> 'NEW.a is a name'
  DO ALSO INSERT INTO ruleq_lit_log VALUES ('fired');

INSERT INTO ruleq_lit (a) VALUES ('V');

-- begin-expected
-- columns: a
-- row: V
-- end-expected
SELECT a FROM ruleq_lit;

-- begin-expected
-- columns: m
-- row: fired
-- end-expected
SELECT m FROM ruleq_lit_log;

DROP TABLE ruleq_lit CASCADE;
DROP TABLE ruleq_lit_log CASCADE;

-- ============================================================================
-- A value that spells NEW.<col> is a value, not a reference the WHERE re-reads
-- ============================================================================

CREATE TABLE ruleq_val (a text, b text);
CREATE TABLE ruleq_val_log (m text);
CREATE RULE ruleq_val_r AS ON INSERT TO ruleq_val
  WHERE NEW.a = NEW.b
  DO ALSO INSERT INTO ruleq_val_log VALUES ('eq');

INSERT INTO ruleq_val (a,b) VALUES ('NEW.b','z');

-- begin-expected
-- columns: a, b
-- row: NEW.b, z
-- end-expected
SELECT a, b FROM ruleq_val;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM ruleq_val_log;

DROP TABLE ruleq_val CASCADE;
DROP TABLE ruleq_val_log CASCADE;

-- ============================================================================
-- A rule's WHERE reads a bytea as a bytea
-- ============================================================================

CREATE TABLE ruleq_bin (b bytea);
CREATE TABLE ruleq_bin_log (h text);
CREATE RULE ruleq_bin_r AS ON INSERT TO ruleq_bin
  WHERE encode(NEW.b,'hex') = '0a0b'
  DO ALSO INSERT INTO ruleq_bin_log VALUES ('fired');

INSERT INTO ruleq_bin (b) VALUES ('\x0a0b'::bytea);
INSERT INTO ruleq_bin (b) VALUES ('\x0c'::bytea);

-- begin-expected
-- columns: h
-- row: fired
-- end-expected
SELECT h FROM ruleq_bin_log;

-- begin-expected
-- columns: encode
-- row: 0a0b
-- row: 0c
-- end-expected
SELECT encode(b,'hex') FROM ruleq_bin ORDER BY 1;

DROP TABLE ruleq_bin CASCADE;
DROP TABLE ruleq_bin_log CASCADE;

-- ============================================================================
-- An UPDATE rule's WHERE reads OLD the same way
-- ============================================================================

CREATE TABLE ruleq_upd (a text);
CREATE TABLE ruleq_upd_log (m text);
INSERT INTO ruleq_upd (a) VALUES ('one');
CREATE RULE ruleq_upd_r AS ON UPDATE TO ruleq_upd
  WHERE OLD.a <> 'OLD.a is a name'
  DO ALSO INSERT INTO ruleq_upd_log VALUES ('fired');

UPDATE ruleq_upd SET a = 'two';

-- begin-expected
-- columns: a
-- row: two
-- end-expected
SELECT a FROM ruleq_upd;

-- begin-expected
-- columns: m
-- row: fired
-- end-expected
SELECT m FROM ruleq_upd_log;

DROP TABLE ruleq_upd CASCADE;
DROP TABLE ruleq_upd_log CASCADE;

-- ============================================================================
-- A qualified DO INSTEAD rule diverts the rows its WHERE holds for, no others
-- ============================================================================

CREATE TABLE ruleq_ins (a text);
CREATE TABLE ruleq_ins_log (m text);
CREATE RULE ruleq_ins_r AS ON INSERT TO ruleq_ins
  WHERE NEW.a = 'skip'
  DO INSTEAD INSERT INTO ruleq_ins_log VALUES (NEW.a);

INSERT INTO ruleq_ins (a) VALUES ('keep'), ('skip');

-- begin-expected
-- columns: a
-- row: keep
-- end-expected
SELECT a FROM ruleq_ins ORDER BY 1;

-- begin-expected
-- columns: m
-- row: skip
-- end-expected
SELECT m FROM ruleq_ins_log ORDER BY 1;

DROP TABLE ruleq_ins CASCADE;
DROP TABLE ruleq_ins_log CASCADE;

-- ============================================================================
-- A VIRTUAL generated column is worked out only where the query needs it
-- ============================================================================

CREATE TABLE vgneed_t (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL);
INSERT INTO vgneed_t (a,k) VALUES (0,'zero');

-- begin-expected
-- columns: k
-- row: zero
-- end-expected
SELECT k FROM vgneed_t;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM vgneed_t;

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT a FROM vgneed_t WHERE k = 'zero';

-- begin-expected
-- columns: max
-- row: 0
-- end-expected
SELECT max(a) FROM vgneed_t;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT g FROM vgneed_t;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT * FROM vgneed_t;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT a FROM vgneed_t WHERE g > 1;

DROP TABLE vgneed_t CASCADE;

-- ============================================================================
-- A virtual column the query does name is still worked out
-- ============================================================================

CREATE TABLE vgneed_ok (a int, d int GENERATED ALWAYS AS (a * 2) VIRTUAL);
INSERT INTO vgneed_ok (a) VALUES (10), (25);

-- begin-expected
-- columns: a, d
-- row: 10, 20
-- row: 25, 50
-- end-expected
SELECT a, d FROM vgneed_ok ORDER BY a;

-- begin-expected
-- columns: a
-- row: 25
-- end-expected
SELECT a FROM vgneed_ok WHERE d = 50;

-- begin-expected
-- columns: a, d
-- row: 25, 50
-- row: 10, 20
-- end-expected
SELECT * FROM vgneed_ok ORDER BY d DESC;

-- begin-expected
-- columns: d
-- row: 20
-- end-expected
SELECT z.d FROM vgneed_ok z WHERE z.a = 10;

DROP TABLE vgneed_ok CASCADE;