-- ============================================================================
-- Column dependencies come from the parse tree, not from expression text
-- ============================================================================

DROP TABLE IF EXISTS w1c_gen CASCADE;
CREATE TABLE w1c_gen (id int, d int, total int GENERATED ALWAYS AS (id * 2) STORED);
ALTER TABLE w1c_gen DROP COLUMN d;

-- begin-expected
-- columns: cols
-- row: id,total
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'w1c_gen';

DROP TABLE IF EXISTS w1c_lit CASCADE;
CREATE TABLE w1c_lit (a int, bb int, g text GENERATED ALWAYS AS ('bb value') STORED);
ALTER TABLE w1c_lit DROP COLUMN bb;

-- begin-expected
-- columns: cols
-- row: a,g
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'w1c_lit';

DROP TABLE IF EXISTS w1c_dep CASCADE;
CREATE TABLE w1c_dep (a int, b int, g int GENERATED ALWAYS AS (a + 1) STORED);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop column a of table w1c_dep because other objects depend on it
-- end-expected-error
ALTER TABLE w1c_dep DROP COLUMN a;

DROP TABLE IF EXISTS w1c_rt CASCADE;
CREATE TABLE w1c_rt (id int, d varchar(10), total int GENERATED ALWAYS AS (id * 2) STORED);
ALTER TABLE w1c_rt ALTER COLUMN d TYPE varchar(30);

-- begin-expected
-- columns: len
-- row: 30
-- end-expected
SELECT character_maximum_length AS len FROM information_schema.columns WHERE table_name = 'w1c_rt' AND column_name = 'd';

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot alter type of a column used by a generated column
-- end-expected-error
ALTER TABLE w1c_rt ALTER COLUMN id TYPE bigint;

-- ============================================================================
-- A view depends on the columns its query reads
-- ============================================================================

DROP TABLE IF EXISTS w1c_ast CASCADE;
CREATE TABLE w1c_ast (keeper int, arge int, istinct int, indow int, imit int, rom int);
CREATE VIEW w1c_astv AS SELECT keeper FROM w1c_ast;
ALTER TABLE w1c_ast DROP COLUMN arge;
ALTER TABLE w1c_ast DROP COLUMN istinct;
ALTER TABLE w1c_ast DROP COLUMN indow;
ALTER TABLE w1c_ast DROP COLUMN imit;
ALTER TABLE w1c_ast DROP COLUMN rom;

-- begin-expected
-- columns: cols
-- row: keeper
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'w1c_ast';

DROP TABLE IF EXISTS w1c_vd CASCADE;
CREATE TABLE w1c_vd (a int, b int);
CREATE VIEW w1c_vdv AS SELECT a FROM w1c_vd;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop column a of table w1c_vd because other objects depend on it
-- end-expected-error
ALTER TABLE w1c_vd DROP COLUMN a;

ALTER TABLE w1c_vd DROP COLUMN b;

-- begin-expected
-- columns: cols
-- row: a
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'w1c_vd';

DROP TABLE IF EXISTS w1c_vl CASCADE;
CREATE TABLE w1c_vl (p int, q int);
CREATE VIEW w1c_vlv AS SELECT p, 'q marks the spot' AS note FROM w1c_vl;
ALTER TABLE w1c_vl DROP COLUMN q;

-- begin-expected
-- columns: cols
-- row: p
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'w1c_vl';

DROP TABLE IF EXISTS w1c_vc CASCADE;
CREATE TABLE w1c_vc (a int, b int);
CREATE VIEW w1c_vcv AS SELECT count(*) AS n FROM w1c_vc;
ALTER TABLE w1c_vc DROP COLUMN b;
ALTER TABLE w1c_vc ALTER COLUMN a TYPE bigint;

-- begin-expected
-- columns: cols
-- row: a:bigint
-- end-expected
SELECT string_agg(column_name || ':' || data_type, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'w1c_vc';

DROP TABLE IF EXISTS w1c_vs CASCADE;
CREATE TABLE w1c_vs (a int, b int);
CREATE VIEW w1c_vsv AS SELECT * FROM w1c_vs;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop column b of table w1c_vs because other objects depend on it
-- end-expected-error
ALTER TABLE w1c_vs DROP COLUMN b;

DROP TABLE IF EXISTS w1c_vx CASCADE;
CREATE TABLE w1c_vx (p int, q int);
CREATE VIEW w1c_vxv AS SELECT q FROM w1c_vx;
CREATE VIEW w1c_vxv2 AS SELECT q FROM w1c_vxv;
ALTER TABLE w1c_vx DROP COLUMN q CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM information_schema.views WHERE table_name IN ('w1c_vxv', 'w1c_vxv2');

DROP TABLE IF EXISTS w1c_vr CASCADE;
CREATE TABLE w1c_vr (a int, b int);
CREATE VIEW w1c_vrv AS SELECT a * 2 AS x FROM w1c_vr;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot alter type of a column used by a view or rule
-- end-expected-error
ALTER TABLE w1c_vr ALTER COLUMN a TYPE bigint;

ALTER TABLE w1c_vr ALTER COLUMN b TYPE bigint;

-- begin-expected
-- columns: cols
-- row: a:integer,b:bigint
-- end-expected
SELECT string_agg(column_name || ':' || data_type, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'w1c_vr';

-- ============================================================================
-- A generation expression holding the letters 'select'
-- ============================================================================

DROP TABLE IF EXISTS w1c_sw CASCADE;
CREATE TABLE w1c_sw (selected int, g int GENERATED ALWAYS AS (selected * 2) STORED);
INSERT INTO w1c_sw (selected) VALUES (5);

-- begin-expected
-- columns: selected, g
-- row: 5, 10
-- end-expected
SELECT selected, g FROM w1c_sw;

DROP TABLE IF EXISTS w1c_sq CASCADE;
CREATE TABLE w1c_sq (a text, g text GENERATED ALWAYS AS (a || 'select') STORED);
INSERT INTO w1c_sq (a) VALUES ('x');

-- begin-expected
-- columns: a, g
-- row: x, xselect
-- end-expected
SELECT a, g FROM w1c_sq;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in column generation expression
-- end-expected-error
CREATE TABLE w1c_sub (a int, g int GENERATED ALWAYS AS ((SELECT 1)) STORED);

DROP VIEW IF EXISTS w1c_astv;
DROP VIEW IF EXISTS w1c_vdv;
DROP VIEW IF EXISTS w1c_vlv;
DROP VIEW IF EXISTS w1c_vcv;
DROP VIEW IF EXISTS w1c_vsv;
DROP VIEW IF EXISTS w1c_vrv;
DROP TABLE IF EXISTS w1c_gen, w1c_lit, w1c_dep, w1c_rt, w1c_ast, w1c_vd, w1c_vl, w1c_vc, w1c_vs, w1c_vx, w1c_vr, w1c_sw, w1c_sq, w1c_sub CASCADE;

-- ============================================================================
-- PL/pgSQL finds INTO where the tokens are, not where the letters are
-- ============================================================================

DROP TABLE IF EXISTS w1c_pt CASCADE;
CREATE TABLE w1c_pt (a text);
INSERT INTO w1c_pt VALUES ('Q');

CREATE FUNCTION w1c_pf1() RETURNS text AS $$ DECLARE v text; BEGIN SELECT ' into me ' INTO v; RETURN v; END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row:  into me 
-- end-expected
SELECT w1c_pf1() AS v;

CREATE FUNCTION w1c_pf2() RETURNS text AS $$ DECLARE v text; BEGIN SELECT ' into ' || a INTO v FROM w1c_pt; RETURN v; END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row:  into Q
-- end-expected
SELECT w1c_pf2() AS v;

CREATE FUNCTION w1c_pf3() RETURNS int AS $$ DECLARE v int; BEGIN SELECT length(' into ') INTO v FROM w1c_pt LIMIT 1; RETURN v; END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: 6
-- end-expected
SELECT w1c_pf3() AS v;

CREATE FUNCTION w1c_pf4() RETURNS text AS $$ DECLARE v text; BEGIN SELECT concat('a',' into ','b') INTO v; RETURN v; END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: a into b
-- end-expected
SELECT w1c_pf4() AS v;

CREATE FUNCTION w1c_pf5() RETURNS text AS $$ DECLARE v text; BEGIN INSERT INTO w1c_pt VALUES ('x') RETURNING a || ' INTO y' INTO v; RETURN v; END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: x INTO y
-- end-expected
SELECT w1c_pf5() AS v;

CREATE FUNCTION w1c_pf6() RETURNS text AS $$ DECLARE v text; BEGIN SELECT 'x INTO y' INTO v; RETURN v; END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: x INTO y
-- end-expected
SELECT w1c_pf6() AS v;

DROP FUNCTION IF EXISTS w1c_pf1(), w1c_pf2(), w1c_pf3(), w1c_pf4(), w1c_pf5(), w1c_pf6();
DROP TABLE IF EXISTS w1c_pt CASCADE;