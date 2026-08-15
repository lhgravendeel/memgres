-- A composite type is a type: the name is looked up in the schemas the search path reaches, a
-- cast of NULL included, and a value written for one is held to the shape the type declares.
--   * a record constructor of the wrong width is a cast that cannot be made, and says how many
--     columns it was given; the same value written as text is input the composite's own reader
--     cannot read, and says the same thing in that reader's words.
--   * a relation's row type counts its columns the same way, and so does an array element.
--   * every write path takes the same route: INSERT, UPDATE, MERGE, ON CONFLICT DO UPDATE and
--     COPY, and a field of a composite is read by its own type's reader, so a nested record is
--     blamed on the inner type.

-- setup
CREATE SCHEMA cvh_hid;
CREATE DOMAIN cvh_hid.cvh_hd AS int NOT NULL;
CREATE TYPE cvh_hid.cvh_hc AS (x int);
CREATE TYPE cvh_hid.cvh_hr AS RANGE (subtype = int4);
CREATE TYPE cvh_hid.cvh_he AS ENUM ('a','b');
CREATE TABLE cvh_hrel (k int);

-- stmt 1: a cast of NULL does not reach a type the search path does not reach
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cvh_hd" does not exist
-- end-expected-error
SELECT NULL::cvh_hd AS d;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cvh_hc" does not exist
-- end-expected-error
SELECT NULL::cvh_hc AS d;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cvh_hr" does not exist
-- end-expected-error
SELECT NULL::cvh_hr AS d;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cvh_he" does not exist
-- end-expected-error
SELECT NULL::cvh_he AS d;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cvh_hd" does not exist
-- end-expected-error
SELECT CAST(NULL AS cvh_hd) AS d;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cvh_hd[]" does not exist
-- end-expected-error
SELECT NULL::cvh_hd[] AS d;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cvh_hd" does not exist
-- end-expected-error
SELECT coalesce(NULL::cvh_hd, 1) AS d;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cvh_he" does not exist
-- end-expected-error
SELECT array[NULL::cvh_he] AS d;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cvh_hd" does not exist
-- end-expected-error
SELECT 1::cvh_hd AS d;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cvh_hc" does not exist
-- end-expected-error
SELECT row(1)::cvh_hc AS d;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cvh_he" does not exist
-- end-expected-error
SELECT 'a'::cvh_he AS d;

-- stmt 2: a relation carries a row type that answers to the relation namespace
-- begin-expected
-- columns: d
-- row: true
-- end-expected
SELECT ((NULL::cvh_hrel) IS NULL)::text AS d;

-- stmt 3: the qualified name reaches it, and the domain's NOT NULL is what refuses the cast
-- begin-expected-error
-- sqlstate: 23502
-- message-like: domain cvh_hid.cvh_hd does not allow null values
-- end-expected-error
SELECT NULL::cvh_hid.cvh_hd AS d;
-- begin-expected
-- columns: d
-- row: true
-- end-expected
SELECT ((NULL::cvh_hid.cvh_hc) IS NULL)::text AS d;

-- stmt 4: on the path the bare name means it again, and off it stops meaning it
SET search_path = public, cvh_hid;
-- begin-expected-error
-- sqlstate: 23502
-- message-like: domain cvh_hd does not allow null values
-- end-expected-error
SELECT NULL::cvh_hd AS d;
-- begin-expected
-- columns: d
-- row: true
-- end-expected
SELECT ((NULL::cvh_hc) IS NULL)::text AS d;
-- begin-expected
-- columns: d
-- row: true
-- end-expected
SELECT ((NULL::cvh_hd[]) IS NULL)::text AS d;
RESET search_path;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "cvh_hc" does not exist
-- end-expected-error
SELECT NULL::cvh_hc AS d;
DROP TABLE cvh_hrel;
DROP SCHEMA cvh_hid CASCADE;

-- stmt 5: a record constructor of the wrong width, and the same value written as text
CREATE TYPE cvh_pr AS (a int, b int);
CREATE TABLE cvh_prel (k int);
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
SELECT row(1,2,3)::cvh_pr AS d;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
SELECT row(1)::cvh_pr AS d;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(1,2,3)"
-- end-expected-error
SELECT '(1,2,3)'::cvh_pr AS d;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(1)"
-- end-expected-error
SELECT '(1)'::cvh_pr AS d;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "()"
-- end-expected-error
SELECT '()'::cvh_pr AS d;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "abc"
-- end-expected-error
SELECT 'abc'::cvh_pr AS d;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(1,2"
-- end-expected-error
SELECT '(1,2'::cvh_pr AS d;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(1,2,3)"
-- end-expected-error
SELECT '(1,2,3)'::text::cvh_pr AS d;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(1,2,3)"
-- end-expected-error
SELECT (SELECT '(1,2,3)')::cvh_pr AS d;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(1,2,3)"
-- end-expected-error
SELECT '(1,2,3)'::cvh_prel AS d;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(1,2,3)"
-- end-expected-error
SELECT '{"(1,2,3)"}'::cvh_pr[] AS d;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
SELECT ARRAY[row(1,2,3)]::cvh_pr[] AS d;

-- stmt 6: every write path takes the same route
CREATE TABLE cvh_hp (k int, c cvh_pr);
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
INSERT INTO cvh_hp VALUES (1, row(1,2,3));
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
INSERT INTO cvh_hp VALUES (1, row(1));
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(1,2,3)"
-- end-expected-error
INSERT INTO cvh_hp VALUES (1, '(1,2,3)');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(1)"
-- end-expected-error
INSERT INTO cvh_hp VALUES (1, '(1)');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "abc"
-- end-expected-error
INSERT INTO cvh_hp VALUES (1, 'abc');
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
INSERT INTO cvh_hp (k, c) VALUES (1, row(1,2,3));
INSERT INTO cvh_hp VALUES (1, row(1,2));
-- begin-expected
-- columns: d
-- row: (1,2)
-- end-expected
SELECT c::text AS d FROM cvh_hp;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
UPDATE cvh_hp SET c = row(9,8,7);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(9,8,7)"
-- end-expected-error
UPDATE cvh_hp SET c = '(9,8,7)';
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
UPDATE cvh_hp SET c = row(9);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(9)"
-- end-expected-error
UPDATE cvh_hp SET c = '(9)';
-- begin-expected
-- columns: d
-- row: (1,2)
-- end-expected
SELECT c::text AS d FROM cvh_hp;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
MERGE INTO cvh_hp t USING (SELECT 2 AS k) s ON t.k = s.k WHEN NOT MATCHED THEN INSERT (k, c) VALUES (s.k, row(1,2,3));
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(1,2,3)"
-- end-expected-error
MERGE INTO cvh_hp t USING (SELECT 2 AS k) s ON t.k = s.k WHEN NOT MATCHED THEN INSERT (k, c) VALUES (s.k, '(1,2,3)');
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
MERGE INTO cvh_hp t USING (SELECT 1 AS k) s ON t.k = s.k WHEN MATCHED THEN UPDATE SET c = row(1,2,3);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(1,2,3)"
-- end-expected-error
MERGE INTO cvh_hp t USING (SELECT 1 AS k) s ON t.k = s.k WHEN MATCHED THEN UPDATE SET c = '(1,2,3)';
CREATE TABLE cvh_ocp (k int PRIMARY KEY, c cvh_pr);
INSERT INTO cvh_ocp VALUES (1, row(1,2));
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
INSERT INTO cvh_ocp VALUES (1, row(3,4)) ON CONFLICT (k) DO UPDATE SET c = row(1,2,3);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(1,2,3)"
-- end-expected-error
INSERT INTO cvh_ocp VALUES (1, row(3,4)) ON CONFLICT (k) DO UPDATE SET c = '(1,2,3)';
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
INSERT INTO cvh_ocp VALUES (1, row(3,4)) ON CONFLICT (k) DO UPDATE SET c = row(9);
-- begin-expected
-- columns: d
-- row: (1,2)
-- end-expected
SELECT c::text AS d FROM cvh_ocp;

-- stmt 7: a nested record is blamed on the inner type
CREATE TYPE cvh_nst AS (x int, y cvh_pr);
CREATE TABLE cvh_hn (k int, c cvh_nst);
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
SELECT row(1,row(2,3,4))::cvh_nst AS d;
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type record to cvh_pr
-- end-expected-error
INSERT INTO cvh_hn VALUES (1, row(1,row(2,3,4)));
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(2,3,4)"
-- end-expected-error
SELECT '(1,"(2,3,4)")'::cvh_nst AS d;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(2)"
-- end-expected-error
SELECT '(1,"(2)")'::cvh_nst AS d;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(2,3,4)"
-- end-expected-error
INSERT INTO cvh_hn VALUES (1, '(1,"(2,3,4)")');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(2)"
-- end-expected-error
INSERT INTO cvh_hn VALUES (1, '(1,"(2)")');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed record literal: "(2,3,4)"
-- end-expected-error
SELECT row(1,'(2,3,4)')::cvh_nst AS d;
-- begin-expected
-- columns: d
-- row: (1,"(2,3)")
-- end-expected
SELECT (row(1,'(2,3)')::cvh_nst)::text AS d;
INSERT INTO cvh_hn VALUES (1, row(1,row(2,3)));
-- begin-expected
-- columns: d
-- row: (1,"(2,3)")
-- end-expected
SELECT c::text AS d FROM cvh_hn;
-- begin-expected
-- columns: d
-- row: 1
-- end-expected
SELECT count(*)::text AS d FROM cvh_hn;

-- cleanup
DROP TABLE cvh_hn;
DROP TABLE cvh_hp;
DROP TABLE cvh_ocp;
DROP TABLE cvh_prel;
DROP TYPE cvh_nst;
DROP TYPE cvh_pr;
