-- source: investigation-2026-08.md
-- finding: 275
-- title: Catalog rows are hand-built Object[] literals that nothing checks against the view's declared column list or types. One loop writes an 11-element row into a 12-
-- begin-expected
-- columns: lo_from_bytea:oid
-- row: 3000000000
-- rowcount: 1
-- end-expected
SELECT lo_from_bytea(3000000000, '\x0102'::bytea);
-- begin-expected
-- columns: oid:oid
-- row: 3000000000
-- rowcount: 1
-- end-expected
SELECT oid FROM pg_largeobject_metadata ORDER BY oid;
-- begin-expected
-- columns: commit_action:varchar
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT commit_action FROM information_schema.tables WHERE table_schema='pg_catalog' AND table_name='pg_class';
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT count(*) > 0 FROM information_schema.tables WHERE commit_action IS NULL;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_pt (id int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_pt ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_pp ON zz_pt FOR SELECT USING (true);
-- begin-expected
-- columns: polroles:text | pg_typeof:text
-- row: {0} | oid[]
-- rowcount: 1
-- end-expected
SELECT polroles::text, pg_typeof(polroles)::text FROM pg_policy WHERE polname='zz_pp';
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f(a int) RETURNS int LANGUAGE sql AS $$ SELECT a $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f(a text) RETURNS text LANGUAGE sql AS $$ SELECT a $$;
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM information_schema.routines WHERE routine_name='zz_f';
