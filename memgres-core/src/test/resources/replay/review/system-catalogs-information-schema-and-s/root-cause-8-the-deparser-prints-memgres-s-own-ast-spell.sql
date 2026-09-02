-- source: review-2026-08.md
-- finding: Root cause 8: the deparser prints memgres's own AST spelling
-- area: System catalogs, information_schema and security
-- title: Root cause 8: the deparser prints memgres's own AST spelling
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_dflt (id int DEFAULT (1 + 2));
-- begin-expected
-- columns: column_default:varchar
-- row: (1 + 2)
-- rowcount: 1
-- end-expected
SELECT column_default FROM information_schema.columns WHERE table_name='zz_dflt' AND column_name='id';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_g (id int, CONSTRAINT zz_gck CHECK (id > 0));
-- begin-expected
-- columns: pg_get_constraintdef:text
-- row: CHECK (id > 0)
-- rowcount: 1
-- end-expected
SELECT pg_get_constraintdef(oid, true) FROM pg_constraint WHERE conname='zz_gck';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_i (name varchar(50), flag boolean);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_i_idx ON zz_i (lower(name)) WHERE flag;
-- begin-expected
-- columns: pg_get_expr:text
-- row: lower((name)::text)
-- rowcount: 1
-- end-expected
SELECT pg_get_expr(indexprs, indrelid) FROM pg_index WHERE indexrelid='zz_i_idx'::regclass;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_pt (id int, owner text, n int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_pt ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_pp ON zz_pt FOR SELECT USING (owner = current_user AND n > 5);
-- begin-expected
-- columns: qual:text
-- row: ((owner = CURRENT_USER) AND (n > 5))
-- rowcount: 1
-- end-expected
SELECT qual FROM pg_policies WHERE policyname='zz_pp';
