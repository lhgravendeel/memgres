-- source: investigation-2026-08.md
-- finding: 274
-- title: The catalog deparser prints memgres's own AST spelling rather than PostgreSQL's normalised form: no outer parentheses on a default or generation expression, red
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_dflt (id int DEFAULT (1 + 2));
-- begin-expected
-- columns: pg_get_expr:text
-- row: (1 + 2)
-- rowcount: 1
-- end-expected
SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef d JOIN pg_class c ON c.oid=d.adrelid WHERE c.relname='zz_dflt';
-- begin-expected
-- columns: column_default:varchar
-- row: (1 + 2)
-- rowcount: 1
-- end-expected
SELECT column_default FROM information_schema.columns WHERE table_name='zz_dflt' AND column_name='id';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_g (id int, c int GENERATED ALWAYS AS (id * 2) STORED);
-- begin-expected
-- columns: generation_expression:varchar
-- row: (id * 2)
-- rowcount: 1
-- end-expected
SELECT generation_expression FROM information_schema.columns WHERE table_name='zz_g' AND column_name='c';
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_g" already exists
-- end-expected-error
CREATE TABLE zz_g (id int, CONSTRAINT zz_gck CHECK (id > 0));
-- begin-expected
-- columns: pg_get_constraintdef:text
-- rowcount: 0
-- end-expected
SELECT pg_get_constraintdef(oid, true) FROM pg_constraint WHERE conname='zz_gck';
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_trgf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_tg BEFORE INSERT ON zz_g FOR EACH ROW WHEN (new.id > 0) EXECUTE FUNCTION zz_trgf();
-- begin-expected
-- columns: pg_get_triggerdef:text
-- row: CREATE TRIGGER zz_tg BEFORE INSERT ON zz_g FOR EACH ROW WHEN (new.id > 0) EXECUTE FUNCTION zz_trgf()
-- rowcount: 1
-- end-expected
SELECT pg_get_triggerdef(oid, true) FROM pg_trigger WHERE tgname='zz_tg';
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
