-- source: review-2026-08.md
-- finding: Root cause 7: the catalogs' expression columns come from a hand-written mini-deparser that knows eight node kinds
-- area: Catalog builders and the wire layer, second pass
-- title: Root cause 7: the catalogs' expression columns come from a hand-written mini-deparser that knows eight node kinds
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_d (a text DEFAULT 'a' || 'b', e text DEFAULT upper('q'));
-- begin-expected
-- columns: column_name:name | column_default:varchar
-- row: a | ('a'::text || 'b'::text)
-- row: e | upper('q'::text)
-- rowcount: 2
-- end-expected
SELECT column_name, column_default FROM information_schema.columns
 WHERE table_name = 'zz_vf2_d' ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_vf2_d" already exists
-- end-expected-error
CREATE TABLE zz_vf2_d (b int DEFAULT '-3'::int, f numeric DEFAULT '1.5'::numeric);
-- begin-expected
-- columns: column_name:name | column_default:varchar
-- row: a | ('a'::text || 'b'::text)
-- row: e | upper('q'::text)
-- rowcount: 2
-- end-expected
SELECT column_name, column_default FROM information_schema.columns
 WHERE table_name = 'zz_vf2_d' ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_df (a text DEFAULT CASE WHEN true THEN 'x' ELSE 'y' END,
                        b text DEFAULT COALESCE(NULL, 'c'),
                        c int  DEFAULT (ARRAY[7,8,9])[2]);
-- begin-expected
-- columns: column_name:name | column_default:varchar
-- row: a | \nCASE\n    WHEN true THEN 'x'::text\n    ELSE 'y'::text\nEND
-- row: b | COALESCE(NULL::text, 'c'::text)
-- row: c | (ARRAY[7, 8, 9])[2]
-- rowcount: 3
-- end-expected
SELECT column_name, column_default FROM information_schema.columns
 WHERE table_name='zz_vf2_df' ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_ix (id int, n int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_ix_p ON zz_vf2_ix (id) WHERE n > 5;
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_ix_e ON zz_vf2_ix ((n + 1));
-- begin-expected
-- columns: pg_get_expr:text
-- row: (n > 5)
-- rowcount: 1
-- end-expected
SELECT pg_get_expr(indpred, indrelid) FROM pg_index WHERE indexrelid='zz_vf2_ix_p'::regclass;
-- begin-expected
-- columns: pg_get_expr:text
-- row: (n + 1)
-- rowcount: 1
-- end-expected
SELECT pg_get_expr(indexprs, indrelid) FROM pg_index WHERE indexrelid='zz_vf2_ix_e'::regclass;
