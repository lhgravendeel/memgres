-- source: investigation-2026-08.md
-- finding: 200
-- title: A PREPARE's declared parameter list is an I/O cast applied at EXECUTE, not a type contract fixed at PREPARE. validatePreparedBody is a hand-written spot-check o
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf2_a (varchar(3)) AS SELECT $1 AS v;
-- begin-expected
-- columns: v:varchar
-- row: abcdef
-- rowcount: 1
-- end-expected
EXECUTE zz_vf2_a('abcdef');
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf2_b (numeric(4,1)) AS SELECT $1 AS v;
-- begin-expected
-- columns: v:numeric
-- row: 1.26
-- rowcount: 1
-- end-expected
EXECUTE zz_vf2_b(1.26);
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf2_pc (int, text) AS SELECT $1 AS a, $2 AS b;
-- begin-expected
-- columns: parameter_types:text
-- row: {integer,text}
-- rowcount: 1
-- end-expected
SELECT parameter_types::text FROM pg_prepared_statements WHERE name='zz_vf2_pc';
-- begin-expected
-- columns: pg_typeof:text
-- row: regtype[]
-- rowcount: 1
-- end-expected
SELECT pg_typeof(parameter_types)::text FROM pg_prepared_statements WHERE name='zz_vf2_pc';
-- begin-expected
-- columns: pg_typeof:text
-- row: regtype[]
-- rowcount: 1
-- end-expected
SELECT pg_typeof(result_types)::text FROM pg_prepared_statements WHERE name='zz_vf2_pc';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf2_ctp AS (a int, b text);
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf2_pcomp (zz_vf2_ctp) AS SELECT ($1).a AS v;
-- begin-expected
-- columns: v:int4
-- row: 3
-- rowcount: 1
-- end-expected
EXECUTE zz_vf2_pcomp(ROW(3,'z'));
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_et" does not exist
-- end-expected-error
PREPARE zz_vf2_ep (int) AS SELECT s FROM zz_vf2_et WHERE id = $1;
-- begin-expected-error
-- sqlstate: 26000
-- message-like: prepared statement "zz_vf2_ep" does not exist
-- end-expected-error
EXECUTE zz_vf2_ep('1'::text);
-- begin-expected-error
-- sqlstate: 26000
-- message-like: prepared statement "zz_vf2_ep" does not exist
-- end-expected-error
EXECUTE zz_vf2_ep(true);
-- begin-expected-error
-- sqlstate: 26000
-- message-like: prepared statement "zz_vf2_ep" does not exist
-- end-expected-error
EXECUTE zz_vf2_ep((SELECT 1));
-- begin-expected-error
-- sqlstate: 26000
-- message-like: prepared statement "zz_vf2_ep" does not exist
-- end-expected-error
EXECUTE zz_vf2_ep(count(*));
-- begin-expected-error
-- sqlstate: 26000
-- message-like: prepared statement "zz_vf2_ep" does not exist
-- end-expected-error
EXECUTE zz_vf2_ep(row_number() OVER ());
-- begin-expected-error
-- sqlstate: 26000
-- message-like: prepared statement "zz_vf2_ep" does not exist
-- end-expected-error
EXECUTE zz_vf2_ep($1);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
PREPARE zz_vf2_q1 () AS SELECT 1 AS v;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "nosuchtype_zz" does not exist
-- end-expected-error
PREPARE zz_vf2_q2 (nosuchtype_zz) AS SELECT $1 AS v;
-- begin-expected-error
-- sqlstate: 42P18
-- message-like: could not determine data type of parameter $1
-- end-expected-error
PREPARE zz_vf2_q3 AS SELECT $2 AS v;
-- begin-expected-error
-- sqlstate: 42P18
-- message-like: could not determine data type of parameter $2
-- end-expected-error
PREPARE zz_vf2_q4 AS SELECT $1 AS a, $3 AS c;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_vt" does not exist
-- end-expected-error
PREPARE zz_vf2_q5 (text) AS SELECT * FROM zz_vf2_vt WHERE id = $1;
-- begin-expected-error
-- sqlstate: 26000
-- message-like: prepared statement "zz_vf2_q5" does not exist
-- end-expected-error
EXECUTE zz_vf2_q5('1');
