-- source: review-2026-08.md
-- finding: Root cause 20: SqlUnparser's default arm prints the Java enum constant
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 20: SqlUnparser's default arm prints the Java enum constant
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
CREATE VIEW zz_v2 AS SELECT a # b AS c1, a << 1 AS c2, a >> 1 AS c3, a & b AS c4 FROM zz_t;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_v2" does not exist
-- end-expected-error
SELECT pg_get_viewdef('zz_v2'::regclass);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
CREATE VIEW zz_v3 AS SELECT a FROM zz_t WHERE a IS DISTINCT FROM b;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
CREATE VIEW zz_v5 AS SELECT a FROM zz_t WHERE ts @@ 'cat'::tsquery;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
CREATE VIEW zz_v6 AS SELECT a FROM zz_t WHERE j ? 'k';
