-- source: review-2026-08.md
-- finding: Root cause 21: DISTINCT and ORDER BY inside a user-aggregate call are applied without an analysis pass
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 21: DISTINCT and ORDER BY inside a user-aggregate call are applied without an analysis pass
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_sum(int,int) RETURNS int LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT $1*10+$2 $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE AGGREGATE zz_agg (int) (SFUNC = zz_sum, STYPE = int);
-- begin-expected
-- columns: zz_agg:int4
-- row: 2
-- rowcount: 1
-- end-expected
SELECT zz_agg(DISTINCT v) FROM (VALUES (2),(2)) t(v);
-- begin-expected
-- columns: zz_agg:int4
-- row: 23
-- rowcount: 1
-- end-expected
SELECT zz_agg(DISTINCT v) FROM (VALUES (2),(2),(3)) t(v);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_cat(text, text) does not exist
-- end-expected-error
CREATE AGGREGATE zz_cagg (text) (SFUNC = zz_cat, STYPE = text, INITCOND = '');
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_cagg(text) does not exist
-- end-expected-error
SELECT zz_cagg(DISTINCT s ORDER BY v) FROM (VALUES ('p',2),('q',1)) t(s,v);
