-- source: review-2026-08.md
-- finding: Root cause 8: a user-defined aggregate is looked up by bare name and evaluated positionally
-- area: PREPARE, EXECUTE, DO and CALL
-- title: Root cause 8: a user-defined aggregate is looked up by bare name and evaluated positionally
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf2_sf(int, int) RETURNS int LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT $1 * 10 + $2 $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE AGGREGATE zz_vf2_ag (int) (SFUNC = zz_vf2_sf, STYPE = int);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_vf2_ag() does not exist
-- end-expected-error
SELECT zz_vf2_ag() FROM (VALUES (1),(2)) t(v);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_vf2_ag() does not exist
-- end-expected-error
SELECT zz_vf2_ag(*) FROM (VALUES (1),(2)) t(v);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_vf2_ag(integer, integer) does not exist
-- end-expected-error
SELECT zz_vf2_ag(v, v) FROM (VALUES (1),(2)) t(v);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_vf2_ag(text) does not exist
-- end-expected-error
SELECT zz_vf2_ag('x'::text) FROM (VALUES (1)) t(v);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_ud_cat(text, text) does not exist
-- end-expected-error
CREATE AGGREGATE zz_ud_agg (text) (SFUNC = zz_ud_cat, STYPE = text, INITCOND = '');
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_ud_agg(text) does not exist
-- end-expected-error
SELECT zz_ud_agg(s ORDER BY s DESC) FROM (VALUES ('p'),('q'),('r')) t(s);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_ud_add(integer, integer) does not exist
-- end-expected-error
CREATE AGGREGATE zz_ud_agg (int) (SFUNC = zz_ud_add, STYPE = int, FINALFUNC = zz_ud_fin);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_ud_agg(integer) does not exist
-- end-expected-error
SELECT zz_ud_agg(v) FROM (VALUES (1)) t(v) WHERE v > 99;
-- begin-expected
-- ok: 0
-- end-expected
CREATE AGGREGATE zz_ud_os (float8 ORDER BY float8) (
  SFUNC = ordered_set_transition, STYPE = internal, FINALFUNC = percentile_cont_float8_final);
-- begin-expected
-- columns: aggkind:char | aggnumdirectargs:int2
-- row: o | 1
-- rowcount: 1
-- end-expected
SELECT aggkind, aggnumdirectargs FROM pg_aggregate WHERE aggfnoid::regproc::text = 'zz_ud_os';
-- begin-expected
-- columns: zz_ud_os:text
-- row: 2
-- rowcount: 1
-- end-expected
SELECT zz_ud_os(0.5) WITHIN GROUP (ORDER BY v)::text FROM (VALUES (1.0),(2.0),(3.0)) t(v);
