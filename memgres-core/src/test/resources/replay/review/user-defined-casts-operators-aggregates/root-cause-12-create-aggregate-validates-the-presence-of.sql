-- source: review-2026-08.md
-- finding: Root cause 12: CREATE AGGREGATE validates the presence of SFUNC and STYPE and the arity of two functions, and nothing else
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 12: CREATE AGGREGATE validates the presence of SFUNC and STYPE and the arity of two functions, and nothing else
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_add(int,int) RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT $1+$2 $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_finex(int,int) RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT 'x'||$1::text $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE AGGREGATE zz_agg (int) (SFUNC=zz_add, STYPE=int, INITCOND='0', FINALFUNC=zz_finex, FINALFUNC_EXTRA);
-- begin-expected
-- ok: 0
-- end-expected
CREATE AGGREGATE zz_hyp (VARIADIC "any" ORDER BY VARIADIC "any") (
  SFUNC = ordered_set_transition_multi, STYPE = internal,
  FINALFUNC = rank_final, FINALFUNC_EXTRA, HYPOTHETICAL);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_sum(integer, integer) does not exist
-- end-expected-error
CREATE AGGREGATE zz_a1 (int) (SFUNC=zz_sum, STYPE=int, COMBINEFUNC=zz_cat);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: parameter "finalfunc_modify" must be READ_ONLY, SHAREABLE, or READ_WRITE
-- end-expected-error
CREATE AGGREGATE zz_a2 (int) (SFUNC=zz_sum, STYPE=int, FINALFUNC_MODIFY=NOSUCH);
-- begin-expected-error
-- sqlstate: 42P13
-- message-like: aggregate minvfunc must not be specified without mstype
-- end-expected-error
CREATE AGGREGATE zz_a3 (int) (SFUNC=zz_sum, STYPE=int, MINVFUNC=zz_sum);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "=###?#"
-- end-expected-error
CREATE AGGREGATE zz_a4 (int) (SFUNC=zz_sum, STYPE=int, SORTOP=###?#);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_notype" does not exist
-- end-expected-error
CREATE AGGREGATE zz_a5 (int) (SFUNC = zz_sum, STYPE = zz_notype);
