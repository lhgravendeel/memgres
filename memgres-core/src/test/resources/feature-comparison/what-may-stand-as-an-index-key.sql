-- ============================================================================
-- What may stand as an index key, and how the definition is written back
--
-- PostgreSQL's grammar has three forms of index key: a column name, a call,
-- and a parenthesised expression. A schema written on a key can therefore only
-- be a function's. What comes back from pg_indexes is the resolved expression,
-- not the text that was written: a qualifier the search path reaches is
-- dropped, a constant standing at a text parameter is a text constant, a
-- default operator class is left out, and a sort direction is written down
-- only where it is not already implied. Every value here was read off
-- PostgreSQL 18.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- A call may name its schema; a column may not
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_e (a int, b text, c text, "A b" text);
CREATE INDEX zw5x_e_01 ON zw5x_e (pg_catalog.lower(c));
CREATE INDEX zw5x_e_02 ON zw5x_e (pg_catalog.length(b));
CREATE INDEX zw5x_e_03 ON zw5x_e (pg_catalog.abs(a) DESC);
CREATE INDEX zw5x_e_04 ON zw5x_e (upper(b), pg_catalog.lower(c), a DESC);
CREATE INDEX zw5x_e_09 ON zw5x_e (lower("A b"));
CREATE INDEX zw5x_e_10 ON zw5x_e ((lower("A b")));

-- begin-expected
-- columns: indexname | indexdef
-- row: zw5x_e_01 | CREATE INDEX zw5x_e_01 ON public.zw5x_e USING btree (lower(c))
-- row: zw5x_e_02 | CREATE INDEX zw5x_e_02 ON public.zw5x_e USING btree (length(b))
-- row: zw5x_e_03 | CREATE INDEX zw5x_e_03 ON public.zw5x_e USING btree (abs(a) DESC)
-- row: zw5x_e_04 | CREATE INDEX zw5x_e_04 ON public.zw5x_e USING btree (upper(b), lower(c), a DESC)
-- row: zw5x_e_09 | CREATE INDEX zw5x_e_09 ON public.zw5x_e USING btree (lower("A b"))
-- row: zw5x_e_10 | CREATE INDEX zw5x_e_10 ON public.zw5x_e USING btree (lower("A b"))
-- end-expected
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'zw5x_e' ORDER BY indexname;

-- An unnamed index over a qualified call is named after the function.
CREATE INDEX ON zw5x_e (pg_catalog.lower(c));

-- begin-expected
-- columns: indexname
-- row: zw5x_e_lower_idx
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'zw5x_e' AND indexname LIKE '%lower%';

-- A qualified name with no argument list is reported at the token where that
-- list should have begun, because PostgreSQL has already read it as a
-- function name.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
CREATE INDEX zw5x_e_x1 ON zw5x_e (zw5x_e.a);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DESC"
-- end-expected-error
CREATE INDEX zw5x_e_x2 ON zw5x_e (zw5x_e.a DESC);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "::"
-- end-expected-error
CREATE INDEX zw5x_e_x3 ON zw5x_e (a::text);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "+"
-- end-expected-error
CREATE INDEX zw5x_e_x4 ON zw5x_e (a + 1);

-- The same expressions in parentheses are keys.
CREATE INDEX zw5x_e_11 ON zw5x_e ((a::text));
CREATE INDEX zw5x_e_12 ON zw5x_e ((a + 1));

-- A collation and an operator class may name their schema too.
CREATE INDEX zw5x_e_05 ON zw5x_e (b COLLATE pg_catalog."C");
CREATE INDEX zw5x_e_07 ON zw5x_e (pg_catalog.lower(c) COLLATE "C" text_pattern_ops DESC NULLS FIRST);
CREATE INDEX zw5x_e_08 ON zw5x_e ((a::text) COLLATE "C" text_pattern_ops ASC NULLS LAST);

-- begin-expected
-- columns: indexdef
-- row: CREATE INDEX zw5x_e_05 ON public.zw5x_e USING btree (b COLLATE "C")
-- end-expected
SELECT indexdef FROM pg_indexes WHERE indexname = 'zw5x_e_05';

-- begin-expected
-- columns: indexdef
-- row: CREATE INDEX zw5x_e_07 ON public.zw5x_e USING btree (lower(c) COLLATE "C" text_pattern_ops DESC)
-- end-expected
SELECT indexdef FROM pg_indexes WHERE indexname = 'zw5x_e_07';

-- begin-expected
-- columns: indexdef
-- row: CREATE INDEX zw5x_e_08 ON public.zw5x_e USING btree (((a)::text) COLLATE "C" text_pattern_ops)
-- end-expected
SELECT indexdef FROM pg_indexes WHERE indexname = 'zw5x_e_08';

-- The schema of an operator class is opened before the class is looked for.
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zw5x_nosuch" does not exist
-- end-expected-error
CREATE INDEX zw5x_e_x5 ON zw5x_e (a zw5x_nosuch.int4_ops);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: operator class "zw5x_nosuch_ops" does not exist for access method "btree"
-- end-expected-error
CREATE INDEX zw5x_e_x6 ON zw5x_e (a zw5x_nosuch_ops);

-- An operator class may be given parameters; none of the btree classes takes
-- any, and the complaint names the class without quoting it.
-- begin-expected-error
-- sqlstate: 22023
-- message-like: operator class text_ops has no options
-- end-expected-error
CREATE INDEX zw5x_e_x7 ON zw5x_e (b text_ops (x = 1));

-- A class that does not exist is still reported first.
-- begin-expected-error
-- sqlstate: 42704
-- message-like: operator class "zw5x_nosuch_ops" does not exist for access method "btree"
-- end-expected-error
CREATE INDEX zw5x_e_x8 ON zw5x_e (b zw5x_nosuch_ops (x = 1));

-- The relation is opened before any of this.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zw5x_nosuchtable" does not exist
-- end-expected-error
CREATE INDEX zw5x_e_x9 ON zw5x_nosuchtable (b text_ops (x = 1));

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zw5x_nosuchtable" does not exist
-- end-expected-error
CREATE INDEX zw5x_e_x10 ON zw5x_nosuchtable (pg_catalog.lower(b));

DROP TABLE zw5x_e;

-- ----------------------------------------------------------------------------
-- A word PostgreSQL reserves is not a key
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_ks (a int, b text);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "CASE"
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_ks (CASE WHEN a > 1 THEN b ELSE 'z' END);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "NOT"
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_ks (NOT a);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ARRAY"
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_ks (ARRAY[a]);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "NULL"
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_ks (NULL);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SELECT"
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_ks (SELECT a);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DEFAULT"
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_ks (DEFAULT);

-- left cannot be a column name, so PostgreSQL has already read it as a
-- function name and complains where its argument list should be.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_ks (left);

-- A word that only names a column never heads a call.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_ks (ROW(a));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "'1 day'"
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_ks (INTERVAL '1 day');

-- The calls SQL spells with a keyword are keys.
CREATE INDEX zw5x_kk_1 ON zw5x_ks (CAST(a AS text));
CREATE INDEX zw5x_kk_2 ON zw5x_ks (COALESCE(b, 'z'));
CREATE INDEX zw5x_kk_3 ON zw5x_ks (GREATEST(a, 1));

-- begin-expected
-- columns: indexname | indexdef
-- row: zw5x_kk_1 | CREATE INDEX zw5x_kk_1 ON public.zw5x_ks USING btree (((a)::text))
-- row: zw5x_kk_2 | CREATE INDEX zw5x_kk_2 ON public.zw5x_ks USING btree (COALESCE(b, 'z'::text))
-- row: zw5x_kk_3 | CREATE INDEX zw5x_kk_3 ON public.zw5x_ks USING btree (GREATEST(a, 1))
-- end-expected
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'zw5x_ks' ORDER BY indexname;

DROP TABLE zw5x_ks;

-- A value function reaches the rule that refuses it for being no more than
-- stable.
CREATE TABLE zw5x_vf (a int);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in index expression must be marked IMMUTABLE
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_vf (CURRENT_DATE);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in index expression must be marked IMMUTABLE
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_vf (CURRENT_TIMESTAMP);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in index expression must be marked IMMUTABLE
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_vf (CURRENT_USER);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in index expression must be marked IMMUTABLE
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_vf (LOCALTIMESTAMP);

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_indexes WHERE tablename = 'zw5x_vf';

DROP TABLE zw5x_vf;

-- ----------------------------------------------------------------------------
-- A definition writes down only the ordering its direction does not imply
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_f (a int);
CREATE INDEX zw5x_f_01 ON zw5x_f (a ASC);
CREATE INDEX zw5x_f_02 ON zw5x_f (a DESC);
CREATE INDEX zw5x_f_03 ON zw5x_f (a ASC NULLS FIRST);
CREATE INDEX zw5x_f_04 ON zw5x_f (a ASC NULLS LAST);
CREATE INDEX zw5x_f_05 ON zw5x_f (a DESC NULLS FIRST);
CREATE INDEX zw5x_f_06 ON zw5x_f (a DESC NULLS LAST);
CREATE INDEX zw5x_f_07 ON zw5x_f (a NULLS FIRST);
CREATE INDEX zw5x_f_08 ON zw5x_f (a NULLS LAST);

-- begin-expected
-- columns: indexname | indexdef
-- row: zw5x_f_01 | CREATE INDEX zw5x_f_01 ON public.zw5x_f USING btree (a)
-- row: zw5x_f_02 | CREATE INDEX zw5x_f_02 ON public.zw5x_f USING btree (a DESC)
-- row: zw5x_f_03 | CREATE INDEX zw5x_f_03 ON public.zw5x_f USING btree (a NULLS FIRST)
-- row: zw5x_f_04 | CREATE INDEX zw5x_f_04 ON public.zw5x_f USING btree (a)
-- row: zw5x_f_05 | CREATE INDEX zw5x_f_05 ON public.zw5x_f USING btree (a DESC)
-- row: zw5x_f_06 | CREATE INDEX zw5x_f_06 ON public.zw5x_f USING btree (a DESC NULLS LAST)
-- row: zw5x_f_07 | CREATE INDEX zw5x_f_07 ON public.zw5x_f USING btree (a NULLS FIRST)
-- row: zw5x_f_08 | CREATE INDEX zw5x_f_08 ON public.zw5x_f USING btree (a)
-- end-expected
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'zw5x_f' ORDER BY indexname;

-- An access method with no order to it refuses a written direction even where
-- that direction is the one it would have taken anyway.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: access method "hash" does not support ASC/DESC options
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_f USING hash (a ASC);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: access method "hash" does not support ASC/DESC options
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_f USING hash (a DESC);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: access method "hash" does not support NULLS FIRST/LAST options
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_f USING hash (a NULLS LAST);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: access method "hash" does not support NULLS FIRST/LAST options
-- end-expected-error
CREATE INDEX zw5x_bad ON zw5x_f USING hash (a NULLS FIRST);

CREATE INDEX zw5x_f_ok ON zw5x_f USING hash (a);

-- begin-expected
-- columns: indexdef
-- row: CREATE INDEX zw5x_f_ok ON public.zw5x_f USING hash (a)
-- end-expected
SELECT indexdef FROM pg_indexes WHERE indexname = 'zw5x_f_ok';

DROP TABLE zw5x_f;

-- ----------------------------------------------------------------------------
-- An operator class that is the type's default is left out
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_oc (a int, b text, n numeric);
CREATE INDEX zw5x_oc_1 ON zw5x_oc (b pg_catalog.text_ops);
CREATE INDEX zw5x_oc_2 ON zw5x_oc (a int4_ops DESC NULLS LAST);
CREATE INDEX zw5x_oc_3 ON zw5x_oc (n numeric_ops);
CREATE INDEX zw5x_oc_4 ON zw5x_oc (b text_pattern_ops);

-- begin-expected
-- columns: indexname | indexdef
-- row: zw5x_oc_1 | CREATE INDEX zw5x_oc_1 ON public.zw5x_oc USING btree (b)
-- row: zw5x_oc_2 | CREATE INDEX zw5x_oc_2 ON public.zw5x_oc USING btree (a DESC NULLS LAST)
-- row: zw5x_oc_3 | CREATE INDEX zw5x_oc_3 ON public.zw5x_oc USING btree (n)
-- row: zw5x_oc_4 | CREATE INDEX zw5x_oc_4 ON public.zw5x_oc USING btree (b text_pattern_ops)
-- end-expected
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'zw5x_oc' ORDER BY indexname;

DROP TABLE zw5x_oc;

-- ----------------------------------------------------------------------------
-- A collation belongs to the key it was written on
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_co (b text);
CREATE INDEX zw5x_co_1 ON zw5x_co (b COLLATE "C");
CREATE INDEX zw5x_co_2 ON zw5x_co ((b COLLATE "C"));
CREATE INDEX zw5x_co_3 ON zw5x_co ((b COLLATE "C") DESC);
CREATE INDEX zw5x_co_4 ON zw5x_co ((upper(b) COLLATE "C"));
CREATE INDEX zw5x_co_5 ON zw5x_co (((b || 'x') COLLATE "C"));
CREATE INDEX zw5x_co_6 ON zw5x_co ((upper(b COLLATE "C")));
CREATE INDEX zw5x_co_7 ON zw5x_co ((b COLLATE pg_catalog."C"));
CREATE INDEX zw5x_co_8 ON zw5x_co ((b COLLATE "C") text_pattern_ops);
CREATE INDEX zw5x_co_9 ON zw5x_co ((b COLLATE "C" || 'x'));

-- A collation nested inside stays where it was written, and is bracketed as a
-- node of its own.
-- begin-expected
-- columns: indexname | indexdef
-- row: zw5x_co_1 | CREATE INDEX zw5x_co_1 ON public.zw5x_co USING btree (b COLLATE "C")
-- row: zw5x_co_2 | CREATE INDEX zw5x_co_2 ON public.zw5x_co USING btree (b COLLATE "C")
-- row: zw5x_co_3 | CREATE INDEX zw5x_co_3 ON public.zw5x_co USING btree (b COLLATE "C" DESC)
-- row: zw5x_co_4 | CREATE INDEX zw5x_co_4 ON public.zw5x_co USING btree (upper(b) COLLATE "C")
-- row: zw5x_co_5 | CREATE INDEX zw5x_co_5 ON public.zw5x_co USING btree (((b || 'x'::text)) COLLATE "C")
-- row: zw5x_co_6 | CREATE INDEX zw5x_co_6 ON public.zw5x_co USING btree (upper((b COLLATE "C")))
-- row: zw5x_co_7 | CREATE INDEX zw5x_co_7 ON public.zw5x_co USING btree (b COLLATE "C")
-- row: zw5x_co_8 | CREATE INDEX zw5x_co_8 ON public.zw5x_co USING btree (b COLLATE "C" text_pattern_ops)
-- row: zw5x_co_9 | CREATE INDEX zw5x_co_9 ON public.zw5x_co USING btree ((((b COLLATE "C") || 'x'::text)))
-- end-expected
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'zw5x_co' ORDER BY indexname;

DROP TABLE zw5x_co;

-- ----------------------------------------------------------------------------
-- A constant standing at a text parameter is a text constant
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_fn (a int, b text, ts timestamp);
CREATE INDEX zw5x_fn_1 ON zw5x_fn (date_trunc('month', ts));
CREATE INDEX zw5x_fn_2 ON zw5x_fn (date_part('year', ts));
CREATE INDEX zw5x_fn_3 ON zw5x_fn (timezone('UTC', ts));
CREATE INDEX zw5x_fn_4 ON zw5x_fn (starts_with(b, 'a'));
CREATE INDEX zw5x_fn_5 ON zw5x_fn (split_part(b, ',', 1));
CREATE INDEX zw5x_fn_6 ON zw5x_fn (make_interval(days => a));

-- An argument written under a parameter's name keeps the name.
-- begin-expected
-- columns: indexname | indexdef
-- row: zw5x_fn_1 | CREATE INDEX zw5x_fn_1 ON public.zw5x_fn USING btree (date_trunc('month'::text, ts))
-- row: zw5x_fn_2 | CREATE INDEX zw5x_fn_2 ON public.zw5x_fn USING btree (date_part('year'::text, ts))
-- row: zw5x_fn_3 | CREATE INDEX zw5x_fn_3 ON public.zw5x_fn USING btree (timezone('UTC'::text, ts))
-- row: zw5x_fn_4 | CREATE INDEX zw5x_fn_4 ON public.zw5x_fn USING btree (starts_with(b, 'a'::text))
-- row: zw5x_fn_5 | CREATE INDEX zw5x_fn_5 ON public.zw5x_fn USING btree (split_part(b, ','::text, 1))
-- row: zw5x_fn_6 | CREATE INDEX zw5x_fn_6 ON public.zw5x_fn USING btree (make_interval(days => a))
-- end-expected
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'zw5x_fn' ORDER BY indexname;

DROP TABLE zw5x_fn;

-- ----------------------------------------------------------------------------
-- A json operator is resolved by what it takes, and AT TIME ZONE keeps its
-- own syntax
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_js (j jsonb, k json, ta text[]);
CREATE INDEX zw5x_js_1 ON zw5x_js ((j -> 'k'));
CREATE INDEX zw5x_js_2 ON zw5x_js ((j ->> 'k'));
CREATE INDEX zw5x_js_3 ON zw5x_js ((j -> 0));
CREATE INDEX zw5x_js_4 ON zw5x_js ((j #> '{a}'));
CREATE INDEX zw5x_js_5 ON zw5x_js ((j #>> '{a}'));
CREATE INDEX zw5x_js_6 ON zw5x_js ((j - 'a'));
CREATE INDEX zw5x_js_7 ON zw5x_js ((ta && ARRAY['x']));
CREATE INDEX zw5x_js_8 ON zw5x_js ((k ->> 'a'));

-- begin-expected
-- columns: indexname | indexdef
-- row: zw5x_js_1 | CREATE INDEX zw5x_js_1 ON public.zw5x_js USING btree (((j -> 'k'::text)))
-- row: zw5x_js_2 | CREATE INDEX zw5x_js_2 ON public.zw5x_js USING btree (((j ->> 'k'::text)))
-- row: zw5x_js_3 | CREATE INDEX zw5x_js_3 ON public.zw5x_js USING btree (((j -> 0)))
-- row: zw5x_js_4 | CREATE INDEX zw5x_js_4 ON public.zw5x_js USING btree (((j #> '{a}'::text[])))
-- row: zw5x_js_5 | CREATE INDEX zw5x_js_5 ON public.zw5x_js USING btree (((j #>> '{a}'::text[])))
-- row: zw5x_js_6 | CREATE INDEX zw5x_js_6 ON public.zw5x_js USING btree (((j - 'a'::text)))
-- row: zw5x_js_7 | CREATE INDEX zw5x_js_7 ON public.zw5x_js USING btree (((ta && ARRAY['x'::text])))
-- row: zw5x_js_8 | CREATE INDEX zw5x_js_8 ON public.zw5x_js USING btree (((k ->> 'a'::text)))
-- end-expected
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'zw5x_js' ORDER BY indexname;

DROP TABLE zw5x_js;

CREATE TABLE zw5x_tz (b text, ts timestamp, tz timestamptz, iv interval);
CREATE INDEX zw5x_tz_1 ON zw5x_tz ((ts AT TIME ZONE 'UTC'));
CREATE INDEX zw5x_tz_2 ON zw5x_tz ((tz AT TIME ZONE 'UTC'));
CREATE INDEX zw5x_tz_3 ON zw5x_tz ((ts AT TIME ZONE b));
CREATE INDEX zw5x_tz_4 ON zw5x_tz ((ts AT TIME ZONE iv));

-- A zone written as a bare string resolves to the call over text; a column
-- keeps the type it has.
-- begin-expected
-- columns: indexname | indexdef
-- row: zw5x_tz_1 | CREATE INDEX zw5x_tz_1 ON public.zw5x_tz USING btree ((ts AT TIME ZONE 'UTC'::text))
-- row: zw5x_tz_2 | CREATE INDEX zw5x_tz_2 ON public.zw5x_tz USING btree ((tz AT TIME ZONE 'UTC'::text))
-- row: zw5x_tz_3 | CREATE INDEX zw5x_tz_3 ON public.zw5x_tz USING btree ((ts AT TIME ZONE b))
-- row: zw5x_tz_4 | CREATE INDEX zw5x_tz_4 ON public.zw5x_tz USING btree ((ts AT TIME ZONE iv))
-- end-expected
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'zw5x_tz' ORDER BY indexname;

DROP TABLE zw5x_tz;

-- ----------------------------------------------------------------------------
-- XMLSERIALIZE keeps its mode, its type and its indentation
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_xs (x xml);
CREATE INDEX zw5x_xs_1 ON zw5x_xs (XMLSERIALIZE(CONTENT x AS text));
CREATE INDEX zw5x_xs_2 ON zw5x_xs (XMLSERIALIZE(DOCUMENT x AS text));
CREATE INDEX zw5x_xs_3 ON zw5x_xs (XMLSERIALIZE(CONTENT x AS text INDENT));
CREATE INDEX zw5x_xs_4 ON zw5x_xs (XMLSERIALIZE(CONTENT x AS text NO INDENT));
CREATE INDEX zw5x_xs_5 ON zw5x_xs (XMLSERIALIZE(CONTENT x AS varchar));

-- A type that is not text is a coercion sitting on the call, and that takes
-- parentheses of its own.
-- begin-expected
-- columns: indexname | indexdef
-- row: zw5x_xs_1 | CREATE INDEX zw5x_xs_1 ON public.zw5x_xs USING btree (XMLSERIALIZE(CONTENT x AS text NO INDENT))
-- row: zw5x_xs_2 | CREATE INDEX zw5x_xs_2 ON public.zw5x_xs USING btree (XMLSERIALIZE(DOCUMENT x AS text NO INDENT))
-- row: zw5x_xs_3 | CREATE INDEX zw5x_xs_3 ON public.zw5x_xs USING btree (XMLSERIALIZE(CONTENT x AS text INDENT))
-- row: zw5x_xs_4 | CREATE INDEX zw5x_xs_4 ON public.zw5x_xs USING btree (XMLSERIALIZE(CONTENT x AS text NO INDENT))
-- row: zw5x_xs_5 | CREATE INDEX zw5x_xs_5 ON public.zw5x_xs USING btree ((XMLSERIALIZE(CONTENT x AS character varying NO INDENT)))
-- end-expected
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'zw5x_xs' ORDER BY indexname;

DROP TABLE zw5x_xs;

-- ----------------------------------------------------------------------------
-- A name the grammar knows is written back in quotes
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_kw ("time" int, "substring" text, "value" int, "int" int, "row" int, t text);
CREATE INDEX zw5x_kw_1 ON zw5x_kw (("time" + 1));
CREATE INDEX zw5x_kw_2 ON zw5x_kw (lower("substring"));
CREATE INDEX zw5x_kw_3 ON zw5x_kw (("value" + 1));
CREATE INDEX zw5x_kw_4 ON zw5x_kw (("int" + 1));
CREATE INDEX zw5x_kw_5 ON zw5x_kw (("row" + 1));
CREATE INDEX zw5x_kw_6 ON zw5x_kw (substring(t, 1, 2));
CREATE INDEX zw5x_kw_7 ON zw5x_kw (left(t, 1));
CREATE INDEX zw5x_kw_8 ON zw5x_kw (right(t, 1));

-- value is unreserved, so a relation column of that name stays bare.
-- begin-expected
-- columns: indexname | indexdef
-- row: zw5x_kw_1 | CREATE INDEX zw5x_kw_1 ON public.zw5x_kw USING btree ((("time" + 1)))
-- row: zw5x_kw_2 | CREATE INDEX zw5x_kw_2 ON public.zw5x_kw USING btree (lower("substring"))
-- row: zw5x_kw_3 | CREATE INDEX zw5x_kw_3 ON public.zw5x_kw USING btree (((value + 1)))
-- row: zw5x_kw_4 | CREATE INDEX zw5x_kw_4 ON public.zw5x_kw USING btree ((("int" + 1)))
-- row: zw5x_kw_5 | CREATE INDEX zw5x_kw_5 ON public.zw5x_kw USING btree ((("row" + 1)))
-- row: zw5x_kw_6 | CREATE INDEX zw5x_kw_6 ON public.zw5x_kw USING btree ("substring"(t, 1, 2))
-- row: zw5x_kw_7 | CREATE INDEX zw5x_kw_7 ON public.zw5x_kw USING btree ("left"(t, 1))
-- row: zw5x_kw_8 | CREATE INDEX zw5x_kw_8 ON public.zw5x_kw USING btree ("right"(t, 1))
-- end-expected
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'zw5x_kw' ORDER BY indexname;

DROP TABLE zw5x_kw;

-- The same rule reaches a constraint, where the domain placeholder is the
-- keyword and a column of that name is not.
CREATE TABLE zw5x_v ("value" int, "time" int);
ALTER TABLE zw5x_v ADD CONSTRAINT zw5x_v_c CHECK ("value" > 0 AND "time" > 0);

-- begin-expected
-- columns: def
-- row: CHECK (((value > 0) AND ("time" > 0)))
-- end-expected
SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conname = 'zw5x_v_c';

DROP TABLE zw5x_v;

CREATE DOMAIN zw5x_dom AS int CHECK (VALUE > 0);

-- begin-expected
-- columns: def
-- row: CHECK ((VALUE > 0))
-- end-expected
SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conname LIKE 'zw5x!_dom%' ESCAPE '!';

DROP DOMAIN zw5x_dom;

CREATE TABLE zw5x_tzc (ts timestamp, j jsonb);
ALTER TABLE zw5x_tzc ADD CONSTRAINT zw5x_tzc_c CHECK ((ts AT TIME ZONE 'UTC') IS NOT NULL);
ALTER TABLE zw5x_tzc ADD CONSTRAINT zw5x_tzc_j CHECK (j @> '{"a":1}');

-- begin-expected
-- columns: defs
-- row: zw5x_tzc_c=CHECK (((ts AT TIME ZONE 'UTC'::text) IS NOT NULL)),zw5x_tzc_j=CHECK ((j @> '{"a": 1}'::jsonb))
-- end-expected
SELECT string_agg(conname || '=' || pg_get_constraintdef(oid), ',' ORDER BY conname) AS defs FROM pg_constraint WHERE conrelid = 'zw5x_tzc'::regclass;

CREATE VIEW zw5x_tzc_v AS SELECT ts AT TIME ZONE 'UTC' AS z FROM zw5x_tzc;

-- begin-expected
-- columns: def
-- row:  SELECT (ts AT TIME ZONE 'UTC'::text) AS z    FROM zw5x_tzc;
-- end-expected
SELECT replace(pg_get_viewdef('zw5x_tzc_v'::regclass, true), chr(10), ' ') AS def;

DROP VIEW zw5x_tzc_v;
DROP TABLE zw5x_tzc;
