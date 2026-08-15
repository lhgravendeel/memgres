-- CURRENT_DATE and its kin are nodes of their own rather than calls, so a stored definition
-- writes them the way the grammar spells them: in capitals and without parentheses. An ordinary
-- call of no arguments keeps its parentheses. Each of them answers a type of its own, and that
-- type is what an untyped constant beside one is stored as -- and what settles the ELSE a CASE
-- never had. The same holds in a column default, in a CHECK constraint and in a rule, which are
-- printed by a second deparser.
-- Every value below was read off PostgreSQL 18. Newlines are written as the two characters
-- backslash-n by the replace() around each call, so one definition fits on one annotated row.

-- setup
CREATE TABLE vfk_t (id int, dt date, tm time, ts timestamp, txt text);

-- stmt 1: the keyword spelling, in both forms of the printed definition
CREATE VIEW vfk_kw AS SELECT current_date AS a, current_time AS b, current_timestamp AS c, localtime AS d, localtimestamp AS e, current_user AS f, session_user AS g, current_role AS h, current_catalog AS i, current_schema AS j;
-- begin-expected
-- columns: d
-- row:  SELECT CURRENT_DATE AS a,\n    CURRENT_TIME AS b,\n    CURRENT_TIMESTAMP AS c,\n    LOCALTIME AS d,\n    LOCALTIMESTAMP AS e,\n    CURRENT_USER AS f,\n    SESSION_USER AS g,\n    CURRENT_ROLE AS h,\n    CURRENT_CATALOG AS i,\n    CURRENT_SCHEMA AS j;
-- end-expected
SELECT replace(pg_get_viewdef('vfk_kw'::regclass, true), chr(10), '\n') AS d;
-- begin-expected
-- columns: d
-- row:  SELECT CURRENT_DATE AS a,\n    CURRENT_TIME AS b,\n    CURRENT_TIMESTAMP AS c,\n    LOCALTIME AS d,\n    LOCALTIMESTAMP AS e,\n    CURRENT_USER AS f,\n    SESSION_USER AS g,\n    CURRENT_ROLE AS h,\n    CURRENT_CATALOG AS i,\n    CURRENT_SCHEMA AS j;
-- end-expected
SELECT replace(pg_get_viewdef('vfk_kw'::regclass, false), chr(10), '\n') AS d;

-- stmt 2: each names a column after itself, and the name is a keyword, so it is quoted
CREATE VIEW vfk_kw2 AS SELECT current_date, current_user FROM vfk_t;
-- begin-expected
-- columns: d
-- row:  SELECT CURRENT_DATE AS "current_date",\n    CURRENT_USER AS "current_user"\n   FROM vfk_t;
-- end-expected
SELECT replace(pg_get_viewdef('vfk_kw2'::regclass, true), chr(10), '\n') AS d;
-- begin-expected
-- columns: d
-- row: current_date,current_user
-- end-expected
SELECT string_agg(attname, ',' ORDER BY attnum) AS d FROM pg_attribute WHERE attrelid = 'vfk_kw2'::regclass AND attnum > 0;

-- stmt 3: an ordinary call of no arguments keeps its parentheses
CREATE VIEW vfk_v1 AS SELECT id FROM vfk_t WHERE dt > current_date AND txt = current_user AND tm > localtime AND ts > localtimestamp AND ts > now();
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM vfk_t\n  WHERE dt > CURRENT_DATE AND txt = CURRENT_USER AND tm > LOCALTIME AND ts > LOCALTIMESTAMP AND ts > now();
-- end-expected
SELECT replace(pg_get_viewdef('vfk_v1'::regclass, true), chr(10), '\n') AS d;

-- stmt 4: CURRENT_USER and CURRENT_SCHEMA answer a name, not text
CREATE VIEW vfk_v2 AS SELECT id FROM vfk_t WHERE current_date > '2020-01-02' AND current_user = 'bob' AND current_schema = 'public';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM vfk_t\n  WHERE CURRENT_DATE > '2020-01-02'::date AND CURRENT_USER = 'bob'::name AND CURRENT_SCHEMA = 'public'::name;
-- end-expected
SELECT replace(pg_get_viewdef('vfk_v2'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW vfk_v3 AS SELECT id FROM vfk_t WHERE localtime > '3:4';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM vfk_t\n  WHERE LOCALTIME > '03:04:00'::time without time zone;
-- end-expected
SELECT replace(pg_get_viewdef('vfk_v3'::regclass, true), chr(10), '\n') AS d;

-- stmt 5: the same type settles the ELSE the CASE never had
CREATE VIEW vfk_v4 AS SELECT CASE WHEN id = 1 THEN current_date END AS a, CASE WHEN id = 2 THEN current_user END AS b FROM vfk_t;
-- begin-expected
-- columns: d
-- row:  SELECT\n        CASE\n            WHEN id = 1 THEN CURRENT_DATE\n            ELSE NULL::date\n        END AS a,\n        CASE\n            WHEN id = 2 THEN CURRENT_USER\n            ELSE NULL::name\n        END AS b\n   FROM vfk_t;
-- end-expected
SELECT replace(pg_get_viewdef('vfk_v4'::regclass, true), chr(10), '\n') AS d;

-- stmt 6: a default, a CHECK constraint and a rule are printed the same way
CREATE TABLE vfk_d (id int, dt date DEFAULT current_date, who text DEFAULT current_user, ts timestamptz DEFAULT current_timestamp, sch text DEFAULT current_schema, CHECK (dt <= current_date));
-- begin-expected
-- columns: d
-- row: id=none dt=CURRENT_DATE who=CURRENT_USER ts=CURRENT_TIMESTAMP sch=CURRENT_SCHEMA
-- end-expected
SELECT string_agg(column_name || '=' || coalesce(column_default, 'none'), ' ' ORDER BY ordinal_position) AS d FROM information_schema.columns WHERE table_name = 'vfk_d';
-- begin-expected
-- columns: d
-- row: CURRENT_DATE CURRENT_USER CURRENT_TIMESTAMP CURRENT_SCHEMA
-- end-expected
SELECT string_agg(pg_get_expr(x.adbin, x.adrelid), ' ' ORDER BY x.adnum) AS d FROM pg_attrdef x WHERE x.adrelid = 'vfk_d'::regclass;
-- begin-expected
-- columns: d
-- row: CHECK ((dt <= CURRENT_DATE))
-- end-expected
SELECT pg_get_constraintdef(oid) AS d FROM pg_constraint WHERE conrelid = 'vfk_d'::regclass AND contype = 'c';
CREATE TABLE vfk_rx (a int);
CREATE TABLE vfk_ry (a int, d date);
CREATE RULE vfk_rr AS ON INSERT TO vfk_rx DO ALSO INSERT INTO vfk_ry VALUES (NEW.a, current_date);
-- begin-expected
-- columns: d
-- row: CREATE RULE vfk_rr AS\n    ON INSERT TO public.vfk_rx DO  INSERT INTO vfk_ry (a, d)\n  VALUES (new.a, CURRENT_DATE);
-- end-expected
SELECT replace(definition, chr(10), '\n') AS d FROM pg_rules WHERE rulename = 'vfk_rr';

-- stmt 7: an index predicate goes through that deparser too
CREATE INDEX vfk_ix ON vfk_ry (a) WHERE d < '2020-01-02';
-- begin-expected
-- columns: d
-- row: CREATE INDEX vfk_ix ON public.vfk_ry USING btree (a) WHERE (d < '2020-01-02'::date)
-- end-expected
SELECT pg_get_indexdef('vfk_ix'::regclass) AS d;

-- cleanup
DROP VIEW vfk_kw, vfk_kw2, vfk_v1, vfk_v2, vfk_v3, vfk_v4;
DROP TABLE vfk_t;
DROP TABLE vfk_d;
DROP TABLE vfk_rx CASCADE;
DROP TABLE vfk_ry;
