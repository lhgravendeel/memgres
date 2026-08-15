-- How PostgreSQL prints a definition it stored: a view body, a column default and an
-- index definition are all deparsed from the analysed tree, not echoed as they were typed.
--   * pg_get_viewdef lays every clause out on a line of its own, breaks before each JOIN,
--     writes a set operator at column 0, spells CASE over four lines, keeps the WITH clause,
--     and qualifies a column exactly when the query is nested or reads more than one FROM item.
--   * a literal carries the type parse analysis read it as; a cast a constant already reads
--     as is folded away in a default, and one it does not is kept with its operand bracketed.
--   * pg_get_indexdef says ON ONLY when the indexed relation is partitioned, and
--     pg_get_expr on pg_index.indpred keeps the outer parentheses of the predicate.
-- Every value below was read off PostgreSQL 18. Newlines are written as the two characters
-- backslash-n by the replace() around each call, so one definition fits on one annotated row.

-- setup
CREATE TABLE pvd_r1 (id int, name text, amt numeric);
CREATE TABLE pvd_r2 (id int, tag text);
CREATE TABLE pvd_r3 (id int, note text);

-- stmt 1: the WITH clause is part of the definition
CREATE VIEW pvd_cte AS WITH q AS (SELECT id FROM pvd_r1) SELECT id FROM q;

-- begin-expected
-- columns: d
-- row: WITH q AS (\n         SELECT pvd_r1.id\n           FROM pvd_r1\n        )\n SELECT id\n   FROM q;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_cte'::regclass, true), chr(10), '\n') AS d;

-- begin-expected
-- columns: d
-- row: WITH q AS (\n         SELECT pvd_r1.id\n           FROM pvd_r1\n        )\n SELECT id\n   FROM q;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_cte'::regclass, false), chr(10), '\n') AS d;

-- begin-expected
-- columns: d
-- row: WITH q AS (\n         SELECT pvd_r1.id\n           FROM pvd_r1\n        )\n SELECT id\n   FROM q;
-- end-expected
SELECT replace(definition, chr(10), '\n') AS d FROM pg_views WHERE viewname = 'pvd_cte';

-- begin-expected
-- columns: d
-- row: WITH q AS (\n         SELECT pvd_r1.id\n           FROM pvd_r1\n        )\n SELECT id\n   FROM q;
-- end-expected
SELECT replace(view_definition, chr(10), '\n') AS d FROM information_schema.views WHERE table_name = 'pvd_cte';

-- stmt 2: a window call is deparsed as SQL
CREATE VIEW pvd_win AS SELECT id, row_number() OVER (PARTITION BY name ORDER BY id) AS rn FROM pvd_r1;

-- begin-expected
-- columns: d
-- row: SELECT id,\n    row_number() OVER (PARTITION BY name ORDER BY id) AS rn\n   FROM pvd_r1;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_win'::regclass, true), chr(10), '\n') AS d;

CREATE VIEW pvd_win2 AS SELECT sum(amt) OVER (PARTITION BY name ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM pvd_r1;

-- begin-expected
-- columns: d
-- row: SELECT sum(amt) OVER (PARTITION BY name ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s\n   FROM pvd_r1;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_win2'::regclass, true), chr(10), '\n') AS d;

CREATE VIEW pvd_win3 AS SELECT count(*) OVER () AS c FROM pvd_r1;

-- begin-expected
-- columns: d
-- row: SELECT count(*) OVER () AS c\n   FROM pvd_r1;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_win3'::regclass, true), chr(10), '\n') AS d;

-- DESC already means NULLS FIRST, so PostgreSQL does not write the clause back
CREATE VIEW pvd_win4 AS SELECT rank() OVER w AS r FROM pvd_r1 WINDOW w AS (ORDER BY id DESC NULLS FIRST);

-- begin-expected
-- columns: d
-- row: SELECT rank() OVER w AS r\n   FROM pvd_r1\n  WINDOW w AS (ORDER BY id DESC);
-- end-expected
SELECT replace(pg_get_viewdef('pvd_win4'::regclass, true), chr(10), '\n') AS d;

-- stmt 3: a column is bare only when the query reads one FROM item and is not nested
CREATE VIEW pvd_sub AS SELECT s.id FROM (SELECT id FROM pvd_r1) s;

-- begin-expected
-- columns: d
-- row: SELECT id\n   FROM ( SELECT pvd_r1.id\n           FROM pvd_r1) s;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_sub'::regclass, true), chr(10), '\n') AS d;

CREATE VIEW pvd_sub2 AS SELECT x.id FROM (SELECT y.id FROM (SELECT id FROM pvd_r1) y) x;

-- begin-expected
-- columns: d
-- row: SELECT id\n   FROM ( SELECT y.id\n           FROM ( SELECT pvd_r1.id\n                   FROM pvd_r1) y) x;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_sub2'::regclass, true), chr(10), '\n') AS d;

CREATE VIEW pvd_using AS SELECT id FROM pvd_r1 JOIN pvd_r2 USING (id);

-- begin-expected
-- columns: d
-- row: SELECT pvd_r1.id\n   FROM pvd_r1\n     JOIN pvd_r2 USING (id);
-- end-expected
SELECT replace(pg_get_viewdef('pvd_using'::regclass, true), chr(10), '\n') AS d;

-- stmt 4: the FROM clause breaks before every JOIN, and the tree is bracketed unpretty
CREATE VIEW pvd_join AS SELECT a.id, b.tag FROM pvd_r1 a LEFT JOIN pvd_r2 b ON a.id = b.id;

-- begin-expected
-- columns: d
-- row: SELECT a.id,\n    b.tag\n   FROM pvd_r1 a\n     LEFT JOIN pvd_r2 b ON a.id = b.id;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_join'::regclass, true), chr(10), '\n') AS d;

-- begin-expected
-- columns: d
-- row: SELECT a.id,\n    b.tag\n   FROM (pvd_r1 a\n     LEFT JOIN pvd_r2 b ON ((a.id = b.id)));
-- end-expected
SELECT replace(pg_get_viewdef('pvd_join'::regclass, false), chr(10), '\n') AS d;

CREATE VIEW pvd_join3 AS SELECT a.id FROM pvd_r1 a JOIN pvd_r2 b ON a.id = b.id JOIN pvd_r3 c ON b.id = c.id;

-- begin-expected
-- columns: d
-- row: SELECT a.id\n   FROM pvd_r1 a\n     JOIN pvd_r2 b ON a.id = b.id\n     JOIN pvd_r3 c ON b.id = c.id;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_join3'::regclass, true), chr(10), '\n') AS d;

-- begin-expected
-- columns: d
-- row: SELECT a.id\n   FROM ((pvd_r1 a\n     JOIN pvd_r2 b ON ((a.id = b.id)))\n     JOIN pvd_r3 c ON ((b.id = c.id)));
-- end-expected
SELECT replace(pg_get_viewdef('pvd_join3'::regclass, false), chr(10), '\n') AS d;

CREATE VIEW pvd_cross AS SELECT a.id, b.tag FROM pvd_r1 a CROSS JOIN pvd_r2 b;

-- begin-expected
-- columns: d
-- row: SELECT a.id,\n    b.tag\n   FROM pvd_r1 a\n     CROSS JOIN pvd_r2 b;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_cross'::regclass, true), chr(10), '\n') AS d;

-- stmt 5: a set operator stands on a line of its own at column 0, and each arm qualifies
CREATE VIEW pvd_un AS SELECT id FROM pvd_r1 UNION ALL SELECT id FROM pvd_r2;

-- begin-expected
-- columns: d
-- row: SELECT pvd_r1.id\n   FROM pvd_r1\nUNION ALL\n SELECT pvd_r2.id\n   FROM pvd_r2;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_un'::regclass, true), chr(10), '\n') AS d;

CREATE VIEW pvd_un3 AS SELECT id FROM pvd_r1 UNION SELECT id FROM pvd_r2 UNION SELECT id FROM pvd_r3;

-- begin-expected
-- columns: d
-- row: SELECT pvd_r1.id\n   FROM pvd_r1\nUNION\n SELECT pvd_r2.id\n   FROM pvd_r2\nUNION\n SELECT pvd_r3.id\n   FROM pvd_r3;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_un3'::regclass, true), chr(10), '\n') AS d;

-- stmt 6: CASE is laid out over lines of its own, and gets the ELSE it means
CREATE VIEW pvd_case AS SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END AS k FROM pvd_r1;

-- begin-expected
-- columns: d
-- row: SELECT\n        CASE\n            WHEN id > 1 THEN 'big'::text\n            ELSE 'small'::text\n        END AS k\n   FROM pvd_r1;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_case'::regclass, true), chr(10), '\n') AS d;

-- begin-expected
-- columns: d
-- row: SELECT\n        CASE\n            WHEN (id > 1) THEN 'big'::text\n            ELSE 'small'::text\n        END AS k\n   FROM pvd_r1;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_case'::regclass, false), chr(10), '\n') AS d;

CREATE VIEW pvd_case2 AS SELECT CASE id WHEN 1 THEN 'a' WHEN 2 THEN 'b' END AS k FROM pvd_r1;

-- begin-expected
-- columns: d
-- row: SELECT\n        CASE id\n            WHEN 1 THEN 'a'::text\n            WHEN 2 THEN 'b'::text\n            ELSE NULL::text\n        END AS k\n   FROM pvd_r1;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_case2'::regclass, true), chr(10), '\n') AS d;

CREATE VIEW pvd_case3 AS SELECT id, CASE WHEN id > 1 THEN 1 ELSE 2 END AS k, name FROM pvd_r1;

-- begin-expected
-- columns: d
-- row: SELECT id,\n        CASE\n            WHEN id > 1 THEN 1\n            ELSE 2\n        END AS k,\n    name\n   FROM pvd_r1;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_case3'::regclass, true), chr(10), '\n') AS d;

-- stmt 7: a sub-select is laid out relative to its opening parenthesis
CREATE VIEW pvd_scal AS SELECT id, (SELECT count(*) FROM pvd_r2) AS n FROM pvd_r1;

-- begin-expected
-- columns: d
-- row: SELECT id,\n    ( SELECT count(*) AS count\n           FROM pvd_r2) AS n\n   FROM pvd_r1;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_scal'::regclass, true), chr(10), '\n') AS d;

CREATE VIEW pvd_ex AS SELECT id FROM pvd_r1 WHERE EXISTS (SELECT 1 FROM pvd_r2 WHERE pvd_r2.id = pvd_r1.id);

-- begin-expected
-- columns: d
-- row: SELECT id\n   FROM pvd_r1\n  WHERE (EXISTS ( SELECT 1\n           FROM pvd_r2\n          WHERE pvd_r2.id = pvd_r1.id));
-- end-expected
SELECT replace(pg_get_viewdef('pvd_ex'::regclass, true), chr(10), '\n') AS d;

CREATE VIEW pvd_insub AS SELECT id FROM pvd_r1 WHERE id IN (SELECT id FROM pvd_r2);

-- begin-expected
-- columns: d
-- row: SELECT id\n   FROM pvd_r1\n  WHERE (id IN ( SELECT pvd_r2.id\n           FROM pvd_r2));
-- end-expected
SELECT replace(pg_get_viewdef('pvd_insub'::regclass, true), chr(10), '\n') AS d;

CREATE VIEW pvd_notin AS SELECT id FROM pvd_r1 WHERE id NOT IN (SELECT id FROM pvd_r2);

-- begin-expected
-- columns: d
-- row: SELECT id\n   FROM pvd_r1\n  WHERE NOT (id IN ( SELECT pvd_r2.id\n           FROM pvd_r2));
-- end-expected
SELECT replace(pg_get_viewdef('pvd_notin'::regclass, true), chr(10), '\n') AS d;

-- stmt 8: casts are canonicalised, COALESCE is upper-cased, BETWEEN and IN are expanded
CREATE VIEW pvd_cast AS SELECT id::text AS t, amt::int AS n, upper(name) AS u FROM pvd_r1;

-- begin-expected
-- columns: d
-- row: SELECT id::text AS t,\n    amt::integer AS n,\n    upper(name) AS u\n   FROM pvd_r1;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_cast'::regclass, true), chr(10), '\n') AS d;

-- begin-expected
-- columns: d
-- row: SELECT (id)::text AS t,\n    (amt)::integer AS n,\n    upper(name) AS u\n   FROM pvd_r1;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_cast'::regclass, false), chr(10), '\n') AS d;

CREATE VIEW pvd_expr AS SELECT coalesce(name, 'n') AS c, id BETWEEN 1 AND 5 AS b, name IN ('a','b') AS i FROM pvd_r1;

-- begin-expected
-- columns: d
-- row: SELECT COALESCE(name, 'n'::text) AS c,\n    id >= 1 AND id <= 5 AS b,\n    name = ANY (ARRAY['a'::text, 'b'::text]) AS i\n   FROM pvd_r1;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_expr'::regclass, true), chr(10), '\n') AS d;

CREATE VIEW pvd_sp AS SELECT name::varchar(4) AS v, nullif(name,'a') AS n, greatest(id,1) AS g, least(id,1) AS l FROM pvd_r1;

-- begin-expected
-- columns: d
-- row: SELECT name::character varying(4) AS v,\n    NULLIF(name, 'a'::text) AS n,\n    GREATEST(id, 1) AS g,\n    LEAST(id, 1) AS l\n   FROM pvd_r1;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_sp'::regclass, true), chr(10), '\n') AS d;

CREATE VIEW pvd_lk AS SELECT id FROM pvd_r1 WHERE name LIKE 'a%' OR name NOT LIKE 'b%' OR name ILIKE 'c%';

-- begin-expected
-- columns: d
-- row: SELECT id\n   FROM pvd_r1\n  WHERE name ~~ 'a%'::text OR name !~~ 'b%'::text OR name ~~* 'c%'::text;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_lk'::regclass, true), chr(10), '\n') AS d;

CREATE VIEW pvd_nb AS SELECT id FROM pvd_r1 WHERE name NOT BETWEEN 'a' AND 'b';

-- begin-expected
-- columns: d
-- row: SELECT id\n   FROM pvd_r1\n  WHERE name < 'a'::text OR name > 'b'::text;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_nb'::regclass, true), chr(10), '\n') AS d;

-- stmt 9: a constant carries the type parse analysis read it as
CREATE VIEW pvd_lit AS SELECT id, name FROM pvd_r1 WHERE name = 'x' AND amt > 1;

-- begin-expected
-- columns: d
-- row: SELECT id,\n    name\n   FROM pvd_r1\n  WHERE name = 'x'::text AND amt > 1::numeric;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_lit'::regclass, true), chr(10), '\n') AS d;

-- begin-expected
-- columns: d
-- row: SELECT id,\n    name\n   FROM pvd_r1\n  WHERE ((name = 'x'::text) AND (amt > (1)::numeric));
-- end-expected
SELECT replace(pg_get_viewdef('pvd_lit'::regclass, false), chr(10), '\n') AS d;

CREATE TABLE pvd_r4 (bi bigint, si smallint, re real, nu numeric(10,2), da date, ch char(3), vc varchar(5));
CREATE VIEW pvd_lits AS SELECT bi, si FROM pvd_r4 WHERE bi = 1 AND si = 2 AND re = 1.5 AND nu = 3 AND da = '2020-01-01' AND ch = 'a';

-- begin-expected
-- columns: d
-- row: SELECT bi,\n    si\n   FROM pvd_r4\n  WHERE bi = 1 AND si = 2 AND re = 1.5::double precision AND nu = 3::numeric AND da = '2020-01-01'::date AND ch = 'a'::bpchar;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_lits'::regclass, true), chr(10), '\n') AS d;

-- an integer column against a string constant reads it back as the number
CREATE VIEW pvd_ints AS SELECT id FROM pvd_r1 WHERE id = '4';

-- begin-expected
-- columns: d
-- row: SELECT id\n   FROM pvd_r1\n  WHERE id = 4;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_ints'::regclass, true), chr(10), '\n') AS d;

-- a constant written with a cast carries one label and no brackets
CREATE VIEW pvd_writ AS SELECT 'v'::text AS a, NULL::text AS b, 1::numeric AS c, 1.9::numeric AS d, 1::int AS e FROM pvd_r1;

-- begin-expected
-- columns: d
-- row: SELECT 'v'::text AS a,\n    NULL::text AS b,\n    1::numeric AS c,\n    1.9 AS d,\n    1 AS e\n   FROM pvd_r1;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_writ'::regclass, true), chr(10), '\n') AS d;

-- stmt 10: every clause starts its own line, and OFFSET is written before LIMIT
CREATE VIEW pvd_agg AS SELECT name, count(*) AS c FROM pvd_r1 GROUP BY name HAVING count(*) > 1 ORDER BY name;

-- begin-expected
-- columns: d
-- row: SELECT name,\n    count(*) AS c\n   FROM pvd_r1\n  GROUP BY name\n HAVING (count(*) > 1)\n  ORDER BY name;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_agg'::regclass, false), chr(10), '\n') AS d;

CREATE VIEW pvd_lo AS SELECT id FROM pvd_r1 ORDER BY id DESC LIMIT 5 OFFSET 2;

-- begin-expected
-- columns: d
-- row: SELECT id\n   FROM pvd_r1\n  ORDER BY id DESC\n OFFSET 2\n LIMIT 5;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_lo'::regclass, true), chr(10), '\n') AS d;

-- stmt 11: the wrap column decides where the select list breaks
CREATE VIEW pvd_wrap AS SELECT id, name, amt FROM pvd_r1 WHERE id > 0;

-- begin-expected
-- columns: d
-- row: SELECT id, name, amt\n   FROM pvd_r1\n  WHERE id > 0;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_wrap'::regclass, 80), chr(10), '\n') AS d;

-- begin-expected
-- columns: d
-- row: SELECT id, name,\n    amt\n   FROM pvd_r1\n  WHERE id > 0;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_wrap'::regclass, 20), chr(10), '\n') AS d;

-- stmt 12: a parenthesised body keeps the ORDER BY and LIMIT written after it
INSERT INTO pvd_r1 VALUES (1,'a',1),(2,'b',2),(3,'c',3),(4,'d',4),(5,'e',5);
INSERT INTO pvd_r2 VALUES (6,'f'),(7,'g');
CREATE VIEW pvd_lim AS (SELECT id FROM pvd_r1 UNION SELECT id FROM pvd_r2) ORDER BY 1 LIMIT 3;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*) AS n FROM pvd_lim;

-- begin-expected
-- columns: d
-- row: SELECT pvd_r1.id\n   FROM pvd_r1\nUNION\n SELECT pvd_r2.id\n   FROM pvd_r2\n  ORDER BY 1\n LIMIT 3;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_lim'::regclass, true), chr(10), '\n') AS d;

CREATE VIEW pvd_lim2 AS (SELECT id FROM pvd_r1) ORDER BY id LIMIT 2;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM pvd_lim2;

-- begin-expected
-- columns: d
-- row: SELECT id\n   FROM pvd_r1\n  ORDER BY id\n LIMIT 2;
-- end-expected
SELECT replace(pg_get_viewdef('pvd_lim2'::regclass, true), chr(10), '\n') AS d;

-- cleanup for the view group
DROP TABLE pvd_r4 CASCADE;
DROP TABLE pvd_r3 CASCADE;
DROP TABLE pvd_r2 CASCADE;
DROP TABLE pvd_r1 CASCADE;

-- stmt 13: a stored default prints as the tree it was analysed into
CREATE TABLE pvd_def (a bigint DEFAULT 1::int, b int DEFAULT 1.9::numeric, c int DEFAULT (2+3), d int DEFAULT '7', e text DEFAULT 'x', f int DEFAULT '-1'::int, g numeric DEFAULT .5, h boolean DEFAULT 'true', i text DEFAULT upper('q'), j text DEFAULT 'a' || 'b', k numeric(10,2) DEFAULT 0.00, l date DEFAULT '2020-01-01'::date);

-- begin-expected
-- columns: attname | e
-- row: a | 1
-- row: b | 1.9
-- row: c | (2 + 3)
-- row: d | 7
-- row: e | 'x'::text
-- row: f | '-1'::integer
-- row: g | 0.5
-- row: h | true
-- row: i | upper('q'::text)
-- row: j | ('a'::text || 'b'::text)
-- row: k | 0.00
-- row: l | '2020-01-01'::date
-- end-expected
SELECT a.attname, pg_get_expr(d.adbin, d.adrelid) AS e FROM pg_attrdef d JOIN pg_class c ON c.oid = d.adrelid JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum WHERE c.relname = 'pvd_def' ORDER BY d.adnum;

-- information_schema reads the same catalogue column and answers identically
-- begin-expected
-- columns: column_name | column_default
-- row: a | 1
-- row: b | 1.9
-- row: c | (2 + 3)
-- row: d | 7
-- row: e | 'x'::text
-- row: f | '-1'::integer
-- row: g | 0.5
-- row: h | true
-- row: i | upper('q'::text)
-- row: j | ('a'::text || 'b'::text)
-- row: k | 0.00
-- row: l | '2020-01-01'::date
-- end-expected
SELECT column_name, column_default FROM information_schema.columns WHERE table_name = 'pvd_def' ORDER BY ordinal_position;

-- the folded text still behaves as it was written
INSERT INTO pvd_def DEFAULT VALUES;

-- begin-expected
-- columns: a | b | c | d | e | f | g | h | i | j | k | l
-- row: 1 | 2 | 5 | 7 | x | -1 | 0.5 | t | Q | ab | 0.00 | 2020-01-01
-- end-expected
SELECT a, b, c, d, e, f, g, h, i, j, k, l FROM pvd_def;

-- stmt 14: a cast the constant does not already read as is kept, with its operand bracketed
CREATE TABLE pvd_keep (l int DEFAULT 1::bigint, m int DEFAULT 1::smallint, n bigint DEFAULT 1::text::int);

-- begin-expected
-- columns: attname | e
-- row: l | (1)::bigint
-- row: m | (1)::smallint
-- row: n | ((1)::text)::integer
-- end-expected
SELECT a.attname, pg_get_expr(d.adbin, d.adrelid) AS e FROM pg_attrdef d JOIN pg_class c ON c.oid = d.adrelid JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum WHERE c.relname = 'pvd_keep' ORDER BY d.adnum;

DROP TABLE pvd_keep;
DROP TABLE pvd_def;

-- stmt 15: an index over a partitioned relation is printed with ON ONLY
CREATE TABLE pvd_pg (i int, s text) PARTITION BY RANGE (i);
CREATE INDEX pvd_pg_idx ON pvd_pg (s);
CREATE TABLE pvd_pg_0 PARTITION OF pvd_pg FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (i);
CREATE TABLE pvd_pg_0_0 PARTITION OF pvd_pg_0 FOR VALUES FROM (0) TO (10);

-- begin-expected
-- columns: d
-- row: CREATE INDEX pvd_pg_idx ON ONLY public.pvd_pg USING btree (s)
-- end-expected
SELECT pg_get_indexdef('pvd_pg_idx'::regclass) AS d;

-- begin-expected
-- columns: d
-- row: CREATE INDEX pvd_pg_0_s_idx ON ONLY public.pvd_pg_0 USING btree (s)
-- end-expected
SELECT pg_get_indexdef('pvd_pg_0_s_idx'::regclass) AS d;

-- the leaf is an ordinary table, so it gets no ONLY
-- begin-expected
-- columns: d
-- row: CREATE INDEX pvd_pg_0_0_s_idx ON public.pvd_pg_0_0 USING btree (s)
-- end-expected
SELECT pg_get_indexdef('pvd_pg_0_0_s_idx'::regclass) AS d;

-- the pretty form drops the schema and keeps the ONLY
-- begin-expected
-- columns: d
-- row: CREATE INDEX pvd_pg_idx ON ONLY pvd_pg USING btree (s)
-- end-expected
SELECT pg_get_indexdef('pvd_pg_idx'::regclass, 0, true) AS d;

-- begin-expected
-- columns: tablename | indexname | indexdef
-- row: pvd_pg | pvd_pg_idx | CREATE INDEX pvd_pg_idx ON ONLY public.pvd_pg USING btree (s)
-- row: pvd_pg_0 | pvd_pg_0_s_idx | CREATE INDEX pvd_pg_0_s_idx ON ONLY public.pvd_pg_0 USING btree (s)
-- row: pvd_pg_0_0 | pvd_pg_0_0_s_idx | CREATE INDEX pvd_pg_0_0_s_idx ON public.pvd_pg_0_0 USING btree (s)
-- end-expected
SELECT tablename, indexname, indexdef FROM pg_indexes WHERE tablename LIKE 'pvd!_pg%' ESCAPE '!' ORDER BY tablename, indexname;

-- a constraint-derived index says ON ONLY too
CREATE TABLE pvd_kp (i int PRIMARY KEY, j text) PARTITION BY RANGE (i);
CREATE TABLE pvd_kp_0 PARTITION OF pvd_kp FOR VALUES FROM (0) TO (10);

-- begin-expected
-- columns: tablename | indexdef
-- row: pvd_kp | CREATE UNIQUE INDEX pvd_kp_pkey ON ONLY public.pvd_kp USING btree (i)
-- row: pvd_kp_0 | CREATE UNIQUE INDEX pvd_kp_0_pkey ON public.pvd_kp_0 USING btree (i)
-- end-expected
SELECT tablename, indexdef FROM pg_indexes WHERE tablename LIKE 'pvd!_kp%' ESCAPE '!' ORDER BY tablename;

CREATE UNIQUE INDEX pvd_kp_u ON pvd_kp (i, j);

-- begin-expected
-- columns: d
-- row: CREATE UNIQUE INDEX pvd_kp_u ON ONLY public.pvd_kp USING btree (i, j)
-- end-expected
SELECT pg_get_indexdef('pvd_kp_u'::regclass) AS d;

DROP TABLE pvd_kp;
DROP TABLE pvd_pg;

-- stmt 16: pg_get_expr on indpred keeps the predicate's outer parentheses
CREATE TABLE pvd_pe (i int, s text);
CREATE INDEX pvd_pe_idx ON pvd_pe (s) WHERE i > 5;
CREATE INDEX pvd_pe_ix2 ON pvd_pe (s) WHERE i > 5 AND s IS NOT NULL;
CREATE INDEX pvd_pe_ix3 ON pvd_pe (lower(s));

-- begin-expected
-- columns: relname | p | e
-- row: pvd_pe_idx | (i > 5) | NULL
-- row: pvd_pe_ix2 | ((i > 5) AND (s IS NOT NULL)) | NULL
-- row: pvd_pe_ix3 | NULL | lower(s)
-- end-expected
SELECT ic.relname, pg_get_expr(i.indpred, i.indrelid) AS p, pg_get_expr(i.indexprs, i.indrelid) AS e FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid WHERE ic.relname LIKE 'pvd!_pe%' ESCAPE '!' ORDER BY ic.relname;

-- cleanup
DROP TABLE pvd_pe;
