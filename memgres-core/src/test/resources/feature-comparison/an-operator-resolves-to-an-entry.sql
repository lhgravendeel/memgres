-- A stored definition shows which entry of pg_operator each spelling resolved to, because parse
-- analysis puts a conversion in front of an operand whose own type that entry does not declare,
-- and the conversion is part of the tree that was stored.
--   * character varying and cidr have no operators of their own, so a comparison with one is
--     text's or inet's; a name has equality against text and is not converted; a character has
--     equality of its own, so the character varying is converted to it instead.
--   * IN is not kept: the list is rewritten into what tests it, an item that is a plain column
--     compared on its own and joined to the rest by OR.
--   * every operator is written under its own spelling, a prefix one with a space after it, and
--     an operand that does not already read as one thing is bracketed.
-- Every value below was read off PostgreSQL 18. Newlines are written as the two characters
-- backslash-n by the replace() around each call, so one definition fits on one annotated row.

-- setup
CREATE TABLE ore_o (id int, vc varchar(10), vc2 varchar, txt text, nme name, ch char(5));
CREATE TABLE ore_ni (id int, cd cidr, ip inet, vb bit varying(8), bt bit(8));
CREATE TABLE ore_nu (id int, nm numeric, nm2 numeric(10,2), bi bigint, si smallint, d double precision, dt date, tm time, ts timestamp);
CREATE TABLE ore_ca (id int, vc varchar(10), txt text, nm numeric, si smallint, bi bigint);
CREATE TABLE ore_jp (id int, jb jsonb, ia int[], ta text[]);
CREATE TABLE ore_in (id int, vc varchar(10), txt text, nme name, nm numeric, bi bigint, si smallint);
CREATE TABLE ore_op (id int, id2 int, tv tsvector, tq tsquery, pt point, bx box, ln lseg, r4 int4range);
CREATE TABLE ore_ky (id int, txt text, bi bigint, si smallint);

-- stmt 1: a character varying is read as text by the operator that compares it
CREATE VIEW ore_v1 AS SELECT id FROM ore_o WHERE vc = 'b';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_o\n  WHERE vc::text = 'b'::text;
-- end-expected
SELECT replace(pg_get_viewdef('ore_v1'::regclass, true), chr(10), '\n') AS d;
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_o\n  WHERE ((vc)::text = 'b'::text);
-- end-expected
SELECT replace(pg_get_viewdef('ore_v1'::regclass, false), chr(10), '\n') AS d;
CREATE VIEW ore_v2 AS SELECT id FROM ore_o WHERE vc = vc2 AND vc = txt AND vc = nme AND vc = ch;
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_o\n  WHERE vc::text = vc2::text AND vc::text = txt AND vc::text = nme AND vc::bpchar = ch;
-- end-expected
SELECT replace(pg_get_viewdef('ore_v2'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_v3 AS SELECT id FROM ore_o WHERE ch = 'b' AND vc < 'b' AND vc <> 'c' AND vc >= 'd';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_o\n  WHERE ch = 'b'::bpchar AND vc::text < 'b'::text AND vc::text <> 'c'::text AND vc::text >= 'd'::text;
-- end-expected
SELECT replace(pg_get_viewdef('ore_v3'::regclass, true), chr(10), '\n') AS d;

-- stmt 2: a bare column of that type is nothing for an operator to convert
CREATE VIEW ore_v4 AS SELECT vc, vc AS z FROM ore_o ORDER BY vc;
-- begin-expected
-- columns: d
-- row:  SELECT vc,\n    vc AS z\n   FROM ore_o\n  ORDER BY vc;
-- end-expected
SELECT replace(pg_get_viewdef('ore_v4'::regclass, true), chr(10), '\n') AS d;

-- stmt 3: the conversion is printed wherever an operand carries one
CREATE VIEW ore_v5 AS SELECT vc || 'x' AS a, vc || vc2 AS b, txt || vc AS c FROM ore_o;
-- begin-expected
-- columns: d
-- row:  SELECT vc::text || 'x'::text AS a,\n    vc::text || vc2::text AS b,\n    txt || vc::text AS c\n   FROM ore_o;
-- end-expected
SELECT replace(pg_get_viewdef('ore_v5'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_v6 AS SELECT id FROM ore_o WHERE vc LIKE 'a%' AND vc ILIKE 'b%' AND vc ~ 'c';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_o\n  WHERE vc::text ~~ 'a%'::text AND vc::text ~~* 'b%'::text AND vc::text ~ 'c'::text;
-- end-expected
SELECT replace(pg_get_viewdef('ore_v6'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_v7 AS SELECT id FROM ore_o WHERE vc BETWEEN 'a' AND 'b';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_o\n  WHERE vc::text >= 'a'::text AND vc::text <= 'b'::text;
-- end-expected
SELECT replace(pg_get_viewdef('ore_v7'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_v8 AS SELECT id FROM ore_o WHERE vc IS NULL OR vc IS DISTINCT FROM 'a';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_o\n  WHERE vc IS NULL OR vc::text IS DISTINCT FROM 'a'::text;
-- end-expected
SELECT replace(pg_get_viewdef('ore_v8'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_v9 AS SELECT id FROM ore_o GROUP BY id HAVING max(vc) > 'a';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_o\n  GROUP BY id\n HAVING max(vc::text) > 'a'::text;
-- end-expected
SELECT replace(pg_get_viewdef('ore_v9'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_va AS SELECT id FROM ore_o WHERE vc = 'b'::varchar AND vc = 'c'::text;
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_o\n  WHERE vc::text = 'b'::character varying::text AND vc::text = 'c'::text;
-- end-expected
SELECT replace(pg_get_viewdef('ore_va'::regclass, true), chr(10), '\n') AS d;

-- stmt 4: a cidr is read as inet, except where the call has a cidr signature of its own
CREATE VIEW ore_n1 AS SELECT id FROM ore_ni WHERE cd = cd AND cd >> ip AND cd = '10.0.0.0/8';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_ni\n  WHERE cd::inet = cd::inet AND cd::inet >> ip AND cd::inet = '10.0.0.0/8'::inet;
-- end-expected
SELECT replace(pg_get_viewdef('ore_n1'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_n2 AS SELECT host(cd) AS a, masklen(cd) AS b, abbrev(cd) AS c FROM ore_ni;
-- begin-expected
-- columns: d
-- row:  SELECT host(cd::inet) AS a,\n    masklen(cd::inet) AS b,\n    abbrev(cd) AS c\n   FROM ore_ni;
-- end-expected
SELECT replace(pg_get_viewdef('ore_n2'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_n3 AS SELECT ip >>= cd AS a, ip <<= cd AS b, ip << cd AS c, ip >> cd AS d FROM ore_ni;
-- begin-expected
-- columns: d
-- row:  SELECT ip >>= cd::inet AS a,\n    ip <<= cd::inet AS b,\n    ip << cd::inet AS c,\n    ip >> cd::inet AS d\n   FROM ore_ni;
-- end-expected
SELECT replace(pg_get_viewdef('ore_n3'::regclass, true), chr(10), '\n') AS d;

-- stmt 5: bit varying is the preferred type of its category, so equality converts the bit
CREATE VIEW ore_n4 AS SELECT id FROM ore_ni WHERE vb = B'1010' AND bt = B'10101010';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_ni\n  WHERE vb = '1010'::"bit"::bit varying AND bt = '10101010'::"bit";
-- end-expected
SELECT replace(pg_get_viewdef('ore_n4'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_n5 AS SELECT vb & bt AS a, vb | bt AS b, vb # bt AS c, vb << 1 AS d, vb || bt AS e FROM ore_ni;
-- begin-expected
-- columns: d
-- row:  SELECT vb::"bit" & bt AS a,\n    vb::"bit" | bt AS b,\n    vb::"bit" # bt AS c,\n    vb::"bit" << 1 AS d,\n    vb || bt::bit varying AS e\n   FROM ore_ni;
-- end-expected
SELECT replace(pg_get_viewdef('ore_n5'::regclass, true), chr(10), '\n') AS d;

-- stmt 6: a number is converted only where no entry takes the pair as written
CREATE VIEW ore_u1 AS SELECT id FROM ore_nu WHERE nm = 1 AND bi = 2 AND si = 3 AND d = 4 AND nm2 = 5;
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_nu\n  WHERE nm = 1::numeric AND bi = 2 AND si = 3 AND d = 4::double precision AND nm2 = 5::numeric;
-- end-expected
SELECT replace(pg_get_viewdef('ore_u1'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_u2 AS SELECT id FROM ore_nu WHERE nm = bi AND bi = si AND d = nm;
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_nu\n  WHERE nm = bi::numeric AND bi = si AND d = nm::double precision;
-- end-expected
SELECT replace(pg_get_viewdef('ore_u2'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_u3 AS SELECT id FROM ore_nu WHERE ts > '2020-01-01' AND ts > dt AND dt > ts AND tm > '3:4';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_nu\n  WHERE ts > '2020-01-01 00:00:00'::timestamp without time zone AND ts > dt AND dt > ts AND tm > '03:04:00'::time without time zone;
-- end-expected
SELECT replace(pg_get_viewdef('ore_u3'::regclass, true), chr(10), '\n') AS d;

-- stmt 7: a call's arguments are read against the signature it resolved to
CREATE VIEW ore_g1 AS SELECT upper(vc) AS a, length(vc) AS b, substr(vc, 1, 2) AS c FROM ore_ca;
-- begin-expected
-- columns: d
-- row:  SELECT upper(vc::text) AS a,\n    length(vc::text) AS b,\n    substr(vc::text, 1, 2) AS c\n   FROM ore_ca;
-- end-expected
SELECT replace(pg_get_viewdef('ore_g1'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_g2 AS SELECT max(vc) AS a, string_agg(vc, ',') AS b, count(vc) AS c FROM ore_ca;
-- begin-expected
-- columns: d
-- row:  SELECT max(vc::text) AS a,\n    string_agg(vc::text, ','::text) AS b,\n    count(vc) AS c\n   FROM ore_ca;
-- end-expected
SELECT replace(pg_get_viewdef('ore_g2'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_g3 AS SELECT abs(si) AS a, round(nm, 2) AS b, trunc(si) AS c, to_char(si, '999') AS d, substr(txt, si) AS e FROM ore_ca;
-- begin-expected
-- columns: d
-- row:  SELECT abs(si) AS a,\n    round(nm, 2) AS b,\n    trunc(si::double precision) AS c,\n    to_char(si::double precision, '999'::text) AS d,\n    substr(txt, si::integer) AS e\n   FROM ore_ca;
-- end-expected
SELECT replace(pg_get_viewdef('ore_g3'::regclass, true), chr(10), '\n') AS d;

-- stmt 8: a construct settles on one type for every arm
CREATE VIEW ore_s1 AS SELECT coalesce(vc, 'x') AS a, coalesce(vc, txt) AS b, CASE WHEN id = 1 THEN vc ELSE 'y' END AS c FROM ore_ca;
-- begin-expected
-- columns: d
-- row:  SELECT COALESCE(vc, 'x'::character varying) AS a,\n    COALESCE(vc, txt::character varying) AS b,\n        CASE\n            WHEN id = 1 THEN vc\n            ELSE 'y'::character varying\n        END AS c\n   FROM ore_ca;
-- end-expected
SELECT replace(pg_get_viewdef('ore_s1'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_s2 AS SELECT greatest(vc, 'a') AS a, least(vc, txt) AS b, nullif(vc, 'z') AS c FROM ore_ca;
-- begin-expected
-- columns: d
-- row:  SELECT GREATEST(vc, 'a'::character varying) AS a,\n    LEAST(vc, txt::character varying) AS b,\n    NULLIF(vc::text, 'z'::text) AS c\n   FROM ore_ca;
-- end-expected
SELECT replace(pg_get_viewdef('ore_s2'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_s3 AS SELECT ARRAY[vc, 'a'] AS a, ARRAY[txt, vc] AS b, ARRAY[1, 2.5] AS c, ARRAY[si, bi] AS d FROM ore_ca;
-- begin-expected
-- columns: d
-- row:  SELECT ARRAY[vc, 'a'::character varying] AS a,\n    ARRAY[txt, vc::text] AS b,\n    ARRAY[1::numeric, 2.5] AS c,\n    ARRAY[si::bigint, bi] AS d\n   FROM ore_ca;
-- end-expected
SELECT replace(pg_get_viewdef('ore_s3'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_s4 AS SELECT CASE WHEN id=1 THEN vc WHEN id=2 THEN txt ELSE 'z' END AS a, CASE vc WHEN 'a' THEN 1 END AS b FROM ore_ca;
-- begin-expected
-- columns: d
-- row:  SELECT\n        CASE\n            WHEN id = 1 THEN vc\n            WHEN id = 2 THEN txt::character varying\n            ELSE 'z'::character varying\n        END AS a,\n        CASE vc\n            WHEN 'a'::text THEN 1\n            ELSE NULL::integer\n        END AS b\n   FROM ore_ca;
-- end-expected
SELECT replace(pg_get_viewdef('ore_s4'::regclass, true), chr(10), '\n') AS d;

-- stmt 9: the constant beside a jsonb path operator is an array of text, and a polymorphic
-- operator converts nothing at all
CREATE VIEW ore_j1 AS SELECT jb #> '{a}' AS a, jb #>> '{a}' AS b, jb ? 'k' AS c, jb ?| ARRAY['k'] AS d, jb ?& ARRAY['k'] AS e, jb #- '{a}' AS f FROM ore_jp;
-- begin-expected
-- columns: d
-- row:  SELECT jb #> '{a}'::text[] AS a,\n    jb #>> '{a}'::text[] AS b,\n    jb ? 'k'::text AS c,\n    jb ?| ARRAY['k'::text] AS d,\n    jb ?& ARRAY['k'::text] AS e,\n    jb #- '{a}'::text[] AS f\n   FROM ore_jp;
-- end-expected
SELECT replace(pg_get_viewdef('ore_j1'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_j2 AS SELECT id FROM ore_jp WHERE ta = '{ a , b }' AND ia = '{ 1, 2 , 3 }' AND ia @> '{ 1 }';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_jp\n  WHERE ta = '{a,b}'::text[] AND ia = '{1,2,3}'::integer[] AND ia @> '{1}'::integer[];
-- end-expected
SELECT replace(pg_get_viewdef('ore_j2'::regclass, true), chr(10), '\n') AS d;

-- stmt 10: a list written with IN is the comparison it stands for
CREATE VIEW ore_i1 AS SELECT id FROM ore_in WHERE txt IN ('a') AND txt IN (nme, 'b') AND id NOT IN (1);
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_in\n  WHERE txt = 'a'::text AND (txt = nme OR txt = 'b'::text) AND id <> 1;
-- end-expected
SELECT replace(pg_get_viewdef('ore_i1'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_i2 AS SELECT id FROM ore_in WHERE id IN (1, 2, id) AND id IN (1+1, 2) AND nm IN (1, 2);
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_in\n  WHERE ((id = ANY (ARRAY[1, 2])) OR id = id) AND (id = ANY (ARRAY[1 + 1, 2])) AND (nm = ANY (ARRAY[1::numeric, 2::numeric]));
-- end-expected
SELECT replace(pg_get_viewdef('ore_i2'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_i3 AS SELECT id FROM ore_in WHERE vc IN ('a','b');
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_in\n  WHERE vc::text = ANY (ARRAY['a'::character varying, 'b'::character varying]::text[]);
-- end-expected
SELECT replace(pg_get_viewdef('ore_i3'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_i4 AS SELECT id FROM ore_in WHERE si + bi IN (1, 2);
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_in\n  WHERE (si + bi) = ANY (ARRAY[1::bigint, 2::bigint]);
-- end-expected
SELECT replace(pg_get_viewdef('ore_i4'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_i5 AS SELECT id FROM ore_in WHERE vc = ANY (ARRAY['a','b']) AND vc = ANY (ARRAY[vc, txt]);
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_in\n  WHERE (vc::text = ANY (ARRAY['a'::text, 'b'::text])) AND (vc::text = ANY (ARRAY[vc, txt::character varying]::text[]));
-- end-expected
SELECT replace(pg_get_viewdef('ore_i5'::regclass, true), chr(10), '\n') AS d;

-- stmt 11: every operator is written under its own spelling
CREATE VIEW ore_p1 AS SELECT id & 1 AS a, id | 2 AS b, id # 3 AS c, id << 1 AS d, id >> 1 AS e, ~ id AS f, - id AS g FROM ore_op;
-- begin-expected
-- columns: d
-- row:  SELECT id & 1 AS a,\n    id | 2 AS b,\n    id # 3 AS c,\n    id << 1 AS d,\n    id >> 1 AS e,\n    ~ id AS f,\n    - id AS g\n   FROM ore_op;
-- end-expected
SELECT replace(pg_get_viewdef('ore_p1'::regclass, true), chr(10), '\n') AS d;
-- begin-expected
-- columns: d
-- row:  SELECT (id & 1) AS a,\n    (id | 2) AS b,\n    (id # 3) AS c,\n    (id << 1) AS d,\n    (id >> 1) AS e,\n    (~ id) AS f,\n    (- id) AS g\n   FROM ore_op;
-- end-expected
SELECT replace(pg_get_viewdef('ore_p1'::regclass, false), chr(10), '\n') AS d;
CREATE VIEW ore_p2 AS SELECT tv @@ tq AS a, pt <-> pt AS b, bx ~= bx AS c, bx <<| bx AS d, ln ## bx AS e, r4 -|- r4 AS f FROM ore_op;
-- begin-expected
-- columns: d
-- row:  SELECT tv @@ tq AS a,\n    pt <-> pt AS b,\n    bx ~= bx AS c,\n    bx <<| bx AS d,\n    ln ## bx AS e,\n    r4 -|- r4 AS f\n   FROM ore_op;
-- end-expected
SELECT replace(pg_get_viewdef('ore_p2'::regclass, true), chr(10), '\n') AS d;

-- stmt 12: SIMILAR TO is a regular expression match, and an escape is a call of its own
CREATE VIEW ore_e1 AS SELECT id FROM ore_o WHERE txt SIMILAR TO 'a%' AND vc NOT SIMILAR TO 'b%';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_o\n  WHERE txt ~ similar_to_escape('a%'::text) AND vc::text !~ similar_to_escape('b%'::text);
-- end-expected
SELECT replace(pg_get_viewdef('ore_e1'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_e2 AS SELECT id FROM ore_o WHERE txt SIMILAR TO 'a%' ESCAPE '#' AND txt LIKE 'b%' ESCAPE '!';
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_o\n  WHERE txt ~ similar_to_escape('a%'::text, '#'::text) AND txt ~~ like_escape('b%'::text, '!'::text);
-- end-expected
SELECT replace(pg_get_viewdef('ore_e2'::regclass, true), chr(10), '\n') AS d;

-- stmt 13: an operand that does not already read as one thing is bracketed
CREATE VIEW ore_b1 AS SELECT id FROM ore_ky WHERE si + bi = 1 AND si * 2 + 1 = 2 AND si + 2 * 3 = 3 AND si - (bi - 1) = 4;
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_ky\n  WHERE (si + bi) = 1 AND (si * 2 + 1) = 2 AND (si + 2 * 3) = 3 AND (si - (bi - 1)) = 4;
-- end-expected
SELECT replace(pg_get_viewdef('ore_b1'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_b2 AS SELECT si + bi AS a, si * 2 + 1 AS b, (si + 1) * 2 AS c, si + bi + si AS d FROM ore_ky;
-- begin-expected
-- columns: d
-- row:  SELECT si + bi AS a,\n    si * 2 + 1 AS b,\n    (si + 1) * 2 AS c,\n    si + bi + si AS d\n   FROM ore_ky;
-- end-expected
SELECT replace(pg_get_viewdef('ore_b2'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_b3 AS SELECT - (si + bi) AS a, - (si * bi) AS b, - (- si) AS c, - si + bi AS d, - si * bi AS e FROM ore_ky;
-- begin-expected
-- columns: d
-- row:  SELECT - (si + bi) AS a,\n    - (si * bi) AS b,\n    - (- si) AS c,\n    (- si) + bi AS d,\n    (- si) * bi AS e\n   FROM ore_ky;
-- end-expected
SELECT replace(pg_get_viewdef('ore_b3'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_b4 AS SELECT id FROM ore_op WHERE id > id2 - 7 AND - id > 3 AND (id + 1) * 2 > 4;
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_op\n  WHERE id > (id2 - 7) AND (- id) > 3 AND ((id + 1) * 2) > 4;
-- end-expected
SELECT replace(pg_get_viewdef('ore_b4'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_b5 AS SELECT id FROM ore_ky WHERE si + bi IN (1, 2) AND si + bi BETWEEN 1 AND 2 AND si + bi IS NULL;
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_ky\n  WHERE ((si + bi) = ANY (ARRAY[1::bigint, 2::bigint])) AND (si + bi) >= 1 AND (si + bi) <= 2 AND (si + bi) IS NULL;
-- end-expected
SELECT replace(pg_get_viewdef('ore_b5'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_b6 AS SELECT id FROM ore_ky WHERE (si = 1) = true AND NOT (si + bi = 1) AND (txt LIKE 'a%') = true;
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_ky\n  WHERE (si = 1) = true AND NOT (si + bi) = 1 AND (txt ~~ 'a%'::text) = true;
-- end-expected
SELECT replace(pg_get_viewdef('ore_b6'::regclass, true), chr(10), '\n') AS d;

-- stmt 14: a sort or grouping key is bracketed unless it is a plain column reference
CREATE VIEW ore_k1 AS SELECT count(*) AS n FROM ore_ky GROUP BY txt || 'x', upper(txt), id + 1, id;
-- begin-expected
-- columns: d
-- row:  SELECT count(*) AS n\n   FROM ore_ky\n  GROUP BY (txt || 'x'::text), (upper(txt)), (id + 1), id;
-- end-expected
SELECT replace(pg_get_viewdef('ore_k1'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_k2 AS SELECT id FROM ore_ky ORDER BY txt DESC, upper(txt) NULLS FIRST, id + 1 DESC, id;
-- begin-expected
-- columns: d
-- row:  SELECT id\n   FROM ore_ky\n  ORDER BY txt DESC, (upper(txt)) NULLS FIRST, (id + 1) DESC, id;
-- end-expected
SELECT replace(pg_get_viewdef('ore_k2'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_k3 AS SELECT DISTINCT ON (id + 1, id) id FROM ore_ky;
-- begin-expected
-- columns: d
-- row:  SELECT DISTINCT ON ((id + 1), id) id\n   FROM ore_ky;
-- end-expected
SELECT replace(pg_get_viewdef('ore_k3'::regclass, true), chr(10), '\n') AS d;
CREATE VIEW ore_k4 AS SELECT count(*) OVER (PARTITION BY txt || 'x', id ORDER BY upper(txt), id) AS n FROM ore_ky;
-- begin-expected
-- columns: d
-- row:  SELECT count(*) OVER (PARTITION BY (txt || 'x'::text), id ORDER BY (upper(txt)), id) AS n\n   FROM ore_ky;
-- end-expected
SELECT replace(pg_get_viewdef('ore_k4'::regclass, true), chr(10), '\n') AS d;

-- stmt 15: a CHECK constraint carries the same conversions, in the form that keeps every bracket
CREATE TABLE ore_cc (id int, vc varchar(10), txt text, cd cidr, ip inet, vb bit varying(8), nm numeric, bi bigint, si smallint, ch char(5), nme name, CONSTRAINT ore_c1 CHECK (vc = 'a'), CONSTRAINT ore_c2 CHECK (cd = '10.0.0.0/8'), CONSTRAINT ore_c3 CHECK (vb = B'1010'), CONSTRAINT ore_c4 CHECK (nm = bi), CONSTRAINT ore_c5 CHECK (vc = ch), CONSTRAINT ore_c6 CHECK (upper(vc) = 'A'), CONSTRAINT ore_c7 CHECK (vc || 'x' = 'y'), CONSTRAINT ore_c8 CHECK (si + bi = 1), CONSTRAINT ore_c9 CHECK (vc IN ('a','b')), CONSTRAINT ore_ca2 CHECK (vc LIKE 'a%'), CONSTRAINT ore_cb CHECK (cd >> ip), CONSTRAINT ore_cd CHECK (vc = nme));
-- begin-expected
-- columns: d
-- row: ore_c1 CHECK (((vc)::text = 'a'::text))\nore_c2 CHECK (((cd)::inet = '10.0.0.0/8'::inet))\nore_c3 CHECK ((vb = ('1010'::"bit")::bit varying))\nore_c4 CHECK ((nm = (bi)::numeric))\nore_c5 CHECK (((vc)::bpchar = ch))\nore_c6 CHECK ((upper((vc)::text) = 'A'::text))\nore_c7 CHECK ((((vc)::text || 'x'::text) = 'y'::text))\nore_c8 CHECK (((si + bi) = 1))\nore_c9 CHECK (((vc)::text = ANY ((ARRAY['a'::character varying, 'b'::character varying])::text[])))\nore_ca2 CHECK (((vc)::text ~~ 'a%'::text))\nore_cb CHECK (((cd)::inet >> ip))\nore_cd CHECK (((vc)::text = nme))
-- end-expected
SELECT replace(string_agg(conname || ' ' || pg_get_constraintdef(oid), chr(10) ORDER BY conname), chr(10), '\n') AS d FROM pg_constraint WHERE conrelid = 'ore_cc'::regclass AND contype = 'c';

-- cleanup
DROP VIEW ore_v1, ore_v2, ore_v3, ore_v4, ore_v5, ore_v6, ore_v7, ore_v8, ore_v9, ore_va;
DROP VIEW ore_n1, ore_n2, ore_n3, ore_n4, ore_n5, ore_u1, ore_u2, ore_u3;
DROP VIEW ore_g1, ore_g2, ore_g3, ore_s1, ore_s2, ore_s3, ore_s4, ore_j1, ore_j2;
DROP VIEW ore_i1, ore_i2, ore_i3, ore_i4, ore_i5, ore_p1, ore_p2, ore_e1, ore_e2;
DROP VIEW ore_b1, ore_b2, ore_b3, ore_b4, ore_b5, ore_b6, ore_k1, ore_k2, ore_k3, ore_k4;
DROP TABLE ore_o;
DROP TABLE ore_ni;
DROP TABLE ore_nu;
DROP TABLE ore_ca;
DROP TABLE ore_jp;
DROP TABLE ore_in;
DROP TABLE ore_op;
DROP TABLE ore_ky;
DROP TABLE ore_cc;
