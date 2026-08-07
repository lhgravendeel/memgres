-- ============================================================================
-- Feature Comparison: a row written out as JSON
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- row_to_json, to_json, to_jsonb and the aggregates built on them turn a row
-- into a JSON object whose members are the row's fields. memgres wrote the
-- text a composite prints as and then quoted it, so a client asking for a row
-- was handed one string with every field run together inside it: "(1,ab,...)"
-- where an object of named members was due.
--
-- The same reading settles what a field is worth once it is in there. An array
-- is a JSON array and not the braces it prints as, a composite field is an
-- object of its own declared names, a bytea is its hex text, and a string of
-- braces that is only text stays quoted text. Asked for it pretty, the line
-- breaks between one field and the next and nowhere else; a jsonb orders its
-- keys and spaces them out; and every one of these functions is strict, so
-- nothing in is nothing out.
-- ============================================================================

SET search_path = public;


DROP TABLE IF EXISTS rj_b;

DROP TABLE IF EXISTS rj_t;

DROP TYPE IF EXISTS rj_comp CASCADE;

CREATE TYPE rj_comp AS (a int, b text);

CREATE TABLE rj_t (id int, name text, c char(5), amt numeric, ok boolean, d date,
                   arr int[], j json, jb jsonb, by bytea, s text);

INSERT INTO rj_t VALUES (1, 'ab', 'ab', 1.50, true, '2020-01-01',
                         ARRAY[1,2], '{"k":1}', '{"k":1}', '\x0102', '{1,2}');

INSERT INTO rj_t VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE rj_b (id int, tags text[], cs rj_comp[], v rj_comp);

INSERT INTO rj_b VALUES (1, ARRAY['a','b c'], ARRAY[ROW(1,'x')::rj_comp], ROW(7,'y')::rj_comp);


-- ============================================================================
-- A row is an object
-- ============================================================================

-- An anonymous row names its fields f1, f2 and so on.
-- begin-expected
-- columns: r
-- row: {"f1":1}
-- end-expected
SELECT row_to_json(row(1))::text AS r;

-- begin-expected
-- columns: r
-- row: {"f1":1,"f2":"a"}
-- end-expected
SELECT row_to_json(row(1, 'a'))::text AS r;

-- begin-expected
-- columns: r
-- row: {"f1":1,"f2":null}
-- end-expected
SELECT row_to_json(row(1, NULL))::text AS r;

-- begin-expected
-- columns: r
-- row: {"f1":1.50,"f2":true,"f3":"2020-01-01"}
-- end-expected
SELECT row_to_json(row(1.50, true, '2020-01-01'::date))::text AS r;

-- begin-expected
-- columns: r
-- row: {"f1":"ab   "}
-- end-expected
SELECT row_to_json(row('ab'::char(5)))::text AS r;

-- begin-expected
-- columns: r
-- row: {"f1":1,"f2":{"f1":2,"f2":"b"}}
-- end-expected
SELECT row_to_json(row(1, row(2, 'b')))::text AS r;

-- begin-expected
-- columns: r
-- row: {"f1":[1,2]}
-- end-expected
SELECT row_to_json(row(ARRAY[1,2]))::text AS r;


-- A row of a relation carries the names its columns were declared with.
-- begin-expected
-- columns: r
-- row: {"id":1,"name":"ab","c":"ab   ","amt":1.50,"ok":true,"d":"2020-01-01","arr":[1,2],"j":{"k":1},"jb":{"k": 1},"by":"\\x0102","s":"{1,2}"}
-- end-expected
SELECT row_to_json(t)::text AS r FROM rj_t t WHERE id = 1;

-- begin-expected
-- columns: r
-- row: {"id":2,"name":null,"c":null,"amt":null,"ok":null,"d":null,"arr":null,"j":null,"jb":null,"by":null,"s":null}
-- end-expected
SELECT row_to_json(t)::text AS r FROM rj_t t WHERE id = 2;

-- begin-expected
-- columns: r
-- row: {"id":1,"name":"ab","c":"ab   ","amt":1.50,"ok":true,"d":"2020-01-01","arr":[1,2],"j":{"k":1},"jb":{"k": 1},"by":"\\x0102","s":"{1,2}"}
-- end-expected
SELECT row_to_json(t.*)::text AS r FROM rj_t t WHERE id = 1;

-- begin-expected
-- columns: r
-- row: {"id":1,"name":"ab","c":"ab   ","amt":1.50,"ok":true,"d":"2020-01-01","arr":[1,2],"j":{"k":1},"jb":{"k": 1},"by":"\\x0102","s":"{1,2}"}
-- end-expected
SELECT row_to_json(rj_t)::text AS r FROM rj_t WHERE id = 1;

-- begin-expected
-- columns: r
-- row: {"a":1,"b":"b"}
-- end-expected
SELECT row_to_json(x)::text AS r FROM (SELECT 1 AS a, 'b' AS b) x;

-- begin-expected
-- columns: r
-- row: {"n":1,"t":"a"}
-- end-expected
SELECT row_to_json(v)::text AS r FROM (VALUES (1, 'a')) v(n, t);

-- begin-expected
-- columns: r
-- row: {"n":1,"t":"x"}
-- end-expected
WITH w AS (SELECT 1 AS n, 'x' AS t) SELECT row_to_json(w)::text AS r FROM w;


-- A composite gives its fields the names it was declared with.
-- begin-expected
-- columns: r
-- row: {"a":1,"b":"a"}
-- end-expected
SELECT row_to_json(ROW(1, 'a')::rj_comp)::text AS r;

-- begin-expected
-- columns: r
-- row: {"a":7,"b":"y"}
-- end-expected
SELECT row_to_json(v)::text AS r FROM rj_b;

-- begin-expected
-- columns: r
-- row: {"a":7,"b":"y"}
-- end-expected
SELECT to_json(v)::text AS r FROM rj_b;

-- begin-expected
-- columns: r
-- row: {"a": 7, "b": "y"}
-- end-expected
SELECT to_jsonb(v)::text AS r FROM rj_b;


-- The row itself is still the text a composite prints as.
-- begin-expected
-- columns: r
-- row: (1,a)
-- end-expected
SELECT (row(1, 'a'))::text AS r;

-- begin-expected
-- columns: r
-- row: (2,,,,,,,,,,)
-- end-expected
SELECT (t)::text AS r FROM rj_t t WHERE id = 2;


-- ============================================================================
-- What a field is worth
-- ============================================================================

-- An array is an array, whatever it prints as.
-- begin-expected
-- columns: r
-- row: {"id":1,"tags":["a","b c"],"cs":[{"a":1,"b":"x"}],"v":{"a":7,"b":"y"}}
-- end-expected
SELECT row_to_json(b)::text AS r FROM rj_b b;

-- begin-expected
-- columns: r
-- row: ["a","b c"]
-- end-expected
SELECT to_json(tags)::text AS r FROM rj_b;

-- begin-expected
-- columns: r
-- row: [{"a":1,"b":"x"}]
-- end-expected
SELECT to_json(cs)::text AS r FROM rj_b;

-- begin-expected
-- columns: r
-- row: [1,2]
-- end-expected
SELECT to_json(arr)::text AS r FROM rj_t WHERE id = 1;

-- begin-expected
-- columns: r
-- row: [1, 2]
-- end-expected
SELECT to_jsonb(ARRAY[1,2])::text AS r;

-- begin-expected
-- columns: r
-- row: ["a", "b"]
-- end-expected
SELECT to_jsonb(ARRAY['a','b'])::text AS r;


-- Text of braces is text, and json is not quoted again.
-- begin-expected
-- columns: r
-- row: "{1,2}"
-- end-expected
SELECT to_json(s)::text AS r FROM rj_t WHERE id = 1;

-- begin-expected
-- columns: r
-- row: {"f1":"{1,2}"}
-- end-expected
SELECT row_to_json(row('{1,2}'::text))::text AS r;

-- begin-expected
-- columns: r
-- row: {"f1":"{\"k\":1}"}
-- end-expected
SELECT row_to_json(row('{"k":1}'::text))::text AS r;

-- begin-expected
-- columns: r
-- row: {"k":1}
-- end-expected
SELECT to_json(j)::text AS r FROM rj_t WHERE id = 1;


-- A bytea is the hex text it prints as.
-- begin-expected
-- columns: r
-- row: "\\x0102"
-- end-expected
SELECT to_json('\x0102'::bytea)::text AS r;


-- Everything else is written the way its own type prints it.
-- begin-expected
-- columns: r
-- row: "x   "
-- end-expected
SELECT to_json('x'::char(4))::text AS r;

-- begin-expected
-- columns: r
-- row: "1 day"
-- end-expected
SELECT to_json('1 day'::interval)::text AS r;

-- begin-expected
-- columns: r
-- row: 1.5
-- end-expected
SELECT to_json(1.5::float8)::text AS r;

-- begin-expected
-- columns: r
-- row: "a\"b"
-- end-expected
SELECT to_json('a"b'::text)::text AS r;


-- ============================================================================
-- Pretty, ordered and strict
-- ============================================================================

-- The line breaks between one field and the next and nowhere else.
-- begin-expected
-- columns: r
-- row: {"f1":1}
-- end-expected
SELECT replace(row_to_json(row(1), true)::text, chr(10), '<lf>') AS r;

-- begin-expected
-- columns: r
-- row: {"f1":1,<lf> "f2":"a"}
-- end-expected
SELECT replace(row_to_json(row(1, 'a'), true)::text, chr(10), '<lf>') AS r;

-- begin-expected
-- columns: r
-- row: {"f1":1,<lf> "f2":"a",<lf> "f3":2}
-- end-expected
SELECT replace(row_to_json(row(1, 'a', 2), true)::text, chr(10), '<lf>') AS r;

-- begin-expected
-- columns: r
-- row: {"f1":1,"f2":"a"}
-- end-expected
SELECT replace(row_to_json(row(1, 'a'), false)::text, chr(10), '<lf>') AS r;


-- jsonb orders its keys and keeps the last of any repeated one.
-- begin-expected
-- columns: r
-- row: {"f1": 1, "f2": "a"}
-- end-expected
SELECT to_jsonb(row(1, 'a'))::text AS r;

-- begin-expected
-- columns: r
-- row: {"a": 2, "bb": 1}
-- end-expected
SELECT jsonb_build_object('bb', 1, 'a', 2)::text AS r;

-- begin-expected
-- columns: r
-- row: {"a": 2, "bb": 3}
-- end-expected
SELECT jsonb_build_object('bb', 1, 'a', 2, 'bb', 3)::text AS r;

-- begin-expected
-- columns: r
-- row: {"bb" : 1, "a" : 2, "bb" : 3}
-- end-expected
SELECT json_build_object('bb', 1, 'a', 2, 'bb', 3)::text AS r;

-- begin-expected
-- columns: r
-- row: [{"f1": 1, "f2": "a"}, 2]
-- end-expected
SELECT jsonb_build_array(row(1, 'a'), 2)::text AS r;

-- begin-expected
-- columns: r
-- row: [{"f1":1,"f2":"a"}, 2]
-- end-expected
SELECT json_build_array(row(1, 'a'), 2)::text AS r;

-- begin-expected
-- columns: r
-- row: {"r" : {"id":2,"name":null,"c":null,"amt":null,"ok":null,"d":null,"arr":null,"j":null,"jb":null,"by":null,"s":null}}
-- end-expected
SELECT json_build_object('r', t)::text AS r FROM rj_t t WHERE id = 2;


-- Nothing in, nothing out.
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT to_json(NULL::int) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT to_jsonb(NULL::int) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT row_to_json(NULL::rj_comp) IS NULL AS r;


-- ============================================================================
-- The aggregates built on them
-- ============================================================================

-- An element with a shape of its own starts a new line.
-- begin-expected
-- columns: r
-- row: [{"id":1,"name":"ab","c":"ab   ","amt":1.50,"ok":true,"d":"2020-01-01","arr":[1,2],"j":{"k":1},"jb":{"k": 1},"by":"\\x0102","s":"{1,2}"}, <lf> {"id":2,"name":null,"c":null,"amt":null,"ok":null,"d":null,"arr":null,"j":null,"jb":null,"by":null,"s":null}]
-- end-expected
SELECT replace(json_agg(t)::text, chr(10), '<lf>') AS r FROM rj_t t;

-- begin-expected
-- columns: r
-- row: [1, 2]
-- end-expected
SELECT replace(json_agg(a)::text, chr(10), '<lf>') AS r
FROM (SELECT 1 AS a UNION ALL SELECT 2) y;

-- begin-expected
-- columns: r
-- row: ["x", "y"]
-- end-expected
SELECT replace(json_agg(a)::text, chr(10), '<lf>') AS r
FROM (SELECT 'x' AS a UNION ALL SELECT 'y') y;

-- begin-expected
-- columns: r
-- row: [["a","b c"]]
-- end-expected
SELECT replace(json_agg(tags)::text, chr(10), '<lf>') AS r FROM rj_b;


-- jsonb_agg holds the same members under jsonb's own ordering.
-- begin-expected
-- columns: r
-- row: [{"c": null, "d": null, "j": null, "s": null, "by": null, "id": 2, "jb": null, "ok": null, "amt": null, "arr": null, "name": null}]
-- end-expected
SELECT jsonb_agg(t)::text AS r FROM rj_t t WHERE id = 2;

-- begin-expected
-- columns: r
-- row: [{"c": null, "d": null, "j": null, "s": null, "by": null, "id": 2, "jb": null, "ok": null, "amt": null, "arr": null, "name": null}]
-- end-expected
SELECT jsonb_agg(t ORDER BY id DESC)::text AS r FROM rj_t t WHERE id = 2;

-- begin-expected
-- columns: r
-- row: [1, 2]
-- end-expected
SELECT jsonb_agg(id)::text AS r FROM rj_t t;


-- The object aggregates take a row for a value just as readily.
-- begin-expected
-- columns: r
-- row: { "2" : {"id":2,"name":null,"c":null,"amt":null,"ok":null,"d":null,"arr":null,"j":null,"jb":null,"by":null,"s":null} }
-- end-expected
SELECT json_object_agg(id, t)::text AS r FROM rj_t t WHERE id = 2;

-- begin-expected
-- columns: r
-- row: {"2": {"c": null, "d": null, "j": null, "s": null, "by": null, "id": 2, "jb": null, "ok": null, "amt": null, "arr": null, "name": null}}
-- end-expected
SELECT jsonb_object_agg(id, t)::text AS r FROM rj_t t WHERE id = 2;


-- A filter, a distinct and an empty group.
-- begin-expected
-- columns: r
-- row: [{"id":2,"name":null,"c":null,"amt":null,"ok":null,"d":null,"arr":null,"j":null,"jb":null,"by":null,"s":null}]
-- end-expected
SELECT json_agg(t) FILTER (WHERE id = 2)::text AS r FROM rj_t t;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT json_agg(a)::text AS r FROM rj_t a WHERE id > 9;

-- begin-expected
-- columns: r
-- row: ["ab", null]
-- end-expected
SELECT json_agg(DISTINCT name)::text AS r FROM rj_t;


-- ============================================================================
-- Reading one back out
-- ============================================================================

-- begin-expected
-- columns: a|b
-- row: 1|x
-- end-expected
SELECT (json_populate_record(NULL::rj_comp, '{"a":1,"b":"x"}')).*;

-- begin-expected
-- columns: a|b
-- row: 1|x
-- end-expected
SELECT (jsonb_populate_record(NULL::rj_comp, '{"a":1,"b":"x"}')).*;

-- begin-expected
-- columns: a|b
-- row: 1|x
-- end-expected
SELECT * FROM json_to_record('{"a":1,"b":"x"}') AS r(a int, b text);

-- begin-expected
-- columns: key|value
-- row: f1|1
-- row: f2|a
-- end-expected
SELECT (json_each_text(row_to_json(row(1, 'a')))).*;

-- begin-expected
-- columns: r
-- row: f1
-- row: f2
-- end-expected
SELECT json_object_keys(row_to_json(row(1, 'a'))) AS r;

-- begin-expected
-- columns: r
-- row: f1
-- row: f2
-- end-expected
SELECT jsonb_object_keys(to_jsonb(row(1, 'a'))) AS r;


DROP TABLE IF EXISTS rj_b;

DROP TABLE IF EXISTS rj_t;

DROP TYPE IF EXISTS rj_comp CASCADE;

