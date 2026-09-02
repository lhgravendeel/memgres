-- source: review-2026-08.md
-- finding: Root cause 3: each container type has a hand-written writer whose escape rules its own reader does not share
-- area: Round-trip and equality invariants
-- title: Root cause 3: each container type has a hand-written writer whose escape rules its own reader does not share
-- begin-expected
-- ok: 0
-- end-expected
CREATE EXTENSION IF NOT EXISTS hstore;
-- begin-expected
-- columns: hstore:text
-- row: "k"=>"a\"b"
-- rowcount: 1
-- end-expected
SELECT hstore('k', 'a"b')::text;
-- begin-expected
-- columns: ok:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT (hstore('k','a"b')::text::hstore -> 'k') = 'a"b' AS ok;
-- begin-expected
-- columns: hstore:text
-- row: "k"=>"a\\b"
-- rowcount: 1
-- end-expected
SELECT hstore('k','a\b')::text;
-- begin-expected
-- columns: ok:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT (hstore('k','a\b')::text::hstore -> 'k') = 'a\b' AS ok;
-- begin-expected
-- columns: lit:text
-- row: {"a\\b"}
-- rowcount: 1
-- end-expected
SELECT ARRAY['a\b']::text AS lit;
-- begin-expected
-- columns: back:_text
-- row: {"\\"}
-- rowcount: 1
-- end-expected
SELECT ARRAY['\']::text::text[] AS back;
-- begin-expected
-- columns: lit2:text
-- row: {"\\\""}
-- rowcount: 1
-- end-expected
SELECT ARRAY['\"']::text AS lit2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf2_rt_en AS ENUM ('e,f', 'z');
-- begin-expected
-- columns: enum_range:text
-- row: {"e,f",z}
-- rowcount: 1
-- end-expected
SELECT enum_range(NULL::zz_vf2_rt_en)::text;
-- begin-expected
-- columns: v:text
-- row: e,f
-- row: z
-- rowcount: 2
-- end-expected
SELECT x::text AS v FROM (SELECT unnest(enum_range(NULL::zz_vf2_rt_en)) AS x) t ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf2_rt_rg AS RANGE (subtype = text);
-- begin-expected
-- columns: zz_vf2_rt_rg:text
-- row: ["a,b",z)
-- rowcount: 1
-- end-expected
SELECT zz_vf2_rt_rg('a,b','z')::text;
-- begin-expected
-- columns: zz_vf2_rt_rg:text
-- row: ["",z)
-- rowcount: 1
-- end-expected
SELECT zz_vf2_rt_rg('','z')::text;
-- begin-expected
-- columns: zz_vf2_rt_rg:text
-- row: ["a b",z)
-- rowcount: 1
-- end-expected
SELECT zz_vf2_rt_rg('a b','z')::text;
