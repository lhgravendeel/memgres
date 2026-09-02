-- source: investigation-2026-08.md
-- finding: 290
-- title: Case folding on parser paths goes through the JVM's default locale, so under a Turkish locale keyword and identifier comparisons miss and valid DDL is refused
-- memgres run with -Duser.language=tr -Duser.country=TR
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_l2 (a interval MINUTE TO SECOND);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_am (j jsonb, t tsvector);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_am_gin ON zz_vf2_am USING GIN (j);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_am_gist ON zz_vf2_am USING GIST (t);
-- begin-expected
-- columns: relname:name | amname:name
-- row: zz_vf2_am_gin | gin
-- row: zz_vf2_am_gist | gist
-- rowcount: 2
-- end-expected
SELECT c.relname, a.amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam
 WHERE c.relname LIKE 'zz\_vf2\_am\_%' ORDER BY 1;
