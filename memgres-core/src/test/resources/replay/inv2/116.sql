-- source: investigation-2026-08.md
-- finding: 116
-- title: Identifier and function-name case folding uses toLowerCase() with the JVM default locale in the aggregate/SRF/CTE lookups while the type-resolution sites use Lo
-- JVM default locale tr-TR (Locale.setDefault(new Locale("tr","TR")) or -Duser.language=tr -Duser.country=TR)
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_users (n int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_users VALUES (999);
-- begin-expected
-- columns: min:int4
-- row: 999
-- rowcount: 1
-- end-expected
SELECT MIN(n) FROM zz_vf_users;
-- begin-expected
-- columns: string_agg:text
-- row: 999
-- rowcount: 1
-- end-expected
SELECT STRING_AGG(n::text, ',') FROM zz_vf_users;
