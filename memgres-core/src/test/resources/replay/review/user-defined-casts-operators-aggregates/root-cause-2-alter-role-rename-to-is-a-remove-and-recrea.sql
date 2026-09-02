-- source: review-2026-08.md
-- finding: Root cause 2: ALTER ROLE ... RENAME TO is a remove-and-recreate that moves only the attribute map
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 2: ALTER ROLE ... RENAME TO is a remove-and-recreate that moves only the attribute map
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t1 (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_grp;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_old;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_t1 TO zz_old;
-- begin-expected
-- ok: 0
-- end-expected
GRANT zz_grp TO zz_old;
-- begin-expected
-- ok: 0
-- end-expected
ALTER ROLE zz_old RENAME TO zz_new;
-- begin-expected
-- columns: has_table_privilege:text | pg_has_role:text
-- row: true | true
-- rowcount: 1
-- end-expected
SELECT has_table_privilege('zz_new','zz_t1','SELECT')::text, pg_has_role('zz_new','zz_grp','MEMBER')::text;
