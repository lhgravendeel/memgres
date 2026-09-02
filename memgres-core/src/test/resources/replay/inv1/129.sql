-- source: investigation.md
-- finding: 129
-- title: `=` as a declaration initialiser is not accepted ⚠️ rejects valid SQL
-- unrunnable: the report wrote this reproducer abbreviated
CREATE FUNCTION f() … AS $$ declare a int = 10; b int = 1; begin … end $$;
--   PG: works — PL/pgSQL accepts `=` as a synonym for `:=` in DECLARE
--   mg: created, then on call: XX000 Internal error: missing semicolon after variable declaration;;
