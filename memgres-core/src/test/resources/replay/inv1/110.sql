-- source: investigation.md
-- finding: 110
-- title: Polymorphic types do not exist ⚠️ high — a missing feature, not a gap
-- unrunnable: the report wrote this reproducer abbreviated
CREATE FUNCTION f(x anyelement) RETURNS anyelement LANGUAGE sql AS $$ SELECT x $$;
--   PG: works | mg: 42704 type "anyelement" does not exist
CREATE FUNCTION f() RETURNS anyelement …;
-- no polymorphic argument to resolve from
--   PG: 42P13 cannot determine result data type | mg: created
CREATE FUNCTION f(x int) RETURNS anyarray …;
-- PG: 42P13 | mg: created
CREATE FUNCTION f(x anyelement) RETURNS anycompatible …;
-- cross-family; PG: 42P13 | mg: created;;
