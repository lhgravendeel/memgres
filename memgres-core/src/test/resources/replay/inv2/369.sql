-- source: investigation-2026-08.md
-- finding: 369
-- title: The quoting functions are re-implemented at each call site rather than being one function: format's %L branch is a two-line inline quote that never reaches quot
-- begin-expected
-- columns: q:text | f:text
-- row: E'a\\b' | E'a\\b'
-- rowcount: 1
-- end-expected
SELECT quote_literal('a\b') AS q, format('%L','a\b') AS f;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf2_rt_idok(t text) RETURNS text LANGUAGE plpgsql AS $$
declare r text; begin
  execute 'select ' || quote_ident(t) || ' from (select ''v''::text as '
          || quote_ident(t) || ') s' into r;
  return r; end $$;
-- begin-expected
-- columns: zz_vf2_rt_idok:text
-- row: v
-- rowcount: 1
-- end-expected
SELECT zz_vf2_rt_idok('current_date');
-- begin-expected
-- columns: zz_vf2_rt_idok:text
-- row: v
-- rowcount: 1
-- end-expected
SELECT zz_vf2_rt_idok('trim');
-- begin-expected
-- columns: zz_vf2_rt_idok:text
-- row: v
-- rowcount: 1
-- end-expected
SELECT zz_vf2_rt_idok('coalesce');
