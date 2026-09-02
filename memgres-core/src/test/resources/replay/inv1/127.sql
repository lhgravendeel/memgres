-- source: investigation.md
-- finding: 127
-- title: `EXIT` / `CONTINUE` label validation is absent ⚠️
-- unrunnable: the report wrote this reproducer abbreviated
$$ begin begin continue; end; end; $$   -- PG: 42601 CONTINUE cannot be used outside a loop | mg: created
$$ begin begin exit; end; end; $$       -- PG: 42601 EXIT cannot be used outside a loop … | mg: created
$$ … loop continue pl_no_such_label; end loop … $$
--   PG: 42601 there is no label "pl_no_such_label" attached to any block or loop … | mg: created
$$ … loop exit pl_no_such_label; end loop … $$                      -- PG: 42601 | mg: created
$$ <<pl_begin_block1>> begin loop continue pl_begin_block1; end loop; … $$
--   PG: 42601 block label "pl_begin_block1" cannot be used in CONTINUE | mg: created;;
