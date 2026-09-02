-- source: investigation.md
-- finding: 128
-- title: Loop end-labels: not parsed where valid, not checked where invalid ⚠️ both directions
-- unrunnable: the report wrote this reproducer abbreviated
-- valid, PG accepts:
<<pl_flbl1>> for i in 1 .. 10 loop exit pl_flbl1;
end loop pl_flbl1;
--   PG: works | mg: 42601 syntax error at or near "pl_flbl1"

-- invalid, PG rejects:
for _i in 1 .. 10 loop exit;
end loop pl_flbl1;
-- unlabeled block
--   PG: 42601 end label specified for unlabeled block | mg: created
<<pl_outer>> begin <<pl_inner>> for … end loop pl_outer;
end;
--   PG: 42601 end label "pl_outer" differs from block's label "pl_inner" | mg: created;;
