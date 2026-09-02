-- source: investigation.md
-- finding: 93
-- title: `pg_index` expression columns are typed `text`
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "indexprs"
-- end-expected-error
indexprs   PG: pg_node_tree   memgres: text
indpred    PG: pg_node_tree   memgres: text;
