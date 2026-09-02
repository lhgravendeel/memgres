-- source: investigation-2026-08.md
-- finding: 37
-- title: JSON_TABLE does not coerce column values to the declared type (so ON ERROR never fires), and its row-generation plan emits only the first sibling NESTED path.
-- begin-expected
-- columns: a:int4
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT * FROM JSON_TABLE(jsonb '[{"a":"zz"}]', '$[*]' COLUMNS (a int PATH '$.a'));
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "zz"
-- end-expected-error
SELECT * FROM JSON_TABLE(jsonb '[{"a":"zz"}]', '$[*]' COLUMNS (a int PATH '$.a' ERROR ON ERROR));
-- begin-expected
-- columns: a:int4
-- row: -1
-- rowcount: 1
-- end-expected
SELECT * FROM JSON_TABLE(jsonb '[{"a":[1,2]}]','$[*]' COLUMNS (a int PATH '$.a' DEFAULT -1 ON ERROR));
-- begin-expected
-- columns: e:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT * FROM JSON_TABLE(jsonb '[{"a":1}]', '$[*]' COLUMNS (e int EXISTS PATH '$.a'));
-- begin-expected
-- columns: av:int4 | bv:int4
-- row: 1 | NULL
-- row: 2 | NULL
-- row: NULL | 3
-- rowcount: 3
-- end-expected
SELECT * FROM JSON_TABLE(jsonb '{"a":[1,2],"b":[3]}', '$' COLUMNS (NESTED '$.a[*]' COLUMNS (av int PATH '$'), NESTED '$.b[*]' COLUMNS (bv int PATH '$'))) ORDER BY 1,2;
-- begin-expected
-- columns: a:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
SELECT * FROM JSON_TABLE(jsonb '[{"a":1},{"a":2}]', '$[*]' AS root COLUMNS (a int PATH '$.a')) ORDER BY 1;
-- begin-expected
-- columns: a:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT * FROM JSON_TABLE(jsonb '[{"a":1}]', '$[*]' COLUMNS (a int PATH '$.a' NULL ON ERROR));
-- begin-expected
-- columns: x:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT * FROM JSON_TABLE(jsonb '[{"a":1}]', '$[*]' COLUMNS (a int PATH '$.a')) AS t(x);
