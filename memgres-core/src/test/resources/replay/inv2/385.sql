-- source: investigation-2026-08.md
-- finding: 385
-- title: RowDescription metadata is derived from memgres's own narrow DataType enum rather than a type catalog. pgTypeSize enumerates eight cases and returns -1 for ever
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_vf_enum" does not exist
-- end-expected-error
CREATE TABLE zz_vf_len (d date, iv interval, u uuid, tz timetz, m macaddr,
                        p point, b box, l lseg, ci circle, mo money, e zz_vf_enum);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf_len" does not exist
-- end-expected-error
SELECT d, iv, u, tz, m, p, b, l, ci, mo, e FROM zz_vf_len;
-- begin-expected
-- columns: name:name | regtype:regtype | char:char
-- row: x | integer | a
-- rowcount: 1
-- end-expected
SELECT 'x'::name, 'int4'::regtype, 'a'::"char";
-- begin-expected
-- columns: varchar:varchar | bpchar:bpchar | numeric:numeric | bit:bit
-- row: abc | ab    | 1.500 | 101
-- rowcount: 1
-- end-expected
SELECT 'abc'::varchar(10), 'ab'::char(5), 1.5::numeric(8,3), '101'::bit(3);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_me (bt bit(4));
-- begin-expected
-- columns: bt:bit
-- rowcount: 0
-- end-expected
SELECT bt FROM zz_vf_me;
-- begin-expected
-- columns: char:char
-- row: a
-- rowcount: 1
-- end-expected
SELECT 'a'::"char";
-- begin-expected
-- columns: row:record
-- row: (1,a)
-- rowcount: 1
-- end-expected
SELECT (1,'a')::record;
-- begin-expected
-- columns: regtype:regtype
-- row: integer
-- rowcount: 1
-- end-expected
SELECT 'int4'::regtype;
-- begin-expected
-- columns: sql_identifier:name
-- row: x
-- rowcount: 1
-- end-expected
SELECT 'x'::information_schema.sql_identifier;
