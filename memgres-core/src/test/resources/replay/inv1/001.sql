-- source: investigation.md
-- finding: 1
-- title: Type checking is systemically permissive ⚠️
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: text = integer
-- end-expected-error
SELECT '5'::text = 5;
-- PG: 42883 operator does not exist: text = integer  | mg: true
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: text > integer
-- end-expected-error
SELECT '5'::text > 4;
-- PG: 42883                                          | mg: true
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer = text
-- end-expected-error
SELECT 1 IS DISTINCT FROM 'a'::text;
-- PG: 42883                                 | mg: true
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer = boolean
-- end-expected-error
SELECT 1::int = true;
-- PG: 42883 integer = boolean                        | mg: false
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: text + integer
-- end-expected-error
SELECT '10'::text + 5;
-- PG: 42883 text + integer                           | mg: 15
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: character varying * integer
-- end-expected-error
SELECT '3'::varchar * 2;
-- PG: 42883 character varying * integer              | mg: 6
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: text / integer
-- end-expected-error
SELECT '7'::text / 2;
-- PG: 42883                                          | mg: 3
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer - text
-- end-expected-error
SELECT 5 - '2'::text;
-- PG: 42883                                          | mg: 3
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: json > json
-- end-expected-error
SELECT '1'::json > '2'::json;
-- PG: 42883                                          | mg: succeeds
-- begin-expected-error
-- sqlstate: 42804
-- message-like: CASE types text and integer cannot be matched
-- end-expected-error
SELECT CASE WHEN true THEN 1 ELSE '2'::text END;
-- PG: 42804 CASE types cannot be matched
-- begin-expected-error
-- sqlstate: 42804
-- message-like: GREATEST types text and integer cannot be matched
-- end-expected-error
SELECT GREATEST('10'::text, 9);
-- PG: 42804 GREATEST types cannot be matched
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
SELECT COALESCE(1::int, 'x');
-- PG: 22P02 invalid input syntax for integer
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "2.5"
-- end-expected-error
SELECT '2.5'::integer;
-- PG: 22P02   | mg: 3
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type bigint: "2.9"
-- end-expected-error
SELECT '2.9'::bigint;
-- PG: 22P02   | mg: 2
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "null"
-- end-expected-error
SELECT 'null'::integer IS NULL;
-- PG: 22P02  | mg: true   (same for boolean, date, uuid)
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT '{"a": 1} trailing'::json;
-- PG: 22P02  | mg: accepted verbatim
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT '{a: 1}'::json;
-- PG: 22P02  | mg: accepted
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT '"abc'::json;
-- PG: 22P02  | mg: accepted
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT '007'::json, '+1'::json, '1.'::json, '.5'::json;
-- PG: 22P02 each | mg: all accepted
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT '[1,2] [3]'::jsonb;
-- PG: 22P02  | mg: [1, 2, , []]   (silently mangled)
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type numeric to boolean
-- end-expected-error
SELECT (0.5::numeric)::boolean;
-- PG: 42846 cannot cast numeric to boolean | mg: false
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type double precision to boolean
-- end-expected-error
SELECT (1.5::float8)::boolean;
-- PG: 42846                                | mg: true
-- begin-expected
-- columns: bool:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'tr'::boolean;
-- PG: true (unique prefix of "true")  | mg: 22P02
-- begin-expected
-- columns: bool:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT 'fals'::boolean;
-- PG: false                           | mg: 22P02
-- begin-expected
-- columns: bool:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'ye'::boolean;
-- PG: true                            | mg: 22P02
-- begin-expected
-- columns: int4:int4
-- row: 42
-- rowcount: 1
-- end-expected
SELECT '0x2a'::int;
-- PG: 42 (hex literal)                | mg: 22P02
-- begin-expected
-- columns: int4:int4
-- row: 1000
-- rowcount: 1
-- end-expected
SELECT '1_000'::int;
-- PG: 1000 (underscore separators)    | mg: 22P02
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT 1e30::float8::bigint;
-- PG: 22003 bigint out of range | mg: 9223372036854775807
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT 'Infinity'::float8::bigint;
-- PG: 22003                     | mg: 9223372036854775807
-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT 'NaN'::float8::bigint;
-- PG: 22003                     | mg: 0
-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT 2147483647.6::float8::int;
-- PG: 22003 integer out of range| mg: 2147483647
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "ty_pos" does not exist
-- end-expected-error
SELECT (-1)::ty_pos;
-- PG: 23514 violates check constraint | mg: -1;
