-- Built-in functions PG 18 has: splitting a qualified name, folding case for comparison,
-- reading what a UUID encodes, and the constructor behind a JSON literal.

-- begin-expected
-- columns: a
-- row: {a,b}
-- end-expected
SELECT parse_ident('a.b')::text AS a;

-- begin-expected
-- columns: a
-- row: {a,b,c}
-- end-expected
SELECT parse_ident('a.b.c')::text AS a;

-- An unquoted part folds to lower case; a quoted one keeps its spelling.
-- begin-expected
-- columns: a
-- row: {"A b",c}
-- end-expected
SELECT parse_ident('"A b".c')::text AS a;

-- begin-expected
-- columns: a
-- row: {abc}
-- end-expected
SELECT parse_ident('ABC')::text AS a;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: is not a valid identifier
-- end-expected-error
SELECT parse_ident('a.b.');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: is not a valid identifier
-- end-expected-error
SELECT parse_ident('"unclosed');

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (parse_ident(NULL) IS NULL)::text AS a;

-- casefold folds for comparison rather than for display.
-- begin-expected
-- columns: a
-- row: abc
-- end-expected
SELECT casefold('AbC') AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (casefold(NULL) IS NULL)::text AS a;

-- The version lives in the four bits after the third dash.
-- begin-expected
-- columns: a
-- row: 4
-- end-expected
SELECT uuid_extract_version('00000000-0000-4000-8000-000000000000'::uuid)::text AS a;

-- begin-expected
-- columns: a
-- row: 7
-- end-expected
SELECT uuid_extract_version(uuidv7())::text AS a;

-- Only version 7 carries the moment it was minted.
-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (uuid_extract_timestamp(uuidv7()) IS NOT NULL)::text AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (uuid_extract_timestamp('00000000-0000-4000-8000-000000000000'::uuid) IS NULL)::text AS a;

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (uuid_extract_version(NULL) IS NULL)::text AS a;

-- json(text) validates and yields a json value.
-- begin-expected
-- columns: a
-- row: {"a":1}
-- end-expected
SELECT json('{"a":1}')::text AS a;

-- begin-expected
-- columns: a
-- row: [1, 2]
-- end-expected
SELECT json('[1, 2]')::text AS a;

-- begin-expected
-- columns: a
-- row: {"a":1}
-- end-expected
SELECT JSON_SERIALIZE(JSON('{"a":1}')) AS a;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type json
-- end-expected-error
SELECT json('{oops');

-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT (json(NULL) IS NULL)::text AS a;
