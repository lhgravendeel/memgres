DROP TABLE IF EXISTS zz_rg_ex CASCADE;

DROP TYPE IF EXISTS zz_rg_fr CASCADE;

CREATE TABLE zz_rg_ex (r int4range, EXCLUDE USING gist (r WITH &&));

INSERT INTO zz_rg_ex VALUES ('[1,10)');

-- begin-expected
-- columns: text
-- row: empty
-- end-expected
SELECT '(5,5)'::int4range::text;

-- begin-expected
-- columns: text
-- row: empty
-- end-expected
SELECT '[5,5)'::int4range::text;

-- begin-expected-error
-- sqlstate: 22000
-- message-like: ERROR: range lower bound must be less than or equal to range upper bound
-- end-expected-error
SELECT '[5,4]'::int4range;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: ERROR: integer out of range
-- end-expected-error
SELECT '[-2147483648,2147483647]'::int4range;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: ERROR: bigint out of range
-- end-expected-error
SELECT '[1,9223372036854775807]'::int8range;

-- begin-expected
-- columns: text
-- row: (1,10]
-- end-expected
SELECT ('(1,5]'::numrange + '(5,10]'::numrange)::text;

-- begin-expected
-- columns: text
-- row: (3,5]
-- end-expected
SELECT ('(1,5]'::numrange * '(3,10]'::numrange)::text;

-- begin-expected
-- columns: text
-- row: (1,5]
-- end-expected
SELECT ('(1,10]'::numrange - '(5,10]'::numrange)::text;

-- begin-expected
-- columns: range_merge
-- row: (1,10]
-- end-expected
SELECT range_merge('(1,5]'::numrange,'(7,10]'::numrange)::text;

-- begin-expected
-- columns: nummultirange
-- row: {[1.5,4.5)}
-- end-expected
SELECT nummultirange(numrange(1.5,2.5), numrange(2.5,4.5))::text;

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '[1.5,2.5)'::numrange = '[1.50,2.50)'::numrange;

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '[1.0,2.0)'::numrange = '[1,2)'::numrange;

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT numrange(1.5,3.0) < numrange(1.9,3.0);

-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT '[1,5)'::int4range &< '[1,3)'::int4range;

-- begin-expected
-- columns: ?column?
-- row: f
-- end-expected
SELECT 'empty'::int4range &< '[1,2)'::int4range;

-- begin-expected
-- columns: ?column?
-- row: t
-- end-expected
SELECT '{[1,5)}'::int4multirange &< '{[3,10)}'::int4multirange;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: operator does not exist: int4range && numrange
-- end-expected-error
SELECT '[1,5)'::int4range && '[2,3)'::numrange;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: ERROR: operator does not exist: int4range + int4multirange
-- end-expected-error
SELECT '[1,5)'::int4range + '{[6,8)}'::int4multirange;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: invalid range bound flags
-- end-expected-error
SELECT int4range(1,5,'x');

-- begin-expected
-- columns: range_intersect_agg
-- row: {[5,10)}
-- end-expected
SELECT range_intersect_agg(m)::text FROM (VALUES ('{[1,10)}'::int4multirange),('{[5,20)}'::int4multirange)) v(m);

-- begin-expected-error
-- sqlstate: 23P01
-- message-like: ERROR: conflicting key value violates exclusion constraint "zz_rg_ex_r_excl"
-- end-expected-error
INSERT INTO zz_rg_ex VALUES ('[9,20)');

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM zz_rg_ex;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: type attribute "nosuchopt" not recognized
-- end-expected-error
CREATE TYPE zz_rg_cr1 AS RANGE (SUBTYPE = int4, NOSUCHOPT = 1);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: ERROR: cannot specify a canonical function without a pre-created shell type
-- end-expected-error
CREATE TYPE zz_rg_cr3 AS RANGE (SUBTYPE = int4, CANONICAL = missingfn);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: ERROR: data type json has no default operator class for access method "btree"
-- end-expected-error
CREATE TYPE zz_rg_cr6 AS RANGE (SUBTYPE = json);

CREATE TYPE zz_rg_fr AS RANGE (subtype = float8);

-- begin-expected
-- columns: text
-- row: [1.5,2.5)
-- end-expected
SELECT '[1.5,2.5)'::zz_rg_fr::text;

-- begin-expected
-- columns: pg_typeof
-- row: zz_rg_fr
-- end-expected
SELECT pg_typeof(zz_rg_fr(1.5,2.5))::text;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM pg_type WHERE typname LIKE 'zz\_rg\_fr%';

DROP TABLE zz_rg_ex;

DROP TYPE zz_rg_fr CASCADE;


-- A range ends at the bracket that closes it; what comes after belongs to nothing.
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: malformed range literal: "[1,2)]"
-- end-expected-error
SELECT '[1,2)]'::int4range;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: malformed range literal: "[1,2))"
-- end-expected-error
SELECT '[1,2))'::int4range;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: malformed range literal: "(1,2)x"
-- end-expected-error
SELECT '(1,2)x'::int4range;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: malformed range literal: "[)"
-- end-expected-error
SELECT '[)'::int4range;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: malformed range literal: "[1,2"
-- end-expected-error
SELECT '[1,2'::int4range;

-- Trailing space is not junk.
-- begin-expected
-- columns: text
-- row: [1,2)
-- end-expected
SELECT '[1,2) '::int4range::text;

-- A multirange literal is read as one wherever it stands, including beside an operator.
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: malformed multirange literal: "[1,2)"
-- end-expected-error
SELECT '[1,2)'::int4multirange;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: malformed multirange literal: "[1,2)"
-- end-expected-error
SELECT '{[1,3),[5,7)}'::int4multirange @> '[1,2)';

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: malformed multirange literal: "{[1,2)"
-- end-expected-error
SELECT '{[1,3)}'::int4multirange @> '{[1,2)';

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: malformed multirange literal: "{}x"
-- end-expected-error
SELECT '{[1,3)}'::int4multirange @> '{}x';
