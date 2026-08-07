-- ============================================================================
-- Feature Comparison: what an array is an array of
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- An array takes its type from its elements, and each element keeps its own --
-- which is why an array of character(5) holds values padded to five and prints
-- them that way. memgres read only the composite and the enum element types,
-- so every other written one answered text[] and the elements were read back
-- as texts: an array of character was not one of character.
--
-- Two things followed from that and are settled here beside it. Subscripting
-- an array the statement built answered jsonb whatever the array was of, since
-- only a column of one was ever read. And array_to_string trimmed every
-- element, dropping the spaces a text element really has as readily as the
-- blanks a bpchar element is declared with.
-- ============================================================================

SET search_path = public;

DROP TABLE IF EXISTS ae_t;

CREATE TABLE ae_t (c char(5), a char(5)[], ta text[]);

INSERT INTO ae_t VALUES ('ab', ARRAY['ab'::char(5)], ARRAY['a  ']);

-- ============================================================================
-- What it is an array of
-- ============================================================================
-- begin-expected
-- columns: r
-- row: character[]
-- end-expected
SELECT pg_typeof(ARRAY['ab'::char(5)])::text AS r;

-- begin-expected
-- columns: r
-- row: character varying[]
-- end-expected
SELECT pg_typeof(ARRAY['a'::varchar])::text AS r;

-- begin-expected
-- columns: r
-- row: text[]
-- end-expected
SELECT pg_typeof(ARRAY['a'::text])::text AS r;

-- begin-expected
-- columns: r
-- row: integer[]
-- end-expected
SELECT pg_typeof(ARRAY[1])::text AS r;

-- begin-expected
-- columns: r
-- row: character[]
-- end-expected
SELECT pg_typeof(ARRAY[c])::text AS r FROM ae_t;

-- begin-expected
-- columns: r
-- row: character[]
-- end-expected
SELECT pg_typeof(a)::text AS r FROM ae_t;

-- Elements that disagree are left to the widening their values show.
-- begin-expected
-- columns: r
-- row: bigint[]
-- end-expected
SELECT pg_typeof(ARRAY[1::smallint, 2::bigint])::text AS r;

-- begin-expected
-- columns: r
-- row: integer[]
-- end-expected
SELECT pg_typeof(ARRAY[1,2,3])::text AS r;

-- ============================================================================
-- What an element keeps
-- ============================================================================
-- begin-expected
-- columns: r
-- row: {"ab   "}
-- end-expected
SELECT (ARRAY['ab'::char(5)])::text AS r;

-- begin-expected
-- columns: r
-- row: {"c    "}
-- end-expected
SELECT ('{c}'::char(5)[])::text AS r;

-- begin-expected
-- columns: r
-- row: {"ab   ","c    "}
-- end-expected
SELECT (ARRAY['ab'::char(5)] || 'c'::char(5))::text AS r;

-- begin-expected
-- columns: r
-- row: {"ab   ","c    "}
-- end-expected
SELECT (ARRAY['ab'::char(5)] || ARRAY['c'::char(5)])::text AS r;

-- begin-expected
-- columns: r
-- row: {"ab   "}
-- end-expected
SELECT a::text AS r FROM ae_t;

-- An array is written with its braces, whatever its elements are.
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "c"
-- end-expected-error
SELECT 'c'::char(5)[];

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "c"
-- end-expected-error
SELECT 'c'::text[];

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "c"
-- end-expected-error
SELECT 'c'::varchar[];

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "c"
-- end-expected-error
SELECT 'c'::int[];

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "c"
-- end-expected-error
SELECT 'c'::name[];

-- ============================================================================
-- Reading one back out
-- ============================================================================
-- begin-expected
-- columns: r
-- row: text
-- end-expected
SELECT pg_typeof((ARRAY['a'::text])[1])::text AS r;

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_typeof((ARRAY[1])[1])::text AS r;

-- begin-expected
-- columns: r
-- row: character
-- end-expected
SELECT pg_typeof((ARRAY['ab'::char(5)])[1])::text AS r;

-- begin-expected
-- columns: r
-- row: character
-- end-expected
SELECT pg_typeof(a[1])::text AS r FROM ae_t;

-- Read as a text it drops the blanks its own type was padded to.
-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT (ARRAY['ab'::char(5)])[1]::text AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT a[1]::text AS r FROM ae_t;

-- begin-expected
-- columns: r
-- row: a  
-- end-expected
SELECT (ARRAY['a  '::text])[1] AS r;

-- array_to_string writes each element as it is held, spaces and all.
-- begin-expected
-- columns: r
-- row: a  
-- end-expected
SELECT array_to_string(ARRAY['a  '::text], ',') AS r;

-- begin-expected
-- columns: r
-- row:   a  
-- end-expected
SELECT array_to_string(ARRAY['  a  '::text], ',') AS r;

-- begin-expected
-- columns: r
-- row: a  |b
-- end-expected
SELECT array_to_string(ARRAY['a  '::text, 'b'], '|') AS r;

-- begin-expected
-- columns: r
-- row: a  
-- end-expected
SELECT array_to_string(ta, ',') AS r FROM ae_t;

-- begin-expected
-- columns: r
-- row: ab   
-- end-expected
SELECT array_to_string(ARRAY['ab'::char(5)], ',') AS r;

-- begin-expected
-- columns: r
-- row: ab   
-- end-expected
SELECT array_to_string(a, ',') AS r FROM ae_t;

-- What had nothing to trim is unchanged.
-- begin-expected
-- columns: r
-- row: 1,2
-- end-expected
SELECT array_to_string(ARRAY[1,2], ',') AS r;

-- begin-expected
-- columns: r
-- row: a,b
-- end-expected
SELECT array_to_string(ARRAY['a','b'], ',') AS r;

-- begin-expected
-- columns: r
-- row: a,b
-- end-expected
SELECT array_to_string(ARRAY['a', NULL, 'b'], ',') AS r;

-- begin-expected
-- columns: r
-- row: a,n,b
-- end-expected
SELECT array_to_string(ARRAY['a', NULL, 'b'], ',', 'n') AS r;

DROP TABLE IF EXISTS ae_t;

