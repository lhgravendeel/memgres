-- ============================================================================
-- Feature Comparison: the blanks a character(n) declaration adds
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL stores a bpchar padded out to its declared width and reads it
-- back trimmed: the conversion from bpchar to any other string type drops the
-- trailing blanks, and every routine declared over text takes its argument
-- through that conversion.
--
-- memgres stored the padding and never dropped it, so the blanks came back out
-- of every one of them: length answered 5 where PostgreSQL answers 2, upper
-- answered "AB   ", reverse answered "   ba", and md5 hashed a different
-- string than PostgreSQL did. Of 69 ways to read one, 37 disagreed.
--
-- The padding is not always dropped. It is part of the value, so it stays
-- wherever the value is handled as a bpchar rather than read as a text: in
-- octet_length, which measures what is stored; in the routines declared over
-- "any", which write each argument as its own type writes itself; and in the
-- value a client is sent for a column of the type.
-- ============================================================================

SET search_path = public;

DROP TABLE IF EXISTS bp_t;

CREATE TABLE bp_t (c char(5), t text);

INSERT INTO bp_t VALUES ('ab', 'ab');

-- ============================================================================
-- Read as another string type
-- ============================================================================
-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT ('ab'::char(5))::text AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT ('ab'::char(5))::varchar AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT ('ab'::char(5))::name AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT c::text AS r FROM bp_t;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT c::varchar AS r FROM bp_t;

-- Read as itself it is what it was declared, blanks and all.
-- begin-expected
-- columns: r
-- row: ab   
-- end-expected
SELECT ('ab'::char(5))::char(5) AS r;

-- begin-expected
-- columns: r
-- row: ab   
-- end-expected
SELECT c AS r FROM bp_t;

-- begin-expected
-- columns: r
-- row: 5
-- end-expected
SELECT octet_length('ab'::char(5))::text AS r;

-- ============================================================================
-- How long it is
-- ============================================================================
-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT length('ab'::char(5))::text AS r;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT length(c)::text AS r FROM bp_t;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT char_length('ab'::char(5))::text AS r;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT character_length('ab'::char(5))::text AS r;

-- begin-expected
-- columns: r
-- row: 16
-- end-expected
SELECT bit_length('ab'::char(5))::text AS r;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT length('ab'::char(5)::text)::text AS r;

-- ============================================================================
-- Read by a routine declared over text
-- ============================================================================
-- begin-expected
-- columns: r
-- row: AB
-- end-expected
SELECT upper('ab'::char(5)) AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT lower('AB'::char(5)) AS r;

-- begin-expected
-- columns: r
-- row: Ab
-- end-expected
SELECT initcap('ab'::char(5)) AS r;

-- begin-expected
-- columns: r
-- row: ba
-- end-expected
SELECT reverse('ab'::char(5)) AS r;

-- begin-expected
-- columns: r
-- row: 187ef4436122d1cc2f40dc2b92f0eba0
-- end-expected
SELECT md5('ab'::char(5)) AS r;

-- begin-expected
-- columns: r
-- row: 97
-- end-expected
SELECT ascii('ab'::char(5))::text AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT substr('ab'::char(5), 1) AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT substr('ab'::char(5), 1, 4) AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT left('ab'::char(5), 4) AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT right('ab'::char(5), 4) AS r;

-- begin-expected
-- columns: r
-- row: .....ab
-- end-expected
SELECT lpad('ab'::char(5), 7, '.') AS r;

-- begin-expected
-- columns: r
-- row: ab.....
-- end-expected
SELECT rpad('ab'::char(5), 7, '.') AS r;

-- begin-expected
-- columns: r
-- row: az
-- end-expected
SELECT replace('ab'::char(5), 'b', 'z') AS r;

-- begin-expected
-- columns: r
-- row: az
-- end-expected
SELECT translate('ab'::char(5), 'b', 'z') AS r;

-- begin-expected
-- columns: r
-- row: az
-- end-expected
SELECT regexp_replace('ab'::char(5), 'b', 'z') AS r;

-- begin-expected
-- columns: r
-- row: abab
-- end-expected
SELECT repeat('ab'::char(5), 2) AS r;

-- begin-expected
-- columns: r
-- row: 'ab'
-- end-expected
SELECT quote_literal('ab'::char(5)) AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT quote_ident('ab'::char(5)) AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT trim('ab'::char(5)) AS r;

-- begin-expected
-- columns: r
-- row: a
-- end-expected
SELECT split_part('ab'::char(5), 'b', 1) AS r;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT strpos('ab'::char(5), 'b')::text AS r;

-- begin-expected
-- columns: r
-- row: {a,""}
-- end-expected
SELECT string_to_array('ab'::char(5), 'b')::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT starts_with('ab'::char(5), 'ab')::text AS r;

-- begin-expected
-- columns: r
-- row: AB
-- end-expected
SELECT upper(c) AS r FROM bp_t;

-- begin-expected
-- columns: r
-- row: ab|
-- end-expected
SELECT ('ab'::char(5) || '|')::text AS r;

-- ============================================================================
-- Read by a routine declared over "any", which writes it as itself
-- ============================================================================
-- begin-expected
-- columns: r
-- row: ab   |
-- end-expected
SELECT concat('ab'::char(5), '|') AS r;

-- begin-expected
-- columns: r
-- row: ab   -x
-- end-expected
SELECT concat_ws('-', 'ab'::char(5), 'x') AS r;

-- begin-expected
-- columns: r
-- row: ab   |
-- end-expected
SELECT format('%s|', 'ab'::char(5)) AS r;

-- begin-expected
-- columns: r
-- row: ab   |
-- end-expected
SELECT concat(c, '|') AS r FROM bp_t;

-- begin-expected
-- columns: r
-- row: "ab   "
-- end-expected
SELECT to_json('ab'::char(5))::text AS r;

-- begin-expected
-- columns: r
-- row: "ab   "
-- end-expected
SELECT to_jsonb('ab'::char(5))::text AS r;

-- ============================================================================
-- What the padding never changed
-- ============================================================================
-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('ab'::char(5) = 'ab')::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('ab'::char(5) = 'ab   ')::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('ab'::char(5) = 'ab'::text)::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('ab'::char(5) < 'ac')::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT (c = 'ab')::text AS r FROM bp_t;

-- begin-expected
-- columns: r
-- row: false
-- end-expected
SELECT ('ab'::char(5) LIKE 'ab')::text AS r;

-- A string that was never blank-padded is untouched by any of this.
-- begin-expected
-- columns: r
-- row: ab  
-- end-expected
SELECT ('ab  '::text) AS r;

-- begin-expected
-- columns: r
-- row: 4
-- end-expected
SELECT length('ab  '::text)::text AS r;

-- begin-expected
-- columns: r
-- row: AB  
-- end-expected
SELECT upper('ab  '::text) AS r;

-- begin-expected
-- columns: r
-- row: ab  
-- end-expected
SELECT ('ab  '::varchar(5)) AS r;

-- begin-expected
-- columns: r
-- row: 4
-- end-expected
SELECT length('ab  '::varchar(5))::text AS r;

-- begin-expected
-- columns: r
-- row:   ba
-- end-expected
SELECT reverse('ab  '::text) AS r;

-- begin-expected
-- columns: r
-- row: ab
-- end-expected
SELECT t AS r FROM bp_t;

DROP TABLE IF EXISTS bp_t;

