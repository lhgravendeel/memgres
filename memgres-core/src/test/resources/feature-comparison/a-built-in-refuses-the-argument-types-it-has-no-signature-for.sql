-- A polymorphic parameter stands for one kind of type, not for any type.
--
-- lower is declared over text, over anyrange and over anymultirange, so lower(true) is 42883
-- "function lower(boolean) does not exist" with the standard hint: a boolean is none of those
-- three. memgres let an argument past every polymorphic parameter, so lower(true) reached the
-- range signature and answered, and so did the whole family that anyarray, anyrange and
-- anymultirange guard.
--
-- The argument is named in the message by the type it was written with -- lower(1.5) says
-- numeric, not integer -- and every call whose argument really is a range, a multirange or an
-- array has to keep answering, which is the second half of the file.

-- ---------------------------------------------------------------------------
-- 1. lower and upper are the string function unless the argument is a range
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(boolean) does not exist
-- end-expected-error
SELECT lower(true);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(integer) does not exist
-- end-expected-error
SELECT lower(1);

-- The type named is the one written, not the one it would widen to.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(numeric) does not exist
-- end-expected-error
SELECT lower(1.5);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(timestamp with time zone) does not exist
-- end-expected-error
SELECT lower(now());

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function upper(boolean) does not exist
-- end-expected-error
SELECT upper(true);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function upper(integer) does not exist
-- end-expected-error
SELECT upper(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function upper(numeric) does not exist
-- end-expected-error
SELECT upper(1.5);

-- ---------------------------------------------------------------------------
-- 2. The range predicates take a range, and a text is not one
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function isempty(integer) does not exist
-- end-expected-error
SELECT isempty(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function isempty(text) does not exist
-- end-expected-error
SELECT isempty('abc'::text);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower_inc(integer) does not exist
-- end-expected-error
SELECT lower_inc(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function upper_inf(integer) does not exist
-- end-expected-error
SELECT upper_inf(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function range_merge(integer, integer) does not exist
-- end-expected-error
SELECT range_merge(1, 2);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function multirange(integer) does not exist
-- end-expected-error
SELECT multirange(1);

-- ---------------------------------------------------------------------------
-- 3. The array functions take an array, and a scalar is not one
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function cardinality(integer) does not exist
-- end-expected-error
SELECT cardinality(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function cardinality(text) does not exist
-- end-expected-error
SELECT cardinality('abc'::text);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function array_length(integer, integer) does not exist
-- end-expected-error
SELECT array_length(1, 1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function array_ndims(integer) does not exist
-- end-expected-error
SELECT array_ndims(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function array_dims(integer) does not exist
-- end-expected-error
SELECT array_dims(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function array_position(integer, integer) does not exist
-- end-expected-error
SELECT array_position(1, 1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function array_append(integer, integer) does not exist
-- end-expected-error
SELECT array_append(1, 1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function array_reverse(integer) does not exist
-- end-expected-error
SELECT array_reverse(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function trim_array(integer, integer) does not exist
-- end-expected-error
SELECT trim_array(1, 1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function generate_subscripts(integer, integer) does not exist
-- end-expected-error
SELECT generate_subscripts(1, 1);

-- width_bucket over an array of bounds is the only two-argument one, and it wants the array.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function width_bucket(integer, integer) does not exist
-- end-expected-error
SELECT width_bucket(1, 2);

-- ---------------------------------------------------------------------------
-- 4. What the same names answer when the argument is what they are declared over
-- ---------------------------------------------------------------------------

-- begin-expected
-- columns: a | b
-- row: abc|ABC
-- end-expected
SELECT lower('ABC'::text) AS a, upper('abc'::text) AS b;

-- begin-expected
-- columns: a | b
-- row: 1|5
-- end-expected
SELECT lower(int4range(1, 5)) AS a, upper(int4range(1, 5)) AS b;

-- begin-expected
-- columns: a | b
-- row: 1.5|5.5
-- end-expected
SELECT lower(numrange(1.5, 5.5)) AS a, upper(numrange(1.5, 5.5)) AS b;

-- begin-expected
-- columns: a | b | c
-- row: t|t|f
-- end-expected
SELECT isempty(int4range(1, 1)) AS a, lower_inc(int4range(1, 5)) AS b,
       upper_inf(int4range(1, 5)) AS c;

-- begin-expected
-- columns: a | b
-- row: [1,9)|{[1,5)}
-- end-expected
SELECT range_merge(int4range(1, 5), int4range(7, 9))::text AS a,
       multirange(int4range(1, 5))::text AS b;

-- begin-expected
-- columns: a | b
-- row: 1|t
-- end-expected
SELECT lower(int4multirange(int4range(1, 5))) AS a, isempty(int4multirange()) AS b;

-- begin-expected
-- columns: a | b | c
-- row: 3|3|1
-- end-expected
SELECT cardinality(ARRAY[1, 2, 3]) AS a, array_length(ARRAY[1, 2, 3], 1) AS b,
       array_ndims(ARRAY[1, 2, 3]) AS c;

-- begin-expected
-- columns: a | b
-- row: [1:3]|2
-- end-expected
SELECT array_dims(ARRAY[1, 2, 3]) AS a, array_position(ARRAY[1, 2, 3], 2) AS b;

-- begin-expected
-- columns: a | b
-- row: {1,2,3}|{1,2}
-- end-expected
SELECT array_append(ARRAY[1, 2], 3)::text AS a, trim_array(ARRAY[1, 2, 3], 1)::text AS b;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT generate_subscripts(ARRAY[1, 2], 1) AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT width_bucket(5, ARRAY[1, 4, 9]) AS a;
