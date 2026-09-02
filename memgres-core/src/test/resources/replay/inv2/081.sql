-- source: investigation-2026-08.md
-- finding: 81
-- title: A lexeme is carried as a java.lang.String, so it inherits Java's case mapping (full, locale-sensitive) and String.compareTo's UTF-16 ordering where PostgreSQL u
-- begin-expected
-- columns: lower:text | casefold:text | ?column?:bool
-- row: ß | ß | t
-- rowcount: 1
-- end-expected
SELECT lower(U&'\1E9E'), casefold(U&'\1E9E'), casefold(U&'\1E9E') = lower(U&'\1E9E');
-- begin-expected
-- columns: length:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT length(casefold(U&'\0130'));
-- begin-expected
-- columns: to_tsvector:text
-- row: 'i':1
-- rowcount: 1
-- end-expected
SELECT to_tsvector('simple', U&'\0130')::text;
-- begin-expected
-- columns: ts_lexize:text
-- row: {i}
-- rowcount: 1
-- end-expected
SELECT ts_lexize('simple', U&'\0130')::text;
-- begin-expected
-- columns: array_to_tsvector:text
-- row: 'é' 'Ａ' '😀'
-- rowcount: 1
-- end-expected
SELECT array_to_tsvector(ARRAY[chr(65313), chr(128512), chr(233)])::text;
-- begin-expected
-- columns: array_to_tsvector:text
-- row: '豈' '𠀀'
-- rowcount: 1
-- end-expected
SELECT array_to_tsvector(ARRAY[chr(63744), chr(131072)])::text;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT array_to_tsvector(ARRAY[chr(128512)]) < array_to_tsvector(ARRAY[chr(57344)]);
