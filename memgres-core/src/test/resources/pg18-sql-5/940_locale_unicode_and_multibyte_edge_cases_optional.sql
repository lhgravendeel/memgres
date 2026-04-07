DROP SCHEMA IF EXISTS test_940 CASCADE;
CREATE SCHEMA test_940;
SET search_path TO test_940;

CREATE TABLE words (
    word_id integer PRIMARY KEY,
    word_text text NOT NULL
);

INSERT INTO words VALUES
(1, 'café'),
(2, 'Cafe'),
(3, '東京'),
(4, 'ångström');

-- begin-expected
-- columns: word_id,char_len,byte_len
-- row: 1|4|5
-- row: 2|4|4
-- row: 3|2|6
-- row: 4|8|10
-- end-expected
SELECT word_id, char_length(word_text) AS char_len, octet_length(word_text) AS byte_len
FROM words
ORDER BY word_id;

-- begin-expected
-- columns: word_id,matches_prefix
-- row: 1|t
-- row: 2|f
-- row: 3|f
-- row: 4|f
-- end-expected
SELECT word_id, word_text LIKE 'caf%' AS matches_prefix
FROM words
ORDER BY word_id;

-- begin-expected
-- columns: word_id,lowered
-- row: 1|café
-- row: 2|cafe
-- row: 3|東京
-- row: 4|ångström
-- end-expected
SELECT word_id, lower(word_text) AS lowered
FROM words
ORDER BY word_id;

