\pset pager off
\pset format unaligned
\pset tuples_only off
\pset null <NULL>
\set VERBOSITY verbose
\set SHOW_CONTEXT always
\set ON_ERROR_STOP off

DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;
SET client_min_messages = notice;
SET extra_float_digits = 0;
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC';

SELECT current_schema() AS current_schema,
       current_setting('TimeZone') AS timezone,
       current_setting('DateStyle') AS datestyle,
       current_setting('IntervalStyle') AS intervalstyle;

-- sequence depth
CREATE SEQUENCE seq1 START 1 INCREMENT 1 MINVALUE 1 MAXVALUE 5 CYCLE;
SELECT nextval('seq1'), nextval('seq1');
SELECT currval('seq1');
SELECT setval('seq1', 4, true);
SELECT nextval('seq1'), nextval('seq1'), nextval('seq1');
SELECT lastval();

CREATE TABLE seqtab(
  id int DEFAULT nextval('seq1'),
  note text
);
ALTER SEQUENCE seq1 OWNED BY seqtab.id;
INSERT INTO seqtab(note) VALUES ('a'), ('b');
SELECT * FROM seqtab ORDER BY id;

CREATE SEQUENCE seq2 START 10 INCREMENT -2 MINVALUE 0 MAXVALUE 10;
SELECT nextval('seq2'), nextval('seq2');

-- XML
SELECT xmlparse(document '<root><a>1</a></root>');
SELECT xmlelement(name person, xmlattributes(1 AS id), 'ann');
SELECT xmlforest('ann' AS name, 42 AS age);
SELECT xmlserialize(content xmlelement(name x, 'abc') AS text);
SELECT xpath('/root/a/text()', xmlparse(document '<root><a>1</a><a>2</a></root>'));
SELECT xmlconcat(xmlelement(name a, 1), xmlelement(name b, 2));

-- full-text search
SELECT to_tsvector('english', 'The quick brown fox');
SELECT to_tsquery('english', 'quick & fox');
SELECT plainto_tsquery('english', 'quick fox');
SELECT phraseto_tsquery('english', 'quick brown');
SELECT to_tsvector('english', 'The quick brown fox') @@ to_tsquery('english', 'quick & fox');
SELECT ts_rank(to_tsvector('english', 'The quick brown fox'), to_tsquery('english', 'quick'));

-- formatting and quoting built-ins
SELECT format('%s %I %L', 'x', 'Mixed Name', 'quote me');
SELECT quote_ident('Mixed Name'), quote_literal(E'x\ny'), quote_nullable(NULL);

-- bad sequence/xml/fts/function cases
SELECT currval('seq_missing');
SELECT setval('seq1', 1000, true);
SELECT nextval('no_such_seq');
SELECT xmlparse(document '<root><a></root>');
SELECT xpath('not_xpath(', xmlparse(document '<root><a>1</a></root>'));
SELECT xmlserialize(document xmlelement(name x, 'abc') AS int);
SELECT to_tsquery('english', 'quick & & fox');
SELECT to_tsvector('no_such_config', 'abc');
SELECT ts_rank('x', to_tsquery('english', 'quick'));
SELECT format('%q', 'x');
SELECT quote_ident(NULL);

DROP SCHEMA compat CASCADE;
