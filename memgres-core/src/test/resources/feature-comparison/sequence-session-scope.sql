-- ============================================================================
-- Feature Comparison: currval is scoped to the session
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- currval reports what this connection last drew from the sequence, so it must
-- be answered from per-session state. A sequence's own counter is shared with
-- every other connection, and reporting from it hands one caller another
-- caller's generated key. Cross-session behaviour is covered by the unit test;
-- this file covers the single-session contract.
-- ============================================================================

DROP SEQUENCE IF EXISTS sss_s CASCADE;
CREATE SEQUENCE sss_s;

-- currval before this session has drawn anything
-- (lastval is deliberately not probed here: it reflects every draw the session
-- has made, so it depends on what ran before this file)
SELECT currval('sss_s')::text AS a;

-- after a draw it reports that value, repeatably
SELECT nextval('sss_s')::text AS a;
SELECT currval('sss_s')::text AS a;
SELECT currval('sss_s')::text AS a;
SELECT lastval()::text AS a;
SELECT nextval('sss_s')::text AS a;
SELECT currval('sss_s')::text AS a;

-- setval defines currval for the calling session
SELECT setval('sss_s', 42)::text AS a;
SELECT currval('sss_s')::text AS a;
SELECT nextval('sss_s')::text AS a;

-- setval with is_called false leaves currval undefined again
DROP SEQUENCE IF EXISTS sss_s2 CASCADE;
CREATE SEQUENCE sss_s2;
SELECT setval('sss_s2', 42, false)::text AS a;
SELECT currval('sss_s2')::text AS a;
SELECT nextval('sss_s2')::text AS a;
SELECT currval('sss_s2')::text AS a;

-- currval names a sequence that does not exist
SELECT currval('sss_no_such')::text AS a;

-- a serial column's sequence behaves the same way
DROP TABLE IF EXISTS sss_t CASCADE;
CREATE TABLE sss_t (i serial, v text);
SELECT currval('sss_t_i_seq')::text AS a;
INSERT INTO sss_t (v) VALUES ('a');
SELECT currval('sss_t_i_seq')::text AS a;
SELECT lastval()::text AS a;
INSERT INTO sss_t (v) VALUES ('b');
SELECT currval('sss_t_i_seq')::text AS a;
SELECT string_agg(i::text, ',' ORDER BY i) AS a FROM sss_t;

-- the sequence counter itself is shared, so a draw always advances it
DROP SEQUENCE IF EXISTS sss_s3 CASCADE;
CREATE SEQUENCE sss_s3;
SELECT nextval('sss_s3')::text AS a;
SELECT nextval('sss_s3')::text AS a;
SELECT last_value::text AS a FROM sss_s3;
-- and a rolled back draw is still consumed
BEGIN;
SELECT nextval('sss_s3')::text AS a;
ROLLBACK;
SELECT nextval('sss_s3')::text AS a;

DROP TABLE IF EXISTS sss_t CASCADE;
DROP SEQUENCE IF EXISTS sss_s3 CASCADE;
DROP SEQUENCE IF EXISTS sss_s2 CASCADE;
DROP SEQUENCE IF EXISTS sss_s CASCADE;
