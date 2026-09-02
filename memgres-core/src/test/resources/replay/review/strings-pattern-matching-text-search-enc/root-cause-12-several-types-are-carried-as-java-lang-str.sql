-- source: review-2026-08.md
-- finding: Root cause 12: several types are carried as java.lang.String at runtime
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Root cause 12: several types are carried as java.lang.String at runtime
-- begin-expected
-- columns: pg_typeof:regtype
-- row: tsvector
-- rowcount: 1
-- end-expected
SELECT pg_typeof(to_tsvector('english','a'));
-- begin-expected
-- columns: pg_typeof:regtype
-- row: regconfig
-- rowcount: 1
-- end-expected
SELECT pg_typeof(get_current_ts_config());
-- begin-expected
-- columns: ?column?:pg_lsn
-- row: 0/16B3757
-- rowcount: 1
-- end-expected
SELECT '0/16B374D'::pg_lsn + 10;
-- begin-expected
-- columns: pg_lsn_larger:pg_lsn
-- row: 0/2
-- rowcount: 1
-- end-expected
SELECT pg_lsn_larger('0/1'::pg_lsn, '0/2'::pg_lsn);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'cat'::tsvector @@@ 'cat'::tsquery;
-- begin-expected
-- columns: ts_match_vq:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT ts_match_vq('a'::tsvector, 'a'::tsquery);
-- begin-expected
-- columns: tsvector_cmp:int4
-- row: -1
-- rowcount: 1
-- end-expected
SELECT tsvector_cmp('a'::tsvector,'b'::tsvector);
-- begin-expected
-- columns: jsonb_to_tsvector:tsvector
-- row: 'cat':1 'dog':3
-- rowcount: 1
-- end-expected
SELECT jsonb_to_tsvector('english', '{"a": "cats and dogs"}'::jsonb, '["string"]');
-- begin-expected
-- columns: xid8:xid8
-- row: 100
-- rowcount: 1
-- end-expected
SELECT '100'::xid8;
-- begin-expected
-- columns: pg_typeof:regtype
-- row: xid8
-- rowcount: 1
-- end-expected
SELECT pg_typeof(pg_current_xact_id());
-- begin-expected
-- columns: pg_typeof:regtype
-- row: pg_snapshot
-- rowcount: 1
-- end-expected
SELECT pg_typeof(pg_current_snapshot());
-- begin-expected
-- columns: txid_snapshot_xmin:int8
-- row: 100
-- rowcount: 1
-- end-expected
SELECT txid_snapshot_xmin('100:200:'::txid_snapshot);
