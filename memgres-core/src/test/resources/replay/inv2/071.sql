-- source: investigation-2026-08.md
-- finding: 71
-- title: Several types have no runtime representation and are carried as java.lang.String, so pg_typeof reports text and operator dispatch sees text on both sides.
-- begin-expected
-- columns: pg_typeof:text
-- row: tsvector
-- rowcount: 1
-- end-expected
SELECT pg_typeof(to_tsvector('english','a'))::text;
-- begin-expected
-- columns: pg_typeof:text
-- row: tsquery
-- rowcount: 1
-- end-expected
SELECT pg_typeof(to_tsquery('english','a'))::text;
-- begin-expected
-- columns: pg_typeof:text
-- row: tsvector
-- rowcount: 1
-- end-expected
SELECT pg_typeof(strip('a'::tsvector))::text;
-- begin-expected
-- columns: pg_typeof:text
-- row: regconfig
-- rowcount: 1
-- end-expected
SELECT pg_typeof(get_current_ts_config())::text;
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
-- columns: tsquery_not:text
-- row: !'a'
-- rowcount: 1
-- end-expected
SELECT tsquery_not('a'::tsquery)::text;
-- begin-expected
-- columns: jsonb_to_tsvector:text
-- row: 'cat':1 'dog':3
-- rowcount: 1
-- end-expected
SELECT jsonb_to_tsvector('english', '{"a": "cats and dogs"}'::jsonb, '["string"]')::text;
-- begin-expected
-- columns: json_to_tsvector:text
-- row: 'cat':1
-- rowcount: 1
-- end-expected
SELECT json_to_tsvector('english', '{"a": "cats"}'::json, '["string"]')::text;
-- begin-expected
-- columns: ?column?:pg_lsn
-- row: 0/16B3757
-- rowcount: 1
-- end-expected
SELECT '0/16B374D'::pg_lsn + 10;
-- begin-expected
-- columns: ?column?:pg_lsn
-- row: 0/16B3743
-- rowcount: 1
-- end-expected
SELECT '0/16B374D'::pg_lsn - 10;
-- begin-expected
-- columns: pg_lsn_larger:pg_lsn
-- row: 0/2
-- rowcount: 1
-- end-expected
SELECT pg_lsn_larger('0/1'::pg_lsn, '0/2'::pg_lsn);
-- begin-expected
-- columns: xid8:xid8
-- row: 100
-- rowcount: 1
-- end-expected
SELECT '100'::xid8;
-- begin-expected
-- columns: pg_typeof:text
-- row: xid8
-- rowcount: 1
-- end-expected
SELECT pg_typeof(pg_current_xact_id())::text;
-- begin-expected
-- columns: pg_typeof:text
-- row: pg_snapshot
-- rowcount: 1
-- end-expected
SELECT pg_typeof(pg_current_snapshot())::text;
-- begin-expected
-- columns: txid_snapshot_xmin:int8
-- row: 100
-- rowcount: 1
-- end-expected
SELECT txid_snapshot_xmin('100:200:'::txid_snapshot);
