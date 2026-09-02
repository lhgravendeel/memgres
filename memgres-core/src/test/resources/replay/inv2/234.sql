-- source: investigation-2026-08.md
-- finding: 234
-- title: NotificationManager keys its registry on channel.toLowerCase() at listen, unlisten and notify, so a quoted mixed-case channel is the same channel as its folded 
-- begin-expected
-- ok: 0
-- end-expected
LISTEN "Zz_Mixed";
-- begin-expected
-- columns: c:text
-- row: Zz_Mixed
-- rowcount: 1
-- end-expected
SELECT c FROM pg_listening_channels() c;
-- begin-expected
-- ok: 0
-- end-expected
UNLISTEN zz_mixed;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_listening_channels();
-- begin-expected
-- ok: 0
-- end-expected
UNLISTEN *;
-- begin-expected
-- ok: 0
-- end-expected
LISTEN zz_dual;
-- begin-expected
-- ok: 0
-- end-expected
LISTEN "ZZ_DUAL";
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_listening_channels();
