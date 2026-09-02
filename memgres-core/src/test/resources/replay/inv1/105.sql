-- source: investigation.md
-- finding: 105
-- title: Thirty operators have neither operand types nor a result type
-- begin-expected
-- columns: oprname:name
-- rowcount: 0
-- end-expected
SELECT oprname FROM pg_operator WHERE oprname = '' OR oprresult = 0;
-- PG: 0  memgres: 30
-- begin-expected
-- columns: oprname:name
-- rowcount: 0
-- end-expected
SELECT oprname FROM pg_operator WHERE oprkind = 'b'
   AND (oprleft = 0 OR oprright = 0);
-- PG: 0  memgres: 30
--   +, -, *, /, %, ||, &&, ~~, …;
