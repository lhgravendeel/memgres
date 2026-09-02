-- source: review-2026-08.md
-- finding: Root cause 8: FETCH/MOVE counts go through a bare Integer.parseInt and no sign is accepted
-- area: SQL-level cursors
-- title: Root cause 8: FETCH/MOVE counts go through a bare Integer.parseInt and no sign is accepted
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "zz_h1" does not exist
-- end-expected-error
FETCH FORWARD +1 FROM zz_h1;
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "zz_h2" does not exist
-- end-expected-error
FETCH FORWARD -1 FROM zz_h2;
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "zz_h3" does not exist
-- end-expected-error
FETCH BACKWARD -1 FROM zz_h3;
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "zz_h4" does not exist
-- end-expected-error
FETCH +2 FROM zz_h4;
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "zz_h5" does not exist
-- end-expected-error
MOVE FORWARD +1 IN zz_h5;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "3000000000"
-- end-expected-error
FETCH 3000000000 FROM zz_h6;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "3000000000"
-- end-expected-error
FETCH FORWARD 3000000000 FROM zz_h7;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "3000000000"
-- end-expected-error
FETCH BACKWARD 3000000000 FROM zz_h8;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "4000000000"
-- end-expected-error
FETCH ABSOLUTE 4000000000 FROM zz_h9;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "9999999999"
-- end-expected-error
FETCH RELATIVE 9999999999 FROM zz_ha;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "4000000000"
-- end-expected-error
MOVE ABSOLUTE 4000000000 IN zz_hb;
