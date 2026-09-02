-- source: investigation-2026-08.md
-- finding: 225
-- title: the cursor statements are matched keyword by keyword in a fixed order and never required to end: parseDeclareCursor reads BINARY then INSENSITIVE then [NO] SCRO
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE zz_g1 SCROLL BINARY CURSOR FOR SELECT 1;
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE zz_g2 INSENSITIVE BINARY CURSOR FOR SELECT 1;
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE zz_g3 BINARY BINARY CURSOR FOR SELECT 1;
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE zz_g4 SCROLL INSENSITIVE CURSOR FOR SELECT 1;
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE zz_g5 NO SCROLL BINARY CURSOR FOR SELECT 1;
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE zz_g6 SCROLL SCROLL CURSOR FOR SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FOR"
-- end-expected-error
DECLARE zz_g9 CURSOR WITH FOR SELECT 1;
-- begin-expected
-- columns: name:text | is_holdable:bool | is_scrollable:bool
-- rowcount: 0
-- end-expected
SELECT name, is_holdable, is_scrollable FROM pg_cursors WHERE name = 'zz_g9';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FOR"
-- end-expected-error
DECLARE zz_ga CURSOR WITHOUT FOR SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "CURSOR"
-- end-expected-error
DECLARE zz_gb NO CURSOR FOR SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "select"
-- end-expected-error
DECLARE select CURSOR FOR SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "all"
-- end-expected-error
DECLARE all CURSOR FOR SELECT 1;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "junk"
-- end-expected-error
FETCH NEXT FROM zz_gd junk;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "1"
-- end-expected-error
MOVE NEXT FROM zz_gd 1 2 3;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "junk"
-- end-expected-error
CLOSE zz_gd junk;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "junk"
-- end-expected-error
CLOSE ALL junk;
-- begin-expected-error
-- sqlstate: 42P11
-- message-like: cannot specify both SCROLL and NO SCROLL
-- end-expected-error
DECLARE zz_g7 SCROLL NO SCROLL CURSOR FOR SELECT 1;
-- begin-expected-error
-- sqlstate: 42P11
-- message-like: cannot specify both SCROLL and NO SCROLL
-- end-expected-error
DECLARE zz_g8 NO SCROLL SCROLL CURSOR FOR SELECT 1;
