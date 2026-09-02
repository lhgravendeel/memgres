-- source: investigation-2026-08.md
-- finding: 171
-- title: GRANT and REVOKE read the object name with a single readIdentifier() for every object kind except TABLE, so a schema qualification leaves the dot for the grante
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_q" does not exist
-- end-expected-error
GRANT USAGE   ON SEQUENCE zz_q.sq      TO zz_qr;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_q" does not exist
-- end-expected-error
GRANT USAGE   ON DOMAIN   zz_q.dm      TO zz_qr;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_q" does not exist
-- end-expected-error
GRANT USAGE   ON TYPE     zz_q.ty      TO zz_qr;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_q" does not exist
-- end-expected-error
GRANT EXECUTE ON FUNCTION zz_q.fn(int) TO zz_qr;
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "zz_q" does not exist
-- end-expected-error
REVOKE USAGE  ON SEQUENCE zz_q.sq    FROM zz_qr;
