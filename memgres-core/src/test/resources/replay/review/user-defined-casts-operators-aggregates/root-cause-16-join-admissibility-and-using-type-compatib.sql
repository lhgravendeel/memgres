-- source: review-2026-08.md
-- finding: Root cause 16: join admissibility and USING type compatibility are decided from hand-written enum lists
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 16: join admissibility and USING type compatibility are decided from hand-written enum lists
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_fa" does not exist
-- end-expected-error
SELECT a.id, b.id FROM zz_fa a FULL JOIN zz_fb b ON a.arr && b.arr;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_fa" does not exist
-- end-expected-error
SELECT a.id, b.id FROM zz_fa a FULL JOIN zz_fb b ON a.j @> b.j;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_fa" does not exist
-- end-expected-error
SELECT a.id, b.id FROM zz_fa a FULL JOIN zz_fb b ON a.t @@ b.q;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_fa" does not exist
-- end-expected-error
SELECT a.id, b.id FROM zz_fa a FULL JOIN zz_fb b ON a.arr <@ b.arr;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_fa" does not exist
-- end-expected-error
SELECT a.id, b.id FROM zz_fa a FULL JOIN zz_fb b ON a.p ~= b.p;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_u1" does not exist
-- end-expected-error
SELECT a,b FROM zz_u1 JOIN zz_u2 USING (k);
-- uuid vs text
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_b1" does not exist
-- end-expected-error
SELECT a,b FROM zz_b1 JOIN zz_b2 USING (k);
-- boolean vs text
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_y1" does not exist
-- end-expected-error
SELECT a,b FROM zz_y1 JOIN zz_y2 USING (k);
-- bytea vs text;
