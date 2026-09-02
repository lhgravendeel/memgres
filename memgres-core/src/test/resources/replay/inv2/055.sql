-- source: investigation-2026-08.md
-- finding: 55
-- title: jsonb_object's NULL-key error carries 22023 where PostgreSQL and the file's own json_build_object use 22004.
-- begin-expected-error
-- sqlstate: 22004
-- message-like: null value not allowed for object key
-- end-expected-error
SELECT jsonb_object(ARRAY[NULL,'a']::text[], ARRAY['1','2']::text[]);
-- begin-expected-error
-- sqlstate: 22004
-- message-like: null value not allowed for object key
-- end-expected-error
SELECT json_object(ARRAY[NULL,'a']::text[], ARRAY['1','2']::text[]);
