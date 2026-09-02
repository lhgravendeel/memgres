-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: The JSON implementation, second pass
-- title: Unrelated singletons
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
