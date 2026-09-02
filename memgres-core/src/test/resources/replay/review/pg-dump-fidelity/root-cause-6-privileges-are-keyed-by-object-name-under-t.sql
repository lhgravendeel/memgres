-- source: review-2026-08.md
-- finding: Root cause 6: privileges are keyed by object *name* under the literal type TABLE, and the policy grammar reads one identifier
-- area: pg_dump fidelity
-- title: Root cause 6: privileges are keyed by object *name* under the literal type TABLE, and the policy grammar reads one identifier
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_vf2_q5" does not exist
-- end-expected-error
GRANT SELECT, USAGE ON SEQUENCE zz_vf2_q5 TO PUBLIC;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_vf2_f5(integer) does not exist
-- end-expected-error
GRANT EXECUTE ON FUNCTION zz_vf2_f5(integer) TO PUBLIC;
-- begin-expected
-- columns: seq_acl:text | func_acl:text
-- row: NULL | NULL
-- rowcount: 1
-- end-expected
SELECT (SELECT array_to_string(relacl,' ') FROM pg_class WHERE relname='zz_vf2_q5') AS seq_acl,
       (SELECT array_to_string(proacl,' ') FROM pg_proc WHERE proname='zz_vf2_f5') AS func_acl;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_vf2_s6;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_s6.zz_vf2_t6 (id integer, n integer);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf2_s6.zz_vf2_t6 ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_vf2_po6 ON zz_vf2_s6.zz_vf2_t6 FOR SELECT USING (n > 0);
