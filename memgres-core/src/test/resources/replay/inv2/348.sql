-- source: investigation-2026-08.md
-- finding: 348
-- title: Five places rebuild SQL text by appending Token.value(), which the lexer has already stripped of its double quotes, and re-quote only STRING_LITERAL — so a quot
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_qq ("c c" int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_qix ON zz_vf2_qq ("c c") WHERE "c c" > 0;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_gg ("a b" int, t int GENERATED ALWAYS AS ("a b" * 2) STORED);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_gr ("select" int, t int GENERATED ALWAYS AS ("select" * 2) STORED);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_qb ("y z" int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
CREATE FUNCTION zz_vf2_fb() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT max("y z") FROM zz_vf2_qb;
-- begin-expected
-- ok: 0
-- end-expected
END;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf2_f2() RETURNS int LANGUAGE sql RETURN (SELECT max("y z") FROM zz_vf2_qb);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_tw ("a b" int, n int);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_vf2_twf() does not exist
-- end-expected-error
CREATE TRIGGER zz_vf2_tg BEFORE INSERT ON zz_vf2_tw FOR EACH ROW WHEN (NEW."a b" > 0) EXECUTE FUNCTION zz_vf2_twf();
