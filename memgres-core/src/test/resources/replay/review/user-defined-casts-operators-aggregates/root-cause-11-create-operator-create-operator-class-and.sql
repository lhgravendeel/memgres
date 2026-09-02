-- source: review-2026-08.md
-- finding: Root cause 11: CREATE OPERATOR, CREATE OPERATOR CLASS and CREATE OPERATOR FAMILY store what they are given and resolve nothing
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 11: CREATE OPERATOR, CREATE OPERATOR CLASS and CREATE OPERATOR FAMILY store what they are given and resolve nothing
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_add(integer, integer) does not exist
-- end-expected-error
CREATE OPERATOR ###+ (LEFTARG=int, RIGHTARG=int, FUNCTION=zz_add, HASHES);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_add(integer, integer) does not exist
-- end-expected-error
CREATE OPERATOR ###- (LEFTARG=int, RIGHTARG=int, FUNCTION=zz_add, MERGES);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_add(integer, integer) does not exist
-- end-expected-error
CREATE OPERATOR ###* (LEFTARG=int, RIGHTARG=int, FUNCTION=zz_add, RESTRICT=eqsel);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_add(integer, integer) does not exist
-- end-expected-error
CREATE OPERATOR ###/ (LEFTARG=int, RIGHTARG=int, FUNCTION=zz_add, JOIN=eqjoinsel);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_eq(integer, integer) does not exist
-- end-expected-error
CREATE OPERATOR ###< (LEFTARG=int, RIGHTARG=int, FUNCTION=zz_eq, RESTRICT=zz_nosel);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_eq(integer, integer) does not exist
-- end-expected-error
CREATE OPERATOR ###> (LEFTARG=int, RIGHTARG=int, FUNCTION=zz_eq, JOIN=zz_nojoin);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: access method "zz_noam" does not exist
-- end-expected-error
CREATE OPERATOR FAMILY zz_f2 USING zz_noam;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: access method "zz_noam" does not exist
-- end-expected-error
CREATE OPERATOR CLASS zz_c1 FOR TYPE int4 USING zz_noam AS OPERATOR 3 =;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_notype" does not exist
-- end-expected-error
CREATE OPERATOR CLASS zz_c2 FOR TYPE zz_notype USING btree AS OPERATOR 3 =, FUNCTION 1 btint4cmp(int4,int4);
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: invalid operator number 9, must be between 1 and 5
-- end-expected-error
CREATE OPERATOR CLASS zz_c3 FOR TYPE int4 USING btree AS OPERATOR 9 =, FUNCTION 1 btint4cmp(int4,int4);
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: invalid operator number 0, must be between 1 and 5
-- end-expected-error
CREATE OPERATOR CLASS zz_c4 FOR TYPE int4 USING btree AS OPERATOR 0 =, FUNCTION 1 btint4cmp(int4,int4);
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: invalid function number 99, must be between 1 and 6
-- end-expected-error
CREATE OPERATOR CLASS zz_c5 FOR TYPE int4 USING btree AS OPERATOR 3 =, FUNCTION 99 btint4cmp(int4,int4);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer ###?# integer
-- end-expected-error
CREATE OPERATOR CLASS zz_c6 FOR TYPE int4 USING btree AS OPERATOR 1 ###?# (int4,int4), FUNCTION 1 btint4cmp(int4,int4);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_nocmp(integer, integer) does not exist
-- end-expected-error
CREATE OPERATOR CLASS zz_c7 FOR TYPE int4 USING btree AS OPERATOR 3 =, FUNCTION 1 zz_nocmp(int4,int4);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: operator family "zz_nofam" does not exist for access method "btree"
-- end-expected-error
CREATE OPERATOR CLASS zz_c9 FOR TYPE int4 USING btree FAMILY zz_nofam AS OPERATOR 3 =, FUNCTION 1 btint4cmp(int4,int4);
-- begin-expected-error
-- sqlstate: 42710
-- message-like: could not make operator class "zz_ca" be default for type int4
-- end-expected-error
CREATE OPERATOR CLASS zz_ca DEFAULT FOR TYPE int4 USING btree AS OPERATOR 3 =, FUNCTION 1 btint4cmp(int4,int4);
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR FAMILY zz_fam USING btree;
-- begin-expected
-- ok: 0
-- end-expected
ALTER OPERATOR FAMILY zz_fam USING btree ADD OPERATOR 3 = (int4, int4);
-- begin-expected
-- columns: count:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_amop WHERE amopfamily=(SELECT oid FROM pg_opfamily WHERE opfname='zz_fam');
-- begin-expected-error
-- sqlstate: 42704
-- message-like: operator 5(bigint,bigint) does not exist in operator family "zz_fam"
-- end-expected-error
ALTER OPERATOR FAMILY zz_fam USING btree DROP OPERATOR 5 (int8, int8);
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: invalid operator number 9, must be between 1 and 5
-- end-expected-error
ALTER OPERATOR FAMILY zz_fam USING btree ADD OPERATOR 9 = (int8, int8);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: bigint ###?# bigint
-- end-expected-error
ALTER OPERATOR FAMILY zz_fam USING btree ADD OPERATOR 3 ###?# (int8, int8);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: operator family "zz_fam" does not exist for access method "hash"
-- end-expected-error
ALTER OPERATOR FAMILY zz_fam USING hash ADD OPERATOR 1 = (int8, int8);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_notype" does not exist
-- end-expected-error
CREATE OPERATOR ###? (LEFTARG = zz_notype, RIGHTARG = int, FUNCTION = zz_add);
