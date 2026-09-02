-- source: review-2026-08.md
-- finding: Root cause 9: the column-definition and constraint-attribute loops are last-one-wins, with no record of what was already said
-- area: The parsers
-- title: Root cause 9: the column-definition and constraint-attribute loops are last-one-wins, with no record of what was already said
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_pk (id int primary key);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: constraint declared INITIALLY DEFERRED must be DEFERRABLE
-- end-expected-error
CREATE TABLE zz_vf2_fk (id int, p int REFERENCES zz_vf2_pk(id) NOT DEFERRABLE INITIALLY DEFERRED);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: constraint declared INITIALLY DEFERRED must be DEFERRABLE
-- end-expected-error
CREATE TABLE zz_vf2_fk2 (id int, p int,
  CONSTRAINT zz_vf2_c1 FOREIGN KEY (p) REFERENCES zz_vf2_pk(id) NOT DEFERRABLE INITIALLY DEFERRED);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: constraint declared INITIALLY DEFERRED must be DEFERRABLE
-- end-expected-error
CREATE TABLE zz_vf2_u1 (a int, CONSTRAINT zz_vf2_c2 UNIQUE (a) NOT DEFERRABLE INITIALLY DEFERRED);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: constraint declared INITIALLY DEFERRED must be DEFERRABLE
-- end-expected-error
CREATE TABLE zz_vf2_p2 (a int PRIMARY KEY NOT DEFERRABLE INITIALLY DEFERRED);
-- begin-expected
-- columns: conname:name | condeferrable:bool | condeferred:bool
-- rowcount: 0
-- end-expected
SELECT conname, condeferrable, condeferred FROM pg_constraint WHERE conname='zz_vf2_c1';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting NULL/NOT NULL declarations for column "a" of table "zz_vf2_n1"
-- end-expected-error
CREATE TABLE zz_vf2_n1 (a int NOT NULL NULL);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting NULL/NOT NULL declarations for column "a" of table "zz_vf2_n2"
-- end-expected-error
CREATE TABLE zz_vf2_n2 (a int NULL NOT NULL);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: multiple default values specified for column "a" of table "zz_vf2_d1"
-- end-expected-error
CREATE TABLE zz_vf2_d1 (a int DEFAULT 1 DEFAULT 2);
-- begin-expected
-- columns: is_nullable:varchar
-- rowcount: 0
-- end-expected
SELECT is_nullable FROM information_schema.columns WHERE table_name='zz_vf2_n1' AND column_name='a';
-- begin-expected
-- columns: column_default:varchar
-- rowcount: 0
-- end-expected
SELECT column_default FROM information_schema.columns WHERE table_name='zz_vf2_d1' AND column_name='a';
