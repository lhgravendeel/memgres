CREATE TABLE w1ix_p (i int PRIMARY KEY);

CREATE TABLE w1ix_q (a int, b int, c int, CONSTRAINT w1ix_q_fk FOREIGN KEY (a) REFERENCES w1ix_p(i) MATCH FULL ON DELETE CASCADE ON UPDATE SET NULL DEFERRABLE INITIALLY DEFERRED, CONSTRAINT w1ix_q_ni CHECK (b > 0) NO INHERIT, CONSTRAINT w1ix_q_ck2 CHECK (c > 0) NOT ENFORCED, CONSTRAINT w1ix_q_u UNIQUE (b) DEFERRABLE);

ALTER TABLE w1ix_q ADD CONSTRAINT w1ix_q_nv CHECK (a > 0) NOT VALID;

-- begin-expected
-- columns: conname, convalidated, conenforced, connoinherit, pg_get_constraintdef
-- row: w1ix_q_ck2, f, f, f, CHECK ((c > 0)) NOT ENFORCED
-- row: w1ix_q_fk, t, t, t, FOREIGN KEY (a) REFERENCES w1ix_p(i) MATCH FULL ON UPDATE SET NULL ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
-- row: w1ix_q_ni, t, t, t, CHECK ((b > 0)) NO INHERIT
-- row: w1ix_q_nv, f, t, f, CHECK ((a > 0)) NOT VALID
-- row: w1ix_q_u, t, t, t, UNIQUE (b) DEFERRABLE
-- end-expected
SELECT conname, convalidated, conenforced, connoinherit, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='w1ix_q'::regclass ORDER BY conname;

CREATE TABLE w1ix_c2 (a int, EXCLUDE USING btree (a WITH =));

-- begin-expected
-- columns: conname, contype, connoinherit, exclop_null
-- row: w1ix_c2_a_excl, x, t, f
-- end-expected
SELECT conname, contype, connoinherit, conexclop IS NULL AS exclop_null FROM pg_constraint WHERE conrelid='w1ix_c2'::regclass AND contype='x';

DROP TABLE w1ix_c2;

DROP TABLE w1ix_q;

DROP TABLE w1ix_p;

CREATE TABLE w1iy_t1 (a int);

ALTER TABLE w1iy_t1 SET (fillfactor = 70);

-- begin-expected
-- columns: reloptions
-- row: {fillfactor=70}
-- end-expected
SELECT reloptions FROM pg_class WHERE relname='w1iy_t1';

CREATE TABLE w1iy_r1 (a int) WITH (autovacuum_enabled = FALSE, vacuum_index_cleanup = OFF, fillfactor = 55);

-- begin-expected
-- columns: reloptions
-- row: {autovacuum_enabled=false,vacuum_index_cleanup=off,fillfactor=55}
-- end-expected
SELECT reloptions FROM pg_class WHERE relname='w1iy_r1';

ALTER TABLE w1iy_r1 SET (autovacuum_enabled = On);

-- begin-expected
-- columns: reloptions
-- row: {vacuum_index_cleanup=off,fillfactor=55,autovacuum_enabled=on}
-- end-expected
SELECT reloptions FROM pg_class WHERE relname='w1iy_r1';

ALTER TABLE w1iy_r1 RESET (fillfactor);

-- begin-expected
-- columns: reloptions
-- row: {vacuum_index_cleanup=off,autovacuum_enabled=on}
-- end-expected
SELECT reloptions FROM pg_class WHERE relname='w1iy_r1';

ALTER TABLE w1iy_r1 RESET (autovacuum_enabled, vacuum_index_cleanup);

-- begin-expected
-- columns: reloptions
-- row: NULL
-- end-expected
SELECT reloptions FROM pg_class WHERE relname='w1iy_r1';

ALTER TABLE w1iy_r1 RESET (fillfactor);

-- begin-expected
-- columns: reloptions
-- row: NULL
-- end-expected
SELECT reloptions FROM pg_class WHERE relname='w1iy_r1';

DROP TABLE w1iy_r1;

DROP TABLE w1iy_t1;

CREATE TABLE w1iz_t3 (a int, d text, CONSTRAINT w1iz_nn NOT NULL d, e int CONSTRAINT w1iz_nn2 NOT NULL);

-- begin-expected
-- columns: conname, contype
-- row: w1iz_nn, n
-- row: w1iz_nn2, n
-- end-expected
SELECT conname, contype FROM pg_constraint WHERE conrelid='w1iz_t3'::regclass AND contype='n' ORDER BY conname;

ALTER TABLE w1iz_t3 DROP CONSTRAINT w1iz_nn;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM pg_constraint WHERE conrelid='w1iz_t3'::regclass AND contype='n';

DROP TABLE w1iz_t3;

CREATE TABLE w1iz_ge (a int, b numeric, c int GENERATED ALWAYS AS (a) STORED, d text GENERATED ALWAYS AS (upper(a::text)) STORED, e numeric GENERATED ALWAYS AS (b / 2) STORED, f int GENERATED ALWAYS AS (a*2) STORED);

-- begin-expected
-- columns: column_name, generation_expression
-- row: a, NULL
-- row: b, NULL
-- row: c, a
-- row: d, upper((a)::text)
-- row: e, (b / (2)::numeric)
-- row: f, (a * 2)
-- end-expected
SELECT column_name, generation_expression FROM information_schema.columns WHERE table_name='w1iz_ge' ORDER BY ordinal_position;

DROP TABLE w1iz_ge;

CREATE TABLE w1iz_t7 (a int, b int, c int);

CREATE VIEW w1iz_v7 AS SELECT b FROM w1iz_t7;

CREATE TABLE w1iz_l7 (LIKE w1iz_v7 INCLUDING ALL);

-- begin-expected
-- columns: column_name
-- row: b
-- end-expected
SELECT column_name FROM information_schema.columns WHERE table_name='w1iz_l7' ORDER BY ordinal_position;

DROP TABLE w1iz_l7;

DROP VIEW w1iz_v7;

DROP TABLE w1iz_t7;

CREATE DOMAIN w1iw_dd AS varchar(10) NOT NULL;

CREATE TABLE w1iw_dt (code w1iw_dd, other w1iw_dd NOT NULL);

-- begin-expected
-- columns: attname, attnotnull
-- row: code, f
-- row: other, t
-- end-expected
SELECT attname, attnotnull FROM pg_attribute WHERE attrelid='w1iw_dt'::regclass AND attnum>0 ORDER BY attnum;

-- begin-expected
-- columns: conname, contype, connoinherit
-- row: w1iw_dd_not_null, n, f
-- row: w1iw_dt_other_not_null, n, f
-- end-expected
SELECT conname, contype, connoinherit FROM pg_constraint WHERE conname LIKE 'w1iw_d%' ORDER BY conname;

-- begin-expected
-- columns: constraint_name, check_clause
-- row: w1iw_dd_not_null, VALUE IS NOT NULL
-- row: w1iw_dt_other_not_null, other IS NOT NULL
-- end-expected
SELECT constraint_name, check_clause FROM information_schema.check_constraints WHERE constraint_name LIKE 'w1iw_d%' ORDER BY constraint_name;

-- begin-expected
-- columns: constraint_schema, constraint_name, domain_name, is_deferrable, initially_deferred
-- row: public, w1iw_dd_not_null, w1iw_dd, NO, NO
-- end-expected
SELECT constraint_schema, constraint_name, domain_name, is_deferrable, initially_deferred FROM information_schema.domain_constraints WHERE domain_name='w1iw_dd';

-- begin-expected
-- columns: column_name, is_nullable
-- row: code, NO
-- row: other, NO
-- end-expected
SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name='w1iw_dt' ORDER BY ordinal_position;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: ERROR: domain w1iw_dd does not allow null values
-- end-expected-error
INSERT INTO w1iw_dt VALUES (NULL, 'x');

DROP TABLE w1iw_dt;

DROP DOMAIN w1iw_dd;

CREATE TABLE w1iv_t5 (a int);

CREATE TABLE w1iv_tr2 (a int);

CREATE FUNCTION w1iv_f5() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER w1iv_tg5c AFTER INSERT ON w1iv_t5 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION w1iv_f5();

CREATE CONSTRAINT TRIGGER w1iv_tg5n AFTER UPDATE ON w1iv_t5 NOT DEFERRABLE FOR EACH ROW EXECUTE FUNCTION w1iv_f5();

CREATE CONSTRAINT TRIGGER w1iv_tg5f AFTER DELETE ON w1iv_t5 FROM w1iv_tr2 DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION w1iv_f5('x');

-- begin-expected
-- columns: tgname, pg_get_triggerdef
-- row: w1iv_tg5c, CREATE CONSTRAINT TRIGGER w1iv_tg5c AFTER INSERT ON public.w1iv_t5 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION w1iv_f5()
-- row: w1iv_tg5f, CREATE CONSTRAINT TRIGGER w1iv_tg5f AFTER DELETE ON public.w1iv_t5 FROM w1iv_tr2 DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION w1iv_f5('x')
-- row: w1iv_tg5n, CREATE CONSTRAINT TRIGGER w1iv_tg5n AFTER UPDATE ON public.w1iv_t5 NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION w1iv_f5()
-- end-expected
SELECT tgname, pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid='w1iv_t5'::regclass AND NOT tgisinternal ORDER BY tgname;

-- begin-expected
-- columns: tgname, hascon, tgdeferrable, tginitdeferred
-- row: w1iv_tg5c, t, t, t
-- row: w1iv_tg5f, t, t, f
-- row: w1iv_tg5n, t, f, f
-- end-expected
SELECT tgname, tgconstraint <> 0 AS hascon, tgdeferrable, tginitdeferred FROM pg_trigger WHERE tgrelid='w1iv_t5'::regclass AND NOT tgisinternal ORDER BY tgname;

-- begin-expected
-- columns: conname, contype, condeferrable, condeferred, connoinherit
-- row: w1iv_tg5c, t, t, t, t
-- row: w1iv_tg5f, t, t, f, t
-- row: w1iv_tg5n, t, f, f, t
-- end-expected
SELECT conname, contype, condeferrable, condeferred, connoinherit FROM pg_constraint WHERE conname LIKE 'w1iv_tg5%' ORDER BY conname;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "DEFERRABLE"
-- end-expected-error
CREATE TRIGGER w1iv_plain AFTER INSERT ON w1iv_t5 DEFERRABLE FOR EACH ROW EXECUTE FUNCTION w1iv_f5();

DROP TABLE w1iv_t5;

DROP TABLE w1iv_tr2;

DROP FUNCTION w1iv_f5();