CREATE TABLE zzc1b_f1 (i int) PARTITION BY RANGE (i);
CREATE TABLE zzc1b_f1_1 PARTITION OF zzc1b_f1 FOR VALUES FROM (1) TO (10);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: ERROR: new row for relation "zzc1b_f1_1" violates partition constraint
-- end-expected-error
INSERT INTO zzc1b_f1_1 VALUES (99);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*)::int AS count FROM zzc1b_f1;

INSERT INTO zzc1b_f1_1 VALUES (5);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: ERROR: new row for relation "zzc1b_f1_1" violates partition constraint
-- end-expected-error
UPDATE zzc1b_f1_1 SET i = 99;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: ERROR: new row for relation "zzc1b_f1_1" violates partition constraint
-- end-expected-error
INSERT INTO zzc1b_f1_1 SELECT 42;

-- begin-expected
-- columns: i
-- row: 5
-- end-expected
SELECT i FROM zzc1b_f1_1 ORDER BY i;

DROP TABLE zzc1b_f1;

CREATE TABLE zzc1b_l1 (s text) PARTITION BY LIST (s);
CREATE TABLE zzc1b_l1_a PARTITION OF zzc1b_l1 FOR VALUES IN ('a');

-- begin-expected-error
-- sqlstate: 23514
-- message-like: ERROR: new row for relation "zzc1b_l1_a" violates partition constraint
-- end-expected-error
INSERT INTO zzc1b_l1_a VALUES ('b');

-- begin-expected-error
-- sqlstate: 23514
-- message-like: ERROR: new row for relation "zzc1b_l1_a" violates partition constraint
-- end-expected-error
INSERT INTO zzc1b_l1_a VALUES (NULL);

DROP TABLE zzc1b_l1;

CREATE TABLE zzc1b_d1 (i int) PARTITION BY RANGE (i);
CREATE TABLE zzc1b_d1_1 PARTITION OF zzc1b_d1 FOR VALUES FROM (1) TO (10);
CREATE TABLE zzc1b_d1_d PARTITION OF zzc1b_d1 DEFAULT;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: ERROR: new row for relation "zzc1b_d1_d" violates partition constraint
-- end-expected-error
INSERT INTO zzc1b_d1_d VALUES (5);

INSERT INTO zzc1b_d1_d VALUES (50);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::int AS count FROM zzc1b_d1_d;

DROP TABLE zzc1b_d1;

CREATE TABLE zzc1b_m1 (a int, b int) PARTITION BY RANGE (a);
CREATE TABLE zzc1b_m1_1 PARTITION OF zzc1b_m1 FOR VALUES FROM (0) TO (10) PARTITION BY RANGE (b);
CREATE TABLE zzc1b_m1_1_1 PARTITION OF zzc1b_m1_1 FOR VALUES FROM (0) TO (10);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: ERROR: new row for relation "zzc1b_m1_1" violates partition constraint
-- end-expected-error
INSERT INTO zzc1b_m1_1 VALUES (99, 5);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: ERROR: new row for relation "zzc1b_m1_1_1" violates partition constraint
-- end-expected-error
INSERT INTO zzc1b_m1_1_1 VALUES (5, 99);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*)::int AS count FROM zzc1b_m1;

DROP TABLE zzc1b_m1;

CREATE TABLE zzc1b_mv (i int, v int) PARTITION BY RANGE (i);
CREATE TABLE zzc1b_mv_1 PARTITION OF zzc1b_mv FOR VALUES FROM (1) TO (10);
CREATE TABLE zzc1b_mv_2 PARTITION OF zzc1b_mv FOR VALUES FROM (10) TO (20);
ALTER TABLE zzc1b_mv_2 ADD CONSTRAINT zzc1b_mvc CHECK (v > 100);
INSERT INTO zzc1b_mv VALUES (5, 1);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: ERROR: new row for relation "zzc1b_mv_2" violates check constraint "zzc1b_mvc"
-- end-expected-error
UPDATE zzc1b_mv SET i = 15;

-- begin-expected
-- columns: c1, c2
-- row: 1, 0
-- end-expected
SELECT (SELECT count(*)::int FROM zzc1b_mv_1) AS c1, (SELECT count(*)::int FROM zzc1b_mv_2) AS c2;

DROP TABLE zzc1b_mv;

CREATE TABLE zzc1b_ha (i int) PARTITION BY HASH (i);
CREATE TABLE zzc1b_ha_0 PARTITION OF zzc1b_ha FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE zzc1b_ha_1 PARTITION OF zzc1b_ha FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE zzc1b_ha_2 PARTITION OF zzc1b_ha FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE zzc1b_ha_3 PARTITION OF zzc1b_ha FOR VALUES WITH (MODULUS 4, REMAINDER 3);
INSERT INTO zzc1b_ha SELECT g FROM generate_series(1,12) g;

-- begin-expected
-- columns: r0, r1, r2, r3
-- row: 1,12, 3,5,8,9,11, 2, 4,6,7,10
-- end-expected
SELECT (SELECT string_agg(i::text, ',' ORDER BY i) FROM zzc1b_ha_0) AS r0,
       (SELECT string_agg(i::text, ',' ORDER BY i) FROM zzc1b_ha_1) AS r1,
       (SELECT string_agg(i::text, ',' ORDER BY i) FROM zzc1b_ha_2) AS r2,
       (SELECT string_agg(i::text, ',' ORDER BY i) FROM zzc1b_ha_3) AS r3;

-- begin-expected
-- columns: s1, s2
-- row: true, true
-- end-expected
SELECT satisfies_hash_partition('zzc1b_ha'::regclass, 4, 0, 1) AS s1,
       satisfies_hash_partition('zzc1b_ha'::regclass, 4, 1, 3) AS s2;

CREATE TABLE zzc1b_hr (i int) PARTITION BY RANGE (i);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: "zzc1b_hr" is not a hash partitioned table
-- end-expected-error
SELECT satisfies_hash_partition('zzc1b_hr'::regclass, 4, 0, 1);

DROP TABLE zzc1b_hr;
DROP TABLE zzc1b_ha;

CREATE TABLE zzc1b_ht (s text) PARTITION BY HASH (s);
CREATE TABLE zzc1b_ht_0 PARTITION OF zzc1b_ht FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE zzc1b_ht_1 PARTITION OF zzc1b_ht FOR VALUES WITH (MODULUS 2, REMAINDER 1);
INSERT INTO zzc1b_ht VALUES ('alpha'),('beta'),('gamma'),('delta');

-- begin-expected
-- columns: t0, t1
-- row: beta,delta, alpha,gamma
-- end-expected
SELECT (SELECT string_agg(s, ',' ORDER BY s) FROM zzc1b_ht_0) AS t0,
       (SELECT string_agg(s, ',' ORDER BY s) FROM zzc1b_ht_1) AS t1;

DROP TABLE zzc1b_ht;

CREATE TABLE zzc1b_hb (v bigint) PARTITION BY HASH (v);
CREATE TABLE zzc1b_hb_0 PARTITION OF zzc1b_hb FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE zzc1b_hb_1 PARTITION OF zzc1b_hb FOR VALUES WITH (MODULUS 2, REMAINDER 1);
INSERT INTO zzc1b_hb SELECT g FROM generate_series(1,8) g;

-- begin-expected
-- columns: b0, b1
-- row: 1,2, 3,4,5,6,7,8
-- end-expected
SELECT (SELECT string_agg(v::text, ',' ORDER BY v) FROM zzc1b_hb_0) AS b0,
       (SELECT string_agg(v::text, ',' ORDER BY v) FROM zzc1b_hb_1) AS b1;

DROP TABLE zzc1b_hb;

CREATE TABLE zzc1b_par (id int, v int);
CREATE TABLE zzc1b_chi () INHERITS (zzc1b_par);
INSERT INTO zzc1b_par VALUES (1,1);
INSERT INTO zzc1b_chi VALUES (2,2);
UPDATE zzc1b_par SET v = v + 100;

-- begin-expected
-- columns: id, v
-- row: 1, 101
-- row: 2, 102
-- end-expected
SELECT id, v FROM zzc1b_par ORDER BY id;

DELETE FROM zzc1b_par WHERE id = 2;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*)::int AS count FROM zzc1b_chi;

DROP TABLE zzc1b_chi;
DROP TABLE zzc1b_par;

CREATE TABLE zzc1b_o2 (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzc1b_o2_1 PARTITION OF zzc1b_o2 FOR VALUES FROM (1) TO (10);
INSERT INTO zzc1b_o2 VALUES (5, 'a');
UPDATE ONLY zzc1b_o2 SET s = 'changed';

-- begin-expected
-- columns: i, s
-- row: 5, a
-- end-expected
SELECT i, s FROM zzc1b_o2 ORDER BY i;

DELETE FROM ONLY zzc1b_o2 WHERE i = 5;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::int AS count FROM zzc1b_o2;

DROP TABLE zzc1b_o2;

CREATE TABLE zzc1b_oi (i int);
CREATE TABLE zzc1b_oic () INHERITS (zzc1b_oi);
INSERT INTO zzc1b_oi VALUES (1);
INSERT INTO zzc1b_oic VALUES (2);
UPDATE ONLY zzc1b_oi SET i = i + 10;

-- begin-expected
-- columns: i
-- row: 2
-- row: 11
-- end-expected
SELECT i FROM zzc1b_oi ORDER BY i;

DELETE FROM ONLY zzc1b_oi;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::int AS count FROM zzc1b_oic;

DROP TABLE zzc1b_oic;
DROP TABLE zzc1b_oi;

CREATE TABLE zzc1b_x1 (id bigint) PARTITION BY RANGE (id);
CREATE TABLE zzc1b_x1_a PARTITION OF zzc1b_x1 FOR VALUES FROM (9007199254740995) TO (9007199254740996);
INSERT INTO zzc1b_x1 VALUES (9007199254740995);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*)::int AS count FROM zzc1b_x1;

DROP TABLE zzc1b_x1;

CREATE TABLE zzc1b_x2 (id bigint) PARTITION BY RANGE (id);
CREATE TABLE zzc1b_x2_a PARTITION OF zzc1b_x2 FOR VALUES FROM (0) TO (9007199254740996);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: ERROR: partition "zzc1b_x2_b" would overlap partition "zzc1b_x2_a"
-- end-expected-error
CREATE TABLE zzc1b_x2_b PARTITION OF zzc1b_x2 FOR VALUES FROM (9007199254740995) TO (MAXVALUE);

DROP TABLE zzc1b_x2;

CREATE TABLE zzc1b_wb (id int, v bigint);
INSERT INTO zzc1b_wb VALUES (1,9007199254740992),(2,9007199254740993),(3,9007199254740995);

-- begin-expected
-- columns: id, v, c
-- row: 1, 9007199254740992, 1
-- row: 2, 9007199254740993, 2
-- row: 3, 9007199254740995, 1
-- end-expected
SELECT id, v, count(*) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS c FROM zzc1b_wb ORDER BY id;

-- begin-expected
-- columns: id, v, c2
-- row: 1, 9007199254740992, 2
-- row: 2, 9007199254740993, 2
-- row: 3, 9007199254740995, 1
-- end-expected
SELECT id, v, count(*) OVER (ORDER BY v RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS c2 FROM zzc1b_wb ORDER BY id;

DROP TABLE zzc1b_wb;

CREATE TABLE zzc1b_pf (i int) PARTITION BY RANGE (i);
CREATE TABLE zzc1b_pf_1 PARTITION OF zzc1b_pf FOR VALUES FROM (1) TO (10) PARTITION BY RANGE (i);
CREATE TABLE zzc1b_pf_1_1 PARTITION OF zzc1b_pf_1 FOR VALUES FROM (1) TO (5);

-- begin-expected
-- columns: root
-- row: zzc1b_pf
-- end-expected
SELECT pg_partition_root('zzc1b_pf_1_1'::regclass)::text AS root;

-- begin-expected
-- columns: relid
-- row: zzc1b_pf_1_1
-- row: zzc1b_pf_1
-- row: zzc1b_pf
-- end-expected
SELECT relid::regclass::text AS relid FROM pg_partition_ancestors('zzc1b_pf_1_1'::regclass);

-- begin-expected
-- columns: attinhcount, attislocal
-- row: 1, f
-- end-expected
SELECT attinhcount, attislocal FROM pg_attribute WHERE attrelid='zzc1b_pf_1'::regclass AND attname='i';

-- begin-expected
-- columns: relhassubclass
-- row: t
-- end-expected
SELECT relhassubclass FROM pg_class WHERE relname='zzc1b_pf';

DROP TABLE zzc1b_pf;

CREATE TABLE zzc1b_pa (a int);
CREATE TABLE zzc1b_pb (b int);
CREATE TABLE zzc1b_pd () INHERITS (zzc1b_pa, zzc1b_pb);

-- begin-expected
-- columns: relname, inhseqno
-- row: zzc1b_pa, 1
-- row: zzc1b_pb, 2
-- end-expected
SELECT p.relname, i.inhseqno FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_class p ON p.oid=i.inhparent WHERE c.relname='zzc1b_pd' ORDER BY i.inhseqno;

-- begin-expected
-- columns: attinhcount, attislocal
-- row: 1, f
-- end-expected
SELECT attinhcount, attislocal FROM pg_attribute WHERE attrelid='zzc1b_pd'::regclass AND attname='a';

-- begin-expected
-- columns: relhassubclass
-- row: t
-- end-expected
SELECT relhassubclass FROM pg_class WHERE relname='zzc1b_pa';

DROP TABLE zzc1b_pd;
DROP TABLE zzc1b_pb;
DROP TABLE zzc1b_pa;

CREATE TABLE zzc1b_ord (id int);

-- begin-expected
-- columns: root
-- row: null
-- end-expected
SELECT pg_partition_root('zzc1b_ord'::regclass)::text AS root;

-- begin-expected
-- columns: attinhcount, attislocal, relhassubclass
-- row: 0, t, f
-- end-expected
SELECT a.attinhcount, a.attislocal, c.relhassubclass FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE a.attrelid='zzc1b_ord'::regclass AND a.attname='id';

DROP TABLE zzc1b_ord;

CREATE FUNCTION zzc1b_faf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.s := NEW.s || '!'; RETURN NEW; END $$;
CREATE TABLE zzc1b_fa (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzc1b_fa_1 PARTITION OF zzc1b_fa FOR VALUES FROM (1) TO (10);
CREATE TRIGGER zzc1b_fa_t BEFORE INSERT ON zzc1b_fa FOR EACH ROW EXECUTE FUNCTION zzc1b_faf();
INSERT INTO zzc1b_fa_1 VALUES (5, 'a');
INSERT INTO zzc1b_fa VALUES (6, 'b');

-- begin-expected
-- columns: i, s
-- row: 5, a!
-- row: 6, b!
-- end-expected
SELECT i, s FROM zzc1b_fa ORDER BY i;

DROP TABLE zzc1b_fa;

CREATE TABLE zzc1b_fb (i int, s text) PARTITION BY RANGE (i);
CREATE TRIGGER zzc1b_fb_t BEFORE INSERT ON zzc1b_fb FOR EACH ROW EXECUTE FUNCTION zzc1b_faf();
CREATE TABLE zzc1b_fb_1 PARTITION OF zzc1b_fb FOR VALUES FROM (1) TO (10);
INSERT INTO zzc1b_fb_1 VALUES (5, 'a');

-- begin-expected
-- columns: i, s
-- row: 5, a!
-- end-expected
SELECT i, s FROM zzc1b_fb ORDER BY i;

DROP TABLE zzc1b_fb;
DROP FUNCTION zzc1b_faf();

CREATE TABLE zzc1b_ck3 (i int, v int CHECK (v > 0)) PARTITION BY RANGE (i);
CREATE TABLE zzc1b_ck3_1 PARTITION OF zzc1b_ck3 FOR VALUES FROM (1) TO (10);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: ERROR: new row for relation "zzc1b_ck3_1" violates check constraint "zzc1b_ck3_v_check"
-- end-expected-error
INSERT INTO zzc1b_ck3 VALUES (5, -1);

DROP TABLE zzc1b_ck3;

CREATE TABLE zzc1b_ref2 (id int PRIMARY KEY);
CREATE TABLE zzc1b_fkq (k int NOT NULL, r int REFERENCES zzc1b_ref2(id)) PARTITION BY RANGE (k);
CREATE TABLE zzc1b_fkqa PARTITION OF zzc1b_fkq FOR VALUES FROM (0) TO (100);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: ERROR: insert or update on table "zzc1b_fkqa" violates foreign key constraint "zzc1b_fkq_r_fkey"
-- end-expected-error
INSERT INTO zzc1b_fkq VALUES (1, 42);

DROP TABLE zzc1b_fkq;
DROP TABLE zzc1b_ref2;

CREATE TABLE zzc1b_ref (id int PRIMARY KEY);
INSERT INTO zzc1b_ref VALUES (1);
CREATE TABLE zzc1b_fkp (k int NOT NULL, r int REFERENCES zzc1b_ref(id)) PARTITION BY RANGE (k);
CREATE TABLE zzc1b_fkpa PARTITION OF zzc1b_fkp FOR VALUES FROM (0) TO (100);
INSERT INTO zzc1b_fkp VALUES (1, 1);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: ERROR: update or delete on table "zzc1b_ref" violates foreign key constraint "zzc1b_fkp_r_fkey" on table "zzc1b_fkp"
-- end-expected-error
DELETE FROM zzc1b_ref WHERE id = 1;

-- begin-expected
-- columns: orphans
-- row: 0
-- end-expected
SELECT count(*)::int AS orphans FROM zzc1b_fkp c WHERE NOT EXISTS (SELECT 1 FROM zzc1b_ref p WHERE p.id = c.r);

DROP TABLE zzc1b_fkp;
DROP TABLE zzc1b_ref;

CREATE TABLE zzc1b_cref (id int PRIMARY KEY);
INSERT INTO zzc1b_cref VALUES (1);
CREATE TABLE zzc1b_cp (k int NOT NULL, r int REFERENCES zzc1b_cref(id) ON DELETE CASCADE) PARTITION BY RANGE (k);
CREATE TABLE zzc1b_cpa PARTITION OF zzc1b_cp FOR VALUES FROM (0) TO (100);
INSERT INTO zzc1b_cp VALUES (1, 1);
DELETE FROM zzc1b_cref WHERE id = 1;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*)::int AS count FROM zzc1b_cp;

DROP TABLE zzc1b_cp;
DROP TABLE zzc1b_cref;

CREATE TABLE zzc1b_pt (id bigint) PARTITION BY RANGE (id);
CREATE TABLE zzc1b_pt1 PARTITION OF zzc1b_pt FOR VALUES FROM (MINVALUE) TO (9007199254740993);
CREATE TABLE zzc1b_pt2 PARTITION OF zzc1b_pt FOR VALUES FROM (9007199254740993) TO (MAXVALUE);
INSERT INTO zzc1b_pt VALUES (9007199254740992);

-- begin-expected
-- columns: c1, c2
-- row: 1, 0
-- end-expected
SELECT (SELECT count(*)::int FROM zzc1b_pt1) AS c1, (SELECT count(*)::int FROM zzc1b_pt2) AS c2;

DROP TABLE zzc1b_pt;