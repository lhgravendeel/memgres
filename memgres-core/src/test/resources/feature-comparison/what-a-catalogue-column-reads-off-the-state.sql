-- begin-expected
-- columns: s | x
-- row: -10 kB | -10240
-- row: -1536 bytes | -1536
-- row: -1 bytes | -1
-- row: 0 bytes | 0
-- row: 1 bytes | 1
-- row: 10 bytes | 10
-- row: 1023 bytes | 1023
-- row: 1024 bytes | 1024
-- row: 1535 bytes | 1535
-- row: 1536 bytes | 1536
-- row: 10239 bytes | 10239
-- row: 10 kB | 10240
-- row: 10 kB | 10241
-- row: 20 kB | 20479
-- row: 20 kB | 20480
-- row: 1024 kB | 1048576
-- row: 10 MB | 10485759
-- row: 10 MB | 10485760
-- row: 20 MB | 20971519
-- row: 20 MB | 20971520
-- row: 1024 MB | 1073741824
-- row: 10 GB | 10737418239
-- row: 10 GB | 10737418240
-- row: 1024 GB | 1099511627776
-- row: 10 PB | 11258999068426240
-- end-expected
SELECT pg_size_pretty(x::bigint) AS s, x FROM (VALUES (0),(1),(10),(1023),(1024),(1535),(1536),(10239),(10240),(10241),(20479),(20480),(1048576),(10485759),(10485760),(20971519),(20971520),(1073741824),(10737418239),(10737418240),(1099511627776),(11258999068426240),(-1536),(-10240),(-1)) v(x) ORDER BY x;
-- begin-expected
-- columns: a | b
-- row: 1.5 bytes | 1536.7 bytes
-- end-expected
SELECT pg_size_pretty(1.5::numeric) AS a, pg_size_pretty(1536.7::numeric) AS b;
-- begin-expected
-- columns: b | s
-- row: 1 | 1
-- row: 1024 | 1 kB
-- row: 1024 | 1kB
-- row: 1572864 | 1.5 MB
-- row: 2147483648 | 2 GB
-- row: 3298534883328 | 3 TB
-- row: 4503599627370496 | 4 PB
-- row: 1 | 1 bytes
-- row: 10 |  10 
-- row: 1000 | 1e3
-- row: -2048 | -2 kB
-- end-expected
SELECT pg_size_bytes(s) AS b, s FROM (VALUES ('1'),('1 kB'),('1kB'),('1.5 MB'),('2 GB'),('3 TB'),('4 PB'),('1 bytes'),(' 10 '),('1e3'),('-2 kB')) v(s);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid size: "1 XB"
-- end-expected-error
SELECT pg_size_bytes('1 XB') AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid size: ""
-- end-expected-error
SELECT pg_size_bytes('') AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid size: "kB"
-- end-expected-error
SELECT pg_size_bytes('kB') AS a;
-- begin-expected
-- columns: a
-- row: 11258999068426240
-- end-expected
SELECT 11258999068426240 AS a;
-- begin-expected
-- columns: a
-- row: 2147483648
-- end-expected
SELECT 2147483648 AS a;
-- begin-expected
-- columns: a
-- row: 2147483647
-- end-expected
SELECT 2147483647 AS a;
-- begin-expected
-- columns: x
-- row: 1
-- row: 11258999068426240
-- end-expected
SELECT x FROM (VALUES (1),(11258999068426240)) v(x);
-- begin-expected
-- columns: a
-- row: 9223372036854775808
-- end-expected
SELECT 9223372036854775808 AS a;
CREATE TABLE jc_t (a int, b text DEFAULT 'x', c int GENERATED ALWAYS AS (a*2) STORED);
ALTER TABLE jc_t DROP COLUMN b;
-- begin-expected
-- columns: n | attnum | attisdropped | attcollation
-- row: tableoid | -6 | f | 0
-- row: cmax | -5 | f | 0
-- row: xmax | -4 | f | 0
-- row: cmin | -3 | f | 0
-- row: xmin | -2 | f | 0
-- row: ctid | -1 | f | 0
-- row: a | 1 | f | 0
-- row: ........pg.dropped.2........ | 2 | t | 100
-- row: c | 3 | f | 0
-- end-expected
SELECT attname::text AS n, attnum, attisdropped, attcollation FROM pg_attribute WHERE attrelid='jc_t'::regclass ORDER BY attnum;
-- begin-expected
-- columns: relnatts
-- row: 3
-- end-expected
SELECT relnatts FROM pg_class WHERE relname='jc_t';
-- begin-expected
-- columns: ordinal_position | c
-- row: 1 | a
-- row: 3 | c
-- end-expected
SELECT ordinal_position, column_name::text AS c FROM information_schema.columns WHERE table_name='jc_t' ORDER BY 1;
ALTER TABLE jc_t ADD COLUMN d int;
-- begin-expected
-- columns: n | attnum
-- row: a | 1
-- row: ........pg.dropped.2........ | 2
-- row: c | 3
-- row: d | 4
-- end-expected
SELECT attname::text AS n, attnum FROM pg_attribute WHERE attrelid='jc_t'::regclass AND attnum>0 ORDER BY attnum;
CREATE TABLE jc_c (s text COLLATE "C", t text);
-- begin-expected
-- columns: n | hascoll
-- row: s | t
-- row: t | t
-- end-expected
SELECT attname::text AS n, attcollation<>0 AS hascoll FROM pg_attribute WHERE attrelid='jc_c'::regclass AND attnum>0 ORDER BY attnum;
CREATE TABLE jc_d (a int DEFAULT 1+1, b text DEFAULT 'q', g int GENERATED ALWAYS AS (a+1) STORED);
-- begin-expected
-- columns: d
-- row: (1 + 1)
-- row: 'q'::text
-- row: (a + 1)
-- end-expected
SELECT pg_get_expr(adbin, adrelid) AS d FROM pg_attrdef WHERE adrelid='jc_d'::regclass ORDER BY adnum;
-- begin-expected
-- columns: d
-- row: (1 + 1)
-- row: 'q'::text
-- row: NULL
-- end-expected
SELECT column_default::text AS d FROM information_schema.columns WHERE table_name='jc_d' ORDER BY ordinal_position;
-- begin-expected
-- columns: g
-- row: (a + 1)
-- end-expected
SELECT pg_get_expr(d.adbin, d.adrelid) AS g FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum WHERE d.adrelid='jc_d'::regclass AND a.attgenerated<>'';
CREATE TABLE jc_k (a int, b int, PRIMARY KEY (a,b));
CREATE TABLE jc_f (x int, y int, FOREIGN KEY (x,y) REFERENCES jc_k);
-- begin-expected
-- columns: n | k | f
-- row: jc_f_x_y_fkey | {1,2} | {1,2}
-- end-expected
SELECT conname::text AS n, conkey::text AS k, confkey::text AS f FROM pg_constraint WHERE conrelid='jc_f'::regclass;
-- begin-expected
-- columns: d
-- row: FOREIGN KEY (x, y) REFERENCES jc_k(a, b)
-- end-expected
SELECT pg_get_constraintdef(oid) AS d FROM pg_constraint WHERE conrelid='jc_f'::regclass;
CREATE INDEX jc_ix ON jc_k (a) WHERE b > 0;
-- begin-expected
-- columns: p
-- row: (b > 0)
-- end-expected
SELECT pg_get_expr(indpred, indrelid) AS p FROM pg_index WHERE indexrelid='jc_ix'::regclass;
-- begin-expected
-- columns: indnkeyatts | indnatts | indisunique | indisprimary
-- row: 1 | 1 | f | f
-- end-expected
SELECT indnkeyatts, indnatts, indisunique, indisprimary FROM pg_index WHERE indexrelid='jc_ix'::regclass;
-- begin-expected
-- columns: relam
-- row: 403
-- end-expected
SELECT relam FROM pg_class WHERE relname='jc_ix';
-- begin-expected
-- columns: a
-- row: btree
-- end-expected
SELECT amname::text AS a FROM pg_am WHERE oid=(SELECT relam FROM pg_class WHERE relname='jc_ix');
-- begin-expected
-- columns: relam
-- row: 2
-- end-expected
SELECT relam FROM pg_class WHERE relname='jc_k';
-- begin-expected
-- columns: reloftype | hastoast
-- row: 0 | f
-- end-expected
SELECT reloftype, reltoastrelid<>0 AS hastoast FROM pg_class WHERE relname='jc_k';
-- begin-expected
-- columns: attstattarget
-- row: NULL
-- end-expected
SELECT attstattarget FROM pg_attribute WHERE attrelid='jc_k'::regclass AND attnum=1;
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::int AS n FROM pg_opfamily WHERE opfname='integer_ops';
-- begin-expected
-- columns: a | b | c | d
-- row: 1024 bytes | 1536 bytes | 1024 kB | 10 bytes
-- end-expected
SELECT pg_size_pretty(1024::bigint) AS a, pg_size_pretty(1536::bigint) AS b, pg_size_pretty(1048576::bigint) AS c, pg_size_pretty(10::bigint) AS d;
-- begin-expected
-- columns: a | b
-- row: -1536 bytes | 1024 GB
-- end-expected
SELECT pg_size_pretty(-1536::bigint) AS a, pg_size_pretty(1099511627776::bigint) AS b;
-- begin-expected
-- columns: a | b | c
-- row: 1024 | 1572864 | 10
-- end-expected
SELECT pg_size_bytes('1 kB') AS a, pg_size_bytes('1.5 MB') AS b, pg_size_bytes('10') AS c;
-- begin-expected
-- columns: a | b
-- row: 1099511627776 | 2251799813685248
-- end-expected
SELECT pg_size_bytes('1 TB') AS a, pg_size_bytes('2 PB') AS b;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid size: "bogus"
-- end-expected-error
SELECT pg_size_bytes('bogus') AS a;
DROP TABLE jc_t, jc_c, jc_d, jc_f, jc_k CASCADE;
