-- The row already in the relation and the row being written are both in scope inside an
-- ON CONFLICT DO UPDATE, and EXCLUDED holds every column the relation holds, so a column written
-- without a relation name answers to both and PostgreSQL refuses to choose. It refuses while the
-- statement is planned, before it has looked for an index to arbitrate on and whether or not any
-- row conflicts; an alias on the relation renames the relation, it does not settle the bare name.
-- The clause is read in an order of its own: a constraint that is not there is reported before
-- the action, one with no index behind it after it, an arbiter naming no column of the relation
-- before it, and the assignments before the WHERE beside them.

-- setup
CREATE TABLE ocs_t (i int PRIMARY KEY, k int);

-- stmt 1: a bare column inside DO UPDATE is ambiguous
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
INSERT INTO ocs_t VALUES (1,2) ON CONFLICT (i) DO UPDATE SET k = 9 WHERE i = 1;
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "k" is ambiguous
-- end-expected-error
INSERT INTO ocs_t VALUES (1,2) ON CONFLICT (i) DO UPDATE SET k = k + 1;
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
INSERT INTO ocs_t VALUES (1,2) ON CONFLICT (i) DO UPDATE SET k = i;

-- stmt 2: either name written out settles it
INSERT INTO ocs_t VALUES (1,2) ON CONFLICT (i) DO UPDATE SET k = 9 WHERE ocs_t.k = 1;
INSERT INTO ocs_t VALUES (1,3) ON CONFLICT (i) DO UPDATE SET k = EXCLUDED.k;
-- begin-expected
-- columns: d
-- row: 3
-- end-expected
SELECT k::text AS d FROM ocs_t WHERE i = 1;
DROP TABLE ocs_t;

-- stmt 3: an alias on the relation does not settle a bare name
CREATE TABLE ocs_al (i int PRIMARY KEY, k int);
INSERT INTO ocs_al VALUES (1,1);
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "k" is ambiguous
-- end-expected-error
INSERT INTO ocs_al AS bb VALUES (1,2) ON CONFLICT (i) DO UPDATE SET k = k + 1;
INSERT INTO ocs_al AS bb VALUES (1,2) ON CONFLICT (i) DO UPDATE SET k = bb.k + 1;
-- begin-expected
-- columns: d
-- row: 2
-- end-expected
SELECT k::text AS d FROM ocs_al WHERE i = 1;
DROP TABLE ocs_al;

-- stmt 4: it is settled while planning, so no row need conflict
CREATE TABLE ocs_pl (i int PRIMARY KEY, k int);
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "k" is ambiguous
-- end-expected-error
INSERT INTO ocs_pl VALUES (99,2) ON CONFLICT (i) DO UPDATE SET k = k + 1;
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "k" is ambiguous
-- end-expected-error
INSERT INTO ocs_pl SELECT 1,2 WHERE false ON CONFLICT (i) DO UPDATE SET k = k + 1;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM ocs_pl;
INSERT INTO ocs_pl VALUES (1,5) ON CONFLICT (i) DO NOTHING;
-- begin-expected
-- columns: d
-- row: 1
-- end-expected
SELECT count(*)::text AS d FROM ocs_pl;
DROP TABLE ocs_pl;

-- stmt 5: the order the clause is read in
CREATE TABLE ocs_or (i int PRIMARY KEY, j text, k int UNIQUE);
ALTER TABLE ocs_or ADD CONSTRAINT ocs_ck CHECK (i > 0);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "ocs_nosuch" for table "ocs_or" does not exist
-- end-expected-error
INSERT INTO ocs_or VALUES (9,'x',90) ON CONFLICT ON CONSTRAINT ocs_nosuch DO UPDATE SET j = j;
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "j" is ambiguous
-- end-expected-error
INSERT INTO ocs_or VALUES (9,'x',90) ON CONFLICT ON CONSTRAINT ocs_ck DO UPDATE SET j = j;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: constraint in ON CONFLICT clause has no associated index
-- end-expected-error
INSERT INTO ocs_or VALUES (9,'x',90) ON CONFLICT ON CONSTRAINT ocs_ck DO UPDATE SET j = 'y';
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "j" is ambiguous
-- end-expected-error
INSERT INTO ocs_or VALUES (9,'x',90) ON CONFLICT (j) DO UPDATE SET j = j;
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO ocs_or VALUES (9,'x',90) ON CONFLICT (j) DO UPDATE SET j = 'y';
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
INSERT INTO ocs_or VALUES (9,'x',90) ON CONFLICT (nosuchcol) DO UPDATE SET j = j;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
INSERT INTO ocs_or VALUES (9,'x',90) ON CONFLICT (nosuchcol) DO NOTHING;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "ocs_or" does not exist
-- end-expected-error
INSERT INTO ocs_or VALUES (9,'x',90) ON CONFLICT (i) DO UPDATE SET nosuchcol = 'x';
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "j" is ambiguous
-- end-expected-error
INSERT INTO ocs_or VALUES (9,'x',90) ON CONFLICT (i) DO UPDATE SET j = j WHERE nosuchcol = 1;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM ocs_or;

-- cleanup
DROP TABLE ocs_or;
