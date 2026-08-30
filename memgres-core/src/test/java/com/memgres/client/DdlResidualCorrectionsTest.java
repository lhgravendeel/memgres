package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The second pass over the residual DDL checks, after each one was re-measured against
 * PostgreSQL 18 rather than assumed.
 *
 * <p>Most of what it pins is the dangerous direction: definitions PostgreSQL runs and memgres was
 * refusing — an inlinable SQL function in a partition key, a partition key expression containing a
 * comma, the only name PostgreSQL gives a NOT NULL constraint, a quoted index name differing only
 * by case, {@code CHECK (...) NOT VALID} written in a CREATE TABLE, a hash partition whose modulus
 * divides another's, and {@code ALTER TABLE i RENAME TO j} aimed at an index. The rest is the
 * ordinary kind: refusals PostgreSQL makes that memgres accepted, each paired here with the
 * neighbouring shapes that must keep being accepted.
 */
class DdlResidualCorrectionsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static List<String> column(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }

    /** Assert the statement is refused with this SQLSTATE and a message containing this text. */
    private static void assertRejected(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    /** Assert the statement is accepted. Used for the shapes that must not start failing. */
    private static void assertAccepted(String sql) {
        assertDoesNotThrow(() -> exec(sql), sql);
    }

    // ---------------------------------------------------------------- SECTION A
    // A partition key expression is judged by what it computes, not by how it is spelled.

    @Test
    void anInlinableSqlFunctionIsAnImmutablePartitionKey() throws Exception {
        exec("CREATE FUNCTION pk_sqlstable(int) RETURNS int LANGUAGE sql STABLE AS 'SELECT $1 + 1'");
        exec("CREATE FUNCTION pk_sqlvol(int) RETURNS int LANGUAGE sql AS 'SELECT $1 + 1'");
        exec("CREATE FUNCTION pk_sqlimm(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 + 1'");
        // PostgreSQL inlines the body before it judges the expression, so what the declaration
        // says about volatility never comes up.
        assertAccepted("CREATE TABLE pk_q1 (a int) PARTITION BY RANGE ((pk_sqlstable(a)))");
        assertAccepted("CREATE TABLE pk_q2 (a int) PARTITION BY RANGE ((pk_sqlvol(a)))");
        assertAccepted("CREATE TABLE pk_q3 (a int) PARTITION BY RANGE ((pk_sqlimm(a)))");
    }

    @Test
    void aBodyThatCannotBeInlinedIsBelievedWhenItSaysItIsNotImmutable() throws Exception {
        exec("CREATE FUNCTION pk_plstable(int) RETURNS int LANGUAGE plpgsql STABLE"
                + " AS 'BEGIN RETURN $1 + 1; END'");
        assertRejected("42P17", "functions in partition key expression must be marked IMMUTABLE",
                "CREATE TABLE pk_q4 (a int) PARTITION BY RANGE ((pk_plstable(a)))");
        // A built-in volatile function is refused whatever it is wrapped in.
        assertRejected("42P17", "functions in partition key expression must be marked IMMUTABLE",
                "CREATE TABLE pk_q5 (a int) PARTITION BY RANGE ((random()::int + a))");
    }

    @Test
    void aPartitionKeyExpressionMayContainACommaOfItsOwn() {
        // The key list was split on every comma, so a call with two arguments was read as two
        // keys and the second half of it reported as a missing column.
        assertAccepted("CREATE TABLE pk_e1 (ts timestamp) PARTITION BY RANGE ((date_trunc('month', ts)))");
        assertAccepted("CREATE TABLE pk_e2 (b int) PARTITION BY RANGE ((coalesce(b, 0)))");
        assertAccepted("CREATE TABLE pk_e3 (s text) PARTITION BY RANGE ((substr(s, 1, 1)))");
        assertAccepted("CREATE TABLE pk_e4 (d date) PARTITION BY RANGE ((date_part('year', d)))");
        assertAccepted("CREATE TABLE pk_e5 (a int, b int) PARTITION BY LIST ((greatest(a, b)))");
        // And a key list that really is several keys still is.
        assertAccepted("CREATE TABLE pk_e6 (a int, b int) PARTITION BY RANGE (a, b)");
        assertAccepted("CREATE TABLE pk_e7 (a int, b int) PARTITION BY RANGE ((a + b), b)");
    }

    @Test
    void aPartitionKeyExpressionOnlyReadsTheRowsColumns() {
        assertRejected("42703", "column \"nosuchcol\" does not exist",
                "CREATE TABLE pk_e8 (a int) PARTITION BY RANGE ((nosuchcol + 1))");
        assertRejected("42703", "column \"nosuchcol\" named in partition key does not exist",
                "CREATE TABLE pk_e9 (a int) PARTITION BY RANGE (nosuchcol)");
    }

    // ---------------------------------------------------------------- SECTION B
    // A NOT NULL constraint has a name, and it is the name every command asks for.

    @Test
    void aNotNullConstraintAnswersToItsDefaultName() throws Exception {
        exec("CREATE TABLE nn_a (id int PRIMARY KEY, a int)");
        exec("ALTER TABLE nn_a ALTER COLUMN a SET NOT NULL");
        assertEquals("[nn_a_a_not_null, nn_a_id_not_null]",
                column("SELECT conname FROM pg_constraint WHERE conrelid = 'nn_a'::regclass"
                        + " AND contype = 'n' ORDER BY conname").toString());
        // Dropping it is what makes the column nullable again, and the insert then works.
        assertAccepted("ALTER TABLE nn_a DROP CONSTRAINT nn_a_a_not_null");
        assertEquals("YES", scalar("SELECT is_nullable FROM information_schema.columns"
                + " WHERE table_name='nn_a' AND column_name='a'"));
        assertAccepted("INSERT INTO nn_a (id) VALUES (1)");
        // And it is gone, so a second drop finds nothing.
        assertRejected("42704", "constraint \"nn_a_a_not_null\" of relation \"nn_a\" does not exist",
                "ALTER TABLE nn_a DROP CONSTRAINT nn_a_a_not_null");
    }

    @Test
    void aColumnDeclaredNotNullHasTheSameConstraint() throws Exception {
        exec("CREATE TABLE nn_b (id int PRIMARY KEY, a int NOT NULL)");
        assertEquals("[nn_b_a_not_null, nn_b_id_not_null]",
                column("SELECT conname FROM pg_constraint WHERE conrelid = 'nn_b'::regclass"
                        + " AND contype = 'n' ORDER BY conname").toString());
        assertAccepted("ALTER TABLE nn_b DROP CONSTRAINT nn_b_a_not_null");
        assertAccepted("INSERT INTO nn_b (id) VALUES (1)");
    }

    @Test
    void aWrittenNameIsTheNameTheCatalogShows() throws Exception {
        exec("CREATE TABLE nn_c (id int PRIMARY KEY, a int, b int NOT NULL)");
        exec("ALTER TABLE nn_c ADD CONSTRAINT nn_c_a NOT NULL a");
        assertEquals("[nn_c_a, nn_c_b_not_null, nn_c_id_not_null]",
                column("SELECT conname FROM pg_constraint WHERE conrelid = 'nn_c'::regclass"
                        + " AND contype = 'n' ORDER BY conname").toString());
        // The written name is the one that can be dropped; the default one was never created.
        assertRejected("42704", "constraint \"nn_c_a_not_null\" of relation \"nn_c\" does not exist",
                "ALTER TABLE nn_c DROP CONSTRAINT nn_c_a_not_null");
        assertAccepted("ALTER TABLE nn_c DROP CONSTRAINT nn_c_a");
        assertAccepted("INSERT INTO nn_c (id, b) VALUES (1, 1)");
    }

    @Test
    void namingAConstraintOverAnAlreadyNotNullColumnCreatesNothing() throws Exception {
        exec("CREATE TABLE nn_d (id int PRIMARY KEY, b int NOT NULL)");
        // There is no constraint left for the written name to be the name of. PostgreSQL folds
        // the declaration into the constraint already there, which keeps its own name, so the
        // statement succeeds and the name it was written with is the name of nothing.
        assertAccepted("ALTER TABLE nn_d ADD CONSTRAINT nn_d_b NOT NULL b");
        assertRejected("42704", "constraint \"nn_d_b\" of relation \"nn_d\" does not exist",
                "ALTER TABLE nn_d DROP CONSTRAINT nn_d_b");
        assertRejected("42704", "constraint \"nn_d_b\" of relation \"nn_d\" does not exist",
                "ALTER TABLE nn_d ALTER CONSTRAINT nn_d_b NO INHERIT");
        assertAccepted("ALTER TABLE nn_d DROP CONSTRAINT nn_d_b_not_null");
    }

    @Test
    void theConstraintCanBeRenamedAndSurvivesARenameOfItsColumn() throws Exception {
        exec("CREATE TABLE nn_e (id int PRIMARY KEY, a int NOT NULL)");
        assertAccepted("ALTER TABLE nn_e RENAME CONSTRAINT nn_e_a_not_null TO nn_e_nn");
        assertEquals("[nn_e_id_not_null, nn_e_nn]",
                column("SELECT conname FROM pg_constraint WHERE conrelid = 'nn_e'::regclass"
                        + " AND contype = 'n' ORDER BY conname").toString());
        assertAccepted("ALTER TABLE nn_e DROP CONSTRAINT nn_e_nn");

        exec("CREATE TABLE nn_f (id int PRIMARY KEY, a int NOT NULL)");
        // The constraint keeps the name it was given, which was derived from the old column name.
        assertAccepted("ALTER TABLE nn_f RENAME COLUMN a TO z");
        assertEquals("[nn_f_a_not_null, nn_f_id_not_null]",
                column("SELECT conname FROM pg_constraint WHERE conrelid = 'nn_f'::regclass"
                        + " AND contype = 'n' ORDER BY conname").toString());
    }

    @Test
    void aKeyColumnsNotNullConstraintBelongsToTheKey() throws Exception {
        exec("CREATE TABLE nn_g (id int PRIMARY KEY, a int)");
        assertRejected("42P16", "column \"id\" is in a primary key",
                "ALTER TABLE nn_g DROP CONSTRAINT nn_g_id_not_null");
    }

    // ---------------------------------------------------------------- SECTION C
    // ALTER DOMAIN changes shared state, so ROLLBACK has to put it back.

    @Test
    void aRolledBackAlterDomainLeavesTheDomainAsItWas() throws Exception {
        exec("CREATE DOMAIN dm_a AS int");
        exec("CREATE TABLE dm_at (id int PRIMARY KEY, a dm_a)");
        exec("BEGIN");
        exec("ALTER DOMAIN dm_a SET NOT NULL");
        exec("ROLLBACK");
        assertAccepted("INSERT INTO dm_at VALUES (1, NULL)");

        exec("DELETE FROM dm_at");
        exec("BEGIN");
        exec("ALTER DOMAIN dm_a SET DEFAULT 7");
        exec("ROLLBACK");
        exec("INSERT INTO dm_at (id) VALUES (2)");
        assertNull(scalar("SELECT a FROM dm_at WHERE id = 2"));

        exec("BEGIN");
        exec("ALTER DOMAIN dm_a ADD CONSTRAINT dm_a_c CHECK (VALUE > 100)");
        exec("ROLLBACK");
        assertAccepted("INSERT INTO dm_at VALUES (3, 5)");
    }

    @Test
    void aCommittedAlterDomainStands() throws Exception {
        exec("CREATE DOMAIN dm_b AS int");
        exec("CREATE TABLE dm_bt (id int PRIMARY KEY, a dm_b)");
        exec("BEGIN");
        exec("ALTER DOMAIN dm_b SET NOT NULL");
        exec("COMMIT");
        assertRejected("23502", "domain dm_b does not allow null values",
                "INSERT INTO dm_bt VALUES (1, NULL)");
        exec("ALTER DOMAIN dm_b DROP NOT NULL");
        assertAccepted("INSERT INTO dm_bt VALUES (2, NULL)");
    }

    @Test
    void anAlterDomainRenameReallyRenames() throws Exception {
        exec("CREATE DOMAIN dm_c AS int");
        exec("CREATE TYPE dm_enum AS ENUM ('x')");
        exec("CREATE TABLE dm_host (a int)");
        // A type name is one name across enums, composites, domains and tables.
        assertRejected("42710", "type \"dm_enum\" already exists",
                "ALTER DOMAIN dm_c RENAME TO dm_enum");
        assertRejected("42710", "type \"dm_host\" already exists",
                "ALTER DOMAIN dm_c RENAME TO dm_host");
        assertAccepted("ALTER DOMAIN dm_c RENAME TO dm_c2");
        assertEquals("dm_c2", scalar("SELECT typname FROM pg_type WHERE typname = 'dm_c2'"));
        assertRejected("3F000", "schema \"dm_nosuch\" does not exist",
                "ALTER DOMAIN dm_c2 SET SCHEMA dm_nosuch");
    }

    // ---------------------------------------------------------------- SECTION D
    // A command aimed at the wrong kind of relation says which kind it wanted.

    @Test
    void refreshNamesTheKindsItAccepts() throws Exception {
        exec("CREATE TABLE rf_t (id int PRIMARY KEY)");
        exec("CREATE VIEW rf_v AS SELECT id FROM rf_t");
        exec("CREATE SEQUENCE rf_s");
        exec("CREATE INDEX rf_i ON rf_t (id)");
        assertRejected("42809", "\"rf_v\" is not a table or materialized view",
                "REFRESH MATERIALIZED VIEW rf_v");
        assertRejected("42809", "\"rf_s\" is not a table or materialized view",
                "REFRESH MATERIALIZED VIEW rf_s");
        assertRejected("42809", "\"rf_i\" is not a table or materialized view",
                "REFRESH MATERIALIZED VIEW rf_i");
        // A plain table gets as far as the materialized-view check and is refused by that.
        assertRejected("0A000", "\"rf_t\" is not a materialized view",
                "REFRESH MATERIALIZED VIEW rf_t");
        assertRejected("42P01", "relation \"rf_nosuch\" does not exist",
                "REFRESH MATERIALIZED VIEW rf_nosuch");
        exec("CREATE MATERIALIZED VIEW rf_m AS SELECT id FROM rf_t");
        assertAccepted("REFRESH MATERIALIZED VIEW rf_m");
    }

    @Test
    void createOrReplaceViewSaysWhatItCannotReplace() throws Exception {
        exec("CREATE TABLE cr_t (a int)");
        exec("CREATE SEQUENCE cr_s");
        exec("CREATE MATERIALIZED VIEW cr_m AS SELECT 1 AS a");
        assertRejected("42809", "\"cr_t\" is not a view",
                "CREATE OR REPLACE VIEW cr_t AS SELECT 1 AS a");
        assertRejected("42809", "\"cr_s\" is not a view",
                "CREATE OR REPLACE VIEW cr_s AS SELECT 1 AS a");
        assertRejected("42809", "\"cr_m\" is not a view",
                "CREATE OR REPLACE VIEW cr_m AS SELECT 1 AS a");
        // A plain CREATE reports the name as taken instead.
        assertRejected("42P07", "relation \"cr_t\" already exists",
                "CREATE VIEW cr_t AS SELECT 1 AS a");
        // And replacing a view really is accepted.
        exec("CREATE VIEW cr_v AS SELECT 1 AS a");
        assertAccepted("CREATE OR REPLACE VIEW cr_v AS SELECT 2 AS a");
        assertEquals("2", scalar("SELECT a FROM cr_v"));
    }

    @Test
    void alterTableOnAnIndexRenamesItAndRefusesTheRest() throws Exception {
        exec("CREATE TABLE ai_t (i int PRIMARY KEY, j int)");
        exec("CREATE INDEX ai_idx ON ai_t (j)");
        assertRejected("42809", "ALTER action ADD COLUMN cannot be performed on relation \"ai_idx\"",
                "ALTER TABLE ai_idx ADD COLUMN z int");
        assertRejected("42809", "ALTER action DROP COLUMN cannot be performed on relation \"ai_idx\"",
                "ALTER TABLE ai_idx DROP COLUMN j");
        assertRejected("42809", "cannot change schema of index \"ai_idx\"",
                "ALTER TABLE ai_idx SET SCHEMA public");
        // The two actions that mean something on an index are run, not refused.
        assertAccepted("ALTER TABLE ai_idx OWNER TO memgres");
        assertAccepted("ALTER TABLE ai_idx RENAME TO ai_idx9");
        assertEquals("[ai_idx9, ai_t_pkey]",
                column("SELECT indexname FROM pg_indexes WHERE tablename = 'ai_t'"
                        + " ORDER BY indexname").toString());
        // An ordinary ALTER TABLE on an actual table keeps working.
        assertAccepted("ALTER TABLE ai_t ADD COLUMN z int");
        assertRejected("42P01", "relation \"ai_nosuch\" does not exist",
                "ALTER TABLE ai_nosuch ADD COLUMN z int");
    }

    @Test
    void aRenameOntoATakenRelationNameIsRefused() throws Exception {
        exec("CREATE TABLE ar_t (i int PRIMARY KEY, j int)");
        exec("CREATE INDEX ar_idx ON ar_t (j)");
        assertRejected("42P07", "relation \"ar_t\" already exists",
                "ALTER INDEX ar_idx RENAME TO ar_t");
        assertAccepted("ALTER INDEX ar_idx RENAME TO ar_idx2");
        assertEquals("[ar_idx2, ar_t_pkey]",
                column("SELECT indexname FROM pg_indexes WHERE tablename = 'ar_t'"
                        + " ORDER BY indexname").toString());
        assertRejected("42P07", "relation \"ar_idx2\" already exists",
                "ALTER TABLE ar_t RENAME TO ar_idx2");
        assertRejected("42P01", "relation \"ar_nosuch\" does not exist",
                "ALTER INDEX ar_nosuch RENAME TO ar_x");
        assertAccepted("ALTER INDEX IF EXISTS ar_nosuch RENAME TO ar_x");
    }

    @Test
    void aRoleIsNotRenamedOntoAnotherRole() throws Exception {
        exec("CREATE ROLE rn_a");
        exec("CREATE ROLE rn_b");
        assertRejected("42710", "role \"rn_b\" already exists", "ALTER ROLE rn_a RENAME TO rn_b");
        assertAccepted("ALTER ROLE rn_a RENAME TO rn_c");
        assertEquals("[rn_b, rn_c]", column("SELECT rolname FROM pg_roles"
                + " WHERE rolname IN ('rn_a','rn_b','rn_c') ORDER BY rolname").toString());
    }

    // ---------------------------------------------------------------- SECTION E
    // An index name belongs to one index, and the spelling is part of the name.

    @Test
    void aQuotedIndexNameDiffersFromTheUnquotedOne() throws Exception {
        exec("CREATE TABLE ix_t (b int PRIMARY KEY)");
        exec("CREATE INDEX ix_mixedcase ON ix_t (b)");
        assertAccepted("CREATE INDEX \"ix_MixedCase\" ON ix_t (b)");
        assertEquals("[ix_MixedCase, ix_mixedcase, ix_t_pkey]",
                column("SELECT indexname FROM pg_indexes WHERE tablename = 'ix_t'"
                        + " ORDER BY indexname").toString());
        // The same name written the same way is still taken.
        assertRejected("42P07", "relation \"ix_mixedcase\" already exists",
                "CREATE INDEX ix_mixedcase ON ix_t (b)");
        assertAccepted("DROP INDEX \"ix_MixedCase\"");
        assertAccepted("DROP INDEX ix_mixedcase");
    }

    @Test
    void anImplicitIndexOwnsItsNameToo() throws Exception {
        exec("CREATE TABLE im_t (id serial PRIMARY KEY, a int UNIQUE)");
        assertRejected("42P07", "relation \"im_t_pkey\" already exists",
                "CREATE TABLE im_t_pkey (x int)");
        assertRejected("42P07", "relation \"im_t_a_key\" already exists",
                "CREATE TABLE im_t_a_key (x int)");
        // The index the key made for itself goes when the key does, not before.
        assertRejected("2BP01", "cannot drop index im_t_pkey because constraint im_t_pkey"
                + " on table im_t requires it", "DROP INDEX im_t_pkey");
        // An index somebody wrote is theirs to drop.
        exec("CREATE UNIQUE INDEX im_written ON im_t (a)");
        assertAccepted("DROP INDEX im_written");
    }

    @Test
    void anUnqualifiedDropIndexOnlyReachesTheSearchPath() throws Exception {
        exec("CREATE SCHEMA iq_s");
        exec("CREATE TABLE iq_s.t (a int PRIMARY KEY, b int)");
        exec("CREATE INDEX iq_idx ON iq_s.t (b)");
        assertRejected("42704", "index \"iq_idx\" does not exist", "DROP INDEX iq_idx");
        assertAccepted("DROP INDEX iq_s.iq_idx");
        assertRejected("42704", "index \"iq_nosuch\" does not exist", "DROP INDEX iq_nosuch");
        assertAccepted("DROP INDEX IF EXISTS iq_nosuch");
    }

    @Test
    void aQualifiedNameIsCheckedAgainstItsOwnSchema() throws Exception {
        exec("CREATE SCHEMA ns_q");
        exec("CREATE VIEW ns_q.thing AS SELECT 1 AS a");
        assertRejected("42P07", "relation \"thing\" already exists",
                "CREATE TABLE ns_q.thing (a int)");
        assertRejected("42P07", "relation \"thing\" already exists",
                "CREATE SEQUENCE ns_q.thing");
        // Another name in that schema is free.
        assertAccepted("CREATE TABLE ns_q.other (a int)");
    }

    // ---------------------------------------------------------------- SECTION F
    // A multi-action ALTER TABLE settles one shape, and either reaches it or leaves none.

    @Test
    void everyDropRunsBeforeTheThingsThatSet() throws Exception {
        exec("CREATE TABLE ma_t (id int PRIMARY KEY, d int DEFAULT 4, c int NOT NULL DEFAULT 0)");
        exec("ALTER TABLE ma_t ALTER COLUMN d SET DEFAULT 11, ALTER COLUMN d DROP DEFAULT");
        assertEquals("11", scalar("SELECT column_default FROM information_schema.columns"
                + " WHERE table_name='ma_t' AND column_name='d'"));
        exec("ALTER TABLE ma_t ALTER COLUMN c SET NOT NULL, ALTER COLUMN c DROP NOT NULL");
        assertEquals("NO", scalar("SELECT is_nullable FROM information_schema.columns"
                + " WHERE table_name='ma_t' AND column_name='c'"));
        // Written the other way round the answer is the same, because the pass decides.
        exec("ALTER TABLE ma_t ALTER COLUMN d DROP DEFAULT, ALTER COLUMN d SET DEFAULT 12");
        assertEquals("12", scalar("SELECT column_default FROM information_schema.columns"
                + " WHERE table_name='ma_t' AND column_name='d'"));
    }

    @Test
    void aRefusedMultiActionAlterLeavesTheTableAsItWas() throws Exception {
        exec("CREATE TABLE ma_u (id int PRIMARY KEY, z int)");
        assertRejected("42703", "column \"z\" does not exist",
                "ALTER TABLE ma_u ADD CONSTRAINT ma_u_c CHECK (z IS NULL OR z > 0), DROP COLUMN z");
        assertEquals("[id, z]", column("SELECT column_name FROM information_schema.columns"
                + " WHERE table_name='ma_u' ORDER BY column_name").toString());

        exec("CREATE TABLE ma_v (id int PRIMARY KEY, z int)");
        exec("INSERT INTO ma_v VALUES (1, NULL)");
        assertRejected("23502", "column \"z\" of relation \"ma_v\" contains null values",
                "ALTER TABLE ma_v ADD COLUMN y int, ALTER COLUMN z SET NOT NULL");
        assertEquals("[id, z]", column("SELECT column_name FROM information_schema.columns"
                + " WHERE table_name='ma_v' ORDER BY column_name").toString());
    }

    @Test
    void anOrdinaryMultiActionAlterStillRuns() throws Exception {
        exec("CREATE TABLE ma_w (id int PRIMARY KEY, a int, b int)");
        assertAccepted("ALTER TABLE ma_w ADD COLUMN c int, ALTER COLUMN a SET DEFAULT 3,"
                + " ALTER COLUMN b SET NOT NULL");
        assertEquals("[a, b, c, id]", column("SELECT column_name FROM information_schema.columns"
                + " WHERE table_name='ma_w' ORDER BY column_name").toString());
        assertAccepted("ALTER TABLE ma_w DROP COLUMN c, ADD COLUMN e int DEFAULT 9");
        assertEquals("[a, b, e, id]", column("SELECT column_name FROM information_schema.columns"
                + " WHERE table_name='ma_w' ORDER BY column_name").toString());
        // A constraint written before the column it reads is still accepted: ADD COLUMN is an
        // earlier pass than ADD CONSTRAINT.
        assertAccepted("ALTER TABLE ma_w ADD CONSTRAINT ma_w_ck CHECK (f > 0), ADD COLUMN f int");
    }

    // ---------------------------------------------------------------- SECTION G
    // What is already stored decides whether a rule about to be declared can hold.

    @Test
    void aNotNullColumnAddedToRowsThatExistNeedsAValueToPutInIt() throws Exception {
        exec("CREATE TABLE ex_t (id int PRIMARY KEY)");
        exec("INSERT INTO ex_t VALUES (1)");
        // DEFAULT NULL fills the rows with nothing, so it is no default at all.
        assertRejected("23502", "column \"c\" of relation \"ex_t\" contains null values",
                "ALTER TABLE ex_t ADD COLUMN c int NOT NULL DEFAULT NULL");
        assertRejected("23502", "column \"d\" of relation \"ex_t\" contains null values",
                "ALTER TABLE ex_t ADD COLUMN d int NOT NULL");
        assertAccepted("ALTER TABLE ex_t ADD COLUMN e int NOT NULL DEFAULT 3");
        assertEquals("3", scalar("SELECT e FROM ex_t"));
        // On an empty table there are no rows to contradict it.
        exec("CREATE TABLE ex_u (id int PRIMARY KEY)");
        assertAccepted("ALTER TABLE ex_u ADD COLUMN c int NOT NULL DEFAULT NULL");
    }

    @Test
    void aRetypeMayNotBreakWhatIsDeclaredOverTheColumn() throws Exception {
        exec("CREATE TABLE ex_w (id int PRIMARY KEY, a text)");
        exec("INSERT INTO ex_w VALUES (1,'1'),(2,'01')");
        exec("CREATE UNIQUE INDEX ex_w_uidx ON ex_w (a)");
        assertRejected("23505", "could not create unique index \"ex_w_uidx\"",
                "ALTER TABLE ex_w ALTER COLUMN a TYPE int USING a::int");
        // The statement is refused whole: the old values are still there.
        assertEquals("[01, 1]", column("SELECT a FROM ex_w ORDER BY id DESC").toString());

        exec("CREATE TABLE ex_x (id int PRIMARY KEY, a text NOT NULL)");
        exec("INSERT INTO ex_x VALUES (1,'1')");
        assertRejected("23502", "column \"a\" of relation \"ex_x\" contains null values",
                "ALTER TABLE ex_x ALTER COLUMN a TYPE int USING NULL");
        assertEquals("1", scalar("SELECT a FROM ex_x"));

        // A conversion that keeps the values distinct is accepted.
        exec("CREATE TABLE ex_y (id int PRIMARY KEY, a text)");
        exec("INSERT INTO ex_y VALUES (1,'1'),(2,'2')");
        exec("CREATE UNIQUE INDEX ex_y_uidx ON ex_y (a)");
        assertAccepted("ALTER TABLE ex_y ALTER COLUMN a TYPE int USING a::int");
        assertEquals("[1, 2]", column("SELECT a FROM ex_y ORDER BY id").toString());
    }

    @Test
    void restartIsAnIdentityAction() throws Exception {
        exec("CREATE TABLE id_t (id int GENERATED BY DEFAULT AS IDENTITY, w int, s serial)");
        assertRejected("55000", "column \"w\" of relation \"id_t\" is not an identity column",
                "ALTER TABLE id_t ALTER COLUMN w RESTART");
        // A serial column's sequence is a default, not an identity.
        assertRejected("55000", "column \"s\" of relation \"id_t\" is not an identity column",
                "ALTER TABLE id_t ALTER COLUMN s RESTART");
        assertRejected("42703", "column \"nosuch\" of relation \"id_t\" does not exist",
                "ALTER TABLE id_t ALTER COLUMN nosuch RESTART");
        assertAccepted("ALTER TABLE id_t ALTER COLUMN id RESTART");
        assertAccepted("ALTER TABLE id_t ALTER COLUMN id RESTART WITH 50");
    }

    // ---------------------------------------------------------------- SECTION H
    // A definition that contradicts itself is refused for what it says.

    @Test
    void aTableHasAtMostOnePrimaryKey() throws Exception {
        assertRejected("42P16", "multiple primary keys for table \"pk_two\" are not allowed",
                "CREATE TABLE pk_two (a int PRIMARY KEY, b int PRIMARY KEY)");
        assertRejected("42P16", "multiple primary keys for table \"pk_two2\" are not allowed",
                "CREATE TABLE pk_two2 (a int PRIMARY KEY, b int, PRIMARY KEY (b))");
        assertRejected("42P16", "multiple primary keys for table \"pk_two3\" are not allowed",
                "CREATE TABLE pk_two3 (a int, b int, PRIMARY KEY (a), PRIMARY KEY (b))");
        // One key over several columns is one key.
        assertAccepted("CREATE TABLE pk_one (a int, b int, PRIMARY KEY (a, b))");
        assertAccepted("CREATE TABLE pk_one2 (a int PRIMARY KEY, b int UNIQUE)");
        exec("CREATE TABLE pk_one3 (a int PRIMARY KEY, b int)");
        assertRejected("42P16", "multiple primary keys for table \"pk_one3\" are not allowed",
                "ALTER TABLE pk_one3 ADD PRIMARY KEY (b)");
        assertRejected("42P16", "multiple primary keys for table \"pk_one3\" are not allowed",
                "ALTER TABLE pk_one3 ADD CONSTRAINT pk_one3_k PRIMARY KEY (b)");
    }

    @Test
    void notValidIsAcceptedWhereThereIsSomethingToDefer() throws Exception {
        // Written in a CREATE TABLE there is nothing already stored to skip over, so the word
        // is spare rather than a syntax error.
        assertAccepted("CREATE TABLE nv_a (id int PRIMARY KEY, a int,"
                + " CONSTRAINT nv_a_c CHECK (a > 0) NOT VALID)");
        assertAccepted("CREATE TABLE nv_b (id int PRIMARY KEY, a int,"
                + " CONSTRAINT nv_b_n NOT NULL a NOT VALID)");
        // A key is enforced by an index built at once, so there is nothing to defer.
        assertRejected("0A000", "UNIQUE constraints cannot be marked NOT VALID",
                "CREATE TABLE nv_c (id int PRIMARY KEY, a int,"
                        + " CONSTRAINT nv_c_u UNIQUE (a) NOT VALID)");
        assertRejected("0A000", "PRIMARY KEY constraints cannot be marked NOT VALID",
                "CREATE TABLE nv_d (id int, a int, CONSTRAINT nv_d_p PRIMARY KEY (a) NOT VALID)");
        exec("CREATE TABLE nv_e (id int PRIMARY KEY, a int)");
        assertRejected("0A000", "UNIQUE constraints cannot be marked NOT VALID",
                "ALTER TABLE nv_e ADD CONSTRAINT nv_e_u UNIQUE (a) NOT VALID");
        assertRejected("0A000", "PRIMARY KEY constraints cannot be marked NOT VALID",
                "ALTER TABLE nv_e ADD CONSTRAINT nv_e_p PRIMARY KEY (a) NOT VALID");
        assertAccepted("ALTER TABLE nv_e ADD CONSTRAINT nv_e_c CHECK (a > 0) NOT VALID");
    }

    @Test
    void addAttributeHasNoIfNotExists() throws Exception {
        exec("CREATE TYPE at_t AS (a int, b text)");
        assertRejected("42601", "syntax error at or near \"NOT\"",
                "ALTER TYPE at_t ADD ATTRIBUTE IF NOT EXISTS c int");
        assertAccepted("ALTER TYPE at_t ADD ATTRIBUTE c int");
        // DROP ATTRIBUTE does have IF EXISTS.
        assertAccepted("ALTER TYPE at_t DROP ATTRIBUTE IF EXISTS c");
        assertAccepted("ALTER TYPE at_t DROP ATTRIBUTE IF EXISTS nosuch");
    }

    // ---------------------------------------------------------------- SECTION I
    // A partition attaches to a partitioned table, over a slot nothing else covers.

    @Test
    void aPartitionNeedsAPartitionedParent() throws Exception {
        exec("CREATE TABLE po_plain (i int)");
        assertRejected("42P17", "\"po_plain\" is not partitioned",
                "CREATE TABLE po_x PARTITION OF po_plain FOR VALUES FROM (1) TO (2)");
        assertRejected("42P17", "\"po_plain\" is not partitioned",
                "CREATE TABLE po_y PARTITION OF po_plain DEFAULT");
        exec("CREATE TABLE po_p (i int) PARTITION BY RANGE (i)");
        assertAccepted("CREATE TABLE po_p1 PARTITION OF po_p FOR VALUES FROM (1) TO (2)");
    }

    @Test
    void aHashModulusOnlyHasToDivideTheOtherOnes() throws Exception {
        exec("CREATE TABLE hp_t (i int) PARTITION BY HASH (i)");
        exec("CREATE TABLE hp_1 PARTITION OF hp_t FOR VALUES WITH (MODULUS 4, REMAINDER 1)");
        // 2 divides 4, and remainder 0 modulo 2 never meets remainder 1 modulo 4.
        assertAccepted("CREATE TABLE hp_2 PARTITION OF hp_t FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
        // Remainder 1 modulo 2 covers remainder 1 modulo 4, which is already taken.
        assertRejected("42P17", "partition \"hp_3\" would overlap partition",
                "CREATE TABLE hp_3 PARTITION OF hp_t FOR VALUES WITH (MODULUS 2, REMAINDER 1)");
        // 3 neither divides 4 nor is divided by it.
        assertRejected("42P17", "every hash partition modulus must be a factor of the next larger modulus",
                "CREATE TABLE hp_4 PARTITION OF hp_t FOR VALUES WITH (MODULUS 3, REMAINDER 0)");
        // A larger modulus that 4 divides is fine where it does not overlap.
        assertAccepted("CREATE TABLE hp_5 PARTITION OF hp_t FOR VALUES WITH (MODULUS 8, REMAINDER 3)");
        assertAccepted("CREATE TABLE hp_6 PARTITION OF hp_t FOR VALUES WITH (MODULUS 8, REMAINDER 7)");
        assertRejected("42P16", "remainder for hash partition must be less than modulus",
                "CREATE TABLE hp_7 PARTITION OF hp_t FOR VALUES WITH (MODULUS 4, REMAINDER 4)");
    }

    // ---------------------------------------------------------------- SECTION J
    // LIKE copies the columns; the rest travels only when it is asked for.

    @Test
    void likeCopiesNoKeyUnlessTheIndexesAreAskedFor() throws Exception {
        exec("CREATE TABLE lk_src (id int PRIMARY KEY, a int UNIQUE)");
        exec("CREATE TABLE lk_plain (LIKE lk_src)");
        assertAccepted("INSERT INTO lk_plain VALUES (1, 1)");
        assertAccepted("INSERT INTO lk_plain VALUES (1, 1)");
        // Only the NOT NULL the key implied travels with the column.
        assertEquals("1", scalar("SELECT count(*)::int FROM pg_constraint"
                + " WHERE conrelid = 'lk_plain'::regclass"));

        exec("CREATE TABLE lk_cons (LIKE lk_src INCLUDING CONSTRAINTS)");
        assertAccepted("INSERT INTO lk_cons VALUES (1, 1)");
        assertAccepted("INSERT INTO lk_cons VALUES (1, 1)");

        // INCLUDING INDEXES is what brings the key, under the new table's own name.
        exec("CREATE TABLE lk_idx (LIKE lk_src INCLUDING INDEXES)");
        assertAccepted("INSERT INTO lk_idx VALUES (1, 1)");
        assertRejected("23505", "duplicate key value violates unique constraint \"lk_idx_pkey\"",
                "INSERT INTO lk_idx VALUES (1, 1)");

        exec("CREATE TABLE lk_all (LIKE lk_src INCLUDING ALL)");
        assertAccepted("INSERT INTO lk_all VALUES (1, 1)");
        assertRejected("23505", "duplicate key value violates unique constraint \"lk_all_pkey\"",
                "INSERT INTO lk_all VALUES (1, 1)");
    }

    // ---------------------------------------------------------------- SECTION K
    // A materialized view is a relation others depend on.

    @Test
    void aViewOverAMaterializedViewBlocksItsDrop() throws Exception {
        exec("CREATE TABLE mv_b (id int PRIMARY KEY)");
        exec("INSERT INTO mv_b VALUES (1)");
        exec("CREATE MATERIALIZED VIEW mv_m AS SELECT id FROM mv_b");
        exec("CREATE VIEW mv_v AS SELECT id FROM mv_m");
        assertRejected("2BP01", "cannot drop materialized view mv_m because other objects depend on it",
                "DROP MATERIALIZED VIEW mv_m");
        assertRejected("2BP01", "cannot drop table mv_b because other objects depend on it",
                "DROP TABLE mv_b");
        assertAccepted("DROP TABLE mv_b CASCADE");
        assertEquals("0", scalar("SELECT count(*)::int FROM information_schema.views"
                + " WHERE table_name = 'mv_v'"));
    }

    @Test
    void cascadeOnTheMaterializedViewTakesItsReadersToo() throws Exception {
        exec("CREATE TABLE mv_b2 (id int PRIMARY KEY)");
        exec("CREATE MATERIALIZED VIEW mv_m2 AS SELECT id FROM mv_b2");
        exec("CREATE VIEW mv_v2 AS SELECT id FROM mv_m2");
        assertAccepted("DROP MATERIALIZED VIEW mv_m2 CASCADE");
        assertEquals("0", scalar("SELECT count(*)::int FROM information_schema.views"
                + " WHERE table_name = 'mv_v2'"));
        // Nothing reading it means nothing blocking it.
        exec("CREATE MATERIALIZED VIEW mv_m3 AS SELECT id FROM mv_b2");
        assertAccepted("DROP MATERIALIZED VIEW mv_m3");
    }

    @Test
    void aBlockedDropNamesTheRelationTheWayItCanBeReached() throws Exception {
        exec("CREATE SCHEMA dq_s");
        exec("CREATE TABLE dq_s.shared (id int PRIMARY KEY)");
        exec("CREATE VIEW dq_s.v AS SELECT id FROM dq_s.shared");
        assertRejected("2BP01", "cannot drop table dq_s.shared because other objects depend on it",
                "DROP TABLE dq_s.shared");
        // A relation the search path reaches is named bare.
        exec("CREATE TABLE dq_pub (id int PRIMARY KEY)");
        exec("CREATE VIEW dq_pub_v AS SELECT id FROM dq_pub");
        assertRejected("2BP01", "cannot drop table dq_pub because other objects depend on it",
                "DROP TABLE dq_pub");
    }

    // ---------------------------------------------------------------- SECTION L
    // Moving an object needs somewhere to move it to.

    @Test
    void setSchemaNeedsASchemaThatExists() throws Exception {
        exec("CREATE SEQUENCE ss_seq");
        exec("CREATE TYPE ss_type AS (a int)");
        exec("CREATE TABLE ss_tab (a int)");
        assertRejected("3F000", "schema \"ss_nosuch\" does not exist",
                "ALTER SEQUENCE ss_seq SET SCHEMA ss_nosuch");
        assertRejected("3F000", "schema \"ss_nosuch\" does not exist",
                "ALTER TYPE ss_type SET SCHEMA ss_nosuch");
        assertRejected("3F000", "schema \"ss_nosuch\" does not exist",
                "ALTER TABLE ss_tab SET SCHEMA ss_nosuch");
        exec("CREATE SCHEMA ss_ok");
        assertAccepted("ALTER SEQUENCE ss_seq SET SCHEMA ss_ok");
        assertAccepted("ALTER TYPE ss_type SET SCHEMA ss_ok");
        assertAccepted("ALTER TABLE ss_tab SET SCHEMA ss_ok");
        assertEquals("1", scalar("SELECT count(*)::int FROM information_schema.tables"
                + " WHERE table_schema='ss_ok' AND table_name='ss_tab'"));
    }
}
