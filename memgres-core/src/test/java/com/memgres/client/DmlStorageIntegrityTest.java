package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Everything that writes a row has to write the right one, and every relation a write reaches has
 * to apply its own rules to it.
 *
 * <p>Neither held. MERGE was a second, thinner DML implementation that ran none of the guards the
 * ordinary paths run and scanned a partitioned table's own empty row list, so it silently did
 * nothing. A partition was a routing target rather than a relation: its parent's CHECK, NOT NULL
 * and DEFAULT never reached it, a write aimed straight at it ignored its own bound, and a key
 * change left the row in the wrong partition. Referential actions rewrote the child row without
 * asking the child's constraints or firing its triggers. And a rule was one string per relation
 * and event, so the second CREATE RULE replaced the first.
 *
 * <p>Every expectation here was read off PostgreSQL 18 before it was written down.
 */
class DmlStorageIntegrityTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** The first column of the first row, as text, or "(no rows)" when the query answers none. */
    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    /** The one value the query returns, read as the number it is. */
    /** The number of rows a write reports having touched. */
    private static int update(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            return st.executeUpdate(sql);
        }
    }

    /**
     * The primary message of the error a statement raises. PostgreSQL sends severity in its own
     * field, so the message on the wire never carries an "ERROR: " prefix; only a client that
     * renders the two together adds one.
     */
    private static String messageOf(String sql) {
        return fieldsOf(sql).getMessage();
    }

    private static long num(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getLong(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** The SQLSTATE a statement raises, or "OK" when it does not raise at all. */
    private static String stateOf(String sql) {
        try {
            exec(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The fields of the error a statement raises, as a client reads them off the wire. */
    private static org.postgresql.util.ServerErrorMessage fieldsOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> exec(sql),
                "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    private static String detailOf(String sql) {
        return fieldsOf(sql).getDetail();
    }

    /** The hint the error carries, which PostgreSQL sends in a field of its own. */
    private static String hintOf(String sql) {
        return fieldsOf(sql).getHint();
    }

    // ------------------------------------------------------------ MERGE runs the guards every other write path runs

    @Test
    void mergeIsRefusedInAReadOnlyTransaction() throws Exception {
        exec("CREATE TABLE wma_ro (id int, v int)");
        exec("INSERT INTO wma_ro VALUES (1,1)");
        exec("BEGIN");
        exec("SET TRANSACTION READ ONLY");
        assertEquals("25006", stateOf("MERGE INTO wma_ro t USING (VALUES (2)) s(id) ON t.id = s.id"
                + " WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, 5)"));
        exec("ROLLBACK");
        exec("BEGIN");
        exec("SET TRANSACTION READ ONLY");
        // PostgreSQL raises before it knows whether the arm would fire at all.
        assertEquals("25006", stateOf("MERGE INTO wma_ro t USING (VALUES (99)) s(id) ON t.id = s.id"
                + " WHEN MATCHED THEN UPDATE SET v = 7"));
        exec("ROLLBACK");
        assertEquals("1", scalar("SELECT count(*) FROM wma_ro"));
        assertEquals("1", scalar("SELECT v FROM wma_ro WHERE id = 1"));
    }

    @Test
    void mergeSeesTheRowsOfAPartitionedTarget() throws Exception {
        exec("CREATE TABLE wma_ff (k int, v text) PARTITION BY RANGE (k)");
        exec("CREATE TABLE wma_ff_1 PARTITION OF wma_ff FOR VALUES FROM (1) TO (10)");
        exec("CREATE TABLE wma_ff_2 PARTITION OF wma_ff FOR VALUES FROM (10) TO (20)");
        exec("CREATE TABLE wma_ffs (k int, v text)");
        exec("INSERT INTO wma_ff VALUES (5,'old')");
        exec("INSERT INTO wma_ffs VALUES (5,'new')");

        exec("MERGE INTO wma_ff t USING wma_ffs u ON t.k = u.k WHEN MATCHED THEN UPDATE SET v = u.v");
        assertEquals("new", scalar("SELECT v FROM wma_ff WHERE k = 5"));

        // With a MATCHED arm present the row is updated, not inserted a second time.
        exec("MERGE INTO wma_ff t USING wma_ffs u ON t.k = u.k"
                + " WHEN MATCHED THEN UPDATE SET v = 'A'"
                + " WHEN NOT MATCHED THEN INSERT (k,v) VALUES (u.k,'a')");
        assertEquals("1", scalar("SELECT count(*) FROM wma_ff"));
        assertEquals("A", scalar("SELECT v FROM wma_ff WHERE k = 5"));

        // A partition-key change moves the row between leaves.
        exec("MERGE INTO wma_ff t USING wma_ffs u ON t.k = u.k WHEN MATCHED THEN UPDATE SET k = 15");
        assertEquals("0", scalar("SELECT count(*) FROM wma_ff_1"));
        assertEquals("1", scalar("SELECT count(*) FROM wma_ff_2"));

        // NOT MATCHED BY SOURCE reaches the leaf row too.
        exec("MERGE INTO wma_ff t USING wma_ffs u ON t.k = u.k WHEN NOT MATCHED BY SOURCE THEN DELETE");
        assertEquals("0", scalar("SELECT count(*) FROM wma_ff"));

        // Two source rows matching one leaf row is PG's 21000.
        exec("INSERT INTO wma_ff VALUES (5,'x')");
        exec("INSERT INTO wma_ffs VALUES (5,'second')");
        assertEquals("21000", stateOf("MERGE INTO wma_ff t USING wma_ffs u ON t.k = u.k"
                + " WHEN MATCHED THEN UPDATE SET v = u.v"));
    }

    @Test
    void aFailedMergeRollsBackTheRowsItRoutedIntoAPartition() throws Exception {
        exec("CREATE TABLE wma_pr (k int, v int, CHECK (v < 100)) PARTITION BY RANGE (k)");
        exec("CREATE TABLE wma_pr_1 PARTITION OF wma_pr FOR VALUES FROM (1) TO (10)");
        exec("CREATE TABLE wma_prs (k int, v int)");
        exec("INSERT INTO wma_prs VALUES (1,1),(2,500)");
        assertEquals("23514", stateOf("MERGE INTO wma_pr t USING wma_prs u ON t.k = u.k"
                + " WHEN NOT MATCHED THEN INSERT (k,v) VALUES (u.k, u.v)"));
        assertEquals("0", scalar("SELECT count(*) FROM wma_pr"));
        assertEquals("0", scalar("SELECT count(*) FROM wma_pr_1"));
    }

    @Test
    void theMergeInsertArmIsHeldToTheSameArityAsAnInsert() throws Exception {
        exec("CREATE TABLE wma_a1 (id int, v int, w int)");
        assertEquals("42601", stateOf("MERGE INTO wma_a1 t USING (VALUES (1,2)) s(id,v) ON t.id = s.id"
                + " WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id, s.v)"));
        assertEquals("42601", stateOf("MERGE INTO wma_a1 t USING (VALUES (1,2)) s(id,v) ON t.id = s.id"
                + " WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v, 5, 6)"));
        assertEquals("42601", stateOf("MERGE INTO wma_a1 t USING (VALUES (1,2)) s(id,v) ON t.id = s.id"
                + " WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id)"));
        assertEquals("0", scalar("SELECT count(*) FROM wma_a1"));
        // Fewer expressions than the relation has columns is legal without a column list.
        exec("MERGE INTO wma_a1 t USING (VALUES (1,2)) s(id,v) ON t.id = s.id"
                + " WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)");
        assertEquals("1", scalar("SELECT count(*) FROM wma_a1 WHERE id = 1 AND v = 2 AND w IS NULL"));
    }

    @Test
    void mergeIsRefusedOnARelationThatCarriesARule() throws Exception {
        exec("CREATE TABLE wma_r6 (i int primary key, v text)");
        exec("CREATE TABLE wma_r6log (m text)");
        exec("CREATE RULE wma_r6_h AS ON INSERT TO wma_r6 DO ALSO INSERT INTO wma_r6log VALUES ('i')");
        assertEquals("0A000", stateOf("MERGE INTO wma_r6 t USING (SELECT 1 AS i) s ON t.i = s.i"
                + " WHEN NOT MATCHED THEN DO NOTHING"));
        assertEquals("0A000", stateOf("MERGE INTO wma_r6 t USING (SELECT 1 AS i) s ON t.i = s.i"
                + " WHEN MATCHED THEN UPDATE SET v = 'z'"));
        assertEquals("0", scalar("SELECT count(*) FROM wma_r6log"));
        // A rule-less view is untouched by the refusal.
        exec("CREATE TABLE wma_rb (i int primary key, v text)");
        exec("CREATE VIEW wma_rbv AS SELECT i, v FROM wma_rb");
        exec("MERGE INTO wma_rbv t USING (VALUES (1,'a')) s(i,v) ON t.i = s.i"
                + " WHEN NOT MATCHED THEN INSERT (i,v) VALUES (s.i, s.v)");
        assertEquals("1", scalar("SELECT count(*) FROM wma_rb"));
    }

    @Test
    void aGeneratedColumnCannotBeAssignedThroughAMergeUpdateArm() throws Exception {
        exec("CREATE TABLE wma_gt (id int PRIMARY KEY, a int, g int GENERATED ALWAYS AS (a*2) STORED)");
        exec("INSERT INTO wma_gt (id,a) VALUES (1,1)");
        assertEquals("428C9", stateOf("MERGE INTO wma_gt t USING (SELECT 1 AS id) s ON t.id = s.id"
                + " WHEN MATCHED THEN UPDATE SET g = 99"));
        // A column reference on the right is refused the same way, not with an internal error.
        assertEquals("428C9", stateOf("MERGE INTO wma_gt t USING (VALUES (1,7)) s(id,n) ON t.id = s.id"
                + " WHEN MATCHED THEN UPDATE SET g = s.n"));
        assertEquals("428C9", stateOf("MERGE INTO wma_gt t USING (VALUES (99)) s(id) ON t.id = s.id"
                + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET g = 42"));
        // DEFAULT is accepted: the value is recomputed from the column's expression.
        exec("MERGE INTO wma_gt t USING (SELECT 1 AS id) s ON t.id = s.id"
                + " WHEN MATCHED THEN UPDATE SET g = DEFAULT");
        assertEquals("2", scalar("SELECT g FROM wma_gt WHERE id = 1"));
    }

    @Test
    void mergeValidatesDomainConstraintsOnBothArms() throws Exception {
        exec("CREATE DOMAIN wma_dom AS int CHECK (VALUE < 100)");
        exec("CREATE TABLE wma_dt (id int PRIMARY KEY, v wma_dom)");
        exec("INSERT INTO wma_dt VALUES (1,1)");
        assertEquals("23514", stateOf("MERGE INTO wma_dt t USING (VALUES (1)) s(id) ON t.id = s.id"
                + " WHEN MATCHED THEN UPDATE SET v = 500"));
        assertEquals("23514", stateOf("MERGE INTO wma_dt t USING (VALUES (2)) s(id) ON t.id = s.id"
                + " WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, 500)"));
        assertEquals("1", scalar("SELECT count(*) FROM wma_dt"));
        assertEquals("1", scalar("SELECT v FROM wma_dt WHERE id = 1"));
    }

    @Test
    void mergeHonoursAViewsCheckOption() throws Exception {
        exec("CREATE TABLE wma_ga (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wma_ga VALUES (1,1)");
        exec("CREATE VIEW wma_gav AS SELECT i, v FROM wma_ga WHERE v < 10 WITH CHECK OPTION");
        assertEquals("44000", stateOf("MERGE INTO wma_gav t USING (VALUES (1,90)) s(i,v) ON t.i = s.i"
                + " WHEN MATCHED THEN UPDATE SET v = s.v"));
        assertEquals("44000", stateOf("MERGE INTO wma_gav t USING (VALUES (7,90)) s(i,v) ON t.i = s.i"
                + " WHEN NOT MATCHED THEN INSERT (i,v) VALUES (s.i, s.v)"));
        assertEquals("1", scalar("SELECT count(*) FROM wma_ga"));
        assertEquals("1", scalar("SELECT v FROM wma_ga WHERE i = 1"));
    }

    @Test
    void mergeWritesThroughAViewWithRenamedColumns() throws Exception {
        exec("CREATE TABLE wma_vb (a int PRIMARY KEY, b int, c int)");
        exec("INSERT INTO wma_vb VALUES (1,20,30)");
        exec("CREATE VIEW wma_vv AS SELECT c AS x, a AS y, b AS z FROM wma_vb");
        exec("MERGE INTO wma_vv t USING (VALUES (1)) s(k) ON t.y = s.k WHEN MATCHED THEN UPDATE SET x = 77");
        assertEquals("77", scalar("SELECT c FROM wma_vb WHERE a = 1"));
        assertEquals("20", scalar("SELECT b FROM wma_vb WHERE a = 1"));
    }

    @Test
    void aMergeWithTrailingTokensIsASyntaxError() throws Exception {
        exec("CREATE TABLE wma_w1 (id int, v int)");
        exec("INSERT INTO wma_w1 VALUES (1,1)");
        assertEquals("42601", stateOf("MERGE INTO wma_w1 t USING (VALUES (1)) s(id) ON t.id = s.id"
                + " WHEN MATCHED THEN DELETE WHERE 1 = 0"));
        assertEquals("1", scalar("SELECT count(*) FROM wma_w1"));
        assertEquals("42601", stateOf("MERGE INTO wma_w1 t USING (VALUES (1)) s(id) ON t.id = s.id"
                + " WHEN MATCHED THEN DELETE wma_garbage"));
        assertEquals("1", scalar("SELECT count(*) FROM wma_w1"));
    }

    @Test
    void mergeResolvesEveryNameBeforeItScans() throws Exception {
        exec("CREATE TABLE wma_e1 (id int, v int)");
        // No row reaches any arm, and PostgreSQL still refuses each of these.
        assertEquals("42703", stateOf("MERGE INTO wma_e1 t USING (SELECT 1 AS id WHERE false) s ON t.id = s.id"
                + " WHEN NOT MATCHED THEN INSERT (nosuch) VALUES (1)"));
        assertEquals("42703", stateOf("MERGE INTO wma_e1 t USING (SELECT 1 AS id WHERE false) s ON t.nosuch = s.id"
                + " WHEN MATCHED THEN UPDATE SET v = 1"));
        assertEquals("42703", stateOf("MERGE INTO wma_e1 t USING (SELECT 1 AS id) s ON t.id = s.id"
                + " WHEN MATCHED THEN UPDATE SET v = t.nosuch"));
        assertEquals("column t.nosuch does not exist",
                messageOf("MERGE INTO wma_e1 t USING (SELECT 1 AS id) s ON t.id = s.id"
                        + " WHEN MATCHED THEN UPDATE SET v = t.nosuch"));
        exec("INSERT INTO wma_e1 VALUES (1,1)");
        assertEquals("0A000", stateOf("MERGE INTO wma_e1 t USING (VALUES (1)) s(id) ON t.id = s.id"
                + " WHEN MATCHED THEN UPDATE SET ctid = '(0,1)'"));
        assertEquals("42703", stateOf("MERGE INTO wma_e1 t USING (VALUES (1,2)) s(id,v) ON t.id = s.id"
                + " WHEN NOT MATCHED THEN INSERT (t.id, t.v) VALUES (s.id, s.v)"));
    }

    @Test
    void mergeReturningWildcardsProjectTheRightRelations() throws Exception {
        exec("CREATE TABLE wma_o1 (ta int, tb int)");
        exec("CREATE TABLE wma_o2 (sa int, sb int, sc int)");
        exec("INSERT INTO wma_o1 VALUES (11,12)");
        exec("INSERT INTO wma_o2 VALUES (11,22,23)");

        try (java.sql.Statement s = conn.createStatement();
             java.sql.ResultSet rs = s.executeQuery(
                     "MERGE INTO wma_o1 t USING wma_o2 s ON t.ta = s.sa"
                     + " WHEN MATCHED THEN UPDATE SET tb = 99 RETURNING *")) {
            assertEquals(5, rs.getMetaData().getColumnCount());
            assertTrue(rs.next());
            assertEquals(11, rs.getInt(1));
            assertEquals(22, rs.getInt(2));
            assertEquals(23, rs.getInt(3));
            assertEquals(11, rs.getInt(4));
            assertEquals(99, rs.getInt(5));
            assertFalse(rs.next());
        }

        try (java.sql.Statement s = conn.createStatement();
             java.sql.ResultSet rs = s.executeQuery(
                     "MERGE INTO wma_o1 t USING wma_o2 s ON t.ta = s.sa"
                     + " WHEN MATCHED THEN UPDATE SET tb = 97 RETURNING s.*")) {
            assertEquals(3, rs.getMetaData().getColumnCount());
            assertTrue(rs.next());
            assertEquals(11, rs.getInt(1));
            assertEquals(22, rs.getInt(2));
            assertEquals(23, rs.getInt(3));
        }

        try (java.sql.Statement s = conn.createStatement();
             java.sql.ResultSet rs = s.executeQuery(
                     "MERGE INTO wma_o1 t USING wma_o2 s ON t.ta = s.sa"
                     + " WHEN MATCHED THEN UPDATE SET tb = 95 RETURNING t.*")) {
            assertEquals(2, rs.getMetaData().getColumnCount());
            assertTrue(rs.next());
            assertEquals(11, rs.getInt(1));
            assertEquals(95, rs.getInt(2));
        }

        // A VALUES list is a source like any other and its columns are returned too.
        try (java.sql.Statement s = conn.createStatement();
             java.sql.ResultSet rs = s.executeQuery(
                     "MERGE INTO wma_o1 t USING (VALUES (11,55)) s(sa,sb) ON t.ta = s.sa"
                     + " WHEN MATCHED THEN UPDATE SET tb = 96 RETURNING *")) {
            assertEquals(4, rs.getMetaData().getColumnCount());
            assertTrue(rs.next());
            assertEquals(11, rs.getInt(1));
            assertEquals(55, rs.getInt(2));
            assertEquals(11, rs.getInt(3));
            assertEquals(96, rs.getInt(4));
        }

        // WHEN NOT MATCHED BY SOURCE has no source row, so the source columns come back null.
        try (java.sql.Statement s = conn.createStatement();
             java.sql.ResultSet rs = s.executeQuery(
                     "MERGE INTO wma_o1 t USING wma_o2 s ON t.ta = 999"
                     + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET tb = 93 RETURNING *")) {
            assertEquals(5, rs.getMetaData().getColumnCount());
            assertTrue(rs.next());
            rs.getInt(1);
            assertTrue(rs.wasNull());
            assertEquals(11, rs.getInt(4));
            assertEquals(93, rs.getInt(5));
        }
    }

    @Test
    void theMergeInsertArmUnderstandsDefaultAndDrawsNoIdentityItWillNotUse() throws Exception {
        exec("CREATE TABLE wma_d1 (id int DEFAULT 7, v int)");
        exec("MERGE INTO wma_d1 t USING (VALUES (99)) s(x) ON t.id = s.x"
                + " WHEN NOT MATCHED THEN INSERT (id, v) VALUES (DEFAULT, 5)");
        assertEquals("1", scalar("SELECT count(*) FROM wma_d1 WHERE id = 7 AND v = 5"));

        exec("CREATE TABLE wma_id1 (id int GENERATED ALWAYS AS IDENTITY, v int)");
        assertEquals("428C9", stateOf("MERGE INTO wma_id1 t USING (VALUES (5)) s(v) ON t.v = s.v"
                + " WHEN NOT MATCHED THEN INSERT (id, v) VALUES (9, s.v)"));
        exec("MERGE INTO wma_id1 t USING (VALUES (5)) s(v) ON t.v = s.v"
                + " WHEN NOT MATCHED THEN INSERT (v) VALUES (s.v)");
        assertEquals("1", scalar("SELECT id FROM wma_id1 WHERE v = 5"));

        // DEFAULT into a generated column is legal, and the value is computed.
        exec("CREATE TABLE wma_gg (id int PRIMARY KEY, a int, g int GENERATED ALWAYS AS (a*2) STORED)");
        exec("MERGE INTO wma_gg t USING (VALUES (2,3)) s(id,a) ON t.id = s.id"
                + " WHEN NOT MATCHED THEN INSERT (id, a, g) VALUES (s.id, s.a, DEFAULT)");
        assertEquals("6", scalar("SELECT g FROM wma_gg WHERE id = 2"));
    }

    @Test
    void mergeFiresTheStatementTriggersOfEveryActionItCouldPerform() throws Exception {
        exec("CREATE TABLE wma_tl (m text)");
        exec("CREATE TABLE wma_tt (id int PRIMARY KEY, v int)");
        exec("INSERT INTO wma_tt VALUES (1,1)");
        exec("CREATE FUNCTION wma_tf() RETURNS trigger LANGUAGE plpgsql AS "
                + "'BEGIN INSERT INTO wma_tl VALUES (TG_WHEN || TG_OP || TG_LEVEL); RETURN NULL; END'");
        exec("CREATE TRIGGER wma_su BEFORE UPDATE ON wma_tt FOR EACH STATEMENT EXECUTE FUNCTION wma_tf()");
        exec("CREATE TRIGGER wma_su2 AFTER UPDATE ON wma_tt FOR EACH STATEMENT EXECUTE FUNCTION wma_tf()");
        exec("CREATE TRIGGER wma_si BEFORE INSERT ON wma_tt FOR EACH STATEMENT EXECUTE FUNCTION wma_tf()");
        exec("CREATE TRIGGER wma_si2 AFTER INSERT ON wma_tt FOR EACH STATEMENT EXECUTE FUNCTION wma_tf()");

        exec("MERGE INTO wma_tt t USING (VALUES (1,3),(5,5)) s(id,v) ON t.id = s.id"
                + " WHEN MATCHED THEN UPDATE SET v = s.v"
                + " WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v)");
        assertEquals("4", scalar("SELECT count(*) FROM wma_tl"));
        assertEquals("1", scalar("SELECT count(*) FROM wma_tl WHERE m = 'BEFOREINSERTSTATEMENT'"));
        assertEquals("1", scalar("SELECT count(*) FROM wma_tl WHERE m = 'AFTERINSERTSTATEMENT'"));
        assertEquals("1", scalar("SELECT count(*) FROM wma_tl WHERE m = 'BEFOREUPDATESTATEMENT'"));
        assertEquals("1", scalar("SELECT count(*) FROM wma_tl WHERE m = 'AFTERUPDATESTATEMENT'"));
    }

    @Test
    void mergeChecksThePrivilegeOfEveryActionItsArmsPerform() throws Exception {
        exec("CREATE TABLE wma_s (id int primary key, n int)");
        exec("INSERT INTO wma_s VALUES (1,10)");
        exec("CREATE ROLE wma_r");
        exec("GRANT SELECT ON wma_s TO wma_r");
        try {
            exec("SET ROLE wma_r");
            assertEquals("42501", stateOf("MERGE INTO wma_s t USING (VALUES (1)) AS v(id) ON t.id = v.id"
                    + " WHEN MATCHED THEN UPDATE SET n = 99"));
            assertEquals("42501", stateOf("MERGE INTO wma_s t USING (VALUES (1)) AS v(id) ON t.id = v.id"
                    + " WHEN MATCHED THEN DELETE"));
            assertEquals("42501", stateOf("MERGE INTO wma_s t USING (VALUES (5)) AS v(id) ON t.id = v.id"
                    + " WHEN NOT MATCHED THEN INSERT (id, n) VALUES (v.id, 1)"));
            // A DO NOTHING arm performs no action, so SELECT alone is enough.
            exec("MERGE INTO wma_s t USING (VALUES (1)) AS v(id) ON t.id = v.id"
                    + " WHEN MATCHED THEN DO NOTHING");
        } finally {
            exec("RESET ROLE");
        }
        assertEquals("1", scalar("SELECT count(*) FROM wma_s"));
        assertEquals("10", scalar("SELECT n FROM wma_s WHERE id = 1"));
    }

    @Test
    void mergeHonoursRowLevelSecurityInAllThreeDirections() throws Exception {
        exec("CREATE TABLE wma_st (id int primary key, owner text, n int)");
        exec("INSERT INTO wma_st VALUES (1,'wma_role',10),(2,'other',20)");
        exec("CREATE ROLE wma_role");
        exec("GRANT SELECT,INSERT,UPDATE,DELETE ON wma_st TO wma_role");
        exec("ALTER TABLE wma_st ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY wma_sel ON wma_st FOR SELECT USING (true)");
        exec("CREATE POLICY wma_upd ON wma_st FOR UPDATE USING (owner = current_user)");
        try {
            exec("SET ROLE wma_role");
            assertEquals("42501", stateOf("MERGE INTO wma_st t USING (VALUES (1),(2)) AS v(id) ON t.id = v.id"
                    + " WHEN MATCHED THEN UPDATE SET n = 99"));
        } finally {
            exec("RESET ROLE");
        }
        assertEquals("10", scalar("SELECT n FROM wma_st WHERE id = 1"));
        assertEquals("20", scalar("SELECT n FROM wma_st WHERE id = 2"));

        // WITH CHECK on the INSERT arm.
        exec("CREATE TABLE wma_ri (id int primary key, owner text, n int)");
        exec("CREATE ROLE wma_role2");
        exec("GRANT SELECT,INSERT,UPDATE,DELETE ON wma_ri TO wma_role2");
        exec("ALTER TABLE wma_ri ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY wma_ri_a ON wma_ri FOR ALL USING (true) WITH CHECK (owner = current_user)");
        try {
            exec("SET ROLE wma_role2");
            assertEquals("42501", stateOf("MERGE INTO wma_ri t USING (VALUES (1,'nobody',5)) s(id,owner,n)"
                    + " ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id,owner,n) VALUES (s.id,s.owner,s.n)"));
        } finally {
            exec("RESET ROLE");
        }
        assertEquals("0", scalar("SELECT count(*) FROM wma_ri"));

        // A row the SELECT policy hides is genuinely NOT MATCHED, so the INSERT arm runs and
        // collides with the primary key that is there but invisible.
        exec("CREATE TABLE wma_sp (id int primary key, owner text, n int)");
        exec("INSERT INTO wma_sp VALUES (1,'other',10)");
        exec("CREATE ROLE wma_role3");
        exec("GRANT SELECT,INSERT,UPDATE,DELETE ON wma_sp TO wma_role3");
        exec("ALTER TABLE wma_sp ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY wma_sp_s ON wma_sp FOR SELECT USING (owner = current_user)");
        // Permissive policies are OR'ed together and a FOR ALL policy applies to SELECT as well,
        // so one written USING (true) would put the row back in view and the MERGE would match
        // it. The two arms get a policy each, leaving the SELECT policy alone over visibility.
        exec("CREATE POLICY wma_sp_i ON wma_sp FOR INSERT WITH CHECK (true)");
        exec("CREATE POLICY wma_sp_u ON wma_sp FOR UPDATE USING (true) WITH CHECK (true)");
        try {
            exec("SET ROLE wma_role3");
            assertEquals("23505", stateOf("MERGE INTO wma_sp t USING (VALUES (1,5)) s(id,n) ON t.id = s.id"
                    + " WHEN MATCHED THEN UPDATE SET n = s.n"
                    + " WHEN NOT MATCHED THEN INSERT (id, owner, n) VALUES (s.id,'mine',s.n)"));
        } finally {
            exec("RESET ROLE");
        }
        assertEquals("10", scalar("SELECT n FROM wma_sp WHERE id = 1"));
    }

    // ------------------------------------------------------------ A partition is a relation with its own rules, not a routing target

    @Test
    void writeAimedAtAPartitionIsCheckedAgainstItsOwnBound() throws Exception {
        exec("CREATE TABLE zzt1b_f1 (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt1b_f1_1 PARTITION OF zzt1b_f1 FOR VALUES FROM (1) TO (10)");
        assertEquals("23514", stateOf("INSERT INTO zzt1b_f1_1 VALUES (99)"));
        assertEquals("0", String.valueOf(scalar("SELECT count(*)::int FROM zzt1b_f1")));
        exec("INSERT INTO zzt1b_f1_1 VALUES (5)");
        assertEquals("23514", stateOf("UPDATE zzt1b_f1_1 SET i = 99"));
        assertEquals("23514", stateOf("INSERT INTO zzt1b_f1_1 SELECT 42"));
        assertEquals("5", String.valueOf(scalar("SELECT string_agg(i::text, ',' ORDER BY i) FROM zzt1b_f1_1")));
        exec("DROP TABLE zzt1b_f1");
    }

    @Test
    void listNullDefaultAndIntermediateBoundsAreEnforcedOnADirectWrite() throws Exception {
        exec("CREATE TABLE zzt1b_l1 (s text) PARTITION BY LIST (s)");
        exec("CREATE TABLE zzt1b_l1_a PARTITION OF zzt1b_l1 FOR VALUES IN ('a')");
        assertEquals("23514", stateOf("INSERT INTO zzt1b_l1_a VALUES ('b')"));
        assertEquals("23514", stateOf("INSERT INTO zzt1b_l1_a VALUES (NULL)"));
        assertEquals("0", String.valueOf(scalar("SELECT count(*)::int FROM zzt1b_l1_a")));

        exec("CREATE TABLE zzt1b_d1 (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt1b_d1_1 PARTITION OF zzt1b_d1 FOR VALUES FROM (1) TO (10)");
        exec("CREATE TABLE zzt1b_d1_d PARTITION OF zzt1b_d1 DEFAULT");
        assertEquals("23514", stateOf("INSERT INTO zzt1b_d1_d VALUES (5)"));
        exec("INSERT INTO zzt1b_d1_d VALUES (50)");
        assertEquals("1", String.valueOf(scalar("SELECT count(*)::int FROM zzt1b_d1_d")));

        exec("CREATE TABLE zzt1b_m1 (a int, b int) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzt1b_m1_1 PARTITION OF zzt1b_m1 FOR VALUES FROM (0) TO (10) PARTITION BY RANGE (b)");
        exec("CREATE TABLE zzt1b_m1_1_1 PARTITION OF zzt1b_m1_1 FOR VALUES FROM (0) TO (10)");
        assertEquals("23514", stateOf("INSERT INTO zzt1b_m1_1 VALUES (99, 5)"));
        assertEquals("23514", stateOf("INSERT INTO zzt1b_m1_1_1 VALUES (5, 99)"));
        assertEquals("0", String.valueOf(scalar("SELECT count(*)::int FROM zzt1b_m1")));

        exec("DROP TABLE zzt1b_m1");
        exec("DROP TABLE zzt1b_d1");
        exec("DROP TABLE zzt1b_l1");
    }

    @Test
    void updateThatMovesARowValidatesTheDestinationPartition() throws Exception {
        exec("CREATE TABLE zzt1b_mv (i int, v int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt1b_mv_1 PARTITION OF zzt1b_mv FOR VALUES FROM (1) TO (10)");
        exec("CREATE TABLE zzt1b_mv_2 PARTITION OF zzt1b_mv FOR VALUES FROM (10) TO (20)");
        exec("ALTER TABLE zzt1b_mv_2 ADD CONSTRAINT zzt1b_mvc CHECK (v > 100)");
        exec("INSERT INTO zzt1b_mv VALUES (5, 1)");
        assertEquals("23514", stateOf("UPDATE zzt1b_mv SET i = 15"));
        assertEquals("1", String.valueOf(scalar("SELECT count(*)::int FROM zzt1b_mv_1")));
        assertEquals("0", String.valueOf(scalar("SELECT count(*)::int FROM zzt1b_mv_2")));
        exec("DROP TABLE zzt1b_mv");
    }

    @Test
    void hashPartitionRoutingMatchesPostgres() throws Exception {
        exec("CREATE TABLE zzt1b_ha (i int) PARTITION BY HASH (i)");
        exec("CREATE TABLE zzt1b_ha_0 PARTITION OF zzt1b_ha FOR VALUES WITH (MODULUS 4, REMAINDER 0)");
        exec("CREATE TABLE zzt1b_ha_1 PARTITION OF zzt1b_ha FOR VALUES WITH (MODULUS 4, REMAINDER 1)");
        exec("CREATE TABLE zzt1b_ha_2 PARTITION OF zzt1b_ha FOR VALUES WITH (MODULUS 4, REMAINDER 2)");
        exec("CREATE TABLE zzt1b_ha_3 PARTITION OF zzt1b_ha FOR VALUES WITH (MODULUS 4, REMAINDER 3)");
        for (int i = 1; i <= 12; i++) exec("INSERT INTO zzt1b_ha VALUES (" + i + ")");
        assertEquals("1,12", String.valueOf(scalar("SELECT string_agg(i::text, ',' ORDER BY i) FROM zzt1b_ha_0")));
        assertEquals("3,5,8,9,11", String.valueOf(scalar("SELECT string_agg(i::text, ',' ORDER BY i) FROM zzt1b_ha_1")));
        assertEquals("2", String.valueOf(scalar("SELECT string_agg(i::text, ',' ORDER BY i) FROM zzt1b_ha_2")));
        assertEquals("4,6,7,10", String.valueOf(scalar("SELECT string_agg(i::text, ',' ORDER BY i) FROM zzt1b_ha_3")));

        exec("CREATE TABLE zzt1b_ht (s text) PARTITION BY HASH (s)");
        exec("CREATE TABLE zzt1b_ht_0 PARTITION OF zzt1b_ht FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
        exec("CREATE TABLE zzt1b_ht_1 PARTITION OF zzt1b_ht FOR VALUES WITH (MODULUS 2, REMAINDER 1)");
        exec("INSERT INTO zzt1b_ht VALUES ('alpha'),('beta'),('gamma'),('delta')");
        assertEquals("beta,delta", String.valueOf(scalar("SELECT string_agg(s, ',' ORDER BY s) FROM zzt1b_ht_0")));
        assertEquals("alpha,gamma", String.valueOf(scalar("SELECT string_agg(s, ',' ORDER BY s) FROM zzt1b_ht_1")));

        exec("CREATE TABLE zzt1b_hb (v bigint) PARTITION BY HASH (v)");
        exec("CREATE TABLE zzt1b_hb_0 PARTITION OF zzt1b_hb FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
        exec("CREATE TABLE zzt1b_hb_1 PARTITION OF zzt1b_hb FOR VALUES WITH (MODULUS 2, REMAINDER 1)");
        for (int i = 1; i <= 8; i++) exec("INSERT INTO zzt1b_hb VALUES (" + i + ")");
        assertEquals("1,2", String.valueOf(scalar("SELECT string_agg(v::text, ',' ORDER BY v) FROM zzt1b_hb_0")));
        assertEquals("3,4,5,6,7,8", String.valueOf(scalar("SELECT string_agg(v::text, ',' ORDER BY v) FROM zzt1b_hb_1")));

        exec("DROP TABLE zzt1b_hb");
        exec("DROP TABLE zzt1b_ht");
        exec("DROP TABLE zzt1b_ha");
    }

    @Test
    void satisfiesHashPartitionAnswersFromTheSameHashAsRouting() throws Exception {
        exec("CREATE TABLE zzt1b_sh (i int) PARTITION BY HASH (i)");
        exec("CREATE TABLE zzt1b_sh_0 PARTITION OF zzt1b_sh FOR VALUES WITH (MODULUS 4, REMAINDER 0)");
        // A boolean travels in the text format as "t" or "f", so that is what the client reads back.
        assertEquals("t", String.valueOf(scalar(
                "SELECT satisfies_hash_partition('zzt1b_sh'::regclass, 4, 0, 1)")));
        assertEquals("t", String.valueOf(scalar(
                "SELECT satisfies_hash_partition('zzt1b_sh'::regclass, 4, 1, 3)")));
        assertEquals("f", String.valueOf(scalar(
                "SELECT satisfies_hash_partition('zzt1b_sh'::regclass, 4, 0, 3)")));
        exec("CREATE TABLE zzt1b_sr (i int) PARTITION BY RANGE (i)");
        assertEquals("22023", stateOf("SELECT satisfies_hash_partition('zzt1b_sr'::regclass, 4, 0, 1)"));
        exec("DROP TABLE zzt1b_sr");
        exec("DROP TABLE zzt1b_sh");
    }

    @Test
    void updateAndDeleteThroughAnInheritanceParentReachTheChildren() throws Exception {
        exec("CREATE TABLE zzt1b_par (id int, v int)");
        exec("CREATE TABLE zzt1b_chi () INHERITS (zzt1b_par)");
        exec("INSERT INTO zzt1b_par VALUES (1,1)");
        exec("INSERT INTO zzt1b_chi VALUES (2,2)");
        exec("UPDATE zzt1b_par SET v = v + 100");
        assertEquals("101,102", String.valueOf(scalar(
                "SELECT string_agg(v::text, ',' ORDER BY id) FROM zzt1b_par")));
        exec("DELETE FROM zzt1b_par WHERE id = 2");
        assertEquals("0", String.valueOf(scalar("SELECT count(*)::int FROM zzt1b_chi")));
        exec("DROP TABLE zzt1b_chi");
        exec("DROP TABLE zzt1b_par");
    }

    @Test
    void onlyKeepsAWriteToTheNamedRelationsOwnStorage() throws Exception {
        exec("CREATE TABLE zzt1b_o2 (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt1b_o2_1 PARTITION OF zzt1b_o2 FOR VALUES FROM (1) TO (10)");
        exec("INSERT INTO zzt1b_o2 VALUES (5, 'a')");
        exec("UPDATE ONLY zzt1b_o2 SET s = 'changed'");
        assertEquals("a", String.valueOf(scalar("SELECT s FROM zzt1b_o2 ORDER BY i")));
        exec("DELETE FROM ONLY zzt1b_o2 WHERE i = 5");
        assertEquals("1", String.valueOf(scalar("SELECT count(*)::int FROM zzt1b_o2")));

        exec("CREATE TABLE zzt1b_oi (i int)");
        exec("CREATE TABLE zzt1b_oic () INHERITS (zzt1b_oi)");
        exec("INSERT INTO zzt1b_oi VALUES (1)");
        exec("INSERT INTO zzt1b_oic VALUES (2)");
        exec("UPDATE ONLY zzt1b_oi SET i = i + 10");
        assertEquals("2,11", String.valueOf(scalar(
                "SELECT string_agg(i::text, ',' ORDER BY i) FROM zzt1b_oi")));
        exec("DELETE FROM ONLY zzt1b_oi");
        assertEquals("1", String.valueOf(scalar("SELECT count(*)::int FROM zzt1b_oic")));

        exec("DROP TABLE zzt1b_oic");
        exec("DROP TABLE zzt1b_oi");
        exec("DROP TABLE zzt1b_o2");
    }

    @Test
    void bigintPartitionBoundsCompareExactly() throws Exception {
        exec("CREATE TABLE zzt1b_x1 (id bigint) PARTITION BY RANGE (id)");
        exec("CREATE TABLE zzt1b_x1_a PARTITION OF zzt1b_x1 "
                + "FOR VALUES FROM (9007199254740995) TO (9007199254740996)");
        exec("INSERT INTO zzt1b_x1 VALUES (9007199254740995)");
        assertEquals("1", String.valueOf(scalar("SELECT count(*)::int FROM zzt1b_x1")));

        exec("CREATE TABLE zzt1b_x2 (id bigint) PARTITION BY RANGE (id)");
        exec("CREATE TABLE zzt1b_x2_a PARTITION OF zzt1b_x2 FOR VALUES FROM (0) TO (9007199254740996)");
        assertEquals("42P17", stateOf("CREATE TABLE zzt1b_x2_b PARTITION OF zzt1b_x2 "
                + "FOR VALUES FROM (9007199254740995) TO (MAXVALUE)"));
        exec("DROP TABLE zzt1b_x2");
        exec("DROP TABLE zzt1b_x1");
    }

    @Test
    void rangeFrameBoundaryOverABigintKeyIsExact() throws Exception {
        exec("CREATE TABLE zzt1b_wb (id int, v bigint)");
        exec("INSERT INTO zzt1b_wb VALUES (1,9007199254740992),(2,9007199254740993),(3,9007199254740995)");
        assertEquals("1,2,1", String.valueOf(scalar(
                "SELECT string_agg(c::text, ',' ORDER BY id) FROM (SELECT id, count(*) OVER "
                + "(ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS c FROM zzt1b_wb) q")));
        assertEquals("2,2,1", String.valueOf(scalar(
                "SELECT string_agg(c::text, ',' ORDER BY id) FROM (SELECT id, count(*) OVER "
                + "(ORDER BY v RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS c FROM zzt1b_wb) q")));
        exec("DROP TABLE zzt1b_wb");
    }

    @Test
    void partitionCatalogsReportTheHierarchy() throws Exception {
        exec("CREATE TABLE zzt1b_pf (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt1b_pf_1 PARTITION OF zzt1b_pf FOR VALUES FROM (1) TO (10) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt1b_pf_1_1 PARTITION OF zzt1b_pf_1 FOR VALUES FROM (1) TO (5)");
        assertEquals("zzt1b_pf", String.valueOf(scalar(
                "SELECT pg_partition_root('zzt1b_pf_1_1'::regclass)::text")));
        assertEquals("zzt1b_pf", String.valueOf(scalar(
                "SELECT pg_partition_root('zzt1b_pf'::regclass)::text")));
        assertEquals("zzt1b_pf_1_1,zzt1b_pf_1,zzt1b_pf", String.valueOf(scalar(
                "SELECT string_agg(relid::regclass::text, ',') FROM pg_partition_ancestors('zzt1b_pf_1_1'::regclass)")));
        assertEquals("1", String.valueOf(scalar(
                "SELECT attinhcount FROM pg_attribute WHERE attrelid='zzt1b_pf_1'::regclass AND attname='i'")));
        // A boolean travels in the text format as "t" or "f", so that is what the client reads back.
        assertEquals("f", String.valueOf(scalar(
                "SELECT attislocal FROM pg_attribute WHERE attrelid='zzt1b_pf_1'::regclass AND attname='i'")));
        assertEquals("t", String.valueOf(scalar(
                "SELECT relhassubclass FROM pg_class WHERE relname='zzt1b_pf'")));

        exec("CREATE TABLE zzt1b_pa (a int)");
        exec("CREATE TABLE zzt1b_pb (b int)");
        exec("CREATE TABLE zzt1b_pd () INHERITS (zzt1b_pa, zzt1b_pb)");
        assertEquals("zzt1b_pa:1,zzt1b_pb:2", String.valueOf(scalar(
                "SELECT string_agg(p.relname || ':' || i.inhseqno, ',' ORDER BY i.inhseqno) "
                + "FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid "
                + "JOIN pg_class p ON p.oid=i.inhparent WHERE c.relname='zzt1b_pd'")));
        assertEquals("t", String.valueOf(scalar(
                "SELECT relhassubclass FROM pg_class WHERE relname='zzt1b_pa'")));

        exec("CREATE TABLE zzt1b_ord (id int)");
        assertEquals("f", String.valueOf(scalar(
                "SELECT relhassubclass FROM pg_class WHERE relname='zzt1b_ord'")));
        assertEquals("t", String.valueOf(scalar(
                "SELECT attislocal FROM pg_attribute WHERE attrelid='zzt1b_ord'::regclass AND attname='id'")));
        assertNull(scalar("SELECT pg_partition_root('zzt1b_ord'::regclass)::text"));

        exec("DROP TABLE zzt1b_ord");
        // A child and both of its parents go in one statement, the way PostgreSQL lets a whole
        // inheritance group be named at once, so no drop has to wait for another to finish.
        exec("DROP TABLE zzt1b_pd, zzt1b_pb, zzt1b_pa");
        exec("DROP TABLE zzt1b_pf");
    }

    @Test
    void aPartitionedTablesRowTriggerFiresForAWriteAimedAtThePartition() throws Exception {
        exec("CREATE FUNCTION zzt1b_faf() RETURNS trigger LANGUAGE plpgsql AS $$ "
                + "BEGIN NEW.s := NEW.s || '!'; RETURN NEW; END $$");
        exec("CREATE TABLE zzt1b_fa (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzt1b_fa_1 PARTITION OF zzt1b_fa FOR VALUES FROM (1) TO (10)");
        exec("CREATE TRIGGER zzt1b_fa_t BEFORE INSERT ON zzt1b_fa "
                + "FOR EACH ROW EXECUTE FUNCTION zzt1b_faf()");
        exec("INSERT INTO zzt1b_fa_1 VALUES (5, 'a')");
        exec("INSERT INTO zzt1b_fa VALUES (6, 'b')");
        assertEquals("a!,b!", String.valueOf(scalar(
                "SELECT string_agg(s, ',' ORDER BY i) FROM zzt1b_fa")));
        exec("DROP TABLE zzt1b_fa");
        exec("DROP FUNCTION zzt1b_faf()");
    }

    // ------------------------------------------------------------ Structure is read from the parse tree, not scanned for in text

    @Test
    void dropColumnUnrelatedToAGeneratedColumnIsAllowed() throws Exception {
        exec("CREATE TABLE w1c_gen (id int, d int, total int GENERATED ALWAYS AS (id * 2) STORED)");
        exec("ALTER TABLE w1c_gen DROP COLUMN d");
        assertEquals("id,total", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name = 'w1c_gen'"));
        exec("DROP TABLE w1c_gen");
    }

    @Test
    void dropColumnNamedOnlyInsideAGeneratedLiteralIsAllowed() throws Exception {
        exec("CREATE TABLE w1c_lit (a int, bb int, g text GENERATED ALWAYS AS ('bb value') STORED)");
        exec("ALTER TABLE w1c_lit DROP COLUMN bb");
        assertEquals("a,g", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name = 'w1c_lit'"));
        exec("DROP TABLE w1c_lit");
    }

    @Test
    void dropColumnAGeneratedColumnReallyReadsIsRefused() throws Exception {
        exec("CREATE TABLE w1c_dep (a int, b int, g int GENERATED ALWAYS AS (a + 1) STORED)");
        SQLException e = assertThrows(SQLException.class, () -> exec("ALTER TABLE w1c_dep DROP COLUMN a"));
        assertEquals("2BP01", e.getSQLState());
        assertTrue(e.getMessage().contains(
                "cannot drop column a of table w1c_dep because other objects depend on it"));
        org.postgresql.util.ServerErrorMessage sem =
                ((org.postgresql.util.PSQLException) e).getServerErrorMessage();
        assertEquals("column g of table w1c_dep depends on column a of table w1c_dep", sem.getDetail());
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.", sem.getHint());
        exec("DROP TABLE w1c_dep");
    }

    @Test
    void alterTypeOfAColumnUnrelatedToAGeneratedColumnIsAllowed() throws Exception {
        exec("CREATE TABLE w1c_rt (id int, d varchar(10), total int GENERATED ALWAYS AS (id * 2) STORED)");
        exec("ALTER TABLE w1c_rt ALTER COLUMN d TYPE varchar(30)");
        assertEquals("30", String.valueOf(scalar("SELECT character_maximum_length::text"
                + " FROM information_schema.columns WHERE table_name = 'w1c_rt' AND column_name = 'd'")));
        exec("DROP TABLE w1c_rt");
    }

    @Test
    void alterTypeOfAColumnAGeneratedColumnReadsIsRefused() throws Exception {
        exec("CREATE TABLE w1c_rt2 (id int, d varchar(10), total int GENERATED ALWAYS AS (id * 2) STORED)");
        SQLException e = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE w1c_rt2 ALTER COLUMN id TYPE bigint"));
        assertEquals("0A000", e.getSQLState());
        assertTrue(e.getMessage().contains("cannot alter type of a column used by a generated column"));
        assertEquals("Column \"id\" is used by generated column \"total\".",
                ((org.postgresql.util.PSQLException) e).getServerErrorMessage().getDetail());
        exec("DROP TABLE w1c_rt2");
    }

    @Test
    void columnsSpelledLikeAstFieldNamesAreDroppable() throws Exception {
        exec("CREATE TABLE w1c_ast (keeper int, arge int, istinct int, indow int, imit int, rom int)");
        exec("CREATE VIEW w1c_astv AS SELECT keeper FROM w1c_ast");
        exec("ALTER TABLE w1c_ast DROP COLUMN arge");
        exec("ALTER TABLE w1c_ast DROP COLUMN istinct");
        exec("ALTER TABLE w1c_ast DROP COLUMN indow");
        exec("ALTER TABLE w1c_ast DROP COLUMN imit");
        exec("ALTER TABLE w1c_ast DROP COLUMN rom");
        assertEquals("keeper", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name = 'w1c_ast'"));
        exec("DROP VIEW w1c_astv");
        exec("DROP TABLE w1c_ast");
    }

    @Test
    void dropColumnAViewReadsUsesPostgresSqlstateDetailAndHint() throws Exception {
        exec("CREATE TABLE w1c_vd (a int, b int)");
        exec("CREATE VIEW w1c_vdv AS SELECT a FROM w1c_vd");
        SQLException e = assertThrows(SQLException.class, () -> exec("ALTER TABLE w1c_vd DROP COLUMN a"));
        assertEquals("2BP01", e.getSQLState());
        assertTrue(e.getMessage().contains(
                "cannot drop column a of table w1c_vd because other objects depend on it"));
        org.postgresql.util.ServerErrorMessage sem =
                ((org.postgresql.util.PSQLException) e).getServerErrorMessage();
        assertEquals("view w1c_vdv depends on column a of table w1c_vd", sem.getDetail());
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.", sem.getHint());
        // the column the view does not read goes without complaint
        exec("ALTER TABLE w1c_vd DROP COLUMN b");
        assertEquals("a", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name = 'w1c_vd'"));
        exec("DROP VIEW w1c_vdv");
        exec("DROP TABLE w1c_vd");
    }

    @Test
    void dropColumnNamedOnlyInAViewsLiteralIsAllowed() throws Exception {
        exec("CREATE TABLE w1c_vl (p int, q int)");
        exec("CREATE VIEW w1c_vlv AS SELECT p, 'q marks the spot' AS note FROM w1c_vl");
        exec("ALTER TABLE w1c_vl DROP COLUMN q");
        assertEquals("p", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name = 'w1c_vl'"));
        exec("DROP VIEW w1c_vlv");
        exec("DROP TABLE w1c_vl");
    }

    @Test
    void aCountStarViewDoesNotDependOnAnyColumn() throws Exception {
        exec("CREATE TABLE w1c_vc (a int, b int)");
        exec("CREATE VIEW w1c_vcv AS SELECT count(*) AS n FROM w1c_vc");
        exec("ALTER TABLE w1c_vc DROP COLUMN b");
        exec("ALTER TABLE w1c_vc ALTER COLUMN a TYPE bigint");
        assertEquals("a:bigint", scalar("SELECT string_agg(column_name || ':' || data_type, ','"
                + " ORDER BY ordinal_position) FROM information_schema.columns"
                + " WHERE table_name = 'w1c_vc'"));
        exec("DROP VIEW w1c_vcv");
        exec("DROP TABLE w1c_vc");
    }

    @Test
    void aSelectStarViewDependsOnEveryColumn() throws Exception {
        exec("CREATE TABLE w1c_vs (a int, b int)");
        exec("CREATE VIEW w1c_vsv AS SELECT * FROM w1c_vs");
        assertEquals("2BP01", stateOf("ALTER TABLE w1c_vs DROP COLUMN b"));
        assertEquals("a,b", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name = 'w1c_vs'"));
        exec("DROP VIEW w1c_vsv");
        exec("DROP TABLE w1c_vs");
    }

    @Test
    void aViewOverAnotherRelationIsNotADependent() throws Exception {
        exec("CREATE TABLE w1c_u1 (a int, b int)");
        exec("CREATE TABLE w1c_u2 (a int, b int)");
        exec("CREATE VIEW w1c_u2v AS SELECT a FROM w1c_u2");
        exec("ALTER TABLE w1c_u1 DROP COLUMN b");
        assertEquals("a", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name = 'w1c_u1'"));
        exec("DROP VIEW w1c_u2v");
        exec("DROP TABLE w1c_u1");
        exec("DROP TABLE w1c_u2");
    }

    @Test
    void dropColumnCascadeTakesTheDependentViewsAndTheViewsOnThem() throws Exception {
        exec("CREATE TABLE w1c_vx (p int, q int)");
        exec("CREATE VIEW w1c_vxv AS SELECT q FROM w1c_vx");
        exec("CREATE VIEW w1c_vxv2 AS SELECT q FROM w1c_vxv");
        exec("ALTER TABLE w1c_vx DROP COLUMN q CASCADE");
        assertEquals("0", String.valueOf(scalar("SELECT count(*)::text FROM information_schema.views"
                + " WHERE table_name IN ('w1c_vxv', 'w1c_vxv2')")));
        assertEquals("p", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name = 'w1c_vx'"));
        exec("DROP TABLE w1c_vx");
    }

    @Test
    void alterTypeOfAColumnAViewReadsNamesTheRuleInDetail() throws Exception {
        exec("CREATE TABLE w1c_vr (a int, b int)");
        exec("CREATE VIEW w1c_vrv AS SELECT a * 2 AS x FROM w1c_vr");
        SQLException e = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE w1c_vr ALTER COLUMN a TYPE bigint"));
        assertEquals("0A000", e.getSQLState());
        assertTrue(e.getMessage().contains("cannot alter type of a column used by a view or rule"));
        assertEquals("rule _RETURN on view w1c_vrv depends on column \"a\"",
                ((org.postgresql.util.PSQLException) e).getServerErrorMessage().getDetail());
        // the column the view does not read retypes normally
        exec("ALTER TABLE w1c_vr ALTER COLUMN b TYPE bigint");
        assertEquals("a:integer,b:bigint", scalar("SELECT string_agg(column_name || ':' || data_type,"
                + " ',' ORDER BY ordinal_position) FROM information_schema.columns"
                + " WHERE table_name = 'w1c_vr'"));
        exec("DROP VIEW w1c_vrv");
        exec("DROP TABLE w1c_vr");
    }

    @Test
    void aGeneratedExpressionMayNameAColumnSpelledLikeSelect() throws Exception {
        exec("CREATE TABLE w1c_sw (selected int, g int GENERATED ALWAYS AS (selected * 2) STORED)");
        exec("INSERT INTO w1c_sw (selected) VALUES (5)");
        assertEquals("5|10", scalar("SELECT selected || '|' || g FROM w1c_sw"));
        exec("DROP TABLE w1c_sw");
    }

    @Test
    void aGeneratedExpressionMayHoldTheWordSelectInALiteral() throws Exception {
        exec("CREATE TABLE w1c_sq (a text, g text GENERATED ALWAYS AS (a || 'select') STORED)");
        exec("INSERT INTO w1c_sq (a) VALUES ('x')");
        assertEquals("xselect", scalar("SELECT g FROM w1c_sq"));
        exec("DROP TABLE w1c_sq");
    }

    @Test
    void aRealSubqueryInAGeneratedExpressionIsStillRefused() throws Exception {
        assertEquals("0A000", stateOf("CREATE TABLE w1c_sub1 (a int, g int GENERATED ALWAYS AS ((SELECT 1)) STORED)"));
        assertEquals("0A000", stateOf("CREATE TABLE w1c_sub2 (a int, g boolean GENERATED ALWAYS AS (EXISTS (SELECT 1)) STORED)"));
        assertEquals("0A000", stateOf("CREATE TABLE w1c_sub3 (a int, g boolean GENERATED ALWAYS AS (a IN (SELECT 1)) STORED)"));
    }

    @Test
    void selectIntoIsNotFoundInsideAStringLiteral() throws Exception {
        exec("CREATE FUNCTION w1c_pf1() RETURNS text AS $$ DECLARE v text;"
                + " BEGIN SELECT ' into me ' INTO v; RETURN v; END $$ LANGUAGE plpgsql");
        assertEquals(" into me ", scalar("SELECT w1c_pf1()"));
        exec("DROP FUNCTION w1c_pf1()");
    }

    @Test
    void selectIntoReadsPastALiteralThatSpellsInto() throws Exception {
        exec("CREATE TABLE w1c_pt (a text)");
        exec("INSERT INTO w1c_pt VALUES ('Q')");
        exec("CREATE FUNCTION w1c_pf2() RETURNS text AS $$ DECLARE v text;"
                + " BEGIN SELECT ' into ' || a INTO v FROM w1c_pt; RETURN v; END $$ LANGUAGE plpgsql");
        assertEquals(" into Q", scalar("SELECT w1c_pf2()"));
        exec("CREATE FUNCTION w1c_pf3() RETURNS text AS $$ DECLARE v text;"
                + " BEGIN SELECT 'x INTO y' INTO v; RETURN v; END $$ LANGUAGE plpgsql");
        assertEquals("x INTO y", scalar("SELECT w1c_pf3()"));
        exec("CREATE FUNCTION w1c_pf4() RETURNS text AS $$ DECLARE v text;"
                + " BEGIN SELECT concat('a',' into ','b') INTO v; RETURN v; END $$ LANGUAGE plpgsql");
        assertEquals("a into b", scalar("SELECT w1c_pf4()"));
        exec("DROP FUNCTION w1c_pf2()");
        exec("DROP FUNCTION w1c_pf3()");
        exec("DROP FUNCTION w1c_pf4()");
        exec("DROP TABLE w1c_pt");
    }

    @Test
    void theStatementIsNotCutAtAFromInsideALiteral() throws Exception {
        exec("CREATE TABLE w1c_pt2 (a text)");
        exec("INSERT INTO w1c_pt2 VALUES ('Q')");
        exec("CREATE FUNCTION w1c_pf5() RETURNS int AS $$ DECLARE v int;"
                + " BEGIN SELECT length(' into ') INTO v FROM w1c_pt2 LIMIT 1; RETURN v; END $$ LANGUAGE plpgsql");
        assertEquals(6, num("SELECT w1c_pf5()"));
        exec("DROP FUNCTION w1c_pf5()");
        exec("DROP TABLE w1c_pt2");
    }

    @Test
    void returningIntoSplitsAtTheIntoTokenNotAtOneInsideTheList() throws Exception {
        exec("CREATE TABLE w1c_pt3 (a text)");
        exec("CREATE FUNCTION w1c_pf6() RETURNS text AS $$ DECLARE v text;"
                + " BEGIN INSERT INTO w1c_pt3 VALUES ('x') RETURNING a || ' INTO y' INTO v;"
                + " RETURN v; END $$ LANGUAGE plpgsql");
        assertEquals("x INTO y", scalar("SELECT w1c_pf6()"));
        exec("CREATE FUNCTION w1c_pf7() RETURNS text AS $$ DECLARE v text;"
                + " BEGIN UPDATE w1c_pt3 SET a = 'u' WHERE a = 'x' RETURNING a || ' into ' INTO v;"
                + " RETURN v; END $$ LANGUAGE plpgsql");
        assertEquals("u into ", scalar("SELECT w1c_pf7()"));
        exec("DROP FUNCTION w1c_pf6()");
        exec("DROP FUNCTION w1c_pf7()");
        exec("DROP TABLE w1c_pt3");
    }

    @Test
    void theClausesThatAlreadyWorkedStillWork() throws Exception {
        exec("CREATE TABLE w1c_pt4 (a text)");
        exec("INSERT INTO w1c_pt4 VALUES ('Q')");
        exec("CREATE FUNCTION w1c_pf8() RETURNS text AS $$ DECLARE v text;"
                + " BEGIN SELECT a INTO STRICT v FROM w1c_pt4 WHERE a = 'Q'; RETURN v; END $$ LANGUAGE plpgsql");
        assertEquals("Q", scalar("SELECT w1c_pf8()"));
        exec("CREATE FUNCTION w1c_pf9() RETURNS text AS $$ DECLARE x text; y text;"
                + " BEGIN SELECT 'p', 'q' INTO x, y; RETURN x || y; END $$ LANGUAGE plpgsql");
        assertEquals("pq", scalar("SELECT w1c_pf9()"));
        exec("CREATE FUNCTION w1c_pfa() RETURNS text AS $$ DECLARE v text;"
                + " BEGIN WITH c AS (SELECT a FROM w1c_pt4) SELECT a INTO v FROM c LIMIT 1;"
                + " RETURN v; END $$ LANGUAGE plpgsql");
        assertEquals("Q", scalar("SELECT w1c_pfa()"));
        exec("CREATE FUNCTION w1c_pfb() RETURNS int AS $$ DECLARE v int;"
                + " BEGIN SELECT extract(year from date '2020-03-04') INTO v; RETURN v; END $$ LANGUAGE plpgsql");
        assertEquals(2020, num("SELECT w1c_pfb()"));
        exec("DROP FUNCTION w1c_pf8()");
        exec("DROP FUNCTION w1c_pf9()");
        exec("DROP FUNCTION w1c_pfa()");
        exec("DROP FUNCTION w1c_pfb()");
        exec("DROP TABLE w1c_pt4");
    }

    // ------------------------------------------------------------ COPY validates its options and routes its rows

    @Test
    void copy_unknownOptionIsRefused() throws Exception {
        exec("CREATE TABLE zzw_o1 (a int, b text)");
        assertEquals("42601", stateOf("COPY zzw_o1 TO STDOUT WITH (NOSUCHOPTION true)"));
        assertEquals("22023", stateOf("COPY zzw_o1 TO STDOUT WITH (FORMAT bogus)"));
        assertEquals("22023", stateOf("COPY zzw_o1 TO STDOUT WITH (ENCODING 'NOSUCHENC')"));
        assertEquals("22023", stateOf("COPY zzw_o1 FROM STDIN WITH (ON_ERROR bogus)"));
        assertEquals("22023", stateOf("COPY zzw_o1 FROM STDIN WITH (LOG_VERBOSITY bogus)"));
        assertEquals("42601", stateOf("COPY zzw_o1 TO STDOUT WITH (HEADER 'bogus')"));
        exec("DROP TABLE zzw_o1");
    }

    @Test
    void copy_delimiterQuoteAndEscapeAreCheckedAsPostgresChecksThem() throws Exception {
        exec("CREATE TABLE zzw_o2 (a int, b text)");
        assertEquals("0A000", stateOf("COPY zzw_o2 TO STDOUT WITH (DELIMITER 'ab')"));
        assertEquals("0A000", stateOf("COPY zzw_o2 TO STDOUT WITH (DELIMITER '')"));
        // Outside CSV the delimiter may not be a character that occurs in escaped data.
        assertEquals("22023", stateOf("COPY zzw_o2 TO STDOUT WITH (DELIMITER 'a')"));
        assertEquals("22023", stateOf("COPY zzw_o2 TO STDOUT WITH (DELIMITER '.')"));
        assertEquals("22023", stateOf("COPY zzw_o2 TO STDOUT WITH (DELIMITER '9')"));
        assertEquals("22023", stateOf("COPY zzw_o2 TO STDOUT WITH (DELIMITER E'\\n')"));
        assertEquals("22023", stateOf("COPY zzw_o2 TO STDOUT WITH (NULL E'a\\nb')"));
        assertEquals("0A000", stateOf("COPY zzw_o2 TO STDOUT WITH (QUOTE '\"')"));
        assertEquals("0A000", stateOf("COPY zzw_o2 TO STDOUT WITH (ESCAPE '#')"));
        assertEquals("0A000", stateOf("COPY zzw_o2 TO STDOUT WITH (FORMAT csv, QUOTE 'ab')"));
        assertEquals("0A000", stateOf("COPY zzw_o2 TO STDOUT WITH (FORMAT csv, QUOTE '')"));
        assertEquals("0A000", stateOf("COPY zzw_o2 TO STDOUT WITH (FORMAT csv, ESCAPE 'ab')"));
        assertEquals("22023", stateOf("COPY zzw_o2 TO STDOUT WITH (FORMAT csv, DELIMITER '\"')"));
        assertEquals("22023",
                stateOf("COPY zzw_o2 TO STDOUT WITH (FORMAT csv, DELIMITER ',', NULL ',')"));
        assertEquals("22023", stateOf("COPY zzw_o2 TO STDOUT WITH (FORMAT csv, NULL '\"')"));
        exec("DROP TABLE zzw_o2");
    }

    @Test
    void copy_binaryModeAndDirectionCrossChecks() throws Exception {
        exec("CREATE TABLE zzw_o3 (a int, b text)");
        assertEquals("42601", stateOf("COPY zzw_o3 TO STDOUT WITH (FORMAT binary, DELIMITER ',')"));
        assertEquals("42601", stateOf("COPY zzw_o3 TO STDOUT WITH (FORMAT binary, NULL 'x')"));
        assertEquals("42601", stateOf("COPY zzw_o3 FROM STDIN WITH (FORMAT binary, DEFAULT '\\D')"));
        assertEquals("0A000", stateOf("COPY zzw_o3 TO STDOUT WITH (FORMAT binary, HEADER)"));
        assertEquals("0A000", stateOf("COPY zzw_o3 TO STDOUT WITH (HEADER MATCH)"));
        assertEquals("22023", stateOf("COPY zzw_o3 TO STDOUT WITH (FREEZE)"));
        assertEquals("22023", stateOf("COPY zzw_o3 TO STDOUT WITH (ON_ERROR ignore)"));
        assertEquals("0A000", stateOf("COPY zzw_o3 TO STDOUT WITH (DEFAULT '\\D')"));
        assertEquals("22023",
                stateOf("COPY zzw_o3 TO STDOUT WITH (FORMAT csv, FORCE_NOT_NULL (a))"));
        assertEquals("22023", stateOf("COPY zzw_o3 TO STDOUT WITH (FORMAT csv, FORCE_NULL (a))"));
        assertEquals("0A000",
                stateOf("COPY zzw_o3 FROM STDIN WITH (FORMAT csv, FORCE_QUOTE (a))"));
        assertEquals("0A000", stateOf("COPY zzw_o3 FROM STDIN WITH (FORCE_NULL (a))"));
        assertEquals("22023",
                stateOf("COPY zzw_o3 FROM STDIN WITH (FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 0)"));
        assertEquals("0A000",
                stateOf("COPY zzw_o3 FROM STDIN WITH (FORMAT csv, DEFAULT '', NULL '')"));
        assertEquals("42703",
                stateOf("COPY zzw_o3 TO STDOUT WITH (FORMAT csv, FORCE_QUOTE (nosuchcol))"));
        exec("DROP TABLE zzw_o3");
    }

    @Test
    void copy_queryFormIsCheckedLikeTheRelationForm() throws Exception {
        assertEquals("22023", stateOf("COPY (SELECT 1) TO STDOUT WITH (FORMAT bogus)"));
        assertEquals("42601", stateOf("COPY (SELECT 1) TO STDOUT WITH (NOSUCHOPT true)"));
        assertEquals("22023", stateOf("COPY (SELECT 1) TO STDOUT WITH (FREEZE)"));
        assertEquals("0A000", stateOf("COPY (SELECT 1) TO STDOUT WITH (FORMAT csv, QUOTE 'ab')"));
        assertEquals("42601", stateOf("COPY (SELECT 1) TO STDOUT WHERE true"));
    }

    @Test
    void copy_refusesRelationsThatHoldNoRowsOfTheirOwn() throws Exception {
        exec("CREATE TABLE zzw_k1 (a int, b text)");
        exec("INSERT INTO zzw_k1 VALUES (1,'x')");
        exec("CREATE VIEW zzw_k1v AS SELECT a, b FROM zzw_k1");
        exec("CREATE MATERIALIZED VIEW zzw_k1m AS SELECT a, b FROM zzw_k1");
        exec("CREATE SEQUENCE zzw_k1s");
        exec("CREATE TABLE zzw_k1p (a int, b text) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw_k1p1 PARTITION OF zzw_k1p FOR VALUES FROM (0) TO (10)");

        assertEquals("42809", stateOf("COPY zzw_k1v TO STDOUT"));
        assertEquals("42809", stateOf("COPY zzw_k1v FROM STDIN"));
        assertEquals("42809", stateOf("COPY zzw_k1m FROM STDIN"));
        assertEquals("42809", stateOf("COPY zzw_k1s TO STDOUT"));
        assertEquals("42809", stateOf("COPY zzw_k1s FROM STDIN"));
        assertEquals("42809", stateOf("COPY zzw_k1p TO STDOUT"));

        exec("DROP TABLE zzw_k1p");
        exec("DROP SEQUENCE zzw_k1s");
        exec("DROP MATERIALIZED VIEW zzw_k1m");
        exec("DROP VIEW zzw_k1v");
        exec("DROP TABLE zzw_k1");
    }

    @Test
    void copy_generatedColumnMayNotBeNamed() throws Exception {
        exec("CREATE TABLE zzw_g1 (a int, g int GENERATED ALWAYS AS (a*2) STORED)");
        exec("INSERT INTO zzw_g1 (a) VALUES (5)");
        assertEquals("42P10", stateOf("COPY zzw_g1 (a,g) TO STDOUT"));
        // Refused at statement time, before any CopyInResponse would have gone out.
        assertEquals("42P10", stateOf("COPY zzw_g1 (a,g) FROM STDIN"));
        exec("DROP TABLE zzw_g1");
    }

    @Test
    void copy_whereIsRefusedOnCopyToAndAnalysedOnCopyFrom() throws Exception {
        exec("CREATE TABLE zzw_w1 (a int, b text)");
        assertEquals("42601", stateOf("COPY zzw_w1 TO STDOUT WHERE a > 0"));
        assertEquals("0A000", stateOf("COPY zzw_w1 FROM STDIN WHERE (SELECT true)"));
        assertEquals("42803", stateOf("COPY zzw_w1 FROM STDIN WHERE count(*) > 0"));
        assertEquals("42P20", stateOf("COPY zzw_w1 FROM STDIN WHERE row_number() OVER () > 0"));
        exec("DROP TABLE zzw_w1");
    }

    private long copyIn(String sql, String data) throws Exception {
        org.postgresql.copy.CopyManager cm =
                new org.postgresql.copy.CopyManager((org.postgresql.core.BaseConnection) conn);
        return cm.copyIn(sql, new java.io.ByteArrayInputStream(
                data.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    @Test
    void copyFrom_countsOnlyTheRowsItStored() throws Exception {
        exec("CREATE TABLE zzw_c1 (a int, b text)");
        assertEquals(1L, copyIn("COPY zzw_c1 FROM STDIN WITH (FORMAT csv) WHERE a > 1", "1,x\n5,y\n"));
        assertEquals("1", scalar("SELECT count(*)::text FROM zzw_c1"));
        exec("DROP TABLE zzw_c1");
    }

    @Test
    void copyFrom_countsOnlyTheRowsABeforeTriggerLetThrough() throws Exception {
        exec("CREATE TABLE zzw_c2 (a int, b text)");
        exec("CREATE FUNCTION zzw_skip() RETURNS trigger AS $$ BEGIN "
                + "IF NEW.a = 2 THEN RETURN NULL; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER zzw_tg BEFORE INSERT ON zzw_c2 "
                + "FOR EACH ROW EXECUTE FUNCTION zzw_skip()");
        assertEquals(2L, copyIn("COPY zzw_c2 FROM STDIN", "1\tx\n2\ty\n3\tz\n"));
        assertEquals("2", scalar("SELECT count(*)::text FROM zzw_c2"));
        exec("DROP TABLE zzw_c2");
        exec("DROP FUNCTION zzw_skip()");
    }

    @Test
    void copyFrom_onErrorIgnoreDoesNotSwallowConstraintViolations() throws Exception {
        exec("CREATE TABLE zzw_u1 (a int, b int NOT NULL CHECK (b < 100))");
        SQLException nn = assertThrows(SQLException.class, () ->
                copyIn("COPY zzw_u1 FROM STDIN WITH (FORMAT csv, ON_ERROR ignore)",
                        "1,1\n2,\n3,3\n"));
        assertEquals("23502", nn.getSQLState());
        assertEquals("0", scalar("SELECT count(*)::text FROM zzw_u1"));

        SQLException chk = assertThrows(SQLException.class, () ->
                copyIn("COPY zzw_u1 FROM STDIN WITH (FORMAT csv, ON_ERROR ignore)",
                        "1,1\n2,500\n3,3\n"));
        assertEquals("23514", chk.getSQLState());
        assertEquals("0", scalar("SELECT count(*)::text FROM zzw_u1"));

        exec("CREATE UNIQUE INDEX zzw_ui ON zzw_u1 (a)");
        SQLException dup = assertThrows(SQLException.class, () ->
                copyIn("COPY zzw_u1 FROM STDIN WITH (FORMAT csv, ON_ERROR ignore)", "1,1\n1,2\n"));
        assertEquals("23505", dup.getSQLState());
        assertEquals("0", scalar("SELECT count(*)::text FROM zzw_u1"));
        exec("DROP TABLE zzw_u1");
    }

    @Test
    void copyFrom_rejectLimitCapsTheSkippedRowsAndTheNoticeReportsThem() throws Exception {
        exec("CREATE TABLE zzw_r1 (a int)");
        SQLException over = assertThrows(SQLException.class, () ->
                copyIn("COPY zzw_r1 FROM STDIN WITH (FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 1)",
                        "1\nbad\nworse\n"));
        assertEquals("22P02", over.getSQLState());
        assertTrue(over.getMessage().contains("skipped more than REJECT_LIMIT (1) rows"),
                over.getMessage());
        assertEquals("0", scalar("SELECT count(*)::text FROM zzw_r1"));

        assertEquals(1L,
                copyIn("COPY zzw_r1 FROM STDIN WITH (FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 2)",
                        "1\nbad\nworse\n"));

        exec("DELETE FROM zzw_r1");
        conn.clearWarnings();
        assertEquals(2L, copyIn("COPY zzw_r1 FROM STDIN WITH (FORMAT csv, ON_ERROR ignore)",
                "1\nbad\n2\n"));
        SQLWarning w = conn.getWarnings();
        assertNotNull(w, "a skipped row is reported by a NOTICE");
        assertTrue(w.getMessage().contains("1 row was skipped due to data type incompatibility"),
                w.getMessage());
        exec("DROP TABLE zzw_r1");
    }

    @Test
    void copyFrom_routesIntoPartitionsAndRefusesARowThatBelongsToNone() throws Exception {
        exec("CREATE TABLE zzw_p1 (a int, b text) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw_p1a PARTITION OF zzw_p1 FOR VALUES FROM (0) TO (10)");
        exec("INSERT INTO zzw_p1 VALUES (1,'p')");
        assertEquals(2L, copyIn("COPY zzw_p1 FROM STDIN WITH (FORMAT csv)", "2,q\n3,x\n"));
        assertEquals("3", scalar("SELECT count(*)::text FROM zzw_p1a"));
        assertEquals("0", scalar("SELECT count(*)::text FROM ONLY zzw_p1"));

        SQLException e = assertThrows(SQLException.class, () ->
                copyIn("COPY zzw_p1 FROM STDIN WITH (FORMAT csv)", "25,q\n"));
        assertEquals("23514", e.getSQLState());
        assertEquals("3", scalar("SELECT count(*)::text FROM zzw_p1"));
        exec("DROP TABLE zzw_p1");
    }

    @Test
    void copyFrom_binaryIsAtomicLikeText() throws Exception {
        exec("CREATE TABLE zzw_b1 (a int, b int NOT NULL)");
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        out.write(new byte[]{'P','G','C','O','P','Y','\n',(byte)0xFF,'\r','\n',0});
        out.write(new byte[]{0,0,0,0});
        out.write(new byte[]{0,0,0,0});
        int[][] rows = {{1,1},{2,2}};
        for (int[] r : rows) {
            out.write(new byte[]{0,2});
            for (int v : r) {
                out.write(new byte[]{0,0,0,4});
                out.write(new byte[]{(byte)(v>>>24),(byte)(v>>>16),(byte)(v>>>8),(byte)v});
            }
        }
        out.write(new byte[]{0,2});
        out.write(new byte[]{0,0,0,4});
        out.write(new byte[]{0,0,0,3});
        out.write(new byte[]{(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF});
        out.write(new byte[]{(byte)0xFF,(byte)0xFF});
        org.postgresql.copy.CopyManager cm =
                new org.postgresql.copy.CopyManager((org.postgresql.core.BaseConnection) conn);
        SQLException e = assertThrows(SQLException.class, () ->
                cm.copyIn("COPY zzw_b1 FROM STDIN WITH (FORMAT binary)",
                        new java.io.ByteArrayInputStream(out.toByteArray())));
        assertEquals("23502", e.getSQLState());
        assertEquals("0", scalar("SELECT count(*)::text FROM zzw_b1"));
        exec("DROP TABLE zzw_b1");
    }

    @Test
    void copyFrom_binaryFrameIsReadRatherThanSeekedOver() throws Exception {
        exec("CREATE TABLE zzw_b2 (a int)");
        org.postgresql.copy.CopyManager cm =
                new org.postgresql.copy.CopyManager((org.postgresql.core.BaseConnection) conn);
        for (String junk : new String[]{"garbage\n", "PGC", ""}) {
            SQLException e = assertThrows(SQLException.class, () ->
                    cm.copyIn("COPY zzw_b2 FROM STDIN WITH (FORMAT binary)",
                            new java.io.ByteArrayInputStream(
                                    junk.getBytes(java.nio.charset.StandardCharsets.UTF_8))));
            assertEquals("22P04", e.getSQLState(), junk);
            assertEquals("COPY file signature not recognized",
                    e.getMessage().replace("ERROR: ", "").trim());
        }
        exec("DROP TABLE zzw_b2");
    }

    @Test
    void copyFrom_emptyLineFailsInTheFirstColumnsInputFunction() throws Exception {
        exec("CREATE TABLE zzw_e1 (a int, b text)");
        SQLException e = assertThrows(SQLException.class, () ->
                copyIn("COPY zzw_e1 FROM STDIN", "1\tx\n\n"));
        assertEquals("22P02", e.getSQLState());
        // A line that is short but converts is still reported as missing data.
        SQLException m = assertThrows(SQLException.class, () ->
                copyIn("COPY zzw_e1 FROM STDIN", "1\n"));
        assertEquals("22P04", m.getSQLState());
        assertTrue(m.getMessage().contains("missing data for column \"b\""), m.getMessage());
        exec("DROP TABLE zzw_e1");
    }

    @Test
    void copyFrom_headerMatchCountUsesPostgresWording() throws Exception {
        exec("CREATE TABLE zzw_h1 (a int, b text)");
        SQLException e = assertThrows(SQLException.class, () ->
                copyIn("COPY zzw_h1 FROM STDIN WITH (FORMAT csv, HEADER MATCH)", "a\n3\n"));
        assertEquals("22P04", e.getSQLState());
        assertTrue(e.getMessage().contains("wrong number of fields in header line: got 1, expected 2"),
                e.getMessage());
        exec("DROP TABLE zzw_h1");
    }

    @Test
    void copyTo_leavesGeneratedColumnsOut() throws Exception {
        exec("CREATE TABLE zzw_g2 (a int, g int GENERATED ALWAYS AS (a*2) STORED)");
        exec("INSERT INTO zzw_g2 (a) VALUES (5)");
        org.postgresql.copy.CopyManager cm =
                new org.postgresql.copy.CopyManager((org.postgresql.core.BaseConnection) conn);
        java.io.StringWriter sw = new java.io.StringWriter();
        cm.copyOut("COPY zzw_g2 TO STDOUT", sw);
        assertEquals("5\n", sw.toString());
        java.io.StringWriter hdr = new java.io.StringWriter();
        cm.copyOut("COPY zzw_g2 TO STDOUT WITH (FORMAT csv, HEADER)", hdr);
        assertEquals("a\n5\n", hdr.toString());
        exec("DROP TABLE zzw_g2");
    }

    // ------------------------------------------------------------ A column definition keeps everything it was written with

    @Test
    void addColumnStoresInlineUniqueAndPrimaryKey() throws Exception {
        exec("CREATE TABLE zzg1_a1 (a int)");
        exec("ALTER TABLE zzg1_a1 ADD COLUMN b int UNIQUE");
        exec("INSERT INTO zzg1_a1 (b) VALUES (1)");
        SQLException dup = assertThrows(SQLException.class, () -> exec("INSERT INTO zzg1_a1 (b) VALUES (1)"));
        assertEquals("23505", dup.getSQLState());
        assertEquals("zzg1_a1_b_key",
                scalar("SELECT conname FROM pg_constraint WHERE conrelid='zzg1_a1'::regclass AND contype='u'"));

        exec("CREATE TABLE zzg1_a2 (a int)");
        exec("ALTER TABLE zzg1_a2 ADD COLUMN b int PRIMARY KEY");
        assertEquals("zzg1_a2_pkey",
                scalar("SELECT conname FROM pg_constraint WHERE conrelid='zzg1_a2'::regclass AND contype='p'"));
        exec("INSERT INTO zzg1_a2 (a,b) VALUES (1,1)");
        SQLException dupPk = assertThrows(SQLException.class, () -> exec("INSERT INTO zzg1_a2 (a,b) VALUES (2,1)"));
        assertEquals("23505", dupPk.getSQLState());

        exec("CREATE TABLE zzg1_a5 (a int)");
        exec("ALTER TABLE zzg1_a5 ADD COLUMN b int CONSTRAINT zzg1_myuq UNIQUE");
        assertEquals("zzg1_myuq",
                scalar("SELECT conname FROM pg_constraint WHERE conrelid='zzg1_a5'::regclass AND contype='u'"));
    }

    @Test
    void addColumnStoresForeignKeyAndCheck() throws Exception {
        exec("CREATE TABLE zzg1_p (id int PRIMARY KEY)");
        exec("INSERT INTO zzg1_p VALUES (1)");
        exec("CREATE TABLE zzg1_c (a int)");
        exec("ALTER TABLE zzg1_c ADD COLUMN r int REFERENCES zzg1_p(id)");
        assertEquals("zzg1_c_r_fkey",
                scalar("SELECT conname FROM pg_constraint WHERE conrelid='zzg1_c'::regclass AND contype='f'"));
        SQLException fk = assertThrows(SQLException.class, () -> exec("INSERT INTO zzg1_c (a,r) VALUES (1, 99)"));
        assertEquals("23503", fk.getSQLState());

        exec("CREATE TABLE zzg1_k (a int)");
        exec("INSERT INTO zzg1_k VALUES (1)");
        exec("ALTER TABLE zzg1_k ADD COLUMN f int CHECK (f > 100) DEFAULT 500");
        assertEquals("zzg1_k_f_check",
                scalar("SELECT conname FROM pg_constraint WHERE conrelid='zzg1_k'::regclass AND contype='c'"));
        SQLException chk = assertThrows(SQLException.class, () -> exec("INSERT INTO zzg1_k (a,f) VALUES (2, 1)"));
        assertEquals("23514", chk.getSQLState());

        exec("CREATE TABLE zzg1_k2 (a int)");
        exec("INSERT INTO zzg1_k2 VALUES (1)");
        SQLException bad = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE zzg1_k2 ADD COLUMN e int CHECK (e > 100) DEFAULT 5"));
        assertEquals("23514", bad.getSQLState());
        assertTrue(bad.getMessage().contains("check constraint \"zzg1_k2_e_check\" of relation \"zzg1_k2\""),
                bad.getMessage());
    }

    @Test
    void addColumnRefusesDuplicateBackfillAndSecondPrimaryKey() throws Exception {
        exec("CREATE TABLE zzg1_u (a int)");
        exec("INSERT INTO zzg1_u VALUES (1)");
        exec("INSERT INTO zzg1_u VALUES (2)");
        SQLException dup = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE zzg1_u ADD COLUMN b int UNIQUE DEFAULT 7"));
        assertEquals("23505", dup.getSQLState());
        assertTrue(dup.getMessage().contains("could not create unique index \"zzg1_u_b_key\""), dup.getMessage());
        // The column is not added, so naming it is a missing column
        SQLException gone = assertThrows(SQLException.class, () -> scalar("SELECT b FROM zzg1_u"));
        assertEquals("42703", gone.getSQLState());

        exec("CREATE TABLE zzg1_q (a int PRIMARY KEY)");
        SQLException twoPk = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE zzg1_q ADD COLUMN b int PRIMARY KEY"));
        assertEquals("42P16", twoPk.getSQLState());
    }

    @Test
    void renameTableKeepsEverythingAboutTheRelation() throws Exception {
        exec("CREATE TABLE zzg1_f6 (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzg1_f6_1 PARTITION OF zzg1_f6 FOR VALUES FROM (1) TO (10)");
        exec("ALTER TABLE zzg1_f6_1 RENAME TO zzg1_f6_x");
        exec("INSERT INTO zzg1_f6 VALUES (5)");
        assertEquals(1L, num("SELECT count(*) FROM zzg1_f6_x"));

        exec("CREATE TABLE zzg1_f7p (a int)");
        exec("CREATE TABLE zzg1_f7c () INHERITS (zzg1_f7p)");
        exec("INSERT INTO zzg1_f7c VALUES (2)");
        exec("ALTER TABLE zzg1_f7p RENAME TO zzg1_f7p2");
        assertEquals(1L, num("SELECT count(*) FROM zzg1_f7p2"));

        exec("CREATE UNLOGGED TABLE zzg1_u1 (a int PRIMARY KEY)");
        exec("ALTER TABLE zzg1_u1 REPLICA IDENTITY FULL");
        exec("ALTER TABLE zzg1_u1 ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE zzg1_u1 FORCE ROW LEVEL SECURITY");
        exec("ALTER TABLE zzg1_u1 RENAME TO zzg1_u2");
        assertEquals("u", scalar("SELECT relpersistence FROM pg_class WHERE relname='zzg1_u2'"));
        assertEquals("f", scalar("SELECT relreplident FROM pg_class WHERE relname='zzg1_u2'"));
        // A boolean travels in the text format as "t" or "f", so that is what the client reads back.
        assertEquals("t", scalar("SELECT relforcerowsecurity FROM pg_class WHERE relname='zzg1_u2'"));

        exec("CREATE TABLE zzg1_r1 (a int) WITH (fillfactor=70)");
        exec("ALTER TABLE zzg1_r1 RENAME TO zzg1_r2");
        assertEquals("{fillfactor=70}",
                String.valueOf(scalar("SELECT reloptions FROM pg_class WHERE relname='zzg1_r2'")));

        exec("CREATE TABLE zzg1_r3 (a int)");
        exec("ALTER TABLE zzg1_r3 ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY zzg1_p1 ON zzg1_r3 USING (a > 0)");
        exec("ALTER TABLE zzg1_r3 RENAME TO zzg1_r4");
        assertEquals(1L, num("SELECT count(*) FROM pg_policies WHERE tablename='zzg1_r4'"));

        exec("CREATE TABLE zzg1_r9 (a int CONSTRAINT zzg1_nn NOT NULL)");
        exec("ALTER TABLE zzg1_r9 RENAME TO zzg1_r10");
        assertEquals("zzg1_nn",
                scalar("SELECT conname FROM pg_constraint WHERE conrelid='zzg1_r10'::regclass ORDER BY 1 LIMIT 1"));
    }

    @Test
    void noPhantomImplicitSequencesInPgClass() throws Exception {
        exec("CREATE TABLE zzg1_r5 (i serial, v text)");
        exec("INSERT INTO zzg1_r5 (v) VALUES ('a')");
        exec("ALTER TABLE zzg1_r5 RENAME TO zzg1_r6");
        assertEquals("zzg1_r5_i_seq",
                scalar("SELECT string_agg(relname, ',' ORDER BY relname) FROM pg_class WHERE relkind='S' AND relname LIKE 'zzg1_r%'"));

        exec("CREATE TABLE zzg1_w2 (i serial, v text)");
        exec("ALTER TABLE zzg1_w2 RENAME COLUMN i TO j");
        assertEquals("zzg1_w2_i_seq",
                scalar("SELECT string_agg(relname, ',' ORDER BY relname) FROM pg_class WHERE relkind='S' AND relname LIKE 'zzg1_w2%'"));

        exec("CREATE TABLE zzg1_z1 (a int GENERATED ALWAYS AS IDENTITY, b int) PARTITION BY RANGE (b)");
        exec("CREATE TABLE zzg1_z1p PARTITION OF zzg1_z1 FOR VALUES FROM (0) TO (10)");
        assertEquals("zzg1_z1_a_seq",
                scalar("SELECT string_agg(relname, ',' ORDER BY relname) FROM pg_class WHERE relkind='S' AND relname LIKE 'zzg1_z1%'"));
        assertEquals(1L, num("SELECT count(*) FROM pg_sequences WHERE sequencename LIKE 'zzg1_z1%'"));
    }

    @Test
    void identitySequenceOptionsAreHonouredAndValidated() throws Exception {
        exec("CREATE TABLE zzg1_i1 (i int GENERATED ALWAYS AS IDENTITY (MINVALUE 1 MAXVALUE 2 CYCLE), j int)");
        exec("INSERT INTO zzg1_i1 (j) VALUES (1),(2),(3)");
        assertEquals("1,2,1", scalar("SELECT string_agg(i::text, ',' ORDER BY j) FROM zzg1_i1"));
        // A boolean travels in the text format as "t" or "f", so that is what the client reads back.
        assertEquals("t", scalar("SELECT cycle FROM pg_sequences WHERE sequencename='zzg1_i1_i_seq'"));
        assertEquals(2L, num("SELECT max_value FROM pg_sequences WHERE sequencename='zzg1_i1_i_seq'"));

        exec("CREATE TABLE zzg1_i2 (i int GENERATED ALWAYS AS IDENTITY (MINVALUE 1 MAXVALUE 2), j int)");
        SQLException exhausted = assertThrows(SQLException.class,
                () -> exec("INSERT INTO zzg1_i2 (j) VALUES (1),(2),(3)"));
        assertEquals("2200H", exhausted.getSQLState());

        exec("CREATE TABLE zzg1_i3 (i int GENERATED ALWAYS AS IDENTITY (SEQUENCE NAME zzg1_myseq), j int)");
        assertEquals("public.zzg1_myseq", scalar("SELECT pg_get_serial_sequence('zzg1_i3','i')"));

        assertEquals("22023", assertThrows(SQLException.class, () ->
                exec("CREATE TABLE zzg1_i6 (i int GENERATED ALWAYS AS IDENTITY (CACHE 0), j int)")).getSQLState());
        assertEquals("22023", assertThrows(SQLException.class, () ->
                exec("CREATE TABLE zzg1_i7 (i int GENERATED ALWAYS AS IDENTITY (MINVALUE 5 MAXVALUE 3), j int)")).getSQLState());
        assertEquals("22023", assertThrows(SQLException.class, () ->
                exec("CREATE TABLE zzg1_i8 (i int GENERATED ALWAYS AS IDENTITY (INCREMENT BY 0), j int)")).getSQLState());

        exec("CREATE TABLE zzg1_i9 (i int GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY -10 MINVALUE 1 MAXVALUE 1000), j int)");
        exec("INSERT INTO zzg1_i9 (j) VALUES (1),(2)");
        assertEquals("100,90", scalar("SELECT string_agg(i::text, ',' ORDER BY j) FROM zzg1_i9"));

        exec("CREATE TABLE zzg1_j4 (b text)");
        exec("INSERT INTO zzg1_j4 VALUES ('r1')");
        exec("ALTER TABLE zzg1_j4 ADD COLUMN a smallint GENERATED ALWAYS AS IDENTITY");
        assertEquals(32767L, num("SELECT max_value FROM pg_sequences WHERE sequencename='zzg1_j4_a_seq'"));
    }

    @Test
    void alterColumnIdentitySequenceOptions() throws Exception {
        exec("CREATE TABLE zzg1_i4 (a int GENERATED BY DEFAULT AS IDENTITY, b text)");
        exec("ALTER TABLE zzg1_i4 ALTER COLUMN a SET INCREMENT BY 10");
        exec("INSERT INTO zzg1_i4 (b) VALUES ('p')");
        exec("INSERT INTO zzg1_i4 (b) VALUES ('q')");
        assertEquals(11, num("SELECT a FROM zzg1_i4 WHERE b='q'"));

        SQLException notIdentity = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE zzg1_i4 ALTER COLUMN b SET INCREMENT BY 5"));
        assertEquals("55000", notIdentity.getSQLState());

        SQLException dropNn = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE zzg1_i4 ALTER COLUMN a DROP NOT NULL"));
        assertEquals("42601", dropNn.getSQLState());

        exec("CREATE TABLE zzg1_j1 (a int GENERATED BY DEFAULT AS IDENTITY, b text)");
        exec("ALTER TABLE zzg1_j1 ALTER COLUMN a SET MAXVALUE 3");
        SQLException belowMin = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE zzg1_j1 ALTER COLUMN a SET MINVALUE 2"));
        assertEquals("22023", belowMin.getSQLState());
        exec("ALTER TABLE zzg1_j1 ALTER COLUMN a SET CYCLE");
        exec("ALTER TABLE zzg1_j1 ALTER COLUMN a SET CACHE 4");
        exec("ALTER TABLE zzg1_j1 ALTER COLUMN a SET START WITH 2");
        assertEquals("2|1|3|true|4", scalar(
                "SELECT start_value || '|' || min_value || '|' || max_value || '|' || cycle || '|' || cache_size"
                + " FROM pg_sequences WHERE sequencename='zzg1_j1_a_seq'"));
    }

    @Test
    void ownedSequencesGoWithTheirColumn() throws Exception {
        exec("CREATE TABLE zzg1_o1 (i serial, v text)");
        exec("ALTER TABLE zzg1_o1 DROP COLUMN i");
        assertEquals(0L, num("SELECT count(*) FROM pg_class WHERE relname='zzg1_o1_i_seq'"));
        exec("CREATE SEQUENCE zzg1_o1_i_seq");

        exec("CREATE TABLE zzg1_o2 (id int GENERATED ALWAYS AS IDENTITY, v text)");
        exec("ALTER TABLE zzg1_o2 ALTER COLUMN id DROP IDENTITY");
        assertEquals(0L, num("SELECT count(*) FROM pg_class WHERE relname='zzg1_o2_id_seq'"));

        exec("CREATE TABLE zzg1_w1 (a int, b text)");
        exec("CREATE SEQUENCE zzg1_w1s");
        exec("ALTER SEQUENCE zzg1_w1s OWNED BY zzg1_w1.a");
        exec("DROP TABLE zzg1_w1");
        assertEquals(0L, num("SELECT count(*) FROM pg_class WHERE relname='zzg1_w1s'"));

        exec("CREATE TABLE zzg1_w3 (i serial, v text)");
        exec("ALTER TABLE zzg1_w3 RENAME COLUMN i TO j");
        assertEquals("public.zzg1_w3_i_seq", scalar("SELECT pg_get_serial_sequence('zzg1_w3','j')"));
        exec("DROP TABLE zzg1_w3 CASCADE");
        assertEquals(0L, num("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzg1_w3%'"));
    }

    @Test
    void pgGetSerialSequenceReadsOwnershipOnly() throws Exception {
        exec("CREATE SEQUENCE zzg1_ds");
        exec("CREATE TABLE zzg1_o3 (a int DEFAULT nextval('zzg1_ds'), b text)");
        assertNull(scalar("SELECT pg_get_serial_sequence('zzg1_o3','a')"));

        exec("CREATE TABLE zzg1_o4 (i serial, v text)");
        SQLException wrongCase = assertThrows(SQLException.class,
                () -> scalar("SELECT pg_get_serial_sequence('zzg1_o4','I')"));
        assertEquals("42703", wrongCase.getSQLState());

        exec("CREATE TABLE zzg1_q5 (\"Cap\" serial, v text)");
        assertEquals("public.\"zzg1_q5_Cap_seq\"", scalar("SELECT pg_get_serial_sequence('zzg1_q5','Cap')"));
        assertEquals("42703", assertThrows(SQLException.class,
                () -> scalar("SELECT pg_get_serial_sequence('zzg1_q5','cap')")).getSQLState());

        exec("CREATE TABLE zzg1_z2 (a int GENERATED ALWAYS AS IDENTITY, b int) PARTITION BY RANGE (b)");
        exec("CREATE TABLE zzg1_z2p PARTITION OF zzg1_z2 FOR VALUES FROM (0) TO (10)");
        assertNull(scalar("SELECT pg_get_serial_sequence('zzg1_z2p','a')"));
        exec("DROP TABLE zzg1_z2 CASCADE");
        assertEquals(0L, num("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzg1_z2%'"));
    }

    @Test
    void sessionSequenceStateFollowsTheSequence() throws Exception {
        exec("CREATE SEQUENCE zzg1_s1");
        scalar("SELECT nextval('zzg1_s1')");
        exec("DROP SEQUENCE zzg1_s1");
        exec("CREATE SEQUENCE zzg1_s1");
        assertEquals("55000", assertThrows(SQLException.class,
                () -> scalar("SELECT currval('zzg1_s1')")).getSQLState());
        assertEquals("55000", assertThrows(SQLException.class,
                () -> scalar("SELECT lastval()")).getSQLState());

        exec("CREATE SEQUENCE zzg1_t1");
        scalar("SELECT nextval('zzg1_t1')");
        exec("ALTER SEQUENCE zzg1_t1 RENAME TO zzg1_t2");
        assertEquals(1L, num("SELECT currval('zzg1_t2')"));

        exec("DISCARD SEQUENCES");
        assertEquals("55000", assertThrows(SQLException.class,
                () -> scalar("SELECT currval('zzg1_t2')")).getSQLState());

        // The same sequence name in two schemas: each keeps its own session state. The name is this
        // test's alone, so nothing another test leaves behind can stand in the way of creating it.
        exec("CREATE SCHEMA zzg1_sc");
        exec("CREATE SEQUENCE zzg1_dual CACHE 5");
        exec("CREATE SEQUENCE zzg1_sc.zzg1_dual CACHE 5");
        assertEquals(1L, num("SELECT nextval('zzg1_dual')"));
        assertEquals(1L, num("SELECT nextval('zzg1_sc.zzg1_dual')"));
        assertEquals(2L, num("SELECT nextval('zzg1_dual')"));
        assertEquals(2L, num("SELECT nextval('zzg1_sc.zzg1_dual')"));

        exec("CREATE SEQUENCE zzg1_s6 CACHE 5 CYCLE");
        assertEquals(1L, num("SELECT nextval('zzg1_s6')"));
        assertEquals(5L, num("SELECT last_value FROM zzg1_s6"));
    }

    @Test
    void sequenceCatalogueAndOptionChecking() throws Exception {
        exec("CREATE SEQUENCE zzg1_s3 CACHE 7");
        assertEquals(7L, num("SELECT seqcache FROM pg_sequence WHERE seqrelid='zzg1_s3'::regclass"));

        exec("CREATE SEQUENCE zzg1_q9 AS integer");
        assertEquals("integer", scalar("SELECT seqtypid::regtype::text FROM pg_sequence WHERE seqrelid='zzg1_q9'::regclass"));
        assertEquals("integer", scalar("SELECT data_type::text FROM pg_sequences WHERE sequencename='zzg1_q9'"));
        exec("CREATE SEQUENCE zzg1_t5 AS smallint");
        assertEquals("smallint", scalar("SELECT seqtypid::regtype::text FROM pg_sequence WHERE seqrelid='zzg1_t5'::regclass"));

        exec("CREATE UNLOGGED SEQUENCE zzg1_s4");
        assertEquals("u", scalar("SELECT relpersistence FROM pg_class WHERE relname='zzg1_s4'"));

        assertEquals("42601", assertThrows(SQLException.class,
                () -> exec("CREATE SEQUENCE zzg1_s8 START 1 START 2")).getSQLState());
        exec("CREATE SEQUENCE zzg1_s9");
        assertEquals("42601", assertThrows(SQLException.class,
                () -> exec("ALTER SEQUENCE zzg1_s9")).getSQLState());
        assertEquals("42601", assertThrows(SQLException.class,
                () -> exec("ALTER SEQUENCE zzg1_s9 START 1 START 2")).getSQLState());
    }

    // ------------------------------------------------------------ A definition is validated before it is built

    @Test
    void addColumnDefaultIsCheckedAgainstTheColumnsWidth() throws Exception {
        exec("CREATE TABLE hdv_a (a int)");
        exec("INSERT INTO hdv_a VALUES (1)");
        // The literal spells random( but is a value, not a call: PG measures 22001 here.
        SQLException tooLong = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE hdv_a ADD COLUMN s varchar(3) DEFAULT 'random(9)'"));
        assertEquals("22001", tooLong.getSQLState());
        // The column is not added, so naming it is 42703.
        SQLException gone = assertThrows(SQLException.class, () -> scalar("SELECT s FROM hdv_a"));
        assertEquals("42703", gone.getSQLState());

        exec("CREATE TABLE hdv_b (a int)");
        exec("INSERT INTO hdv_b VALUES (1)");
        SQLException volatileTooLong = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE hdv_b ADD COLUMN u varchar(3) DEFAULT gen_random_uuid()::text"));
        assertEquals("22001", volatileTooLong.getSQLState());

        // A volatile default that does fit still gives every existing row its own value.
        exec("CREATE TABLE hdv_c (a int)");
        exec("INSERT INTO hdv_c VALUES (1), (2)");
        exec("ALTER TABLE hdv_c ADD COLUMN u text DEFAULT gen_random_uuid()::text");
        assertEquals(2L, num("SELECT count(DISTINCT u) FROM hdv_c"));
        exec("DROP TABLE hdv_a");
        exec("DROP TABLE hdv_b");
        exec("DROP TABLE hdv_c");
    }

    @Test
    void alterTableOnQualifiedNameReachesThatSchemasRelation() throws Exception {
        exec("CREATE SCHEMA hdq_sch");
        exec("CREATE TABLE hdq_src (a int)");
        exec("CREATE VIEW hdq_dup AS SELECT a FROM hdq_src");
        exec("CREATE TABLE hdq_sch.hdq_dup (b int)");
        exec("INSERT INTO hdq_sch.hdq_dup VALUES (7)");
        exec("ALTER TABLE hdq_sch.hdq_dup RENAME TO hdq_dup2");
        assertEquals(7, num("SELECT b FROM hdq_sch.hdq_dup2"));
        // The public view was never touched, and still reads its (empty) source table.
        assertEquals(0L, num("SELECT count(*) FROM public.hdq_dup"));
        exec("DROP VIEW public.hdq_dup");
        exec("DROP TABLE hdq_sch.hdq_dup2");
        exec("DROP TABLE hdq_src");
        exec("DROP SCHEMA hdq_sch");
    }

    @Test
    void dropColumnDependsOnWhatTheViewReallyReads() throws Exception {
        // "e" occurs inside "keepme" and inside SELECT, but the view does not read it.
        exec("CREATE TABLE hdd_t1 (keepme int, e int)");
        exec("CREATE VIEW hdd_v1 AS SELECT keepme FROM hdd_t1");
        exec("ALTER TABLE hdd_t1 DROP COLUMN e");
        assertEquals(1L, num(
                "SELECT count(*) FROM information_schema.columns WHERE table_name='hdd_t1'"));

        // A view that really reads the column blocks the drop, with PG's code, DETAIL and HINT.
        exec("CREATE TABLE hdd_t2 (i int, v text)");
        exec("CREATE VIEW hdd_v2 AS SELECT i, v FROM hdd_t2");
        SQLException blocked = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE hdd_t2 DROP COLUMN v"));
        assertEquals("2BP01", blocked.getSQLState());
        assertTrue(blocked.getMessage().contains(
                "cannot drop column v of table hdd_t2 because other objects depend on it"),
                blocked.getMessage());

        // CASCADE drops the view rather than skipping the check.
        exec("ALTER TABLE hdd_t2 DROP COLUMN v CASCADE");
        assertEquals(0L, num(
                "SELECT count(*) FROM pg_class WHERE relname='hdd_v2'"));
        exec("DROP VIEW hdd_v1");
        exec("DROP TABLE hdd_t1");
        exec("DROP TABLE hdd_t2");
    }

    @Test
    void identityOptionsAreReadAndCheckedLikeASequences() throws Exception {
        exec("CREATE TABLE hdi_1 (i int GENERATED ALWAYS AS IDENTITY"
                + " (START WITH 100 INCREMENT BY -10 MINVALUE 1 MAXVALUE 1000), j int)");
        exec("INSERT INTO hdi_1 (j) VALUES (1)");
        exec("INSERT INTO hdi_1 (j) VALUES (2)");
        assertEquals(100, num("SELECT i FROM hdi_1 WHERE j = 1"));
        assertEquals(90, num("SELECT i FROM hdi_1 WHERE j = 2"));

        SQLException belowMin = assertThrows(SQLException.class, () -> exec(
                "CREATE TABLE hdi_2 (i int GENERATED ALWAYS AS IDENTITY (START WITH -5), j int)"));
        assertEquals("22023", belowMin.getSQLState());
        assertTrue(belowMin.getMessage().contains(
                "START value (-5) cannot be less than MINVALUE (1)"), belowMin.getMessage());

        SQLException fractional = assertThrows(SQLException.class, () -> exec(
                "CREATE TABLE hdi_3 (i int GENERATED ALWAYS AS IDENTITY (START WITH 1.5), j int)"));
        assertEquals("22P02", fractional.getSQLState());

        SQLException zeroIncrement = assertThrows(SQLException.class, () -> exec(
                "CREATE TABLE hdi_4 (i int GENERATED ALWAYS AS IDENTITY (INCREMENT BY 0), j int)"));
        assertEquals("22023", zeroIncrement.getSQLState());
        assertTrue(zeroIncrement.getMessage().contains("INCREMENT must not be zero"),
                zeroIncrement.getMessage());
        exec("DROP TABLE hdi_1");
    }

    @Test
    void setStatisticsIsBoundedTheWayPostgresBoundsIt() throws Exception {
        exec("CREATE TABLE hds_r3 (a int, b int)");
        assertNull(scalar("SELECT attstattarget FROM pg_attribute"
                + " WHERE attrelid='hds_r3'::regclass AND attname='a'"));
        exec("ALTER TABLE hds_r3 ALTER COLUMN a SET STATISTICS 100000");
        assertEquals(10000, num("SELECT attstattarget FROM pg_attribute"
                + " WHERE attrelid='hds_r3'::regclass AND attname='a'"));
        exec("ALTER TABLE hds_r3 ALTER COLUMN a SET STATISTICS -1");
        assertNull(scalar("SELECT attstattarget FROM pg_attribute"
                + " WHERE attrelid='hds_r3'::regclass AND attname='a'"));
        // PostgreSQL's grammar takes SignedIconst here, so these are syntax errors, not values.
        assertEquals("42601", assertThrows(SQLException.class,
                () -> exec("ALTER TABLE hds_r3 ALTER COLUMN a SET STATISTICS 2147483648")).getSQLState());
        assertEquals("42601", assertThrows(SQLException.class,
                () -> exec("ALTER TABLE hds_r3 ALTER COLUMN a SET STATISTICS 'x'")).getSQLState());
        exec("DROP TABLE hds_r3");
    }

    @Test
    void alterColumnSetExpressionAndColumnOptions() throws Exception {
        exec("CREATE TABLE hde_g1 (a int, c int GENERATED ALWAYS AS (a+1) STORED)");
        exec("INSERT INTO hde_g1 (a) VALUES (5)");
        exec("ALTER TABLE hde_g1 ALTER COLUMN c SET EXPRESSION AS (a*10)");
        assertEquals(50, num("SELECT c FROM hde_g1"));
        SQLException notGenerated = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE hde_g1 ALTER COLUMN a SET EXPRESSION AS (c*2)"));
        assertEquals("55000", notGenerated.getSQLState());

        exec("CREATE TABLE hde_o1 (a int)");
        exec("ALTER TABLE hde_o1 ALTER COLUMN a SET (n_distinct = 5)");
        exec("ALTER TABLE hde_o1 ALTER COLUMN a RESET (n_distinct)");
        exec("DROP TABLE hde_g1");
        exec("DROP TABLE hde_o1");
    }

    @Test
    void includeAndNotNullNoInheritParse() throws Exception {
        exec("CREATE TABLE hdp_inc (a int, b int, UNIQUE (a) INCLUDE (b))");
        exec("CREATE TABLE hdp_inc2 (a int, b int, PRIMARY KEY (a) INCLUDE (b))");
        // The payload column is not part of the key: two rows differing only in b still collide.
        exec("INSERT INTO hdp_inc VALUES (1, 1)");
        assertEquals("23505", assertThrows(SQLException.class,
                () -> exec("INSERT INTO hdp_inc VALUES (1, 2)")).getSQLState());
        exec("CREATE TABLE hdp_nn (a int NOT NULL NO INHERIT)");
        exec("CREATE TABLE hdp_ni (a int NOT NULL NO INHERIT, b int)");
        exec("DROP TABLE hdp_inc");
        exec("DROP TABLE hdp_inc2");
        exec("DROP TABLE hdp_nn");
        exec("DROP TABLE hdp_ni");
    }

    @Test
    void keyColumnListsAreChecked() throws Exception {
        SQLException missing = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE hdk_u1 (a int, UNIQUE (nosuchcol))"));
        assertEquals("42703", missing.getSQLState());
        assertTrue(missing.getMessage().contains(
                "column \"nosuchcol\" named in key does not exist"), missing.getMessage());

        SQLException pkMissing = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE hdk_u4 (a int, PRIMARY KEY (nosuchcol))"));
        assertEquals("42703", pkMissing.getSQLState());
        assertTrue(pkMissing.getMessage().contains(
                "column \"nosuchcol\" named in key does not exist"), pkMissing.getMessage());

        SQLException twicePk = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE hdk_u2 (a int, PRIMARY KEY (a, a))"));
        assertEquals("42701", twicePk.getSQLState());
        assertTrue(twicePk.getMessage().contains(
                "column \"a\" appears twice in primary key constraint"), twicePk.getMessage());

        SQLException twiceUnique = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE hdk_u5 (a int, b int, UNIQUE (a, a))"));
        assertEquals("42701", twiceUnique.getSQLState());

        SQLException badMethod = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE hdk_u3 (a int, EXCLUDE USING gin (a WITH =))"));
        assertEquals("0A000", badMethod.getSQLState());
        assertTrue(badMethod.getMessage().contains(
                "access method \"gin\" does not support exclusion constraints"),
                badMethod.getMessage());
    }

    @Test
    void conflictingColumnClausesAreRefused() {
        SQLException twoDefaults = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE hdc_cf1 (a int DEFAULT 1 DEFAULT 2)"));
        assertEquals("42601", twoDefaults.getSQLState());
        assertTrue(twoDefaults.getMessage().contains(
                "multiple default values specified for column \"a\" of table \"hdc_cf1\""),
                twoDefaults.getMessage());

        SQLException nullConflict = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE hdc_cf2 (a int NOT NULL NULL)"));
        assertEquals("42601", nullConflict.getSQLState());
        assertTrue(nullConflict.getMessage().contains(
                "conflicting NULL/NOT NULL declarations for column \"a\" of table \"hdc_cf2\""),
                nullConflict.getMessage());

        SQLException serialDefault = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE hdc_cf3 (i serial DEFAULT 1)"));
        assertEquals("42601", serialDefault.getSQLState());

        SQLException bothGenerations = assertThrows(SQLException.class, () -> exec(
                "CREATE TABLE hdc_cf4 (a int, b int GENERATED ALWAYS AS (a) STORED"
                + " GENERATED ALWAYS AS IDENTITY)"));
        assertEquals("42601", bothGenerations.getSQLState());
        assertTrue(bothGenerations.getMessage().contains(
                "both identity and generation expression specified for column \"b\""),
                bothGenerations.getMessage());

        SQLException byDefaultExpr = assertThrows(SQLException.class, () -> exec(
                "CREATE TABLE hdc_gc3 (a int, b int GENERATED BY DEFAULT AS (a) STORED)"));
        assertEquals("42601", byDefaultExpr.getSQLState());
        assertTrue(byDefaultExpr.getMessage().contains(
                "for a generated column, GENERATED ALWAYS must be specified"),
                byDefaultExpr.getMessage());
    }

    @Test
    void definitionRulesThatWereNotEnforced() throws Exception {
        exec("CREATE TABLE hdr_gc2 (a int, c int GENERATED ALWAYS AS (a*2) STORED)");
        SQLException usingOnGenerated = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE hdr_gc2 ALTER COLUMN c TYPE text USING c::text"));
        assertEquals("42611", usingOnGenerated.getSQLState());

        assertEquals("42P17", assertThrows(SQLException.class,
                () -> exec("CREATE TABLE hdr_p1 (a int, b int) PARTITION BY LIST (a, b)")).getSQLState());

        exec("CREATE TABLE hdr_p2 (a int, b int) PARTITION BY RANGE (a)");
        exec("CREATE TABLE hdr_p2_1 PARTITION OF hdr_p2 FOR VALUES FROM (1) TO (10)");
        assertEquals("42809", assertThrows(SQLException.class,
                () -> exec("ALTER TABLE hdr_p2_1 ADD COLUMN c int")).getSQLState());

        assertEquals("42P16", assertThrows(SQLException.class,
                () -> exec("CREATE TEMP TABLE public.hdr_tt (a int)")).getSQLState());

        exec("CREATE TABLE hdr_misc (a int)");
        assertEquals("42601", assertThrows(SQLException.class,
                () -> exec("ALTER TABLE hdr_misc RENAME COLUMN a TO c, ADD COLUMN d int")).getSQLState());

        exec("CREATE TABLE hdr_ser (a int)");
        SQLException serialType = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE hdr_ser ALTER COLUMN a TYPE serial"));
        assertEquals("42704", serialType.getSQLState());
        assertTrue(serialType.getMessage().contains("type \"serial\" does not exist"),
                serialType.getMessage());
        exec("DROP TABLE hdr_gc2");
        exec("DROP TABLE hdr_p2");
        exec("DROP TABLE hdr_misc");
        exec("DROP TABLE hdr_ser");
    }

    @Test
    void constraintDefinitionAndValidationErrors() throws Exception {
        exec("CREATE TABLE hdn_k1 (a int)");
        exec("INSERT INTO hdn_k1 VALUES (0)");
        SQLException divide = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE hdn_k1 ADD CONSTRAINT hdn_k1_ck CHECK (100 / a > 0)"));
        assertEquals("22012", divide.getSQLState());

        exec("CREATE TABLE hdn_nv (a int)");
        exec("INSERT INTO hdn_nv VALUES (NULL)");
        exec("ALTER TABLE hdn_nv ADD CONSTRAINT hdn_nn1 NOT NULL a NOT VALID");

        assertEquals("42703", assertThrows(SQLException.class,
                () -> exec("CREATE TABLE hdn_ck1 (a int CHECK (nosuchcol > 0))")).getSQLState());
        SQLException systemCol = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE hdn_ck2 (a int CHECK (xmin > 0))"));
        assertEquals("42P10", systemCol.getSQLState());
        assertTrue(systemCol.getMessage().contains(
                "system column \"xmin\" reference in check constraint is invalid"),
                systemCol.getMessage());
        SQLException dupName = assertThrows(SQLException.class, () -> exec(
                "CREATE TABLE hdn_ck4 (a int, CONSTRAINT hdn_c1 CHECK (a>0),"
                + " CONSTRAINT hdn_c1 CHECK (a<9))"));
        assertEquals("42710", dupName.getSQLState());

        exec("CREATE TABLE hdn_rc (id int CONSTRAINT hdn_pk1 PRIMARY KEY)");
        SQLException renameMissing = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE hdn_rc RENAME CONSTRAINT hdn_nosuch TO x"));
        assertEquals("42704", renameMissing.getSQLState());
        assertTrue(renameMissing.getMessage().contains(
                "constraint \"hdn_nosuch\" for table \"hdn_rc\" does not exist"),
                renameMissing.getMessage());
        exec("DROP TABLE hdn_k1");
        exec("DROP TABLE hdn_nv");
        exec("DROP TABLE hdn_rc");
    }

    @Test
    void objectKindAndTypeNameErrors() throws Exception {
        exec("CREATE SEQUENCE hdo_sqt");
        SQLException badType = assertThrows(SQLException.class,
                () -> exec("ALTER SEQUENCE hdo_sqt AS bogustype"));
        assertEquals("42704", badType.getSQLState());
        assertTrue(badType.getMessage().contains("type \"bogustype\" does not exist"),
                badType.getMessage());
        // A real type that no sequence can be built on keeps the sequence's own wording.
        assertEquals("22023", assertThrows(SQLException.class,
                () -> exec("ALTER SEQUENCE hdo_sqt AS text")).getSQLState());

        exec("CREATE TABLE hdo_orv (i int, v text)");
        exec("CREATE VIEW hdo_orvv AS SELECT i, v FROM hdo_orv");
        SQLException retyped = assertThrows(SQLException.class, () -> exec(
                "CREATE OR REPLACE VIEW hdo_orvv AS SELECT i, i AS v FROM hdo_orv"));
        assertEquals("42P16", retyped.getSQLState());
        assertTrue(retyped.getMessage().contains(
                "cannot change data type of view column \"v\" from text to integer"),
                retyped.getMessage());

        exec("CREATE FUNCTION hdo_trgf() RETURNS trigger AS $$ BEGIN RETURN NULL; END $$"
                + " LANGUAGE plpgsql");
        SQLException truncTrigger = assertThrows(SQLException.class, () -> exec(
                "CREATE TRIGGER hdo_trg INSTEAD OF TRUNCATE ON hdo_orvv FOR EACH STATEMENT"
                + " EXECUTE FUNCTION hdo_trgf()"));
        assertEquals("42809", truncTrigger.getSQLState());

        // ALTER VIEW no longer swallows what a view cannot take, nor outright nonsense.
        assertEquals("42809", assertThrows(SQLException.class,
                () -> exec("ALTER VIEW hdo_orvv ADD COLUMN q int")).getSQLState());
        assertEquals("42809", assertThrows(SQLException.class,
                () -> exec("ALTER VIEW hdo_orvv DROP COLUMN v")).getSQLState());
        assertEquals("42601", assertThrows(SQLException.class,
                () -> exec("ALTER VIEW hdo_orvv NO SUCH ACTION")).getSQLState());
        exec("DROP FUNCTION hdo_trgf()");
        exec("DROP VIEW hdo_orvv");
        exec("DROP TABLE hdo_orv");
        exec("DROP SEQUENCE hdo_sqt");
    }

    // ------------------------------------------------------------ An object is undone by identity, and described as it was written

    @Test
    void rollbackOfCreateFunctionLeavesOtherOverloadsAndSchemasAlone() throws Exception {
        exec("CREATE FUNCTION w1i_sf(a int) RETURNS text AS $$ SELECT 'int:' || a $$ LANGUAGE sql");
        assertEquals("int:1", scalar("SELECT w1i_sf(1)"));
        exec("BEGIN");
        exec("CREATE FUNCTION w1i_sf(a text) RETURNS text AS $$ SELECT 'text:' || a $$ LANGUAGE sql");
        exec("ROLLBACK");
        // PG 18: the pre-existing overload is untouched, and only it remains
        assertEquals("int:2", scalar("SELECT w1i_sf(2)"));
        assertEquals("1", scalar("SELECT count(*) FROM pg_proc WHERE proname='w1i_sf'"));

        exec("CREATE SCHEMA w1i_s1");
        exec("CREATE FUNCTION public.w1i_sh() RETURNS int AS $$ SELECT 11 $$ LANGUAGE sql");
        exec("BEGIN");
        exec("CREATE FUNCTION w1i_s1.w1i_sh() RETURNS int AS $$ SELECT 22 $$ LANGUAGE sql");
        exec("ROLLBACK");
        assertEquals("11", scalar("SELECT public.w1i_sh()"));
    }

    @Test
    void rollbackOfCreateOrReplaceFunctionRestoresThePreviousBody() throws Exception {
        exec("CREATE FUNCTION w1i_sg() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql");
        assertEquals("1", scalar("SELECT w1i_sg()"));
        exec("BEGIN");
        exec("CREATE OR REPLACE FUNCTION w1i_sg() RETURNS int AS $$ SELECT 2 $$ LANGUAGE sql");
        assertEquals("2", scalar("SELECT w1i_sg()"));
        exec("ROLLBACK");
        // PG 18: the definition the statement displaced is back
        assertEquals("1", scalar("SELECT w1i_sg()"));
    }

    @Test
    void rollbackOfDropFunctionRestoresIt() throws Exception {
        exec("CREATE FUNCTION w1i_sk() RETURNS int AS $$ SELECT 7 $$ LANGUAGE sql");
        exec("BEGIN");
        exec("DROP FUNCTION w1i_sk()");
        exec("ROLLBACK");
        // PG 18: DDL is transactional, so the drop never happened
        assertEquals("7", scalar("SELECT w1i_sk()"));
        assertEquals("1", scalar("SELECT count(*) FROM pg_proc WHERE proname='w1i_sk'"));
    }

    @Test
    void rollbackOfAlterFunctionRenameRestoresTheOldName() throws Exception {
        exec("CREATE FUNCTION w1i_sl() RETURNS int AS $$ SELECT 8 $$ LANGUAGE sql");
        exec("BEGIN");
        exec("ALTER FUNCTION w1i_sl() RENAME TO w1i_sl2");
        exec("ROLLBACK");
        // PG 18: the routine answers to the name it had
        assertEquals("w1i_sl",
                scalar("SELECT string_agg(proname, ',' ORDER BY proname) FROM pg_proc"
                        + " WHERE proname LIKE 'w1i_sl%'"));
        assertEquals("8", scalar("SELECT w1i_sl()"));
    }

    @Test
    void onCommitDeleteRowsDoesNotFollowTheNameToALaterTable() throws Exception {
        exec("CREATE TEMP TABLE w1i_tt (x int) ON COMMIT DELETE ROWS");
        exec("BEGIN");
        exec("COMMIT");
        exec("DROP TABLE w1i_tt");
        exec("CREATE TEMP TABLE w1i_tt (x int)");
        exec("INSERT INTO w1i_tt VALUES (1),(2)");
        exec("BEGIN");
        exec("COMMIT");
        // PG 18: the second table carries no ON COMMIT clause, so its rows stay
        assertEquals("2", scalar("SELECT count(*) FROM w1i_tt"));
    }

    @Test
    void rolledBackOnCommitDeleteRowsRegistrationDoesNotTruncateALaterTable() throws Exception {
        exec("BEGIN");
        exec("CREATE TEMP TABLE w1i_tw (x int) ON COMMIT DELETE ROWS");
        exec("ROLLBACK");
        exec("CREATE TEMP TABLE w1i_tw (x int)");
        exec("INSERT INTO w1i_tw VALUES (1),(2),(3)");
        exec("BEGIN");
        exec("COMMIT");
        // PG 18: the registration went with the transaction that made it
        assertEquals("3", scalar("SELECT count(*) FROM w1i_tw"));
    }

    @Test
    void storageParametersAreStoredMergedAndReset() throws Exception {
        exec("CREATE TABLE w1i_t1 (a int)");
        exec("ALTER TABLE w1i_t1 SET (fillfactor = 70)");
        assertEquals("{fillfactor=70}",
                scalar("SELECT reloptions::text FROM pg_class WHERE relname='w1i_t1'"));

        exec("CREATE TABLE w1i_r1 (a int) WITH (autovacuum_enabled = FALSE,"
                + " vacuum_index_cleanup = OFF, fillfactor = 55)");
        // PG 18 stores a boolean or enumerated value in the case it reads back in
        assertEquals("{autovacuum_enabled=false,vacuum_index_cleanup=off,fillfactor=55}",
                scalar("SELECT reloptions::text FROM pg_class WHERE relname='w1i_r1'"));
        exec("ALTER TABLE w1i_r1 SET (fillfactor = 90)");
        assertEquals("{autovacuum_enabled=false,vacuum_index_cleanup=off,fillfactor=90}",
                scalar("SELECT reloptions::text FROM pg_class WHERE relname='w1i_r1'"));
        // Re-setting an option moves it to the end of the array
        exec("ALTER TABLE w1i_r1 SET (autovacuum_enabled = On)");
        assertEquals("{vacuum_index_cleanup=off,fillfactor=90,autovacuum_enabled=on}",
                scalar("SELECT reloptions::text FROM pg_class WHERE relname='w1i_r1'"));
        exec("ALTER TABLE w1i_r1 RESET (fillfactor)");
        assertEquals("{vacuum_index_cleanup=off,autovacuum_enabled=on}",
                scalar("SELECT reloptions::text FROM pg_class WHERE relname='w1i_r1'"));
        // The last option going leaves NULL, not an empty array; and RESET of an option that was
        // never set is accepted
        exec("ALTER TABLE w1i_r1 RESET (autovacuum_enabled, vacuum_index_cleanup)");
        assertNull(scalar("SELECT reloptions::text FROM pg_class WHERE relname='w1i_r1'"));
        exec("ALTER TABLE w1i_r1 RESET (fillfactor)");
        assertNull(scalar("SELECT reloptions::text FROM pg_class WHERE relname='w1i_r1'"));
    }

    @Test
    void aNamedNotNullConstraintKeepsItsName() throws Exception {
        exec("CREATE TABLE w1i_t3 (a int, d text, CONSTRAINT w1i_nn NOT NULL d,"
                + " e int CONSTRAINT w1i_nn2 NOT NULL)");
        assertEquals("w1i_nn,w1i_nn2",
                scalar("SELECT string_agg(conname, ',' ORDER BY conname) FROM pg_constraint"
                        + " WHERE conrelid='w1i_t3'::regclass AND contype='n'"));
        assertEquals("w1i_nn,w1i_nn2",
                scalar("SELECT string_agg(constraint_name, ',' ORDER BY constraint_name)"
                        + " FROM information_schema.table_constraints WHERE table_name='w1i_t3'"));
        // The name the writer chose is the one DROP CONSTRAINT answers to
        exec("ALTER TABLE w1i_t3 DROP CONSTRAINT w1i_nn");
        assertEquals("1", scalar("SELECT count(*) FROM pg_constraint"
                + " WHERE conrelid='w1i_t3'::regclass AND contype='n'"));
    }

    @Test
    void generationExpressionIsDeparsedNotEchoed() throws Exception {
        exec("CREATE TABLE w1i_ge (a int, b numeric,"
                + " c int GENERATED ALWAYS AS (a) STORED,"
                + " d text GENERATED ALWAYS AS (upper(a::text)) STORED,"
                + " e numeric GENERATED ALWAYS AS (b / 2) STORED,"
                + " f int GENERATED ALWAYS AS (a*2) STORED)");
        // PG 18 prints the analysed tree: it brackets what needs it and leaves a bare column bare
        assertEquals("a", scalar("SELECT generation_expression FROM information_schema.columns"
                + " WHERE table_name='w1i_ge' AND column_name='c'"));
        assertEquals("upper((a)::text)",
                scalar("SELECT generation_expression FROM information_schema.columns"
                        + " WHERE table_name='w1i_ge' AND column_name='d'"));
        assertEquals("(b / (2)::numeric)",
                scalar("SELECT generation_expression FROM information_schema.columns"
                        + " WHERE table_name='w1i_ge' AND column_name='e'"));
        assertEquals("(a * 2)",
                scalar("SELECT generation_expression FROM information_schema.columns"
                        + " WHERE table_name='w1i_ge' AND column_name='f'"));
        // pg_attrdef must agree with information_schema, since pg_dump reads it
        assertEquals("(a * 2)", scalar("SELECT d.adbin FROM pg_attrdef d"
                + " JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum"
                + " WHERE d.adrelid='w1i_ge'::regclass AND a.attname='f'"));
    }

    @Test
    void constraintDefPrintsEveryClauseTheConstraintCarries() throws Exception {
        exec("CREATE TABLE w1i_p (i int PRIMARY KEY)");
        exec("CREATE TABLE w1i_q (a int, b int, c int,"
                + " CONSTRAINT w1i_q_fk FOREIGN KEY (a) REFERENCES w1i_p(i) MATCH FULL"
                + "   ON DELETE CASCADE ON UPDATE SET NULL DEFERRABLE INITIALLY DEFERRED,"
                + " CONSTRAINT w1i_q_ni CHECK (b > 0) NO INHERIT,"
                + " CONSTRAINT w1i_q_ck2 CHECK (c > 0) NOT ENFORCED,"
                + " CONSTRAINT w1i_q_u UNIQUE (b) DEFERRABLE)");
        exec("ALTER TABLE w1i_q ADD CONSTRAINT w1i_q_nv CHECK (a > 0) NOT VALID");
        assertEquals("FOREIGN KEY (a) REFERENCES w1i_p(i) MATCH FULL ON UPDATE SET NULL"
                        + " ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conname='w1i_q_fk'"));
        assertEquals("CHECK ((b > 0)) NO INHERIT",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conname='w1i_q_ni'"));
        assertEquals("CHECK ((a > 0)) NOT VALID",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conname='w1i_q_nv'"));
        assertEquals("UNIQUE (b) DEFERRABLE",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conname='w1i_q_u'"));
        // A constraint nobody enforces cannot have been validated, and PG says so once
        assertEquals("CHECK ((c > 0)) NOT ENFORCED",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conname='w1i_q_ck2'"));
        assertEquals("false", scalar("SELECT convalidated::text FROM pg_constraint"
                + " WHERE conname='w1i_q_ck2'"));
    }

    @Test
    void connoinheritAndConexclopMatchPostgres() throws Exception {
        exec("CREATE TABLE w1i_cp (i int PRIMARY KEY)");
        exec("CREATE TABLE w1i_c1 (id int PRIMARY KEY, u int UNIQUE, f int REFERENCES w1i_cp(i),"
                + " CONSTRAINT w1i_c1_ck CHECK (id > 0), CONSTRAINT w1i_c1_ni CHECK (u > 0) NO INHERIT)");
        // PG 18 marks every index-backed constraint non-inheritable by construction
        assertEquals("w1i_c1_ck=f,w1i_c1_f_fkey=t,w1i_c1_id_not_null=f,w1i_c1_ni=t,"
                        + "w1i_c1_pkey=t,w1i_c1_u_key=t",
                scalar("SELECT string_agg(conname || '=' || CASE WHEN connoinherit THEN 't' ELSE 'f' END,"
                        + " ',' ORDER BY conname) FROM pg_constraint WHERE conrelid='w1i_c1'::regclass"));
        exec("CREATE TABLE w1i_c2 (a int, EXCLUDE USING btree (a WITH =))");
        assertEquals("true", scalar("SELECT connoinherit::text FROM pg_constraint"
                + " WHERE conrelid='w1i_c2'::regclass AND contype='x'"));
        assertEquals("false", scalar("SELECT (conexclop IS NULL)::text FROM pg_constraint"
                + " WHERE conrelid='w1i_c2'::regclass AND contype='x'"));
        exec("CREATE DOMAIN w1i_dd AS varchar(10) NOT NULL");
        assertEquals("false", scalar("SELECT connoinherit::text FROM pg_constraint"
                + " WHERE conname='w1i_dd_not_null'"));
    }

    @Test
    void aNotNullDomainConstrainsTheDomainNotEveryColumnUsingIt() throws Exception {
        exec("CREATE DOMAIN w1i_de AS varchar(10) NOT NULL");
        exec("CREATE TABLE w1i_dt (code w1i_de, other w1i_de NOT NULL)");
        // PG 18: attnotnull stays false for a column merely declared with the domain
        assertEquals("code=f,other=t",
                scalar("SELECT string_agg(attname || '=' || CASE WHEN attnotnull THEN 't' ELSE 'f' END,"
                        + " ',' ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid='w1i_dt'::regclass AND attnum>0"));
        assertEquals("w1i_de_not_null,w1i_dt_other_not_null",
                scalar("SELECT string_agg(conname, ',' ORDER BY conname) FROM pg_constraint"
                        + " WHERE conname LIKE 'w1i_d%'"));
        // ...but is_nullable is attnotnull OR the domain's own typnotnull, so both say NO
        assertEquals("code=NO,other=NO",
                scalar("SELECT string_agg(column_name || '=' || is_nullable, ',' ORDER BY ordinal_position)"
                        + " FROM information_schema.columns WHERE table_name='w1i_dt'"));
        assertEquals("w1i_de_not_null=VALUE IS NOT NULL,w1i_dt_other_not_null=other IS NOT NULL",
                scalar("SELECT string_agg(constraint_name || '=' || check_clause, ',' ORDER BY constraint_name)"
                        + " FROM information_schema.check_constraints WHERE constraint_name LIKE 'w1i_d%'"));
        assertEquals("w1i_de_not_null",
                scalar("SELECT constraint_name FROM information_schema.domain_constraints"
                        + " WHERE domain_name='w1i_de'"));
        // The value is still rejected, by the domain's own rule
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO w1i_dt VALUES (NULL, 'x')"));
        assertEquals("23502", ex.getSQLState());
    }

    @Test
    void constraintTriggerIsRecordedAndDeparsedAsOne() throws Exception {
        exec("CREATE TABLE w1i_t5 (a int)");
        exec("CREATE TABLE w1i_tr2 (a int)");
        exec("CREATE FUNCTION w1i_f5() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
        exec("CREATE CONSTRAINT TRIGGER w1i_tg5c AFTER INSERT ON w1i_t5"
                + " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION w1i_f5()");
        // NOT DEFERRABLE and the FROM clause are both part of PG's grammar for the form
        exec("CREATE CONSTRAINT TRIGGER w1i_tg5n AFTER UPDATE ON w1i_t5"
                + " NOT DEFERRABLE FOR EACH ROW EXECUTE FUNCTION w1i_f5()");
        exec("CREATE CONSTRAINT TRIGGER w1i_tg5f AFTER DELETE ON w1i_t5 FROM w1i_tr2"
                + " DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION w1i_f5('x')");
        assertEquals("CREATE CONSTRAINT TRIGGER w1i_tg5c AFTER INSERT ON public.w1i_t5"
                        + " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION w1i_f5()",
                scalar("SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname='w1i_tg5c'"));
        assertEquals("CREATE CONSTRAINT TRIGGER w1i_tg5n AFTER UPDATE ON public.w1i_t5"
                        + " NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION w1i_f5()",
                scalar("SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname='w1i_tg5n'"));
        assertEquals("true", scalar("SELECT (tgconstraint <> 0)::text FROM pg_trigger"
                + " WHERE tgname='w1i_tg5c'"));
        assertEquals("true", scalar("SELECT (tgconstrrelid <> 0)::text FROM pg_trigger"
                + " WHERE tgname='w1i_tg5f'"));
        assertEquals("t", scalar("SELECT contype FROM pg_constraint WHERE conname='w1i_tg5c'"));
        // A plain trigger has no deferrability, and PG refuses the word outright
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("CREATE TRIGGER w1i_plain AFTER INSERT ON w1i_t5 DEFERRABLE"
                        + " FOR EACH ROW EXECUTE FUNCTION w1i_f5()"));
        assertEquals("42601", ex.getSQLState());
    }

    @Test
    void likeOverAViewCopiesTheViewsOwnColumns() throws Exception {
        exec("CREATE TABLE w1i_t7 (a int, b int, c int)");
        exec("CREATE VIEW w1i_v7 AS SELECT b FROM w1i_t7");
        exec("CREATE TABLE w1i_l7 (LIKE w1i_v7 INCLUDING ALL)");
        // PG 18: the view projects one column, so the copy has one column
        assertEquals("b", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name='w1i_l7'"));
    }

    // ------------------------------------------------------------ A rule is one of several, and a policy decides what a write may see

@Test
void everyRuleOnAnEventFiresInRuleNameOrder() throws Exception {
    exec("CREATE TABLE zzw2d_r (i int)");
    exec("CREATE TABLE zzw2d_rl (seq serial, m text)");
    exec("CREATE RULE zzw2d_rz AS ON INSERT TO zzw2d_r DO ALSO INSERT INTO zzw2d_rl (m) VALUES ('z')");
    exec("CREATE RULE zzw2d_rm AS ON INSERT TO zzw2d_r DO ALSO INSERT INTO zzw2d_rl (m) VALUES ('m')");
    exec("CREATE RULE zzw2d_ra AS ON INSERT TO zzw2d_r DO ALSO INSERT INTO zzw2d_rl (m) VALUES ('a')");
    exec("INSERT INTO zzw2d_r VALUES (1)");
    assertEquals("a,m,z",
            String.valueOf(scalar("SELECT string_agg(m, ',' ORDER BY seq) FROM zzw2d_rl")));
    exec("DROP TABLE zzw2d_r");
    exec("DROP TABLE zzw2d_rl");
}

@Test
void droppingOneRuleLeavesTheOthersFiring() throws Exception {
    exec("CREATE TABLE zzw2d_t (i int)");
    exec("CREATE TABLE zzw2d_l (seq serial, m text)");
    exec("CREATE RULE zzw2d_ra AS ON INSERT TO zzw2d_t DO ALSO INSERT INTO zzw2d_l (m) VALUES ('a')");
    exec("CREATE RULE zzw2d_rb AS ON INSERT TO zzw2d_t DO ALSO INSERT INTO zzw2d_l (m) VALUES ('b')");
    exec("CREATE RULE zzw2d_rc AS ON INSERT TO zzw2d_t DO ALSO INSERT INTO zzw2d_l (m) VALUES ('c')");
    exec("DROP RULE zzw2d_rc ON zzw2d_t");
    exec("INSERT INTO zzw2d_t VALUES (1)");
    assertEquals("a,b",
            String.valueOf(scalar("SELECT string_agg(m, ',' ORDER BY m) FROM zzw2d_l")));
    exec("DROP TABLE zzw2d_t");
    exec("DROP TABLE zzw2d_l");
}

@Test
void disablingOneRuleLeavesTheOthersFiring() throws Exception {
    exec("CREATE TABLE zzw2d_dt (i int)");
    exec("CREATE TABLE zzw2d_dl (m text)");
    exec("CREATE RULE zzw2d_d1 AS ON INSERT TO zzw2d_dt DO ALSO INSERT INTO zzw2d_dl VALUES ('a')");
    exec("CREATE RULE zzw2d_d2 AS ON INSERT TO zzw2d_dt DO ALSO INSERT INTO zzw2d_dl VALUES ('b')");
    exec("ALTER TABLE zzw2d_dt DISABLE RULE zzw2d_d2");
    exec("INSERT INTO zzw2d_dt VALUES (1)");
    assertEquals("a", String.valueOf(scalar("SELECT string_agg(m, ',' ORDER BY m) FROM zzw2d_dl")));
    exec("ALTER TABLE zzw2d_dt ENABLE RULE zzw2d_d2");
    exec("INSERT INTO zzw2d_dt VALUES (2)");
    assertEquals("a,a,b", String.valueOf(scalar("SELECT string_agg(m, ',' ORDER BY m) FROM zzw2d_dl")));
    exec("DROP TABLE zzw2d_dt");
    exec("DROP TABLE zzw2d_dl");
}

@Test
void conditionalInsteadInsertOnlyDivertsTheRowsItMatches() throws Exception {
    exec("CREATE TABLE zzw2d_ct (i int, v text)");
    exec("CREATE TABLE zzw2d_cl (i int, v text)");
    exec("CREATE RULE zzw2d_cr AS ON INSERT TO zzw2d_ct WHERE NEW.i > 10"
            + " DO INSTEAD INSERT INTO zzw2d_cl VALUES (NEW.i, NEW.v)");
    exec("INSERT INTO zzw2d_ct VALUES (5,'small')");
    exec("INSERT INTO zzw2d_ct VALUES (50,'big')");
    assertEquals("5|small",
            String.valueOf(scalar("SELECT string_agg(i || '|' || v, ',' ORDER BY i) FROM zzw2d_ct")));
    assertEquals("50|big",
            String.valueOf(scalar("SELECT string_agg(i || '|' || v, ',' ORDER BY i) FROM zzw2d_cl")));
    exec("DROP TABLE zzw2d_ct");
    exec("DROP TABLE zzw2d_cl");
}

@Test
void conditionalInsteadNothingKeepsTheRowsItDoesNotMatch() throws Exception {
    exec("CREATE TABLE zzw2d_nt (i int)");
    exec("CREATE RULE zzw2d_nr AS ON INSERT TO zzw2d_nt WHERE NEW.i < 0 DO INSTEAD NOTHING");
    exec("INSERT INTO zzw2d_nt VALUES (5)");
    exec("INSERT INTO zzw2d_nt VALUES (-5)");
    assertEquals("5", String.valueOf(scalar("SELECT string_agg(i::text, ',' ORDER BY i) FROM zzw2d_nt")));
    exec("DROP TABLE zzw2d_nt");
}

@Test
void conditionalInsteadNothingOnDeleteKeepsOnlyTheMatchedRow() throws Exception {
    exec("CREATE TABLE zzw2d_dd (i int)");
    exec("INSERT INTO zzw2d_dd VALUES (1),(2)");
    exec("CREATE RULE zzw2d_ddr AS ON DELETE TO zzw2d_dd WHERE OLD.i = 1 DO INSTEAD NOTHING");
    exec("DELETE FROM zzw2d_dd");
    assertEquals("1", String.valueOf(scalar("SELECT string_agg(i::text, ',' ORDER BY i) FROM zzw2d_dd")));
    exec("DROP TABLE zzw2d_dd");
}

@Test
void conditionalInsteadUpdateLetsTheStatementRunForTheRest() throws Exception {
    exec("CREATE TABLE zzw2d_ut (i int primary key, v int)");
    exec("INSERT INTO zzw2d_ut VALUES (1,10),(2,20)");
    exec("CREATE TABLE zzw2d_ul (i int)");
    exec("CREATE RULE zzw2d_ur AS ON UPDATE TO zzw2d_ut WHERE OLD.i = 1"
            + " DO INSTEAD INSERT INTO zzw2d_ul VALUES (OLD.i)");
    exec("UPDATE zzw2d_ut SET v = 99");
    assertEquals("1|10,2|99",
            String.valueOf(scalar("SELECT string_agg(i || '|' || v, ',' ORDER BY i) FROM zzw2d_ut")));
    assertEquals("1", String.valueOf(scalar("SELECT string_agg(i::text, ',' ORDER BY i) FROM zzw2d_ul")));
    exec("DROP TABLE zzw2d_ut");
    exec("DROP TABLE zzw2d_ul");
}

@Test
void conditionalAlsoInsertLogsOnlyTheRowsItMatches() throws Exception {
    exec("CREATE TABLE zzw2d_q (i int)");
    exec("CREATE TABLE zzw2d_ql (i int)");
    exec("CREATE RULE zzw2d_qr AS ON INSERT TO zzw2d_q WHERE NEW.i > 10"
            + " DO ALSO INSERT INTO zzw2d_ql VALUES (NEW.i)");
    exec("INSERT INTO zzw2d_q VALUES (5)");
    exec("INSERT INTO zzw2d_q VALUES (50)");
    assertEquals("50", String.valueOf(scalar("SELECT string_agg(i::text, ',' ORDER BY i) FROM zzw2d_ql")));
    assertEquals("2", String.valueOf(scalar("SELECT count(*) FROM zzw2d_q")));
    exec("DROP TABLE zzw2d_q");
    exec("DROP TABLE zzw2d_ql");
}

@Test
void unqualifiedDeleteFiresRowTriggers() throws Exception {
    exec("CREATE TABLE zzw2d_lg (seq serial, t text)");
    exec("CREATE FUNCTION zzw2d_lf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
            + " INSERT INTO zzw2d_lg (t) VALUES (TG_WHEN||'/'||TG_OP||'/'||OLD.id); RETURN OLD; END $$");
    exec("CREATE TABLE zzw2d_t (id int PRIMARY KEY)");
    exec("INSERT INTO zzw2d_t VALUES (1),(2)");
    exec("CREATE TRIGGER zzw2d_tb BEFORE DELETE ON zzw2d_t FOR EACH ROW EXECUTE FUNCTION zzw2d_lf()");
    exec("CREATE TRIGGER zzw2d_ta AFTER DELETE ON zzw2d_t FOR EACH ROW EXECUTE FUNCTION zzw2d_lf()");
    exec("DELETE FROM zzw2d_t");
    assertEquals("2", String.valueOf(scalar(
            "SELECT count(*) FROM zzw2d_lg WHERE t LIKE 'BEFORE/DELETE/%'")));
    assertEquals("2", String.valueOf(scalar(
            "SELECT count(*) FROM zzw2d_lg WHERE t LIKE 'AFTER/DELETE/%'")));
    assertEquals("0", String.valueOf(scalar("SELECT count(*) FROM zzw2d_t")));
    exec("DROP TABLE zzw2d_t");
    exec("DROP TABLE zzw2d_lg");
    exec("DROP FUNCTION zzw2d_lf()");
}

@Test
void unqualifiedDeleteHonoursABeforeTriggerVeto() throws Exception {
    exec("CREATE TABLE zzw2d_vt (id int PRIMARY KEY)");
    exec("INSERT INTO zzw2d_vt VALUES (1),(2)");
    exec("CREATE FUNCTION zzw2d_veto() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$");
    exec("CREATE TRIGGER zzw2d_vb BEFORE DELETE ON zzw2d_vt FOR EACH ROW EXECUTE FUNCTION zzw2d_veto()");
    exec("DELETE FROM zzw2d_vt");
    assertEquals("1,2", String.valueOf(scalar("SELECT string_agg(id::text, ',' ORDER BY id) FROM zzw2d_vt")));
    exec("DROP TABLE zzw2d_vt");
    exec("DROP FUNCTION zzw2d_veto()");
}

@Test
void unqualifiedDeleteBuildsTheTransitionTable() throws Exception {
    exec("CREATE TABLE zzw2d_lg2 (seq serial, t text)");
    exec("CREATE TABLE zzw2d_st (id int)");
    exec("INSERT INTO zzw2d_st VALUES (1),(2),(3)");
    exec("CREATE FUNCTION zzw2d_sf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
            + " INSERT INTO zzw2d_lg2 (t) VALUES (TG_WHEN||'/'||TG_OP||'/STMT/'"
            + "||(SELECT count(*) FROM zzw2d_ot)); RETURN NULL; END $$");
    exec("CREATE TRIGGER zzw2d_sa AFTER DELETE ON zzw2d_st REFERENCING OLD TABLE AS zzw2d_ot"
            + " FOR EACH STATEMENT EXECUTE FUNCTION zzw2d_sf()");
    exec("DELETE FROM zzw2d_st");
    assertEquals("AFTER/DELETE/STMT/3",
            String.valueOf(scalar("SELECT string_agg(t, ',' ORDER BY seq) FROM zzw2d_lg2")));
    exec("DROP TABLE zzw2d_st");
    exec("DROP TABLE zzw2d_lg2");
    exec("DROP FUNCTION zzw2d_sf()");
}

@Test
void returnNewFromABeforeDeleteTriggerSkipsTheRow() throws Exception {
    exec("CREATE TABLE zzw2d_v (id int primary key, n int)");
    exec("INSERT INTO zzw2d_v VALUES (1,10),(2,20)");
    exec("CREATE FUNCTION zzw2d_retnew() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$");
    exec("CREATE TRIGGER zzw2d_bd BEFORE DELETE ON zzw2d_v FOR EACH ROW EXECUTE FUNCTION zzw2d_retnew()");
    exec("DELETE FROM zzw2d_v WHERE id = 1");
    assertEquals("1,2", String.valueOf(scalar("SELECT string_agg(id::text, ',' ORDER BY id) FROM zzw2d_v")));
    exec("DROP TABLE zzw2d_v");
    exec("DROP FUNCTION zzw2d_retnew()");
}

@Test
void insteadOfDeleteTriggerWriteIsNotUndone() throws Exception {
    exec("CREATE TABLE zzw2d_b (id int, note text)");
    exec("INSERT INTO zzw2d_b VALUES (1,'a')");
    exec("CREATE VIEW zzw2d_bv AS SELECT id, note FROM zzw2d_b");
    exec("CREATE FUNCTION zzw2d_softdel() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
            + " UPDATE zzw2d_b SET note = 'deleted-by-trigger' WHERE id = OLD.id; RETURN OLD; END $$");
    exec("CREATE TRIGGER zzw2d_iod INSTEAD OF DELETE ON zzw2d_bv FOR EACH ROW"
            + " EXECUTE FUNCTION zzw2d_softdel()");
    exec("DELETE FROM zzw2d_bv WHERE id = 1");
    assertEquals("1|deleted-by-trigger",
            String.valueOf(scalar("SELECT string_agg(id || '|' || note, ',' ORDER BY id) FROM zzw2d_b")));
    exec("DROP VIEW zzw2d_bv");
    exec("DROP TABLE zzw2d_b");
    exec("DROP FUNCTION zzw2d_softdel()");
}

@Test
void returnNewFromAnInsteadOfDeleteTriggerSkipsTheRow() throws Exception {
    exec("CREATE TABLE zzw2d_ib (id int, note text)");
    exec("INSERT INTO zzw2d_ib VALUES (1,'a'),(2,'b')");
    exec("CREATE VIEW zzw2d_ivw AS SELECT id, note FROM zzw2d_ib");
    exec("CREATE FUNCTION zzw2d_iretnew() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$");
    exec("CREATE TRIGGER zzw2d_iod2 INSTEAD OF DELETE ON zzw2d_ivw FOR EACH ROW"
            + " EXECUTE FUNCTION zzw2d_iretnew()");
    assertEquals(0, update("DELETE FROM zzw2d_ivw WHERE id = 1"));
    exec("DROP VIEW zzw2d_ivw");
    exec("DROP TABLE zzw2d_ib");
    exec("DROP FUNCTION zzw2d_iretnew()");
}

    // ------------------------------------------------------------ A referential action is a write on the child, under the child's rules

    @Test
    void onDeleteSetNullIsRefusedByTheChildsNotNull() throws Exception {
        exec("CREATE TABLE zzw2f_p1 (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_c1 (p int NOT NULL REFERENCES zzw2f_p1(id) ON DELETE SET NULL)");
        exec("INSERT INTO zzw2f_p1 VALUES (1)");
        exec("INSERT INTO zzw2f_c1 VALUES (1)");
        assertEquals("23502", stateOf("DELETE FROM zzw2f_p1 WHERE id = 1"));
        assertEquals("Failing row contains (null).", detailOf("DELETE FROM zzw2f_p1 WHERE id = 1"));
        assertEquals("1", scalar("SELECT count(*) FROM zzw2f_p1"));
        assertEquals("1", scalar("SELECT p FROM zzw2f_c1"));
    }

    @Test
    void onUpdateSetNullIsRefusedAndTheParentKeyStays() throws Exception {
        exec("CREATE TABLE zzw2f_p2 (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_c2 (p int NOT NULL REFERENCES zzw2f_p2(id) ON UPDATE SET NULL)");
        exec("INSERT INTO zzw2f_p2 VALUES (1)");
        exec("INSERT INTO zzw2f_c2 VALUES (1)");
        assertEquals("23502", stateOf("UPDATE zzw2f_p2 SET id = 2"));
        assertEquals("1", scalar("SELECT id FROM zzw2f_p2"));
        assertEquals("1", scalar("SELECT p FROM zzw2f_c2"));
    }

    @Test
    void onDeleteSetDefaultObeysTheChildsOwnRules() throws Exception {
        exec("CREATE TABLE zzw2f_p3 (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_c3 (p int REFERENCES zzw2f_p3(id) ON DELETE SET DEFAULT DEFAULT 99, CHECK (p < 50))");
        exec("INSERT INTO zzw2f_p3 VALUES (1),(99)");
        exec("INSERT INTO zzw2f_c3 VALUES (1)");
        assertEquals("23514", stateOf("DELETE FROM zzw2f_p3 WHERE id = 1"));
        assertEquals("Failing row contains (99).", detailOf("DELETE FROM zzw2f_p3 WHERE id = 1"));
        assertEquals("1", scalar("SELECT p FROM zzw2f_c3"));
        assertEquals("2", scalar("SELECT count(*) FROM zzw2f_p3"));

        exec("CREATE TABLE zzw2f_p4 (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_c4 (p int NOT NULL REFERENCES zzw2f_p4(id) ON DELETE SET DEFAULT)");
        exec("INSERT INTO zzw2f_p4 VALUES (1)");
        exec("INSERT INTO zzw2f_c4 VALUES (1)");
        assertEquals("23502", stateOf("DELETE FROM zzw2f_p4 WHERE id = 1"));
        assertEquals("1", scalar("SELECT p FROM zzw2f_c4"));
    }

    @Test
    void onDeleteSetNullIsRefusedByANotNullDomain() throws Exception {
        exec("CREATE DOMAIN zzw2f_dom AS int NOT NULL");
        exec("CREATE TABLE zzw2f_p5 (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_c5 (p zzw2f_dom REFERENCES zzw2f_p5(id) ON DELETE SET NULL)");
        exec("INSERT INTO zzw2f_p5 VALUES (1)");
        exec("INSERT INTO zzw2f_c5 VALUES (1)");
        assertEquals("23502", stateOf("DELETE FROM zzw2f_p5 WHERE id = 1"));
        assertEquals("1", scalar("SELECT p FROM zzw2f_c5"));
    }

    @Test
    void onUpdateActionsObeyTheChildsCheckConstraint() throws Exception {
        exec("CREATE TABLE zzw2f_p6 (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_c6 (p int REFERENCES zzw2f_p6(id) ON UPDATE SET DEFAULT DEFAULT 99, CHECK (p < 50))");
        exec("INSERT INTO zzw2f_p6 VALUES (1),(99)");
        exec("INSERT INTO zzw2f_c6 VALUES (1)");
        assertEquals("23514", stateOf("UPDATE zzw2f_p6 SET id = 5 WHERE id = 1"));
        assertEquals("1", scalar("SELECT p FROM zzw2f_c6"));

        exec("CREATE TABLE zzw2f_p7 (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_c7 (p int REFERENCES zzw2f_p7(id) ON UPDATE CASCADE, CHECK (p < 50))");
        exec("INSERT INTO zzw2f_p7 VALUES (1)");
        exec("INSERT INTO zzw2f_c7 VALUES (1)");
        assertEquals("23514", stateOf("UPDATE zzw2f_p7 SET id = 100"));
        assertEquals("Failing row contains (100).", detailOf("UPDATE zzw2f_p7 SET id = 100"));
        assertEquals("1", scalar("SELECT p FROM zzw2f_c7"));
        assertEquals("1", scalar("SELECT id FROM zzw2f_p7"));
    }

    @Test
    void setDefaultCannotDriveTwoChildrenOntoOneUniqueKey() throws Exception {
        exec("CREATE TABLE zzw2f_q1 (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_q2 (p int UNIQUE REFERENCES zzw2f_q1(id) ON DELETE SET DEFAULT DEFAULT 99)");
        exec("INSERT INTO zzw2f_q1 VALUES (1),(2),(99)");
        exec("INSERT INTO zzw2f_q2 VALUES (1),(2)");
        assertEquals("23505", stateOf("DELETE FROM zzw2f_q1 WHERE id IN (1,2)"));
        assertEquals("Key (p)=(99) already exists.", detailOf("DELETE FROM zzw2f_q1 WHERE id IN (1,2)"));
        // The statement is atomic: the first child must not be left holding the default.
        assertEquals("1", scalar("SELECT min(p) FROM zzw2f_q2"));
        assertEquals("2", scalar("SELECT max(p) FROM zzw2f_q2"));
        assertEquals("3", scalar("SELECT count(*) FROM zzw2f_q1"));
    }

    @Test
    void onUpdateCascadeRecomputesTheChildsStoredGeneratedColumn() throws Exception {
        exec("CREATE TABLE zzw2f_p8 (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_c8 (p int REFERENCES zzw2f_p8(id) ON UPDATE CASCADE, g int GENERATED ALWAYS AS (p*10) STORED)");
        exec("INSERT INTO zzw2f_p8 VALUES (1)");
        exec("INSERT INTO zzw2f_c8 (p) VALUES (1)");
        exec("UPDATE zzw2f_p8 SET id = 3");
        assertEquals("3", scalar("SELECT p FROM zzw2f_c8"));
        assertEquals("30", scalar("SELECT g FROM zzw2f_c8"));
    }

    @Test
    void selfReferentialCascadeFiresARowTriggerForEveryCascadedRow() throws Exception {
        exec("CREATE TABLE zzw2f_lg (seq serial, t text)");
        exec("CREATE FUNCTION zzw2f_lf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "INSERT INTO zzw2f_lg (t) VALUES (TG_WHEN||'/'||TG_OP); "
                + "IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$");
        exec("CREATE TABLE zzw2f_t (id int PRIMARY KEY, pid int REFERENCES zzw2f_t(id) ON DELETE CASCADE)");
        exec("INSERT INTO zzw2f_t VALUES (1,NULL),(2,1),(3,2)");
        exec("CREATE TRIGGER zzw2f_tt AFTER DELETE ON zzw2f_t FOR EACH ROW EXECUTE FUNCTION zzw2f_lf()");
        exec("DELETE FROM zzw2f_t WHERE id = 1");
        assertEquals("3", scalar("SELECT count(*) FROM zzw2f_lg"));
        assertEquals("0", scalar("SELECT count(*) FROM zzw2f_t"));
    }

    @Test
    void crossTableCascadeFiresBothDeleteTimingsOnTheChild() throws Exception {
        exec("CREATE TABLE zzw2f_lg2 (seq serial, t text)");
        exec("CREATE FUNCTION zzw2f_lf2() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "INSERT INTO zzw2f_lg2 (t) VALUES (TG_WHEN||'/'||TG_OP); "
                + "IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$");
        exec("CREATE TABLE zzw2f_pa (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_ch (id int PRIMARY KEY, pid int REFERENCES zzw2f_pa(id) ON DELETE CASCADE)");
        exec("INSERT INTO zzw2f_pa VALUES (1)");
        exec("INSERT INTO zzw2f_ch VALUES (10,1)");
        exec("CREATE TRIGGER zzw2f_tt2 BEFORE DELETE ON zzw2f_ch FOR EACH ROW EXECUTE FUNCTION zzw2f_lf2()");
        exec("CREATE TRIGGER zzw2f_tt2a AFTER DELETE ON zzw2f_ch FOR EACH ROW EXECUTE FUNCTION zzw2f_lf2()");
        exec("DELETE FROM zzw2f_pa WHERE id = 1");
        assertEquals("BEFORE/DELETE", scalar("SELECT t FROM zzw2f_lg2 ORDER BY seq LIMIT 1"));
        assertEquals("AFTER/DELETE", scalar("SELECT t FROM zzw2f_lg2 ORDER BY seq DESC LIMIT 1"));
        assertEquals("2", scalar("SELECT count(*) FROM zzw2f_lg2"));
    }

    @Test
    void onUpdateCascadeFiresTheChildsAfterUpdateRowTrigger() throws Exception {
        exec("CREATE TABLE zzw2f_lg3 (seq serial, t text)");
        exec("CREATE FUNCTION zzw2f_lf3() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "INSERT INTO zzw2f_lg3 (t) VALUES (TG_WHEN||'/'||TG_OP||'/'||OLD.pid||'->'||NEW.pid); "
                + "RETURN NEW; END $$");
        exec("CREATE TABLE zzw2f_pb (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_cb (id int PRIMARY KEY, pid int REFERENCES zzw2f_pb(id) ON UPDATE CASCADE)");
        exec("INSERT INTO zzw2f_pb VALUES (1)");
        exec("INSERT INTO zzw2f_cb VALUES (10,1)");
        exec("CREATE TRIGGER zzw2f_tt3 AFTER UPDATE ON zzw2f_cb FOR EACH ROW EXECUTE FUNCTION zzw2f_lf3()");
        exec("UPDATE zzw2f_pb SET id = 2");
        assertEquals("AFTER/UPDATE/1->2", scalar("SELECT t FROM zzw2f_lg3 ORDER BY seq"));
        assertEquals("2", scalar("SELECT pid FROM zzw2f_cb"));
    }

    @Test
    void onDeleteSetNullFiresTheChildsAfterUpdateTrigger() throws Exception {
        exec("CREATE TABLE zzw2f_lg4 (seq serial, t text)");
        exec("CREATE FUNCTION zzw2f_lf4() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "INSERT INTO zzw2f_lg4 (t) VALUES (TG_WHEN||'/'||TG_OP); RETURN NEW; END $$");
        exec("CREATE TABLE zzw2f_pn (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_cn (id int PRIMARY KEY, pid int REFERENCES zzw2f_pn(id) ON DELETE SET NULL)");
        exec("INSERT INTO zzw2f_pn VALUES (1)");
        exec("INSERT INTO zzw2f_cn VALUES (10,1)");
        exec("CREATE TRIGGER zzw2f_tt4 AFTER UPDATE ON zzw2f_cn FOR EACH ROW EXECUTE FUNCTION zzw2f_lf4()");
        exec("DELETE FROM zzw2f_pn WHERE id = 1");
        assertEquals("AFTER/UPDATE", scalar("SELECT t FROM zzw2f_lg4 ORDER BY seq"));
        assertNull(scalar("SELECT pid FROM zzw2f_cn"));
    }

    @Test
    void aBeforeUpdateTriggerRewritesACascadedUpdate() throws Exception {
        exec("CREATE TABLE zzw2f_sl (seq serial, t text)");
        exec("CREATE FUNCTION zzw2f_sf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "INSERT INTO zzw2f_sl (t) VALUES ('saw'); NEW.note := 'touched'; RETURN NEW; END $$");
        exec("CREATE TABLE zzw2f_s1 (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_s2 (id int PRIMARY KEY, p int REFERENCES zzw2f_s1(id) ON UPDATE CASCADE, note text)");
        exec("INSERT INTO zzw2f_s1 VALUES (1)");
        exec("INSERT INTO zzw2f_s2 VALUES (10,1,'orig')");
        exec("CREATE TRIGGER zzw2f_st BEFORE UPDATE ON zzw2f_s2 FOR EACH ROW EXECUTE FUNCTION zzw2f_sf()");
        exec("UPDATE zzw2f_s1 SET id = 7");
        assertEquals("7", scalar("SELECT p FROM zzw2f_s2"));
        assertEquals("touched", scalar("SELECT note FROM zzw2f_s2"));
        assertEquals("1", scalar("SELECT count(*) FROM zzw2f_sl"));
    }

    @Test
    void aBeforeDeleteTriggerVetoesACascadedDelete() throws Exception {
        exec("CREATE FUNCTION zzw2f_skip() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$");
        exec("CREATE TABLE zzw2f_bp (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_bd (id int PRIMARY KEY, p int REFERENCES zzw2f_bp(id) ON DELETE CASCADE)");
        exec("INSERT INTO zzw2f_bp VALUES (1)");
        exec("INSERT INTO zzw2f_bd VALUES (10,1)");
        exec("CREATE TRIGGER zzw2f_bt BEFORE DELETE ON zzw2f_bd FOR EACH ROW EXECUTE FUNCTION zzw2f_skip()");
        exec("DELETE FROM zzw2f_bp WHERE id = 1");
        assertEquals("0", scalar("SELECT count(*) FROM zzw2f_bp"));
        assertEquals("1", scalar("SELECT count(*) FROM zzw2f_bd"));
    }

    @Test
    void aRolledBackCascadeRestoresBothTables() throws Exception {
        exec("CREATE TABLE zzw2f_x1 (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw2f_x2 (id int PRIMARY KEY, p int REFERENCES zzw2f_x1(id) ON DELETE CASCADE)");
        exec("INSERT INTO zzw2f_x1 VALUES (1),(2)");
        exec("INSERT INTO zzw2f_x2 VALUES (10,1),(20,2)");
        exec("BEGIN");
        exec("DELETE FROM zzw2f_x1");
        exec("ROLLBACK");
        assertEquals("2", scalar("SELECT count(*) FROM zzw2f_x1"));
        assertEquals("2", scalar("SELECT count(*) FROM zzw2f_x2"));
        exec("DELETE FROM zzw2f_x1 WHERE id = 1");
        assertEquals("1", scalar("SELECT count(*) FROM zzw2f_x2"));
    }

    // ------------------------------------------------------------ A write sees what the statement is allowed to see

    @Test
    void writeThroughAViewReachesOnlyTheRowsTheViewShows() throws Exception {
        exec("CREATE TABLE zzw2j_vb (id int PRIMARY KEY, owner text, a int)");
        exec("INSERT INTO zzw2j_vb VALUES (1,'me',5),(2,'you',50),(3,'me',8)");
        exec("CREATE VIEW zzw2j_vv AS SELECT id, a FROM zzw2j_vb WHERE a < 10");

        // A row the view does not show is not a row the UPDATE can touch.
        exec("UPDATE zzw2j_vv SET a = 6 WHERE id = 2");
        assertEquals("50", String.valueOf(scalar("SELECT a FROM zzw2j_vb WHERE id = 2")));

        // Nor one the unqualified DELETE can remove.
        exec("DELETE FROM zzw2j_vv");
        assertEquals("1", String.valueOf(scalar("SELECT count(*) FROM zzw2j_vb")));
        assertEquals("50", String.valueOf(scalar("SELECT a FROM zzw2j_vb WHERE id = 2")));

        // A view qualified on a column it does not project bounds the write just the same.
        exec("INSERT INTO zzw2j_vb VALUES (1,'me',5),(3,'me',8)");
        exec("CREATE VIEW zzw2j_mine AS SELECT id, a FROM zzw2j_vb WHERE owner = 'me'");
        exec("DELETE FROM zzw2j_mine WHERE id = 2");
        assertEquals("3", String.valueOf(scalar("SELECT count(*) FROM zzw2j_vb")));
        exec("DELETE FROM zzw2j_mine");
        assertEquals("1", String.valueOf(scalar("SELECT count(*) FROM zzw2j_vb")));
        assertEquals("you", String.valueOf(scalar("SELECT owner FROM zzw2j_vb")));

        exec("DROP VIEW zzw2j_mine");
        exec("DROP VIEW zzw2j_vv");
        exec("DROP TABLE zzw2j_vb");
    }

    @Test
    void aViewOverAViewCarriesBothQualifications() throws Exception {
        exec("CREATE TABLE zzw2j_lb (id int PRIMARY KEY, owner text, a int)");
        exec("INSERT INTO zzw2j_lb VALUES (1,'me',5),(2,'you',50),(3,'me',8)");
        exec("CREATE VIEW zzw2j_lmine AS SELECT id, a FROM zzw2j_lb WHERE owner = 'me'");
        exec("CREATE VIEW zzw2j_lsmall AS SELECT id AS k, a AS v FROM zzw2j_lmine WHERE a < 7");

        assertEquals("1", String.valueOf(scalar("SELECT count(*) FROM zzw2j_lsmall")));
        exec("DELETE FROM zzw2j_lsmall");
        assertEquals("2", String.valueOf(scalar("SELECT count(*) FROM zzw2j_lb")));
        assertEquals("0", String.valueOf(scalar("SELECT count(*) FROM zzw2j_lb WHERE id = 1")));

        exec("DROP VIEW zzw2j_lsmall");
        exec("DROP VIEW zzw2j_lmine");
        exec("DROP TABLE zzw2j_lb");
    }

    @Test
    void aDomainOnACompositeFieldIsEnforced() throws Exception {
        exec("CREATE DOMAIN zzw2j_d AS int CHECK (VALUE > 0)");
        exec("CREATE DOMAIN zzw2j_dt AS text NOT NULL CHECK (VALUE = lower(VALUE))");
        exec("CREATE TYPE zzw2j_c AS (a zzw2j_d, b zzw2j_dt)");
        exec("CREATE TABLE zzw2j_ct (id int, c zzw2j_c)");

        assertEquals("23514", stateOf("SELECT ROW(-1,'x')::zzw2j_c"));
        assertEquals("23514", stateOf("SELECT ROW(5,'NOPE')::zzw2j_c"));
        assertEquals("23502", stateOf("SELECT ROW(5,NULL)::zzw2j_c"));
        assertEquals("23514", stateOf("SELECT '(-1,x)'::zzw2j_c"));
        assertEquals("23514", stateOf("INSERT INTO zzw2j_ct VALUES (2, ROW(-1,'ok')::zzw2j_c)"));
        assertEquals("23514",
                stateOf("SELECT ARRAY[ROW(5,'a')::zzw2j_c, ROW(-1,'b')::zzw2j_c]"));

        // A value every field's domain accepts is stored, and reads back as PG writes it.
        exec("INSERT INTO zzw2j_ct VALUES (5, ROW(5,'ok')::zzw2j_c)");
        assertEquals("(5,ok)", String.valueOf(scalar("SELECT c FROM zzw2j_ct WHERE id = 5")));
        assertEquals("23514",
                stateOf("UPDATE zzw2j_ct SET c = ROW(-7,'x')::zzw2j_c WHERE id = 5"));
        assertEquals("(5,ok)", String.valueOf(scalar("SELECT c FROM zzw2j_ct WHERE id = 5")));

        exec("DROP TABLE zzw2j_ct");
        exec("DROP TYPE zzw2j_c");
        exec("DROP DOMAIN zzw2j_dt");
        exec("DROP DOMAIN zzw2j_d");
    }

    @Test
    void repeatableReadStillWritesTheRowsItCanSee() throws Exception {
        exec("CREATE TABLE zzw2j_rr (i int PRIMARY KEY, v int)");
        exec("INSERT INTO zzw2j_rr VALUES (1,10)");

        exec("BEGIN ISOLATION LEVEL REPEATABLE READ");
        assertEquals("1", String.valueOf(scalar("SELECT count(*) FROM zzw2j_rr")));
        exec("INSERT INTO zzw2j_rr VALUES (2,20)");
        exec("UPDATE zzw2j_rr SET v = v + 1");
        assertEquals("2", String.valueOf(scalar("SELECT count(*) FROM zzw2j_rr")));
        exec("COMMIT");

        assertEquals("11", String.valueOf(scalar("SELECT v FROM zzw2j_rr WHERE i = 1")));
        assertEquals("21", String.valueOf(scalar("SELECT v FROM zzw2j_rr WHERE i = 2")));

        // And a DELETE inside the same shape still empties what it can see.
        exec("BEGIN ISOLATION LEVEL REPEATABLE READ");
        exec("DELETE FROM zzw2j_rr");
        assertEquals("0", String.valueOf(scalar("SELECT count(*) FROM zzw2j_rr")));
        exec("COMMIT");
        assertEquals("0", String.valueOf(scalar("SELECT count(*) FROM zzw2j_rr")));

        exec("DROP TABLE zzw2j_rr");
    }

    // ------------------------------------------------------------ Singletons

@Test
void addColumnTakesTheDomainsOwnDefault() throws Exception {
    exec("CREATE DOMAIN zzt_wd AS int DEFAULT 7");
    exec("CREATE TABLE zzt_wt (a int)");
    exec("INSERT INTO zzt_wt VALUES (1),(2)");
    exec("ALTER TABLE zzt_wt ADD COLUMN b zzt_wd");
    assertEquals("7", scalar("SELECT b FROM zzt_wt WHERE a = 1"));
    assertEquals("7", scalar("SELECT b FROM zzt_wt WHERE a = 2"));
    // PG records no pg_attrdef entry of the column's own: the default comes from the domain
    assertNull(scalar("SELECT column_default FROM information_schema.columns"
            + " WHERE table_name = 'zzt_wt' AND column_name = 'b'"));
    exec("DROP TABLE zzt_wt");
    exec("DROP DOMAIN zzt_wd");
}

@Test
void addColumnOfNotNullDomainFillsExistingRows() throws Exception {
    exec("CREATE DOMAIN zzt_wdn AS int NOT NULL DEFAULT 7");
    exec("CREATE TABLE zzt_wt2 (a int)");
    exec("INSERT INTO zzt_wt2 VALUES (1)");
    exec("ALTER TABLE zzt_wt2 ADD COLUMN b zzt_wdn");
    assertEquals("7", scalar("SELECT b FROM zzt_wt2 WHERE a = 1"));
    exec("DROP TABLE zzt_wt2");
    exec("DROP DOMAIN zzt_wdn");
}

@Test
void twoParentsMergeTheirNotNullFlags() throws Exception {
    exec("CREATE TABLE zzt_fta (shared int, a int)");
    exec("CREATE TABLE zzt_ftb (shared int NOT NULL, b int)");
    exec("CREATE TABLE zzt_ftc () INHERITS (zzt_fta, zzt_ftb)");
    assertEquals("NO", scalar("SELECT is_nullable FROM information_schema.columns"
            + " WHERE table_name = 'zzt_ftc' AND column_name = 'shared'"));
    assertEquals("23502", stateOf("INSERT INTO zzt_ftc (a) VALUES (1)"));
    // A child and both of its parents go in one statement, the way PostgreSQL lets a whole
    // inheritance group be named at once, so no drop has to wait for another to finish.
    exec("DROP TABLE zzt_ftc, zzt_fta, zzt_ftb");
}

@Test
void twoParentsMayNotDisagreeAboutOneCheckName() throws Exception {
    exec("CREATE TABLE zzt_fka (x int CONSTRAINT zzt_k CHECK (x > 0))");
    exec("CREATE TABLE zzt_fkb (x int CONSTRAINT zzt_k CHECK (x < 0))");
    assertEquals("42710", stateOf("CREATE TABLE zzt_fkc () INHERITS (zzt_fka, zzt_fkb)"));
    exec("CREATE TABLE zzt_fkd (x int CONSTRAINT zzt_k2 CHECK (x > 0))");
    exec("CREATE TABLE zzt_fke (x int CONSTRAINT zzt_k2 CHECK (x > 0))");
    // The same expression under the same name is one constraint, and PG takes it
    exec("CREATE TABLE zzt_fkf () INHERITS (zzt_fkd, zzt_fke)");
    // A child and both of its parents go in one statement, the way PostgreSQL lets a whole
    // inheritance group be named at once, so no drop has to wait for another to finish.
    exec("DROP TABLE zzt_fkf, zzt_fkd, zzt_fke, zzt_fka, zzt_fkb");
}

@Test
void writingThroughAViewFiresTheBaseTablesRowTriggers() throws Exception {
    exec("CREATE TABLE zzt_wb (a int, b int)");
    exec("CREATE TABLE zzt_wlog (m text)");
    exec("CREATE VIEW zzt_w1 AS SELECT a, b FROM zzt_wb");
    exec("CREATE FUNCTION zzt_wf() RETURNS trigger AS $$ BEGIN"
            + " INSERT INTO zzt_wlog VALUES (TG_OP || ':' || NEW.a); RETURN NEW; END; $$ LANGUAGE plpgsql");
    exec("CREATE TRIGGER zzt_wtg BEFORE INSERT ON zzt_wb FOR EACH ROW EXECUTE FUNCTION zzt_wf()");
    exec("CREATE TRIGGER zzt_wtgu BEFORE UPDATE ON zzt_wb FOR EACH ROW EXECUTE FUNCTION zzt_wf()");
    exec("INSERT INTO zzt_wb VALUES (1, 2)");
    exec("INSERT INTO zzt_w1 VALUES (3, 4)");
    exec("UPDATE zzt_w1 SET b = 9 WHERE a = 1");
    assertEquals("3", scalar("SELECT count(*) FROM zzt_wlog"));
    assertEquals("1", scalar("SELECT count(*) FROM zzt_wlog WHERE m = 'INSERT:3'"));
    assertEquals("1", scalar("SELECT count(*) FROM zzt_wlog WHERE m = 'UPDATE:1'"));
    exec("DROP VIEW zzt_w1");
    exec("DROP TABLE zzt_wb");
    exec("DROP TABLE zzt_wlog");
    exec("DROP FUNCTION zzt_wf()");
}

@Test
void replicationRoleIsDecidedPerTrigger() throws Exception {
    exec("CREATE TABLE zzt_t8 (i int)");
    exec("CREATE TABLE zzt_t8r (i int)");
    exec("CREATE TABLE zzt_l8 (m text)");
    exec("CREATE FUNCTION zzt_f8() RETURNS trigger AS $$ BEGIN"
            + " INSERT INTO zzt_l8 VALUES (TG_NAME); RETURN NULL; END; $$ LANGUAGE plpgsql");
    exec("CREATE TRIGGER zzt_tg8 AFTER INSERT ON zzt_t8 FOR EACH ROW EXECUTE FUNCTION zzt_f8()");
    exec("ALTER TABLE zzt_t8 ENABLE ALWAYS TRIGGER zzt_tg8");
    exec("CREATE TRIGGER zzt_tg8r AFTER INSERT ON zzt_t8r FOR EACH ROW EXECUTE FUNCTION zzt_f8()");
    exec("ALTER TABLE zzt_t8r ENABLE REPLICA TRIGGER zzt_tg8r");
    // origin: the REPLICA trigger stays silent
    exec("INSERT INTO zzt_t8r VALUES (1)");
    assertEquals("0", scalar("SELECT count(*) FROM zzt_l8"));
    exec("SET session_replication_role = 'replica'");
    exec("INSERT INTO zzt_t8 VALUES (1)");
    exec("INSERT INTO zzt_t8r VALUES (2)");
    assertEquals("2", scalar("SELECT count(*) FROM zzt_l8"));
    exec("SET session_replication_role = 'origin'");
    exec("DROP TABLE zzt_t8");
    exec("DROP TABLE zzt_t8r");
    exec("DROP TABLE zzt_l8");
    exec("DROP FUNCTION zzt_f8()");
}

@Test
void beforeInsertTriggerReturningOldSkipsTheRow() throws Exception {
    exec("CREATE TABLE zzt_t11 (i int)");
    exec("CREATE FUNCTION zzt_f11() RETURNS trigger AS $$ BEGIN RETURN OLD; END; $$ LANGUAGE plpgsql");
    exec("CREATE TRIGGER zzt_tg11 BEFORE INSERT ON zzt_t11 FOR EACH ROW EXECUTE FUNCTION zzt_f11()");
    exec("INSERT INTO zzt_t11 VALUES (1)");
    assertEquals("0", scalar("SELECT count(*) FROM zzt_t11"));
    exec("DROP TABLE zzt_t11");
    exec("DROP FUNCTION zzt_f11()");
}

@Test
void bareOldIsNullInsideAnInsertTrigger() throws Exception {
    exec("CREATE TABLE zzt_o1 (id int, tag text)");
    exec("CREATE FUNCTION zzt_o1f() RETURNS trigger AS $$ BEGIN"
            + " IF OLD IS NULL THEN NEW.tag := 'oldnull'; ELSE NEW.tag := 'oldnotnull'; END IF;"
            + " RETURN NEW; END $$ LANGUAGE plpgsql");
    exec("CREATE TRIGGER zzt_o1t BEFORE INSERT ON zzt_o1 FOR EACH ROW EXECUTE FUNCTION zzt_o1f()");
    exec("INSERT INTO zzt_o1 VALUES (1, 'x')");
    assertEquals("oldnull", scalar("SELECT tag FROM zzt_o1 WHERE id = 1"));
    exec("DROP TABLE zzt_o1");
    exec("DROP FUNCTION zzt_o1f()");
}

@Test
void positionalInsertRefusesAViewsComputedColumn() throws Exception {
    exec("CREATE TABLE zzt_v3t (i int, n int)");
    exec("CREATE VIEW zzt_v3v AS SELECT i, n*2 AS dn FROM zzt_v3t");
    assertEquals("0A000", stateOf("INSERT INTO zzt_v3v VALUES (1, 4)"));
    assertEquals("View columns that are not columns of their base relation are not updatable.",
            detailOf("INSERT INTO zzt_v3v VALUES (1, 4)"));
    assertEquals("0", scalar("SELECT count(*) FROM zzt_v3t"));
    // A VALUES list that stops before the computed column is accepted, as it is in PG
    exec("INSERT INTO zzt_v3v VALUES (1)");
    assertEquals("1", scalar("SELECT count(*) FROM zzt_v3t"));
    exec("DROP VIEW zzt_v3v");
    exec("DROP TABLE zzt_v3t");
}

@Test
void returningThroughAnInsteadRuleIsRefusedAndWritesNothing() throws Exception {
    exec("CREATE TABLE zzt_r5 (i int primary key, v text)");
    exec("CREATE VIEW zzt_r5v AS SELECT i, v FROM zzt_r5");
    exec("CREATE RULE zzt_r5_r AS ON INSERT TO zzt_r5v DO INSTEAD"
            + " INSERT INTO zzt_r5 VALUES (NEW.i, NEW.v)");
    assertEquals("0A000", stateOf("INSERT INTO zzt_r5v VALUES (1,'a') RETURNING i"));
    assertEquals("0", scalar("SELECT count(*) FROM zzt_r5"));
    // Without RETURNING the rule still runs
    exec("INSERT INTO zzt_r5v VALUES (1,'a')");
    assertEquals("1", scalar("SELECT count(*) FROM zzt_r5"));
    exec("DROP VIEW zzt_r5v");
    exec("DROP TABLE zzt_r5");
}

@Test
void onConflictIsRefusedBesideAnInsertOrUpdateRule() throws Exception {
    exec("CREATE TABLE zzt_r6 (i int primary key, v text)");
    exec("CREATE TABLE zzt_r6log (m text)");
    exec("CREATE RULE zzt_r6_h AS ON INSERT TO zzt_r6 DO ALSO INSERT INTO zzt_r6log VALUES ('i')");
    assertEquals("0A000", stateOf("INSERT INTO zzt_r6 VALUES (1,'a') ON CONFLICT (i) DO NOTHING"));
    // A DELETE rule cannot rewrite an INSERT, so PG leaves ON CONFLICT alone
    exec("CREATE TABLE zzt_r8 (i int primary key, v text)");
    exec("CREATE RULE zzt_r8_s AS ON DELETE TO zzt_r8 DO INSTEAD NOTHING");
    exec("INSERT INTO zzt_r8 VALUES (1,'a') ON CONFLICT (i) DO NOTHING");
    assertEquals("1", scalar("SELECT count(*) FROM zzt_r8"));
    exec("DROP TABLE zzt_r8");
    exec("DROP TABLE zzt_r6");
    exec("DROP TABLE zzt_r6log");
}

@Test
void tablesampleReadsInheritanceChildren() throws Exception {
    exec("CREATE TABLE zzt_tp (a int)");
    exec("CREATE TABLE zzt_tc () INHERITS (zzt_tp)");
    exec("INSERT INTO zzt_tp VALUES (1)");
    exec("INSERT INTO zzt_tc VALUES (2)");
    assertEquals("2", scalar("SELECT count(*) FROM zzt_tp TABLESAMPLE BERNOULLI (100)"));
    assertEquals("2", scalar("SELECT count(*) FROM zzt_tp"));
    exec("DROP TABLE zzt_tc");
    exec("DROP TABLE zzt_tp");
}

@Test
void anRlsPolicyThatRaisesFailsTheQuery() throws Exception {
    exec("CREATE TABLE zzt_rls (a int)");
    exec("INSERT INTO zzt_rls VALUES (0),(1)");
    exec("ALTER TABLE zzt_rls ENABLE ROW LEVEL SECURITY");
    exec("CREATE POLICY zzt_rp ON zzt_rls FOR SELECT USING (100 / a > 0)");
    exec("CREATE ROLE zzt_r2 LOGIN");
    exec("GRANT SELECT ON zzt_rls TO zzt_r2");
    // The role has to go back even when the expectation below is disappointed: it is session state,
    // and every later test on this connection would otherwise run as zzt_r2.
    try {
        exec("SET ROLE zzt_r2");
        assertEquals("22012", stateOf("SELECT a FROM zzt_rls ORDER BY a"));
    } finally {
        exec("RESET ROLE");
    }
    exec("DROP TABLE zzt_rls");
    exec("DROP ROLE zzt_r2");
}

@Test
void rollbackToSavepointUndoesSetConstraints() throws Exception {
    exec("CREATE TABLE zzt_pp (i int PRIMARY KEY)");
    exec("INSERT INTO zzt_pp VALUES (1)");
    exec("CREATE TABLE zzt_cc (id int PRIMARY KEY,"
            + " p int REFERENCES zzt_pp(i) DEFERRABLE INITIALLY IMMEDIATE)");
    exec("BEGIN");
    exec("SAVEPOINT s");
    exec("SET CONSTRAINTS ALL DEFERRED");
    exec("ROLLBACK TO s");
    assertEquals("23503", stateOf("INSERT INTO zzt_cc VALUES (1, 555)"));
    exec("ROLLBACK");
    exec("DROP TABLE zzt_cc");
    exec("DROP TABLE zzt_pp");
}

@Test
void rangePartitionBoundsOnAnEnumKeyOrderByDeclaration() throws Exception {
    exec("CREATE TYPE zzt_e AS ENUM ('lo','mid','hi')");
    exec("CREATE TABLE zzt_pe (e zzt_e NOT NULL) PARTITION BY RANGE (e)");
    exec("CREATE TABLE zzt_pe1 PARTITION OF zzt_pe FOR VALUES FROM ('lo') TO ('hi')");
    exec("INSERT INTO zzt_pe VALUES ('mid')");
    assertEquals("mid", scalar("SELECT e FROM zzt_pe"));
    assertEquals("FOR VALUES FROM ('lo') TO ('hi')",
            scalar("SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE relname = 'zzt_pe1'"));
    exec("DROP TABLE zzt_pe");
    exec("DROP TYPE zzt_e");
}

@Test
void aPartitionBoundIsCheckedAgainstTheKeysDomain() throws Exception {
    exec("CREATE DOMAIN zzt_dp AS int CHECK (VALUE > 0)");
    exec("CREATE TABLE zzt_pd (k zzt_dp NOT NULL) PARTITION BY RANGE (k)");
    assertEquals("23514",
            stateOf("CREATE TABLE zzt_pd1 PARTITION OF zzt_pd FOR VALUES FROM (-100) TO (0)"));
    exec("DROP TABLE zzt_pd");
    exec("DROP DOMAIN zzt_dp");
}

@Test
void aTriggerOnAViewCarriesTheViewsOidInPgTrigger() throws Exception {
    exec("CREATE TABLE zzt_b3 (id int, note text)");
    exec("CREATE VIEW zzt_vv3 AS SELECT id, note FROM zzt_b3");
    exec("CREATE FUNCTION zzt_vf3() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN OLD; END $$");
    exec("CREATE TRIGGER zzt_tv3 INSTEAD OF DELETE ON zzt_vv3"
            + " FOR EACH ROW EXECUTE FUNCTION zzt_vf3()");
    assertEquals("1", scalar("SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid"
            + " WHERE c.relname = 'zzt_vv3'"));
    exec("DROP VIEW zzt_vv3");
    exec("DROP TABLE zzt_b3");
    exec("DROP FUNCTION zzt_vf3()");
}

@Test
void viewStorageParametersMayBeWrittenWithoutAValue() throws Exception {
    exec("CREATE VIEW zzt_sv WITH (security_barrier) AS SELECT 1 AS x");
    exec("CREATE VIEW zzt_sv2 WITH (security_invoker) AS SELECT 1 AS x");
    exec("CREATE TABLE zzt_ro (a int)");
    exec("CREATE VIEW zzt_sv5 WITH (security_barrier, security_invoker) AS SELECT a FROM zzt_ro");
    assertEquals("{security_barrier=true}",
            scalar("SELECT reloptions FROM pg_class WHERE relname = 'zzt_sv'"));
    assertEquals("0A000", stateOf("CREATE VIEW zzt_sv4 WITH (check_option = cascaded) AS SELECT 1 AS x"));
    exec("DROP VIEW zzt_sv5");
    exec("DROP TABLE zzt_ro");
    exec("DROP VIEW zzt_sv2");
    exec("DROP VIEW zzt_sv");
}

@Test
void columnNotFoundUsesPostgresWording() throws Exception {
    exec("CREATE TABLE zzt_colmsg (id int)");
    assertEquals("column \"nosuchcol\" of relation \"zzt_colmsg\" does not exist",
            messageOf("ALTER TABLE zzt_colmsg ALTER COLUMN nosuchcol SET NOT NULL"));
    assertEquals("column \"nosuchcol\" of relation \"zzt_colmsg\" does not exist",
            messageOf("ALTER TABLE zzt_colmsg ALTER COLUMN nosuchcol DROP DEFAULT"));
    assertEquals("column \"nosuchcol\" of relation \"zzt_colmsg\" does not exist",
            messageOf("ALTER TABLE zzt_colmsg DROP COLUMN nosuchcol"));
    // PG leaves the relation out of the rename
    assertEquals("column \"nosuchcol\" does not exist",
            messageOf("ALTER TABLE zzt_colmsg RENAME COLUMN nosuchcol TO x"));
    assertEquals("column \"ctid\" of relation \"zzt_colmsg\" does not exist",
            messageOf("INSERT INTO zzt_colmsg (ctid) VALUES ('(0,1)')"));
    assertEquals("column \"nosuchcol\" does not exist",
            messageOf("INSERT INTO zzt_colmsg VALUES (1) RETURNING nosuchcol"));
    // An UPDATE assignment to a system column keeps its own complaint
    assertEquals("0A000", stateOf("UPDATE zzt_colmsg SET ctid = '(0,1)'"));
    exec("DROP TABLE zzt_colmsg");
}

@Test
void returningAliasSurvivesIntoTheRowDescription() throws Exception {
    exec("CREATE TABLE zzt_lab (a int)");
    exec("INSERT INTO zzt_lab VALUES (1)");
    try (java.sql.Statement st = conn.createStatement();
         java.sql.ResultSet rs = st.executeQuery("UPDATE zzt_lab SET a = 3 RETURNING a AS r2")) {
        assertEquals("r2", rs.getMetaData().getColumnLabel(1));
    }
    try (java.sql.Statement st = conn.createStatement();
         java.sql.ResultSet rs = st.executeQuery("DELETE FROM zzt_lab RETURNING a AS r3")) {
        assertEquals("r3", rs.getMetaData().getColumnLabel(1));
    }
    exec("DROP TABLE zzt_lab");
}

@Test
void theseObjectsBelongToTheRelationsOwner() throws Exception {
    exec("CREATE TABLE zzt_own2 (id int)");
    exec("CREATE FUNCTION zzt_ofn() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql");
    exec("CREATE ROLE zzt_role2 LOGIN");
    exec("GRANT SELECT ON zzt_own2 TO zzt_role2");
    exec("CREATE POLICY zzt_pol ON zzt_own2 FOR SELECT USING (true)");
    // The role has to go back even when an expectation below is disappointed: it is session state,
    // and every later test on this connection would otherwise run as zzt_role2.
    try {
        exec("SET ROLE zzt_role2");
        assertEquals("must be owner of table zzt_own2",
                messageOf("CREATE INDEX zzt_ix2 ON zzt_own2 (id)"));
        assertEquals("must be owner of table zzt_own2",
                messageOf("COMMENT ON TABLE zzt_own2 IS 'x'"));
        assertEquals("must be owner of table zzt_own2",
                messageOf("CREATE POLICY zzt_pol2 ON zzt_own2 FOR SELECT USING (true)"));
        assertEquals("must be owner of relation zzt_own2",
                messageOf("DROP POLICY zzt_pol ON zzt_own2"));
        assertEquals("permission denied for table zzt_own2",
                messageOf("CREATE TRIGGER zzt_trg2 BEFORE INSERT ON zzt_own2"
                        + " FOR EACH ROW EXECUTE FUNCTION zzt_ofn()"));
    } finally {
        exec("RESET ROLE");
    }
    exec("DROP TABLE zzt_own2");
    exec("DROP FUNCTION zzt_ofn()");
    exec("DROP ROLE zzt_role2");
}

    // ------------------------------------------------------------ A generated column and a rule body are evaluated, not pasted together

    @Test
    void generatedColumnIsComputedFromTheExpressionNotFromItsText() throws Exception {
        exec("CREATE TABLE zga_g1 (a text, b text, g text GENERATED ALWAYS AS (a || b) STORED)");
        exec("INSERT INTO zga_g1 (a, b) VALUES ('b', 'x')");
        // 'b' is a value, not a second reading of column b
        assertEquals("bx", scalar("SELECT g FROM zga_g1"));
        assertEquals("2", scalar("SELECT length(g)::text FROM zga_g1"));
        // a boolean cast to text spells itself out; t/f is only the wire form
        assertEquals("false", scalar("SELECT (g IS NULL)::text FROM zga_g1"));
        exec("DROP TABLE zga_g1");
    }

    @Test
    void generatedExpressionStringLiteralsAreNotRewritten() throws Exception {
        exec("CREATE TABLE zga_g2 (n int, s text GENERATED ALWAYS AS (n::text || 'n') STORED)");
        exec("INSERT INTO zga_g2 (n) VALUES (4)");
        assertEquals("4n", scalar("SELECT s FROM zga_g2"));

        exec("CREATE TABLE zga_g2b (a text, b text,"
                + " g text GENERATED ALWAYS AS (coalesce(a, 'b') || coalesce(b, 'a')) STORED)");
        exec("INSERT INTO zga_g2b (a, b) VALUES (NULL, NULL)");
        assertEquals("ba", scalar("SELECT g FROM zga_g2b"));

        exec("CREATE TABLE zga_g2c (a text, g text GENERATED ALWAYS AS (a || 'a') STORED)");
        exec("INSERT INTO zga_g2c (a) VALUES ('M')");
        assertEquals("Ma", scalar("SELECT g FROM zga_g2c"));

        exec("DROP TABLE zga_g2");
        exec("DROP TABLE zga_g2b");
        exec("DROP TABLE zga_g2c");
    }

    @Test
    void generatedColumnTakesValuesThatHaveNoLiteralSpelling() throws Exception {
        exec("CREATE TABLE zga_g3 (a text, g text GENERATED ALWAYS AS (a || '!') STORED)");
        exec("INSERT INTO zga_g3 (a) VALUES ('p$1q')");
        exec("INSERT INTO zga_g3 (a) VALUES ('back\\slash')");
        assertEquals("back\\slash!", scalar("SELECT g FROM zga_g3 ORDER BY g LIMIT 1"));
        assertEquals("p$1q!", scalar("SELECT g FROM zga_g3 ORDER BY g DESC LIMIT 1"));

        exec("CREATE TABLE zga_g3b (b bytea, t timestamp, d date, arr int[],"
                + " l int GENERATED ALWAYS AS (length(b)) STORED,"
                + " u timestamp GENERATED ALWAYS AS (t + interval '1 day') STORED,"
                + " y int GENERATED ALWAYS AS (extract(year from d)) STORED,"
                + " n int GENERATED ALWAYS AS (array_length(arr, 1)) STORED)");
        exec("INSERT INTO zga_g3b (b, t, d, arr)"
                + " VALUES ('\\x010203'::bytea, '2020-01-01 10:00', '2020-03-04', '{7,8,9}')");
        assertEquals("3", scalar("SELECT l::text FROM zga_g3b"));
        assertEquals("2020-01-02 10:00:00", scalar("SELECT u::text FROM zga_g3b"));
        assertEquals("2020", scalar("SELECT y::text FROM zga_g3b"));
        assertEquals("3", scalar("SELECT n::text FROM zga_g3b"));

        exec("DROP TABLE zga_g3");
        exec("DROP TABLE zga_g3b");
    }

    @Test
    void aGeneratedColumnThatCannotBeComputedFailsTheStatement() throws Exception {
        // Both rows compute a key of their own, so both are written
        exec("CREATE TABLE zga_g4 (a text, b text,"
                + " g text GENERATED ALWAYS AS (a || b) STORED PRIMARY KEY)");
        exec("INSERT INTO zga_g4 (a, b) VALUES ('b', 'x')");
        exec("INSERT INTO zga_g4 (a, b) VALUES ('b', 'y')");
        assertEquals("bx/by", scalar("SELECT string_agg(g, '/' ORDER BY g) FROM zga_g4"));

        // And an expression that raises is the statement's error, not a made-up value
        exec("CREATE TABLE zga_g4b (a int, g int GENERATED ALWAYS AS (100 / a) STORED)");
        assertEquals("22012", stateOf("INSERT INTO zga_g4b (a) VALUES (0)"));
        assertEquals("0", scalar("SELECT count(*)::text FROM zga_g4b"));

        exec("DROP TABLE zga_g4");
        exec("DROP TABLE zga_g4b");
    }

    @Test
    void rowDataIsNotSplicedIntoTheGeneratedExpression() throws Exception {
        exec("CREATE TABLE zga_g5src (a text)");
        exec("INSERT INTO zga_g5src VALUES ('Q')");
        exec("CREATE TABLE zga_g5 (a text, b text, g text GENERATED ALWAYS AS (a || b) STORED)");
        exec("INSERT INTO zga_g5 (a, b) VALUES ('b', ' || (SELECT count(*) FROM zga_g5src) || ')");
        // The two values concatenated as text: the subquery in the data is data
        assertEquals("b || (SELECT count(*) FROM zga_g5src) || ", scalar("SELECT g FROM zga_g5"));
        assertEquals("b", scalar("SELECT left(g, 1) FROM zga_g5"));
        exec("DROP TABLE zga_g5");
        exec("DROP TABLE zga_g5src");
    }

    @Test
    void ruleActionStringLiteralsAreLeftAlone() throws Exception {
        exec("CREATE TABLE zga_r1 (a text)");
        exec("CREATE TABLE zga_r1log (what text)");
        exec("CREATE RULE zga_r1r AS ON INSERT TO zga_r1"
                + " DO ALSO INSERT INTO zga_r1log VALUES ('NEW.a is the name')");
        assertEquals(1, update("INSERT INTO zga_r1 (a) VALUES ('V')"));
        assertEquals("NEW.a is the name", scalar("SELECT what FROM zga_r1log"));
        assertEquals("V", scalar("SELECT a FROM zga_r1"));

        exec("CREATE TABLE zga_r2 (a text)");
        exec("CREATE TABLE zga_r2log (what text)");
        exec("INSERT INTO zga_r2 VALUES ('one')");
        exec("CREATE RULE zga_r2r AS ON UPDATE TO zga_r2"
                + " DO ALSO INSERT INTO zga_r2log VALUES ('OLD.a means the old row')");
        assertEquals(1, update("UPDATE zga_r2 SET a = 'two'"));
        assertEquals("two", scalar("SELECT a FROM zga_r2"));
        assertEquals("OLD.a means the old row", scalar("SELECT what FROM zga_r2log"));

        exec("DROP TABLE zga_r1 CASCADE");
        exec("DROP TABLE zga_r1log");
        exec("DROP TABLE zga_r2 CASCADE");
        exec("DROP TABLE zga_r2log");
    }

    @Test
    void anInsertRuleReadsNewAsValues() throws Exception {
        exec("CREATE TABLE zga_r3 (b bytea, t timestamp, arr int[])");
        exec("CREATE TABLE zga_r3log (h text, ts text, n int)");
        exec("CREATE RULE zga_r3r AS ON INSERT TO zga_r3 DO ALSO INSERT INTO zga_r3log"
                + " VALUES (encode(NEW.b, 'hex'), NEW.t::text, array_length(NEW.arr, 1))");
        exec("INSERT INTO zga_r3 (b, t, arr)"
                + " VALUES ('\\x0a0b'::bytea, '2021-05-06 07:08:09', '{1,2}')");
        assertEquals("0a0b", scalar("SELECT h FROM zga_r3log"));
        assertEquals("2021-05-06 07:08:09", scalar("SELECT ts FROM zga_r3log"));
        assertEquals("2", scalar("SELECT n::text FROM zga_r3log"));
        exec("DROP TABLE zga_r3 CASCADE");
        exec("DROP TABLE zga_r3log");
    }

    @Test
    void aValueThatSpellsARowReferenceIsAValue() throws Exception {
        exec("CREATE TABLE zga_r4 (a text, b text)");
        exec("CREATE TABLE zga_r4log (x text, y text)");
        exec("CREATE RULE zga_r4r AS ON INSERT TO zga_r4"
                + " DO ALSO INSERT INTO zga_r4log VALUES (NEW.a, NEW.b)");
        assertEquals(1, update("INSERT INTO zga_r4 (a, b) VALUES ('NEW.b', 'z')"));
        assertEquals("NEW.b/z", scalar("SELECT x || '/' || y FROM zga_r4log"));
        assertEquals("NEW.b/z", scalar("SELECT a || '/' || b FROM zga_r4"));

        exec("CREATE TABLE zga_r5 (a text, b text)");
        exec("CREATE TABLE zga_r5log (x text)");
        exec("CREATE RULE zga_r5r AS ON UPDATE TO zga_r5"
                + " DO ALSO INSERT INTO zga_r5log VALUES (OLD.a || '/' || NEW.a)");
        exec("INSERT INTO zga_r5 VALUES ('OLD.b', 'k')");
        assertEquals(1, update("UPDATE zga_r5 SET a = 'NEW.b'"));
        assertEquals("OLD.b/NEW.b", scalar("SELECT x FROM zga_r5log"));

        exec("DROP TABLE zga_r4 CASCADE");
        exec("DROP TABLE zga_r4log");
        exec("DROP TABLE zga_r5 CASCADE");
        exec("DROP TABLE zga_r5log");
    }

    // ------------------------------------------------------------ What a parent declares reaches every relation under it

    @Test
    void aCheckAddedToAPartitionedParentReachesItsPartitions() throws Exception {
        exec("CREATE TABLE zzw3b_p (a int, b text) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw3b_p_p0 PARTITION OF zzw3b_p FOR VALUES FROM (0) TO (10)");
        exec("ALTER TABLE zzw3b_p ADD CONSTRAINT zzw3b_ck CHECK (b <> 'bad')");
        // Row storage lives in the leaf, so a rule the parent alone carried enforced nothing.
        assertEquals("zzw3b_ck", scalar("SELECT conname FROM pg_constraint"
                + " WHERE conrelid = 'zzw3b_p_p0'::regclass AND contype = 'c'"));
        assertEquals("23514", stateOf("INSERT INTO zzw3b_p_p0 VALUES (1, 'bad')"));
        assertEquals("new row for relation \"zzw3b_p_p0\" violates check constraint \"zzw3b_ck\"",
                messageOf("INSERT INTO zzw3b_p_p0 VALUES (1, 'bad')"));
        assertEquals("Failing row contains (1, bad).",
                detailOf("INSERT INTO zzw3b_p_p0 VALUES (1, 'bad')"));
        assertEquals(0, num("SELECT count(*)::int FROM zzw3b_p"));
        exec("DROP TABLE zzw3b_p");
    }

    @Test
    void aCheckAddedToAnInheritanceParentReachesItsChildren() throws Exception {
        exec("CREATE TABLE zzw3b_par (a int, b text)");
        exec("CREATE TABLE zzw3b_chi () INHERITS (zzw3b_par)");
        exec("ALTER TABLE zzw3b_par ADD CONSTRAINT zzw3b_ck2 CHECK (a > 0)");
        assertEquals("zzw3b_ck2", scalar("SELECT conname FROM pg_constraint"
                + " WHERE conrelid = 'zzw3b_chi'::regclass AND contype = 'c'"));
        assertEquals("23514", stateOf("INSERT INTO zzw3b_chi VALUES (-1, 'x')"));
        assertEquals(0, num("SELECT count(*)::int FROM zzw3b_chi"));
        // A key and a foreign key are the child's own business, and PostgreSQL passes neither
        // down an inheritance link.
        exec("ALTER TABLE zzw3b_par ADD PRIMARY KEY (a)");
        assertEquals(0, num("SELECT count(*)::int FROM pg_constraint"
                + " WHERE conrelid = 'zzw3b_chi'::regclass AND contype = 'p'"));
        exec("DROP TABLE zzw3b_chi");
        exec("DROP TABLE zzw3b_par");
    }

    @Test
    void aChildCreatedWithInheritsCarriesItsParentsChecks() throws Exception {
        exec("CREATE TABLE zzw3b_cp2 (a int CHECK (a > 0))");
        exec("CREATE TABLE zzw3b_cc2 () INHERITS (zzw3b_cp2)");
        assertEquals(1, num("SELECT count(*)::int FROM pg_constraint"
                + " WHERE conrelid = 'zzw3b_cc2'::regclass AND contype = 'c'"));
        // The copy answers to the name the parent gave it, which is the name a violation reports.
        assertEquals("new row for relation \"zzw3b_cc2\" violates check constraint"
                + " \"zzw3b_cp2_a_check\"", messageOf("INSERT INTO zzw3b_cc2 VALUES (-1)"));
        assertEquals(0, num("SELECT count(*)::int FROM zzw3b_cc2"));
        // NO INHERIT says the rule was never going to travel.
        exec("CREATE TABLE zzw3b_ni (a int CHECK (a > 0) NO INHERIT)");
        exec("CREATE TABLE zzw3b_nic () INHERITS (zzw3b_ni)");
        assertEquals(1, update("INSERT INTO zzw3b_nic VALUES (-1)"));
        exec("DROP TABLE zzw3b_nic");
        exec("DROP TABLE zzw3b_ni");
        exec("DROP TABLE zzw3b_cc2");
        exec("DROP TABLE zzw3b_cp2");
    }

    @Test
    void setNotNullReachesPartitionsAndInheritanceChildren() throws Exception {
        exec("CREATE TABLE zzw3b_n1 (a int, b text) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw3b_n1_0 PARTITION OF zzw3b_n1 FOR VALUES FROM (0) TO (10)");
        exec("ALTER TABLE zzw3b_n1 ALTER COLUMN b SET NOT NULL");
        assertEquals("NO", scalar("SELECT is_nullable FROM information_schema.columns"
                + " WHERE table_name = 'zzw3b_n1_0' AND column_name = 'b'"));
        assertEquals("23502", stateOf("INSERT INTO zzw3b_n1_0 (a) VALUES (1)"));
        assertEquals("null value in column \"b\" of relation \"zzw3b_n1_0\""
                + " violates not-null constraint",
                messageOf("INSERT INTO zzw3b_n1_0 (a) VALUES (1)"));
        // ...and the partition stops refusing one when the partitioned table does.
        exec("ALTER TABLE zzw3b_n1 ALTER COLUMN b DROP NOT NULL");
        assertEquals("YES", scalar("SELECT is_nullable FROM information_schema.columns"
                + " WHERE table_name = 'zzw3b_n1_0' AND column_name = 'b'"));
        exec("CREATE TABLE zzw3b_n2 (a int, b text)");
        exec("CREATE TABLE zzw3b_n2c () INHERITS (zzw3b_n2)");
        exec("ALTER TABLE zzw3b_n2 ALTER COLUMN b SET NOT NULL");
        assertEquals("23502", stateOf("INSERT INTO zzw3b_n2c (a) VALUES (1)"));
        assertEquals(0, num("SELECT count(*)::int FROM zzw3b_n2c"));
        exec("DROP TABLE zzw3b_n2c");
        exec("DROP TABLE zzw3b_n2");
        exec("DROP TABLE zzw3b_n1");
    }

    @Test
    void setDefaultReachesPartitionsAndInheritanceChildren() throws Exception {
        exec("CREATE TABLE zzw3b_d2 (a int, b text)");
        exec("CREATE TABLE zzw3b_d2c () INHERITS (zzw3b_d2)");
        exec("ALTER TABLE zzw3b_d2 ALTER COLUMN b SET DEFAULT 'q'");
        exec("INSERT INTO zzw3b_d2c (a) VALUES (1)");
        assertEquals("q", scalar("SELECT b FROM zzw3b_d2c WHERE a = 1"));
        // A default declared on the parent is the hierarchy's default: it replaces the one a
        // partition set for itself rather than deferring to it.
        exec("CREATE TABLE zzw3b_gf (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzw3b_gf_1 PARTITION OF zzw3b_gf FOR VALUES FROM (1) TO (10)");
        exec("ALTER TABLE zzw3b_gf_1 ALTER COLUMN s SET DEFAULT 'child'");
        exec("ALTER TABLE zzw3b_gf ALTER COLUMN s SET DEFAULT 'parent'");
        assertEquals("'parent'::text", scalar("SELECT column_default"
                + " FROM information_schema.columns"
                + " WHERE table_name = 'zzw3b_gf_1' AND column_name = 's'"));
        // ONLY is the way to ask for the named relation alone, and DROP DEFAULT recurses too.
        exec("ALTER TABLE ONLY zzw3b_d2 ALTER COLUMN b SET DEFAULT 'only'");
        assertEquals("'q'::text", scalar("SELECT column_default FROM information_schema.columns"
                + " WHERE table_name = 'zzw3b_d2c' AND column_name = 'b'"));
        exec("ALTER TABLE zzw3b_d2 ALTER COLUMN b DROP DEFAULT");
        assertNull(scalar("SELECT column_default FROM information_schema.columns"
                + " WHERE table_name = 'zzw3b_d2c' AND column_name = 'b'"));
        exec("DROP TABLE zzw3b_gf");
        exec("DROP TABLE zzw3b_d2c");
        exec("DROP TABLE zzw3b_d2");
    }

    @Test
    void truncateOfAnInheritanceParentEmptiesItsChildren() throws Exception {
        exec("CREATE TABLE zzw3b_tp (i int)");
        exec("CREATE TABLE zzw3b_tpc () INHERITS (zzw3b_tp)");
        exec("INSERT INTO zzw3b_tp VALUES (1)");
        exec("INSERT INTO zzw3b_tpc VALUES (2)");
        // ONLY is the one way to leave the child holding its rows.
        exec("TRUNCATE ONLY zzw3b_tp");
        assertEquals(1, num("SELECT count(*)::int FROM zzw3b_tpc"));
        exec("TRUNCATE zzw3b_tp");
        assertEquals(0, num("SELECT count(*)::int FROM zzw3b_tpc"));
        assertEquals(0, num("SELECT count(*)::int FROM zzw3b_tp"));
        exec("DROP TABLE zzw3b_tpc");
        exec("DROP TABLE zzw3b_tp");
    }

    @Test
    void anInheritedConstraintMayNotBeDroppedOnTheDescendant() throws Exception {
        exec("CREATE TABLE zzw3b_ct (a int, b text CHECK (b <> 'bad')) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw3b_ct_0 PARTITION OF zzw3b_ct FOR VALUES FROM (0) TO (10)");
        assertEquals("42P16", stateOf("ALTER TABLE zzw3b_ct_0 DROP CONSTRAINT zzw3b_ct_b_check"));
        assertEquals("cannot drop inherited constraint \"zzw3b_ct_b_check\""
                + " of relation \"zzw3b_ct_0\"",
                messageOf("ALTER TABLE zzw3b_ct_0 DROP CONSTRAINT zzw3b_ct_b_check"));
        // The partition goes on enforcing the rule it was refused permission to drop.
        assertEquals("23514", stateOf("INSERT INTO zzw3b_ct_0 VALUES (1, 'bad')"));
        exec("CREATE TABLE zzw3b_cq (a int)");
        exec("CREATE TABLE zzw3b_cqc () INHERITS (zzw3b_cq)");
        exec("ALTER TABLE zzw3b_cq ADD CONSTRAINT zzw3b_cqk CHECK (a > 0)");
        assertEquals("42P16", stateOf("ALTER TABLE zzw3b_cqc DROP CONSTRAINT zzw3b_cqk"));
        // A constraint the descendant declared for itself is still its own to drop.
        exec("ALTER TABLE zzw3b_cqc ADD CONSTRAINT zzw3b_cqown CHECK (a < 100)");
        exec("ALTER TABLE zzw3b_cqc DROP CONSTRAINT zzw3b_cqown");
        exec("DROP TABLE zzw3b_cqc");
        exec("DROP TABLE zzw3b_cq");
        exec("DROP TABLE zzw3b_ct");
    }

    @Test
    void inheritRefusesAChildThatEnforcesLessThanItsParent() throws Exception {
        exec("CREATE TABLE zzw3b_gdp (a int CHECK (a > 0))");
        exec("CREATE TABLE zzw3b_gdc (a int)");
        assertEquals("42804", stateOf("ALTER TABLE zzw3b_gdc INHERIT zzw3b_gdp"));
        assertEquals("child table is missing constraint \"zzw3b_gdp_a_check\"",
                messageOf("ALTER TABLE zzw3b_gdc INHERIT zzw3b_gdp"));
        // The link is not made, so nothing is attached enforcing less than the parent.
        assertEquals(0, num("SELECT count(*)::int FROM pg_inherits"
                + " WHERE inhrelid = 'zzw3b_gdc'::regclass"));
        // The rule under some other name is not the rule.
        exec("ALTER TABLE zzw3b_gdc ADD CONSTRAINT zzw3b_other CHECK (a > 0)");
        assertEquals("42804", stateOf("ALTER TABLE zzw3b_gdc INHERIT zzw3b_gdp"));
        exec("CREATE TABLE zzw3b_gde (a int CONSTRAINT zzw3b_gdp_a_check CHECK (a > 0))");
        exec("ALTER TABLE zzw3b_gde INHERIT zzw3b_gdp");
        assertEquals(1, num("SELECT count(*)::int FROM pg_inherits"
                + " WHERE inhrelid = 'zzw3b_gde'::regclass"));
        // A column the parent declares NOT NULL has to be NOT NULL on the child as well.
        exec("CREATE TABLE zzw3b_q1 (a int NOT NULL)");
        exec("CREATE TABLE zzw3b_q1c (a int)");
        assertEquals("42804", stateOf("ALTER TABLE zzw3b_q1c INHERIT zzw3b_q1"));
        assertEquals("column \"a\" in child table \"zzw3b_q1c\" must be marked NOT NULL",
                messageOf("ALTER TABLE zzw3b_q1c INHERIT zzw3b_q1"));
        exec("DROP TABLE zzw3b_q1c");
        exec("DROP TABLE zzw3b_q1");
        exec("DROP TABLE zzw3b_gde");
        exec("DROP TABLE zzw3b_gdc");
        exec("DROP TABLE zzw3b_gdp");
    }

    @Test
    void aNewRuleIsJudgedAgainstTheRowsAlreadyStoredBelow() throws Exception {
        exec("CREATE TABLE zzw3b_vc (a int, b text) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw3b_vc_1 PARTITION OF zzw3b_vc FOR VALUES FROM (0) TO (10)");
        exec("INSERT INTO zzw3b_vc VALUES (1, 'bad')");
        // The parent stores nothing of its own, so a scan of its own rows passed vacuously.
        assertEquals("23514",
                stateOf("ALTER TABLE zzw3b_vc ADD CONSTRAINT zzw3b_vck CHECK (b <> 'bad')"));
        assertEquals("check constraint \"zzw3b_vck\" of relation \"zzw3b_vc_1\""
                + " is violated by some row",
                messageOf("ALTER TABLE zzw3b_vc ADD CONSTRAINT zzw3b_vck CHECK (b <> 'bad')"));
        exec("CREATE TABLE zzw3b_nn (a int, b text) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw3b_nn_1 PARTITION OF zzw3b_nn FOR VALUES FROM (0) TO (10)");
        exec("INSERT INTO zzw3b_nn (a) VALUES (1)");
        assertEquals("23502", stateOf("ALTER TABLE zzw3b_nn ALTER COLUMN b SET NOT NULL"));
        assertEquals("column \"b\" of relation \"zzw3b_nn_1\" contains null values",
                messageOf("ALTER TABLE zzw3b_nn ALTER COLUMN b SET NOT NULL"));
        exec("CREATE TABLE zzw3b_ip (a int, b text)");
        exec("CREATE TABLE zzw3b_ic () INHERITS (zzw3b_ip)");
        exec("INSERT INTO zzw3b_ic VALUES (1, NULL)");
        assertEquals("column \"b\" of relation \"zzw3b_ic\" contains null values",
                messageOf("ALTER TABLE zzw3b_ip ALTER COLUMN b SET NOT NULL"));
        assertEquals("check constraint \"zzw3b_ick\" of relation \"zzw3b_ic\""
                + " is violated by some row",
                messageOf("ALTER TABLE zzw3b_ip ADD CONSTRAINT zzw3b_ick CHECK (a > 5)"));
        exec("DROP TABLE zzw3b_ic");
        exec("DROP TABLE zzw3b_ip");
        exec("DROP TABLE zzw3b_nn");
        exec("DROP TABLE zzw3b_vc");
    }

    @Test
    void aPartitionCreatedLaterInheritsWhatTheParentAlreadyHas() throws Exception {
        exec("CREATE TABLE zzw3b_fh (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE INDEX zzw3b_fh_idx ON zzw3b_fh (s)");
        exec("CREATE TABLE zzw3b_fh_1 PARTITION OF zzw3b_fh FOR VALUES FROM (1) TO (10)");
        // The index is a rule about the hierarchy, not about the partitions declared first.
        assertEquals(1, num("SELECT count(*)::int FROM pg_index i"
                + " JOIN pg_class c ON i.indrelid = c.oid WHERE c.relname = 'zzw3b_fh_1'"));
        exec("INSERT INTO zzw3b_fh VALUES (5, 'x')");
        assertEquals("5", scalar("SELECT i FROM zzw3b_fh WHERE s = 'x'"));
        // Creating a partition over rows the default is already holding would strand them.
        exec("CREATE TABLE zzw3b_dr (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzw3b_dr_d PARTITION OF zzw3b_dr DEFAULT");
        exec("INSERT INTO zzw3b_dr VALUES (5)");
        assertEquals("23514",
                stateOf("CREATE TABLE zzw3b_dr_1 PARTITION OF zzw3b_dr FOR VALUES FROM (1) TO (10)"));
        assertEquals("updated partition constraint for default partition \"zzw3b_dr_d\""
                + " would be violated by some row",
                messageOf("CREATE TABLE zzw3b_dr_1 PARTITION OF zzw3b_dr"
                        + " FOR VALUES FROM (1) TO (10)"));
        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname = 'zzw3b_dr_1'"));
        // A bound that claims none of the default's rows is accepted as it always was.
        exec("CREATE TABLE zzw3b_dr_2 PARTITION OF zzw3b_dr FOR VALUES FROM (20) TO (30)");
        assertEquals(1, num("SELECT count(*)::int FROM zzw3b_dr_d"));
        exec("DROP TABLE zzw3b_dr");
        exec("DROP TABLE zzw3b_fh");
    }

    @Test
    void thePartitionKeyColumnMayNotBeDroppedOrRetyped() throws Exception {
        exec("CREATE TABLE zzw3b_kc (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzw3b_kc_1 PARTITION OF zzw3b_kc FOR VALUES FROM (1) TO (10)");
        assertEquals("42P16", stateOf("ALTER TABLE zzw3b_kc DROP COLUMN i"));
        assertEquals("cannot drop column \"i\" because it is part of the partition key"
                + " of relation \"zzw3b_kc\"", messageOf("ALTER TABLE zzw3b_kc DROP COLUMN i"));
        assertEquals("cannot alter column \"i\" because it is part of the partition key"
                + " of relation \"zzw3b_kc\"",
                messageOf("ALTER TABLE zzw3b_kc ALTER COLUMN i TYPE bigint"));
        assertEquals(2, num("SELECT count(*)::int FROM information_schema.columns"
                + " WHERE table_name = 'zzw3b_kc'"));
        // A column the key does not read is dropped as it always was.
        exec("ALTER TABLE zzw3b_kc DROP COLUMN s");
        // An expression key stands for every column the expression names, and the guard holds
        // whether or not the table has any partitions yet.
        exec("CREATE TABLE zzw3b_ke (i int, s text) PARTITION BY RANGE ((i + 1))");
        assertEquals("42P16", stateOf("ALTER TABLE zzw3b_ke DROP COLUMN i"));
        exec("ALTER TABLE zzw3b_ke DROP COLUMN s");
        exec("CREATE TABLE zzw3b_km (i int, s text) PARTITION BY RANGE (i, s)");
        assertEquals("42P16", stateOf("ALTER TABLE zzw3b_km ALTER COLUMN s TYPE varchar(9)"));
        exec("DROP TABLE zzw3b_km");
        exec("DROP TABLE zzw3b_ke");
        exec("DROP TABLE zzw3b_kc");
    }

    @Test
    void attachPartitionRequiresNotNullWhereThePartitionedTableHasIt() throws Exception {
        exec("CREATE TABLE zzw3b_at (i int NOT NULL, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzw3b_at_c (i int, s text)");
        exec("INSERT INTO zzw3b_at_c VALUES (5, 'a')");
        assertEquals("42804", stateOf("ALTER TABLE zzw3b_at ATTACH PARTITION zzw3b_at_c"
                + " FOR VALUES FROM (1) TO (10)"));
        assertEquals("column \"i\" in child table \"zzw3b_at_c\" must be marked NOT NULL",
                messageOf("ALTER TABLE zzw3b_at ATTACH PARTITION zzw3b_at_c"
                        + " FOR VALUES FROM (1) TO (10)"));
        // The attach did not happen, so the partitioned table did not take the row.
        assertEquals(0, num("SELECT count(*)::int FROM zzw3b_at"));
        exec("ALTER TABLE zzw3b_at_c ALTER COLUMN i SET NOT NULL");
        exec("ALTER TABLE zzw3b_at ATTACH PARTITION zzw3b_at_c FOR VALUES FROM (1) TO (10)");
        assertEquals(1, num("SELECT count(*)::int FROM zzw3b_at"));
        // The rule is about the parent's own nullability, not about the key: a nullable key
        // column is accepted when the partitioned table's is nullable too.
        exec("CREATE TABLE zzw3b_a2 (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzw3b_a2_c (i int, s text)");
        exec("ALTER TABLE zzw3b_a2 ATTACH PARTITION zzw3b_a2_c FOR VALUES FROM (1) TO (10)");
        assertEquals("YES", scalar("SELECT is_nullable FROM information_schema.columns"
                + " WHERE table_name = 'zzw3b_a2_c' AND column_name = 'i'"));
        exec("DROP TABLE zzw3b_a2");
        exec("DROP TABLE zzw3b_at");
    }

    // ------------------------------------------------------------ A definition is checked before it is built

    @Test
    void tablesampleReadsASchemaQualifiedRelation() throws Exception {
        exec("CREATE SCHEMA zzc3_ss");
        exec("CREATE TABLE zzc3_ss.zzc3_ts (a int)");
        exec("INSERT INTO zzc3_ss.zzc3_ts VALUES (1),(2),(3)");
        // PG 18: 3 -- the sampled relation is the one the qualifier names.
        assertEquals(3L, num("SELECT count(*) FROM zzc3_ss.zzc3_ts TABLESAMPLE BERNOULLI (100)"));
        exec("DROP TABLE zzc3_ss.zzc3_ts");
        exec("DROP SCHEMA zzc3_ss");
    }

    @Test
    void repeatableTakesADoublePrecisionSeed() throws Exception {
        exec("CREATE TABLE zzc3_ts2 (a int)");
        exec("INSERT INTO zzc3_ts2 SELECT g FROM generate_series(1,20) g");
        // PG 18: 20. REPEATABLE is declared over double precision, so 1.5 is a seed.
        assertEquals(20L, num("SELECT count(*) FROM zzc3_ts2 TABLESAMPLE BERNOULLI (100) REPEATABLE (1.5)"));
        assertEquals("2202G", stateOf("SELECT count(*) FROM zzc3_ts2 TABLESAMPLE BERNOULLI (100) REPEATABLE (NULL)"));
        assertEquals("2202H", stateOf("SELECT count(*) FROM zzc3_ts2 TABLESAMPLE BERNOULLI (NULL)"));
        exec("DROP TABLE zzc3_ts2");
    }

    @Test
    void sequenceFunctionsNameTheKindOfRelationTheyFound() throws Exception {
        exec("CREATE TABLE zzc3_ntq (a int)");
        exec("CREATE VIEW zzc3_ntqv AS SELECT * FROM zzc3_ntq");
        exec("CREATE INDEX zzc3_ntqi ON zzc3_ntq (a)");
        // PG 18: 42809 cannot open relation "zzc3_ntq", with the kind on the detail line.
        assertEquals("42809", stateOf("SELECT nextval('zzc3_ntq')"));
        assertEquals("cannot open relation \"zzc3_ntq\"", messageOf("SELECT nextval('zzc3_ntq')"));
        assertEquals("This operation is not supported for tables.", detailOf("SELECT nextval('zzc3_ntq')"));
        assertEquals("This operation is not supported for tables.", detailOf("SELECT currval('zzc3_ntq')"));
        assertEquals("This operation is not supported for tables.", detailOf("SELECT setval('zzc3_ntq', 1)"));
        assertEquals("This operation is not supported for tables.",
                detailOf("SELECT pg_sequence_last_value('zzc3_ntq'::regclass)"));
        assertEquals("This operation is not supported for views.", detailOf("SELECT nextval('zzc3_ntqv')"));
        assertEquals("This operation is not supported for indexes.", detailOf("SELECT nextval('zzc3_ntqi')"));
        // A name that reaches no relation at all is still 42P01.
        assertEquals("42P01", stateOf("SELECT nextval('zzc3_nosuch')"));
        exec("DROP VIEW zzc3_ntqv");
        exec("DROP TABLE zzc3_ntq");
    }

    @Test
    void aRegclassArgumentIsReadAsARelationName() throws Exception {
        // PG 18: the quotes are identifier quoting, so the message quotes the name back.
        assertEquals("relation \"zzc3_NOSUCH\" does not exist", messageOf("SELECT nextval('\"zzc3_NOSUCH\"')"));
        assertEquals("relation \"zzc3_NOSUCH\" does not exist", messageOf("SELECT currval('\"zzc3_NOSUCH\"')"));
        assertEquals("relation \"zzc3_NOSUCH\" does not exist", messageOf("SELECT setval('\"zzc3_NOSUCH\"', 1)"));
        // An unquoted name is folded, and a qualified one is quoted back whole.
        assertEquals("relation \"zzc3_nosuch2\" does not exist", messageOf("SELECT nextval('ZZC3_NOSUCH2')"));
        assertEquals("relation \"public.zzc3_nosuch3\" does not exist",
                messageOf("SELECT nextval('public.zzc3_nosuch3')"));
    }

    @Test
    void setStatisticsPointsAtTheLiteralAsWritten() throws Exception {
        exec("CREATE TABLE zzc3_r3 (a int, b int)");
        // PG 18: syntax error at or near "'x'" -- the quotes are part of what was written.
        assertEquals("42601", stateOf("ALTER TABLE zzc3_r3 ALTER COLUMN a SET STATISTICS 'x'"));
        assertEquals("syntax error at or near \"'x'\"",
                messageOf("ALTER TABLE zzc3_r3 ALTER COLUMN a SET STATISTICS 'x'"));
        exec("DROP TABLE zzc3_r3");
    }

    @Test
    void aTypedTableTakesItsColumnsFromTheType() throws Exception {
        exec("CREATE TYPE zzc3_ct AS (x int, y text)");
        exec("CREATE TABLE zzc3_of OF zzc3_ct");
        assertEquals("x,y", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name='zzc3_of'"));
        exec("INSERT INTO zzc3_of VALUES (1,'a')");
        assertEquals("a", scalar("SELECT y FROM zzc3_of WHERE x = 1"));
        // A constraint list is what may be written after the type.
        exec("CREATE TABLE zzc3_of2 OF zzc3_ct (PRIMARY KEY (x))");
        assertEquals(1L, num("SELECT count(*) FROM pg_constraint"
                + " WHERE conname='zzc3_of2_pkey' AND contype='p'"));
        // PG 18: 42704 for a type that is not there, 42809 for a table's own row type.
        assertEquals("42704", stateOf("CREATE TABLE zzc3_of3 OF zzc3_nosuchtype"));
        exec("CREATE TABLE zzc3_ofx (a int)");
        assertEquals("42809", stateOf("CREATE TABLE zzc3_of4 OF zzc3_ofx"));
        exec("DROP TABLE zzc3_ofx");
        exec("DROP TABLE zzc3_of2");
        exec("DROP TABLE zzc3_of");
        exec("DROP TYPE zzc3_ct");
    }

    @Test
    void aDomainKeepsEveryCheckItWasWrittenWith() throws Exception {
        exec("CREATE DOMAIN zzc3_d10 AS int CHECK (VALUE > 0) CHECK (VALUE < 10)");
        // PG 18: the first is <domain>_check, the second <domain>_check1, and both are enforced.
        assertEquals("23514", stateOf("SELECT (-1)::zzc3_d10"));
        assertEquals("value for domain zzc3_d10 violates check constraint \"zzc3_d10_check\"",
                messageOf("SELECT (-1)::zzc3_d10"));
        assertEquals("value for domain zzc3_d10 violates check constraint \"zzc3_d10_check1\"",
                messageOf("SELECT 11::zzc3_d10"));
        assertEquals("5", scalar("SELECT (5::zzc3_d10)::text"));
        assertEquals("zzc3_d10_check,zzc3_d10_check1", scalar("SELECT string_agg(conname, ',' ORDER BY conname)"
                + " FROM pg_constraint WHERE contypid = 'zzc3_d10'::regtype"));
        // A CONSTRAINT clause names the CHECK that follows it, and the next unnamed one takes the
        // first free generated name.
        exec("CREATE DOMAIN zzc3_d12 AS int CONSTRAINT k CHECK (VALUE > 0) CHECK (VALUE < 9)");
        assertEquals("k,zzc3_d12_check", scalar("SELECT string_agg(conname, ',' ORDER BY conname)"
                + " FROM pg_constraint WHERE contypid = 'zzc3_d12'::regtype"));
        assertEquals("value for domain zzc3_d12 violates check constraint \"k\"",
                messageOf("SELECT (-1)::zzc3_d12"));
        exec("DROP DOMAIN zzc3_d12");
        exec("DROP DOMAIN zzc3_d10");
    }

    @Test
    void aDomainRefusesTwoConstraintsOfOneName() throws Exception {
        // PG 18: 42710, and nothing is created -- the type does not exist afterwards.
        assertEquals("42710",
                stateOf("CREATE DOMAIN zzc3_d11 AS int CONSTRAINT k CHECK (VALUE > 0) CONSTRAINT k CHECK (VALUE < 9)"));
        assertEquals("constraint \"k\" for domain \"zzc3_d11\" already exists",
                messageOf("CREATE DOMAIN zzc3_d11 AS int CONSTRAINT k CHECK (VALUE > 0) CONSTRAINT k CHECK (VALUE < 9)"));
        assertEquals("42704", stateOf("SELECT (-1)::zzc3_d11"));
    }

    @Test
    void aGeneratedExpressionIsReadWithTheColumnsType() throws Exception {
        // PG 18: 22P02 invalid input syntax for type integer: "abc".
        assertEquals("22P02", stateOf("CREATE TABLE zzc3_gc1 (a int, b int GENERATED ALWAYS AS ('abc') STORED)"));
        assertEquals("invalid input syntax for type integer: \"abc\"",
                messageOf("CREATE TABLE zzc3_gc1 (a int, b int GENERATED ALWAYS AS ('abc') STORED)"));
        // One the type can read is taken, and the column really holds it.
        exec("CREATE TABLE zzc3_gc2 (a int, b int GENERATED ALWAYS AS ('12') STORED)");
        exec("INSERT INTO zzc3_gc2 (a) VALUES (1)");
        assertEquals("12", scalar("SELECT b::text FROM zzc3_gc2"));
        exec("DROP TABLE zzc3_gc2");
    }

    @Test
    void likeContributesColumnsThatCanClash() throws Exception {
        exec("CREATE TABLE zzc3_lsrc (a int, b text)");
        // PG 18: 42701 -- a LIKE column and a written one of that name are the same clash.
        assertEquals("42701", stateOf("CREATE TABLE zzc3_l1 (a int, LIKE zzc3_lsrc)"));
        assertEquals("column \"a\" specified more than once",
                messageOf("CREATE TABLE zzc3_l1 (a int, LIKE zzc3_lsrc)"));
        assertEquals("42701", stateOf("CREATE TABLE zzc3_l2 (LIKE zzc3_lsrc, a int)"));
        assertEquals("42701", stateOf("CREATE TABLE zzc3_l3 (LIKE zzc3_lsrc, LIKE zzc3_lsrc)"));
        // An inherited column is merged, not refused.
        exec("CREATE TABLE zzc3_lp (a int)");
        exec("CREATE TABLE zzc3_l4 (LIKE zzc3_lsrc) INHERITS (zzc3_lp)");
        exec("DROP TABLE zzc3_l4");
        exec("DROP TABLE zzc3_lp");
        exec("DROP TABLE zzc3_lsrc");
    }

    @Test
    void likeOnASequenceIsRefused() throws Exception {
        exec("CREATE SEQUENCE zzc3_lseq");
        // PG 18: 42809, with the kind on the detail line.
        assertEquals("42809", stateOf("CREATE TABLE zzc3_l5 (LIKE zzc3_lseq)"));
        assertEquals("relation \"zzc3_lseq\" is invalid in LIKE clause",
                messageOf("CREATE TABLE zzc3_l5 (LIKE zzc3_lseq)"));
        assertEquals("This operation is not supported for sequences.",
                detailOf("CREATE TABLE zzc3_l5 (LIKE zzc3_lseq)"));
        // A view is a relation with columns, so LIKE copies it.
        exec("CREATE TABLE zzc3_lvt (a int)");
        exec("CREATE VIEW zzc3_lv AS SELECT a FROM zzc3_lvt");
        exec("CREATE TABLE zzc3_l6 (LIKE zzc3_lv)");
        exec("DROP TABLE zzc3_l6");
        exec("DROP VIEW zzc3_lv");
        exec("DROP TABLE zzc3_lvt");
        exec("DROP SEQUENCE zzc3_lseq");
    }

    @Test
    void anUnknownCollationIsRefusedWhereItIsWritten() throws Exception {
        exec("CREATE TABLE zzc3_misc (a int)");
        // PG 18: 42704, and the column is not created.
        assertEquals("42704", stateOf("ALTER TABLE zzc3_misc ADD COLUMN e text COLLATE \"nosuch_collation\""));
        assertEquals("collation \"nosuch_collation\" for encoding \"UTF8\" does not exist",
                messageOf("ALTER TABLE zzc3_misc ADD COLUMN e text COLLATE \"nosuch_collation\""));
        assertEquals(0L, num("SELECT count(*) FROM information_schema.columns"
                + " WHERE table_name='zzc3_misc' AND column_name='e'"));
        assertEquals("42704", stateOf("CREATE TABLE zzc3_coll (a text COLLATE \"nosuch2\")"));
        assertEquals("42704", stateOf("CREATE DOMAIN zzc3_dc AS text COLLATE \"nosuch3\""));
        // A collation that does exist is still accepted.
        exec("ALTER TABLE zzc3_misc ADD COLUMN f text COLLATE \"C\"");
        exec("DROP TABLE zzc3_misc");
    }

    @Test
    void clusterOnNamesAnIndexOfThisRelation() throws Exception {
        exec("CREATE TABLE zzc3_cm (a int PRIMARY KEY, b int)");
        exec("CREATE INDEX zzc3_cmi ON zzc3_cm (b)");
        exec("CREATE TABLE zzc3_co (c int)");
        exec("CREATE INDEX zzc3_coi ON zzc3_co (c)");
        // PG 18: 42704 for a name that reaches no index, 42809 for another relation's.
        assertEquals("42704", stateOf("ALTER TABLE zzc3_cm CLUSTER ON zzc3_nosuchindex"));
        assertEquals("index \"zzc3_nosuchindex\" for table \"zzc3_cm\" does not exist",
                messageOf("ALTER TABLE zzc3_cm CLUSTER ON zzc3_nosuchindex"));
        assertEquals("42809", stateOf("ALTER TABLE zzc3_cm CLUSTER ON zzc3_coi"));
        // Its own index, and its key's index, are both accepted.
        exec("ALTER TABLE zzc3_cm CLUSTER ON zzc3_cmi");
        exec("ALTER TABLE zzc3_cm CLUSTER ON zzc3_cm_pkey");
        exec("DROP TABLE zzc3_co");
        exec("DROP TABLE zzc3_cm");
    }

    @Test
    void aModifierOnATypeThatHasNoneIsRefused() throws Exception {
        // PG 18: the words its grammar has a production for stop at the parenthesis.
        assertEquals("42601", stateOf("CREATE TABLE zzc3_i5 (a int(5))"));
        assertEquals("syntax error at or near \"(\"", messageOf("CREATE TABLE zzc3_i5 (a int(5))"));
        assertEquals("syntax error at or near \"(\"", messageOf("CREATE TABLE zzc3_i6 (a boolean(3))"));
        assertEquals("syntax error at or near \"(\"", messageOf("SELECT 1::int(5)"));
        // A type read as a plain name is looked up and found to have no modifier.
        assertEquals("type modifier is not allowed for type \"text\"",
                messageOf("CREATE TABLE zzc3_i7 (a text(5))"));
        assertEquals("type modifier is not allowed for type \"int4\"",
                messageOf("CREATE TABLE zzc3_i8 (a int4(5))"));
        assertEquals("type modifier is not allowed for type \"date\"",
                messageOf("CREATE TABLE zzc3_i9 (a date(3))"));
        // The types that do carry one are untouched.
        exec("CREATE TABLE zzc3_i10 (a varchar(5), b numeric(10,2), c timestamp(3), d float(5), e bit(3))");
        assertEquals("5", scalar("SELECT character_maximum_length::text FROM information_schema.columns"
                + " WHERE table_name='zzc3_i10' AND column_name='a'"));
        exec("DROP TABLE zzc3_i10");
    }

    @Test
    void aDeferrableKeyCannotBackAForeignKey() throws Exception {
        exec("CREATE TABLE zzc3_fu (d int UNIQUE DEFERRABLE)");
        // PG 18: 55000, worded by how the reference was written.
        assertEquals("55000", stateOf("CREATE TABLE zzc3_fc (p int REFERENCES zzc3_fu(d))"));
        assertEquals("cannot use a deferrable unique constraint for referenced table \"zzc3_fu\"",
                messageOf("CREATE TABLE zzc3_fc (p int REFERENCES zzc3_fu(d))"));
        exec("CREATE TABLE zzc3_fp (a int PRIMARY KEY DEFERRABLE)");
        assertEquals("cannot use a deferrable primary key for referenced table \"zzc3_fp\"",
                messageOf("CREATE TABLE zzc3_fq (b int REFERENCES zzc3_fp)"));
        // An ordinary key still backs one.
        exec("CREATE TABLE zzc3_fp2 (a int PRIMARY KEY)");
        exec("CREATE TABLE zzc3_fq2 (b int REFERENCES zzc3_fp2)");
        exec("DROP TABLE zzc3_fq2");
        exec("DROP TABLE zzc3_fp2");
        exec("DROP TABLE zzc3_fp");
        exec("DROP TABLE zzc3_fu");
    }

    @Test
    void anExclusionOnNotEqualIsEnforced() throws Exception {
        exec("CREATE TABLE zzc3_x6 (a int, EXCLUDE USING gist (a WITH <>))");
        assertEquals(1, update("INSERT INTO zzc3_x6 VALUES (1)"));
        // PG 18: 23P01 -- 2 is unlike 1, and unlike is what this constraint excludes.
        assertEquals("23P01", stateOf("INSERT INTO zzc3_x6 VALUES (2)"));
        assertEquals("conflicting key value violates exclusion constraint \"zzc3_x6_a_excl\"",
                messageOf("INSERT INTO zzc3_x6 VALUES (2)"));
        // A value like the stored one is not excluded.
        assertEquals(1, update("INSERT INTO zzc3_x6 VALUES (1)"));
        exec("DROP TABLE zzc3_x6");
    }

    @Test
    void aNonCommutativeExclusionOperatorIsRefused() throws Exception {
        // PG 18: 42809, with the operator named by the types it would have compared.
        assertEquals("42809", stateOf("CREATE TABLE zzc3_ex1 (a int, EXCLUDE USING btree (a WITH <))"));
        assertEquals("operator <(integer,integer) is not commutative",
                messageOf("CREATE TABLE zzc3_ex1 (a int, EXCLUDE USING btree (a WITH <))"));
        assertEquals("Only commutative operators can be used in exclusion constraints.",
                detailOf("CREATE TABLE zzc3_ex1 (a int, EXCLUDE USING btree (a WITH <))"));
        assertEquals("42809", stateOf("CREATE TABLE zzc3_ex2 (a int, EXCLUDE USING btree (a WITH >=))"));
        assertEquals("42809",
                stateOf("CREATE TABLE zzc3_ex3 (a int, b int, EXCLUDE USING btree (a WITH =, b WITH <))"));
        // Equality is commutative, so the ordinary form is untouched.
        exec("CREATE TABLE zzc3_ex4 (a int, EXCLUDE (a WITH =))");
        exec("DROP TABLE zzc3_ex4");
    }

    @Test
    void aSetReturningFunctionInACheckIsRefused() throws Exception {
        // PG 18: 0A000 -- a CHECK answers about one row and an SRF answers with rows.
        assertEquals("0A000", stateOf("CREATE TABLE zzc3_ck3 (a int CHECK (generate_series(1,a) > 0))"));
        assertEquals("set-returning functions are not allowed in check constraints",
                messageOf("CREATE TABLE zzc3_ck3 (a int CHECK (generate_series(1,a) > 0))"));
        exec("CREATE TABLE zzc3_ck4 (a int)");
        assertEquals("0A000", stateOf("ALTER TABLE zzc3_ck4 ADD CHECK (generate_series(1,a) > 0)"));
        // An ordinary CHECK is still stored and enforced.
        exec("ALTER TABLE zzc3_ck4 ADD CHECK (a > 0)");
        assertEquals("23514", stateOf("INSERT INTO zzc3_ck4 VALUES (-1)"));
        exec("DROP TABLE zzc3_ck4");
    }

    @Test
    void aViewColumnDefaultReachesTheCatalogue() throws Exception {
        exec("CREATE TABLE zzc3_v2t (i int, v text)");
        exec("CREATE VIEW zzc3_v2v AS SELECT i, v FROM zzc3_v2t");
        exec("ALTER VIEW zzc3_v2v ALTER COLUMN v SET DEFAULT 'zz'");
        // PG 18: 'zz'::text
        assertEquals("'zz'::text", scalar("SELECT column_default FROM information_schema.columns"
                + " WHERE table_name='zzc3_v2v' AND column_name='v'"));
        exec("INSERT INTO zzc3_v2v (i) VALUES (1)");
        assertEquals("zz", scalar("SELECT v FROM zzc3_v2t WHERE i = 1"));
        exec("ALTER VIEW zzc3_v2v ALTER COLUMN v DROP DEFAULT");
        assertNull(scalar("SELECT column_default FROM information_schema.columns"
                + " WHERE table_name='zzc3_v2v' AND column_name='v'"));
        exec("DROP VIEW zzc3_v2v");
        exec("DROP TABLE zzc3_v2t");
    }

    @Test
    void renamingABaseColumnLeavesTheViewsOwnNameAlone() throws Exception {
        exec("CREATE TABLE zzc3_v1t (i int, v text)");
        exec("INSERT INTO zzc3_v1t VALUES (1,'a')");
        exec("CREATE VIEW zzc3_v1v AS SELECT i, v FROM zzc3_v1t");
        exec("ALTER TABLE zzc3_v1t RENAME COLUMN v TO v2");
        // PG 18: the view still publishes v, and reads it from v2.
        assertEquals("a", scalar("SELECT v FROM zzc3_v1v"));
        assertEquals("i,v", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name='zzc3_v1v'"));
        assertTrue(scalar("SELECT pg_get_viewdef('zzc3_v1v'::regclass)").contains("v2 AS v"));
        exec("DROP VIEW zzc3_v1v");
        exec("DROP TABLE zzc3_v1t");
    }

    // ------------------------------------------------------------ A trigger and a rule are objects with dependencies

    @Test
    void aRowTriggerOnAPartitionedTableIsClonedOntoEveryPartition() throws Exception {
        exec("CREATE TABLE dtr_ptl (m text)");
        exec("CREATE FUNCTION dtr_ptf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "INSERT INTO dtr_ptl VALUES (TG_NAME||'@'||TG_TABLE_NAME); RETURN NULL; END $$");
        exec("CREATE TABLE dtr_pt (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE dtr_pta PARTITION OF dtr_pt FOR VALUES FROM (0) TO (100)");
        exec("CREATE TRIGGER dtr_ptg AFTER INSERT ON dtr_pt FOR EACH ROW EXECUTE FUNCTION dtr_ptf()");
        // a partition created after the trigger gets a copy too
        exec("CREATE TABLE dtr_ptb PARTITION OF dtr_pt FOR VALUES FROM (100) TO (200)");
        String rows = scalar("SELECT string_agg(c.relname || '=' || n::text, ',' ORDER BY c.relname)"
                + "  FROM (SELECT t.tgrelid, count(*) AS n FROM pg_trigger t"
                + "         WHERE NOT t.tgisinternal GROUP BY t.tgrelid) s"
                + "  JOIN pg_class c ON c.oid = s.tgrelid"
                + " WHERE c.relname IN ('dtr_pt','dtr_pta','dtr_ptb')");
        assertEquals("dtr_pt=1,dtr_pta=1,dtr_ptb=1", rows);
        // the copies point back at the trigger they came from; the original does not
        assertEquals("2", scalar("SELECT count(*)::text FROM pg_trigger"
                + " WHERE tgname = 'dtr_ptg' AND tgparentid <> 0"));
        exec("DROP TABLE dtr_pt CASCADE");
        exec("DROP TABLE dtr_ptl");
        exec("DROP FUNCTION dtr_ptf() CASCADE");
    }

    @Test
    void aPartitionsCopyOfATriggerCannotBeDroppedOnItsOwn() throws Exception {
        exec("CREATE FUNCTION dtr_df() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$");
        exec("CREATE TABLE dtr_dp (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE dtr_dpa PARTITION OF dtr_dp FOR VALUES FROM (0) TO (100)");
        exec("CREATE TRIGGER dtr_dtg AFTER INSERT ON dtr_dp FOR EACH ROW EXECUTE FUNCTION dtr_df()");
        assertEquals("2BP01", stateOf("DROP TRIGGER dtr_dtg ON dtr_dpa"));
        assertEquals("cannot drop trigger dtr_dtg on table dtr_dpa because trigger dtr_dtg on table"
                + " dtr_dp requires it", messageOf("DROP TRIGGER dtr_dtg ON dtr_dpa"));
        // dropping the original takes the partition's copy with it
        exec("DROP TRIGGER dtr_dtg ON dtr_dp");
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_trigger WHERE tgname = 'dtr_dtg'"));
        exec("DROP TABLE dtr_dp CASCADE");
        exec("DROP FUNCTION dtr_df() CASCADE");
    }

    @Test
    void aClonedPartitionTriggerFiresOnceForOneRow() throws Exception {
        // Requires the DmlExecutor.rowTriggersFor change reported under unresolved: with the copy
        // registered on the partition and the walk up to the parent still in place the trigger
        // fires twice for one row.
        exec("CREATE TABLE dtr_fl (m text)");
        exec("CREATE FUNCTION dtr_ff() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "INSERT INTO dtr_fl VALUES (TG_NAME||'@'||TG_TABLE_NAME); RETURN NULL; END $$");
        exec("CREATE TABLE dtr_fp (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE dtr_fpa PARTITION OF dtr_fp FOR VALUES FROM (0) TO (100)");
        exec("CREATE TRIGGER dtr_ftg AFTER INSERT ON dtr_fp FOR EACH ROW EXECUTE FUNCTION dtr_ff()");
        update("INSERT INTO dtr_fp VALUES (5)");
        update("INSERT INTO dtr_fpa VALUES (6)");
        assertEquals("2", scalar("SELECT count(*)::text FROM dtr_fl"));
        assertEquals("dtr_ftg@dtr_fpa", scalar("SELECT DISTINCT m FROM dtr_fl"));
        exec("DROP TABLE dtr_fp CASCADE");
        exec("DROP TABLE dtr_fl");
        exec("DROP FUNCTION dtr_ff() CASCADE");
    }

    @Test
    void aTriggerIsRefusedOnASequenceAndOnAMaterializedView() throws Exception {
        exec("CREATE FUNCTION dtr_kf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$");
        exec("CREATE SEQUENCE dtr_ks");
        exec("CREATE MATERIALIZED VIEW dtr_kmv AS SELECT 1 AS i");
        String onSequence = "CREATE TRIGGER dtr_ktg BEFORE INSERT ON dtr_ks"
                + " FOR EACH ROW EXECUTE FUNCTION dtr_kf()";
        assertEquals("42809", stateOf(onSequence));
        assertEquals("relation \"dtr_ks\" cannot have triggers", messageOf(onSequence));
        assertEquals("This operation is not supported for sequences.", detailOf(onSequence));
        String onMatview = "CREATE TRIGGER dtr_ktg2 BEFORE INSERT ON dtr_kmv"
                + " FOR EACH ROW EXECUTE FUNCTION dtr_kf()";
        assertEquals("42809", stateOf(onMatview));
        assertEquals("relation \"dtr_kmv\" cannot have triggers", messageOf(onMatview));
        assertEquals("This operation is not supported for materialized views.", detailOf(onMatview));
        exec("DROP MATERIALIZED VIEW dtr_kmv");
        exec("DROP SEQUENCE dtr_ks");
        exec("DROP FUNCTION dtr_kf() CASCADE");
    }

    @Test
    void aViewTakesAStatementTriggerButNotARowOne() throws Exception {
        exec("CREATE FUNCTION dtr_vf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$");
        exec("CREATE TABLE dtr_vt (i int)");
        exec("CREATE VIEW dtr_vv AS SELECT i FROM dtr_vt");
        exec("CREATE VIEW dtr_vk AS SELECT 1 AS i");
        exec("CREATE TRIGGER dtr_vtg1 AFTER INSERT ON dtr_vv FOR EACH STATEMENT EXECUTE FUNCTION dtr_vf()");
        exec("CREATE TRIGGER dtr_vtg2 BEFORE INSERT ON dtr_vv FOR EACH STATEMENT EXECUTE FUNCTION dtr_vf()");
        // a view that could never be written through still takes a statement-level trigger
        exec("CREATE TRIGGER dtr_vtg3 AFTER INSERT ON dtr_vk FOR EACH STATEMENT EXECUTE FUNCTION dtr_vf()");
        assertEquals("42809", stateOf("CREATE TRIGGER dtr_vtg4 AFTER INSERT ON dtr_vv"
                + " FOR EACH ROW EXECUTE FUNCTION dtr_vf()"));
        assertEquals("42809", stateOf("CREATE TRIGGER dtr_vtg5 AFTER INSERT ON dtr_vv"
                + " REFERENCING NEW TABLE AS dtr_nt FOR EACH STATEMENT EXECUTE FUNCTION dtr_vf()"));
        assertEquals("Triggers on views cannot have transition tables.",
                detailOf("CREATE TRIGGER dtr_vtg6 AFTER INSERT ON dtr_vv"
                        + " REFERENCING NEW TABLE AS dtr_nt FOR EACH STATEMENT EXECUTE FUNCTION dtr_vf()"));
        exec("DROP VIEW dtr_vk");
        exec("DROP VIEW dtr_vv");
        exec("DROP TABLE dtr_vt");
        exec("DROP FUNCTION dtr_vf() CASCADE");
    }

    @Test
    void aRowTriggerWithATransitionTableIsRefusedOnAPartition() throws Exception {
        exec("CREATE FUNCTION dtr_tf2() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$");
        exec("CREATE TABLE dtr_tp (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE dtr_tpa PARTITION OF dtr_tp FOR VALUES FROM (0) TO (100)");
        String onPartition = "CREATE TRIGGER dtr_ttg AFTER INSERT ON dtr_tpa"
                + " REFERENCING NEW TABLE AS dtr_nt FOR EACH ROW EXECUTE FUNCTION dtr_tf2()";
        assertEquals("0A000", stateOf(onPartition));
        assertEquals("ROW triggers with transition tables are not supported on partitions",
                messageOf(onPartition));
        String onParent = "CREATE TRIGGER dtr_ttg2 AFTER INSERT ON dtr_tp"
                + " REFERENCING NEW TABLE AS dtr_nt FOR EACH ROW EXECUTE FUNCTION dtr_tf2()";
        assertEquals("0A000", stateOf(onParent));
        assertEquals("\"dtr_tp\" is a partitioned table", messageOf(onParent));
        // the statement-level spelling is accepted on both
        exec("CREATE TRIGGER dtr_ttg3 AFTER INSERT ON dtr_tpa REFERENCING NEW TABLE AS dtr_nt"
                + " FOR EACH STATEMENT EXECUTE FUNCTION dtr_tf2()");
        exec("CREATE TRIGGER dtr_ttg4 AFTER INSERT ON dtr_tp REFERENCING NEW TABLE AS dtr_nt"
                + " FOR EACH STATEMENT EXECUTE FUNCTION dtr_tf2()");
        exec("DROP TABLE dtr_tp CASCADE");
        exec("DROP FUNCTION dtr_tf2() CASCADE");
    }

    @Test
    void aTriggerWhenConditionMayReadASystemColumn() throws Exception {
        exec("CREATE FUNCTION dtr_wf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$");
        exec("CREATE TABLE dtr_wt (i int)");
        exec("CREATE TRIGGER dtr_wtg1 AFTER INSERT ON dtr_wt FOR EACH ROW"
                + " WHEN (NEW.ctid IS NOT NULL) EXECUTE FUNCTION dtr_wf()");
        exec("CREATE TRIGGER dtr_wtg2 AFTER INSERT ON dtr_wt FOR EACH ROW"
                + " WHEN (NEW.xmin IS NOT NULL) EXECUTE FUNCTION dtr_wf()");
        // a column that really is not there is still reported
        assertEquals("42703", stateOf("CREATE TRIGGER dtr_wtg3 AFTER INSERT ON dtr_wt FOR EACH ROW"
                + " WHEN (NEW.nosuch IS NOT NULL) EXECUTE FUNCTION dtr_wf()"));
        exec("DROP TABLE dtr_wt CASCADE");
        exec("DROP FUNCTION dtr_wf() CASCADE");
    }

    @Test
    void aConstraintTriggerHasNoReferencingClause() throws Exception {
        exec("CREATE FUNCTION dtr_cf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$");
        exec("CREATE TABLE dtr_ct (i int)");
        String withReferencing = "CREATE CONSTRAINT TRIGGER dtr_ctg AFTER INSERT ON dtr_ct"
                + " REFERENCING NEW TABLE AS dtr_nt FOR EACH ROW EXECUTE FUNCTION dtr_cf()";
        assertEquals("42601", stateOf(withReferencing));
        assertEquals("syntax error at or near \"REFERENCING\"", messageOf(withReferencing));
        // the plain form is still accepted
        exec("CREATE CONSTRAINT TRIGGER dtr_ctg2 AFTER INSERT ON dtr_ct DEFERRABLE INITIALLY DEFERRED"
                + " FOR EACH ROW EXECUTE FUNCTION dtr_cf()");
        exec("DROP TABLE dtr_ct CASCADE");
        exec("DROP FUNCTION dtr_cf() CASCADE");
    }

    @Test
    void aFunctionATriggerExecutesCannotBeDropped() throws Exception {
        exec("CREATE TABLE dtr_ft (i int)");
        exec("CREATE FUNCTION dtr_gf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$");
        exec("CREATE TRIGGER dtr_gtg AFTER INSERT ON dtr_ft FOR EACH ROW EXECUTE FUNCTION dtr_gf()");
        assertEquals("2BP01", stateOf("DROP FUNCTION dtr_gf()"));
        assertEquals("cannot drop function dtr_gf() because other objects depend on it",
                messageOf("DROP FUNCTION dtr_gf()"));
        assertEquals("trigger dtr_gtg on table dtr_ft depends on function dtr_gf()",
                detailOf("DROP FUNCTION dtr_gf()"));
        // CASCADE takes the trigger with it
        exec("DROP FUNCTION dtr_gf() CASCADE");
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_trigger WHERE tgname = 'dtr_gtg'"));
        exec("DROP TABLE dtr_ft CASCADE");
    }

    @Test
    void aRulesActionsAreAnalysedWhenTheRuleIsWritten() throws Exception {
        exec("CREATE TABLE dtr_ra (i int)");
        exec("CREATE TABLE dtr_rlog (i int)");
        assertEquals("42P01", stateOf("CREATE RULE dtr_r1 AS ON INSERT TO dtr_ra"
                + " DO ALSO INSERT INTO dtr_nosuch VALUES (1)"));
        assertEquals("relation \"dtr_nosuch\" does not exist",
                messageOf("CREATE RULE dtr_r1 AS ON INSERT TO dtr_ra"
                        + " DO ALSO INSERT INTO dtr_nosuch VALUES (1)"));
        assertEquals("42703", stateOf("CREATE RULE dtr_r2 AS ON INSERT TO dtr_ra"
                + " DO ALSO INSERT INTO dtr_rlog VALUES (NEW.nosuchcol)"));
        assertEquals("column new.nosuchcol does not exist",
                messageOf("CREATE RULE dtr_r2 AS ON INSERT TO dtr_ra"
                        + " DO ALSO INSERT INTO dtr_rlog VALUES (NEW.nosuchcol)"));
        assertEquals("42601", stateOf("CREATE RULE dtr_r3 AS ON INSERT TO dtr_ra"
                + " DO ALSO CREATE TABLE dtr_illegal (x int)"));
        assertEquals("42P01", stateOf("CREATE RULE dtr_r4 AS ON INSERT TO dtr_ra"
                + " DO ALSO SELECT * FROM dtr_nosuch2"));
        // nothing of the sort was stored
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_rules WHERE tablename = 'dtr_ra'"));
        // and a sound rule is still written
        exec("CREATE RULE dtr_r5 AS ON INSERT TO dtr_ra DO ALSO INSERT INTO dtr_rlog VALUES (NEW.i)");
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_rules WHERE tablename = 'dtr_ra'"));
        exec("DROP TABLE dtr_ra CASCADE");
        exec("DROP TABLE dtr_rlog CASCADE");
    }

    @Test
    void aRuleIsRefusedOnASequenceAndASecondOneOnAView() throws Exception {
        exec("CREATE SEQUENCE dtr_rs");
        String onSequence = "CREATE RULE dtr_rsr AS ON INSERT TO dtr_rs DO INSTEAD NOTHING";
        assertEquals("42809", stateOf(onSequence));
        assertEquals("relation \"dtr_rs\" cannot have rules", messageOf(onSequence));
        assertEquals("This operation is not supported for sequences.", detailOf(onSequence));
        exec("CREATE VIEW dtr_rv AS SELECT 1 AS i");
        String onView = "CREATE RULE dtr_rvr AS ON SELECT TO dtr_rv DO INSTEAD SELECT 2 AS i";
        assertEquals("55000", stateOf(onView));
        assertEquals("\"dtr_rv\" is already a view", messageOf(onView));
        exec("CREATE MATERIALIZED VIEW dtr_rmv AS SELECT 1 AS i");
        assertEquals("0A000", stateOf("CREATE RULE dtr_rmr AS ON INSERT TO dtr_rmv DO INSTEAD NOTHING"));
        exec("DROP MATERIALIZED VIEW dtr_rmv");
        exec("DROP VIEW dtr_rv");
        exec("DROP SEQUENCE dtr_rs");
    }

    @Test
    void aRelationARuleWritesToCannotBeDroppedFromUnderIt() throws Exception {
        // Requires the DdlTableExecutor.dropSingleTable hooks reported under unresolved.
        exec("CREATE TABLE dtr_dep (i int)");
        exec("CREATE TABLE dtr_deplog (i int)");
        exec("CREATE RULE dtr_depr AS ON INSERT TO dtr_dep DO ALSO INSERT INTO dtr_deplog VALUES (NEW.i)");
        assertEquals("2BP01", stateOf("DROP TABLE dtr_deplog"));
        assertEquals("cannot drop table dtr_deplog because other objects depend on it",
                messageOf("DROP TABLE dtr_deplog"));
        assertEquals("rule dtr_depr on table dtr_dep depends on table dtr_deplog",
                detailOf("DROP TABLE dtr_deplog"));
        // the ruled relation can still be written to
        assertEquals(1, update("INSERT INTO dtr_dep VALUES (1)"));
        assertEquals("1", scalar("SELECT count(*)::text FROM dtr_deplog"));
        // CASCADE takes the rule away, and the writes go on
        exec("DROP TABLE dtr_deplog CASCADE");
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_rules WHERE tablename = 'dtr_dep'"));
        assertEquals(1, update("INSERT INTO dtr_dep VALUES (2)"));
        exec("DROP TABLE dtr_dep CASCADE");
    }

    // ------------------------------------------------------------ A deferred check fires at the end of the statement

    @Test
    void deferredUniqueAndPrimaryKeySwapWithinOneAutocommitStatement() throws Exception {
        exec("CREATE TABLE zzed_du (id int PRIMARY KEY, pos int,"
                + " CONSTRAINT zzed_du_u UNIQUE (pos) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO zzed_du VALUES (1,1),(2,2)");
        // PostgreSQL checks a DEFERRABLE INITIALLY DEFERRED constraint when the statement's own
        // implicit transaction commits, not as each row is written, so the swap goes through even
        // though the table is momentarily a duplicate.
        assertEquals(2, update("UPDATE zzed_du SET pos = 3 - pos"));
        assertEquals("2", String.valueOf(scalar("SELECT pos FROM zzed_du WHERE id = 1")));
        assertEquals("1", String.valueOf(scalar("SELECT pos FROM zzed_du WHERE id = 2")));

        exec("CREATE TABLE zzed_dpk (id int, CONSTRAINT zzed_dpk_pk PRIMARY KEY (id)"
                + " DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO zzed_dpk VALUES (1),(2)");
        assertEquals(2, update("UPDATE zzed_dpk SET id = 3 - id"));
        assertEquals("2", String.valueOf(num("SELECT count(*) FROM zzed_dpk")));

        exec("DROP TABLE zzed_dpk");
        exec("DROP TABLE zzed_du");
    }

    @Test
    void deferredUniqueStillReportsADuplicateThatOutlivesTheStatement() throws Exception {
        exec("CREATE TABLE zzed_dux (id int PRIMARY KEY, pos int,"
                + " CONSTRAINT zzed_dux_u UNIQUE (pos) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO zzed_dux VALUES (1,1)");
        // Postponing the check is not excusing it: the duplicate is still there when the statement
        // ends, so PostgreSQL reports it -- with the same SQLSTATE and the same DETAIL as before.
        assertEquals("23505", stateOf("INSERT INTO zzed_dux VALUES (2,1)"));
        assertTrue(messageOf("INSERT INTO zzed_dux VALUES (2,1)")
                .contains("duplicate key value violates unique constraint"));
        assertEquals("Key (pos)=(1) already exists.", detailOf("INSERT INTO zzed_dux VALUES (2,1)"));
        // And the refused statement leaves nothing behind: its implicit transaction rolled back.
        assertEquals("1", String.valueOf(num("SELECT count(*) FROM zzed_dux")));
        exec("DROP TABLE zzed_dux");
    }

    @Test
    void deferredForeignKeyIsSatisfiedByAnAfterTriggerAndByALaterCteArm() throws Exception {
        exec("CREATE TABLE zzed_par (id int PRIMARY KEY)");
        exec("CREATE TABLE zzed_chi (id int PRIMARY KEY, pid int, CONSTRAINT zzed_fk"
                + " FOREIGN KEY (pid) REFERENCES zzed_par(id) DEFERRABLE INITIALLY DEFERRED)");
        exec("CREATE FUNCTION zzed_f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO zzed_par VALUES (NEW.pid); RETURN NEW; END $$");
        exec("CREATE TRIGGER zzed_tg AFTER INSERT ON zzed_chi FOR EACH ROW"
                + " EXECUTE FUNCTION zzed_f()");
        // The key is missing when the row is written and present when the statement ends, and the
        // end of the statement is when PostgreSQL looks.
        assertEquals(1, update("INSERT INTO zzed_chi VALUES (1, 100)"));
        assertEquals("1", String.valueOf(num("SELECT count(*) FROM zzed_chi")));
        assertEquals("1", String.valueOf(num("SELECT count(*) FROM zzed_par")));

        exec("CREATE TABLE zzed_par2 (id int PRIMARY KEY)");
        exec("CREATE TABLE zzed_chi2 (id int PRIMARY KEY, pid int REFERENCES zzed_par2(id)"
                + " DEFERRABLE INITIALLY DEFERRED)");
        // Same rule for a data-modifying WITH item: the whole statement, all arms of it, is what
        // the deferred check is made against.
        assertEquals(1, update("WITH ins AS (INSERT INTO zzed_chi2 VALUES (1,5) RETURNING pid)"
                + " INSERT INTO zzed_par2 SELECT pid FROM ins"));
        assertEquals("1", String.valueOf(num("SELECT count(*) FROM zzed_chi2")));
        assertEquals("1", String.valueOf(num("SELECT count(*) FROM zzed_par2")));

        exec("DROP TABLE zzed_chi2");
        exec("DROP TABLE zzed_par2");
        exec("DROP TRIGGER zzed_tg ON zzed_chi");
        exec("DROP TABLE zzed_chi");
        exec("DROP TABLE zzed_par");
        exec("DROP FUNCTION zzed_f()");
    }

    @Test
    void deferredForeignKeyLeftUnsatisfiedIsStillRefusedInAutocommit() throws Exception {
        exec("CREATE TABLE zzed_p3 (id int PRIMARY KEY)");
        exec("CREATE TABLE zzed_c3 (id int PRIMARY KEY, pid int, CONSTRAINT zzed_fk3"
                + " FOREIGN KEY (pid) REFERENCES zzed_p3(id) DEFERRABLE INITIALLY DEFERRED)");
        assertEquals("23503", stateOf("INSERT INTO zzed_c3 VALUES (2, 999)"));
        assertTrue(messageOf("INSERT INTO zzed_c3 VALUES (2, 999)")
                .contains("violates foreign key constraint"));
        assertEquals("Key (pid)=(999) is not present in table \"zzed_p3\".",
                detailOf("INSERT INTO zzed_c3 VALUES (2, 999)"));
        assertEquals("0", String.valueOf(num("SELECT count(*) FROM zzed_c3")));
        exec("DROP TABLE zzed_c3");
        exec("DROP TABLE zzed_p3");
    }

    @Test
    void deferredConstraintTriggerRunsAfterEveryImmediateTriggerOfTheSameStatement() throws Exception {
        exec("CREATE TABLE zzed_ct (i int)");
        exec("CREATE TABLE zzed_ctl (seq serial, t text)");
        exec("CREATE FUNCTION zzed_ctf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO zzed_ctl(t) VALUES ('deferred:'||NEW.i); RETURN NULL; END $$");
        exec("CREATE FUNCTION zzed_ctg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO zzed_ctl(t) VALUES ('immediate:'||NEW.i); RETURN NULL; END $$");
        // The deferred one is registered first, so registration order alone would put it first.
        exec("CREATE CONSTRAINT TRIGGER zzed_ctt AFTER INSERT ON zzed_ct"
                + " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION zzed_ctf()");
        exec("CREATE TRIGGER zzed_ctu AFTER INSERT ON zzed_ct FOR EACH ROW"
                + " EXECUTE FUNCTION zzed_ctg()");
        exec("INSERT INTO zzed_ct VALUES (1),(2)");
        assertEquals("immediate:1,immediate:2,deferred:1,deferred:2",
                String.valueOf(scalar("SELECT string_agg(t, ',' ORDER BY seq) FROM zzed_ctl")));
        exec("DROP TABLE zzed_ct");
        exec("DROP TABLE zzed_ctl");
        exec("DROP FUNCTION zzed_ctf()");
        exec("DROP FUNCTION zzed_ctg()");
    }

    @Test
    void onConflictDoUpdateRefusesGeneratedAndIdentityColumnsBeforeAnyRowIsWritten() throws Exception {
        exec("CREATE TABLE zzed_g (id int PRIMARY KEY, a int,"
                + " g int GENERATED ALWAYS AS (a*2) STORED)");
        // Nothing conflicts, so the DO UPDATE list never runs -- and PostgreSQL refuses the
        // statement all the same, because it settles the list while planning. No row goes in.
        assertEquals("428C9", stateOf("INSERT INTO zzed_g (id,a) VALUES (1,1)"
                + " ON CONFLICT (id) DO UPDATE SET g = 1"));
        assertEquals("0", String.valueOf(num("SELECT count(*) FROM zzed_g")));

        exec("INSERT INTO zzed_g (id,a) VALUES (1,1)");
        assertEquals("428C9", stateOf("INSERT INTO zzed_g (id,a) VALUES (1,7)"
                + " ON CONFLICT (id) DO UPDATE SET g = excluded.a"));
        assertTrue(messageOf("INSERT INTO zzed_g (id,a) VALUES (1,7)"
                        + " ON CONFLICT (id) DO UPDATE SET g = 3")
                .contains("column \"g\" can only be updated to DEFAULT"));
        assertEquals("2", String.valueOf(scalar("SELECT g FROM zzed_g WHERE id = 1")));

        exec("CREATE TABLE zzed_idt (k int PRIMARY KEY, i int GENERATED ALWAYS AS IDENTITY, j int)");
        exec("INSERT INTO zzed_idt (k,j) VALUES (1,1)");
        assertEquals("428C9", stateOf("INSERT INTO zzed_idt (k,j) VALUES (1,2)"
                + " ON CONFLICT (k) DO UPDATE SET i = 5"));
        // Nothing recomputes an identity after a write, so the refusal is what keeps the client's
        // value out of the column.
        assertEquals("1", String.valueOf(scalar("SELECT i FROM zzed_idt WHERE k = 1")));
        assertEquals("1", String.valueOf(scalar("SELECT j FROM zzed_idt WHERE k = 1")));

        exec("DROP TABLE zzed_idt");
        exec("DROP TABLE zzed_g");
    }

    @Test
    void onConflictDoUpdateAppliesSetColumnEqualsDefault() throws Exception {
        exec("CREATE TABLE zzed_dflt (id int PRIMARY KEY, v int DEFAULT 42, w int)");
        exec("INSERT INTO zzed_dflt (id, v, w) VALUES (1, 7, 7)");
        // DO UPDATE is an UPDATE, so DEFAULT means the column's own default here too.
        assertEquals(1, update("INSERT INTO zzed_dflt (id, v, w) VALUES (1, 8, 8)"
                + " ON CONFLICT (id) DO UPDATE SET v = DEFAULT"));
        assertEquals("42", String.valueOf(scalar("SELECT v FROM zzed_dflt WHERE id = 1")));
        assertEquals("7", String.valueOf(scalar("SELECT w FROM zzed_dflt WHERE id = 1")));
        // A column with no default of its own goes to NULL.
        assertEquals(1, update("INSERT INTO zzed_dflt (id, v, w) VALUES (1, 8, 8)"
                + " ON CONFLICT (id) DO UPDATE SET w = DEFAULT"));
        assertNull(scalar("SELECT w FROM zzed_dflt WHERE id = 1"));

        exec("CREATE TABLE zzed_gd (id int PRIMARY KEY, a int,"
                + " g int GENERATED ALWAYS AS (a*2) STORED)");
        exec("INSERT INTO zzed_gd (id,a) VALUES (1,1)");
        // DEFAULT is the one thing a generated column accepts, and it means "compute it again".
        assertEquals(1, update("INSERT INTO zzed_gd (id,a) VALUES (1,9)"
                + " ON CONFLICT (id) DO UPDATE SET g = DEFAULT, a = 11"));
        assertEquals("11", String.valueOf(scalar("SELECT a FROM zzed_gd WHERE id = 1")));
        assertEquals("22", String.valueOf(scalar("SELECT g FROM zzed_gd WHERE id = 1")));

        exec("DROP TABLE zzed_gd");
        exec("DROP TABLE zzed_dflt");
    }

    // ------------------------------------------------------------ A composite carries its type, and a statement sees one snapshot

    @Test
    void domainOnACompositeFieldJudgesEveryWaySpellingOfAWrite() throws Exception {
        exec("CREATE DOMAIN fiso_d AS int CHECK (VALUE > 0)");
        exec("CREATE TYPE fiso_c AS (a fiso_d, b text)");
        exec("CREATE TABLE fiso_ct (id int, c fiso_c)");
        try {
            // A composite written as a bare text literal builds a value of every field's type,
            // so the domain judges the field it types.
            assertEquals("23514", stateOf("INSERT INTO fiso_ct VALUES (1, '(-1,x)')"));
            assertTrue(messageOf("INSERT INTO fiso_ct VALUES (1, '(-1,x)')")
                    .contains("violates check constraint"));
            assertEquals("0", scalar("SELECT count(*)::text FROM fiso_ct"));

            // An assignment to one field builds a value of that field's type too.
            exec("INSERT INTO fiso_ct VALUES (2, ROW(5,'y')::fiso_c)");
            assertEquals("23514", stateOf("UPDATE fiso_ct SET c.a = -9 WHERE id = 2"));
            assertEquals("(5,y)", scalar("SELECT c::text FROM fiso_ct WHERE id = 2"));

            // What the domain accepts is still written, by either spelling.
            update("UPDATE fiso_ct SET c.a = 6 WHERE id = 2");
            assertEquals("(6,y)", scalar("SELECT c::text FROM fiso_ct WHERE id = 2"));
            exec("INSERT INTO fiso_ct VALUES (3, '(7,z)')");
            assertEquals("7", scalar("SELECT ((c).a)::text FROM fiso_ct WHERE id = 3"));
            assertEquals("z", scalar("SELECT (c).b FROM fiso_ct WHERE id = 3"));
        } finally {
            exec("DROP TABLE IF EXISTS fiso_ct");
            exec("DROP TYPE IF EXISTS fiso_c");
            exec("DROP DOMAIN IF EXISTS fiso_d");
        }
    }

    @Test
    void aFieldOfACompositeArrayElementIsReachable() throws Exception {
        exec("CREATE TYPE fiso_c9 AS (a int, b text)");
        exec("CREATE TABLE fiso_ct10 (id int, cs fiso_c9[])");
        exec("INSERT INTO fiso_ct10 VALUES (1, ARRAY[ROW(1,'x')::fiso_c9, ROW(2,'y')::fiso_c9])");
        try {
            // The whole column reads as the array of composites it is.
            assertEquals("{\"(1,x)\",\"(2,y)\"}", scalar("SELECT cs::text FROM fiso_ct10"));
            // A subscript of it is a value of the composite, so a field of it is reachable.
            assertEquals("1", scalar("SELECT ((cs[1]).a)::text FROM fiso_ct10"));
            assertEquals("y", scalar("SELECT (cs[2]).b FROM fiso_ct10"));
            // A subscript past the end selects nothing, so the field is null rather than an error.
            assertEquals("none",
                    scalar("SELECT coalesce(((cs[3]).a)::text, 'none') FROM fiso_ct10"));
            // unnest of an array of a composite returns the composite, so the FROM item supplies
            // one column per field.
            assertEquals("2", scalar("SELECT count(*)::text FROM fiso_ct10, unnest(cs) AS u"));
            assertEquals("x",
                    scalar("SELECT u.b FROM fiso_ct10, unnest(cs) AS u WHERE u.a = 1"));
            assertEquals("y",
                    scalar("SELECT u.b FROM fiso_ct10, unnest(cs) AS u WHERE u.a = 2"));
        } finally {
            exec("DROP TABLE IF EXISTS fiso_ct10");
            exec("DROP TYPE IF EXISTS fiso_c9");
        }
    }

    // ------------------------------------------------------------ A rule qualification is evaluated, and a virtual column only where it is read

@Test
void insertRuleQualificationKeepsItsOwnStringLiterals() throws Exception {
    // PG 18: the qualification is part of the rewritten query, so a NEW. written inside one of
    // its own string literals is part of that string. Measured: INSERT 0 1, log holds 'fired'.
    exec("CREATE TABLE zzw4a_q3 (a text)");
    exec("CREATE TABLE zzw4a_q3log (m text)");
    exec("CREATE RULE zzw4a_q3r AS ON INSERT TO zzw4a_q3 WHERE NEW.a <> 'NEW.a is a name' "
            + "DO ALSO INSERT INTO zzw4a_q3log VALUES ('fired')");
    try {
        assertEquals(1, update("INSERT INTO zzw4a_q3 (a) VALUES ('V')"));
        assertEquals("V", scalar("SELECT a FROM zzw4a_q3"));
        assertEquals("fired", scalar("SELECT m FROM zzw4a_q3log"));
    } finally {
        exec("DROP TABLE zzw4a_q3 CASCADE");
        exec("DROP TABLE zzw4a_q3log CASCADE");
    }
}

@Test
void aValueThatSpellsNewColumnIsAValue() throws Exception {
    // PG 18: 'NEW.b' <> 'z', so the rule does not fire and the base row is written. Measured:
    // INSERT 0 1, log empty, base row NEW.b | z.
    exec("CREATE TABLE zzw4a_q2 (a text, b text)");
    exec("CREATE TABLE zzw4a_q2log (m text)");
    exec("CREATE RULE zzw4a_q2r AS ON INSERT TO zzw4a_q2 WHERE NEW.a = NEW.b "
            + "DO ALSO INSERT INTO zzw4a_q2log VALUES ('eq')");
    try {
        assertEquals(1, update("INSERT INTO zzw4a_q2 (a,b) VALUES ('NEW.b','z')"));
        assertEquals("NEW.b", scalar("SELECT a FROM zzw4a_q2"));
        assertEquals("z", scalar("SELECT b FROM zzw4a_q2"));
        assertEquals(0, num("SELECT count(*) FROM zzw4a_q2log"));
    } finally {
        exec("DROP TABLE zzw4a_q2 CASCADE");
        exec("DROP TABLE zzw4a_q2log CASCADE");
    }
}

@Test
void ruleQualificationReadsAByteaAsAValue() throws Exception {
    // PG 18: the qualification reads NEW.b as bytea, so encode(NEW.b,'hex') is the row's own
    // hex. Measured: the 0a0b row fires, the 0c row does not.
    exec("CREATE TABLE zzw4a_q1 (b bytea)");
    exec("CREATE TABLE zzw4a_q1log (h text)");
    exec("CREATE RULE zzw4a_q1r AS ON INSERT TO zzw4a_q1 WHERE encode(NEW.b,'hex') = '0a0b' "
            + "DO ALSO INSERT INTO zzw4a_q1log VALUES ('fired')");
    try {
        exec("INSERT INTO zzw4a_q1 (b) VALUES ('\\x0a0b'::bytea)");
        exec("INSERT INTO zzw4a_q1 (b) VALUES ('\\x0c'::bytea)");
        assertEquals(1, num("SELECT count(*) FROM zzw4a_q1log"));
        assertEquals("fired", scalar("SELECT h FROM zzw4a_q1log"));
        assertEquals(2, num("SELECT count(*) FROM zzw4a_q1"));
    } finally {
        exec("DROP TABLE zzw4a_q1 CASCADE");
        exec("DROP TABLE zzw4a_q1log CASCADE");
    }
}

@Test
void updateRuleQualificationKeepsItsOwnStringLiterals() throws Exception {
    // PG 18: OLD is a relation the qualification reads, so an OLD. inside one of its literals is
    // part of that literal. Measured: the row becomes 'two' and the log holds 'fired'.
    exec("CREATE TABLE zzw4a_u1 (a text)");
    exec("CREATE TABLE zzw4a_u1log (m text)");
    exec("INSERT INTO zzw4a_u1 (a) VALUES ('one')");
    exec("CREATE RULE zzw4a_u1r AS ON UPDATE TO zzw4a_u1 WHERE OLD.a <> 'OLD.a is a name' "
            + "DO ALSO INSERT INTO zzw4a_u1log VALUES ('fired')");
    try {
        assertEquals(1, update("UPDATE zzw4a_u1 SET a = 'two'"));
        assertEquals("two", scalar("SELECT a FROM zzw4a_u1"));
        assertEquals("fired", scalar("SELECT m FROM zzw4a_u1log"));
    } finally {
        exec("DROP TABLE zzw4a_u1 CASCADE");
        exec("DROP TABLE zzw4a_u1log CASCADE");
    }
}

@Test
void qualifiedInsteadRuleDivertsOnlyTheRowsItsWhereHoldsFor() throws Exception {
    // PG 18: the rule speaks for the row it matches and the statement writes the rest, so the
    // INSERT of two rows reports 1. Measured: base holds 'keep', log holds 'skip'.
    exec("CREATE TABLE zzw4a_i1 (a text)");
    exec("CREATE TABLE zzw4a_i1log (m text)");
    exec("CREATE RULE zzw4a_i1r AS ON INSERT TO zzw4a_i1 WHERE NEW.a = 'skip' "
            + "DO INSTEAD INSERT INTO zzw4a_i1log VALUES (NEW.a)");
    try {
        assertEquals(1, update("INSERT INTO zzw4a_i1 (a) VALUES ('keep'), ('skip')"));
        assertEquals("keep", scalar("SELECT a FROM zzw4a_i1 ORDER BY 1"));
        assertEquals("skip", scalar("SELECT m FROM zzw4a_i1log ORDER BY 1"));
    } finally {
        exec("DROP TABLE zzw4a_i1 CASCADE");
        exec("DROP TABLE zzw4a_i1log CASCADE");
    }
}

@Test
void virtualColumnIsWorkedOutOnlyWhereTheQueryNeedsIt() throws Exception {
    // PG 18: a virtual column is computed where it is read and nowhere else, so a generation
    // expression that raises leaves the rest of the relation readable. Measured: k -> zero,
    // count(*) -> 1, WHERE k='zero' -> 0, max(a) -> 0.
    exec("CREATE TABLE zzw4a_v3 (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
    try {
        assertEquals(1, update("INSERT INTO zzw4a_v3 (a,k) VALUES (0,'zero')"));
        assertEquals("zero", scalar("SELECT k FROM zzw4a_v3"));
        assertEquals(1, num("SELECT count(*) FROM zzw4a_v3"));
        assertEquals("0", scalar("SELECT a FROM zzw4a_v3 WHERE k='zero'"));
        assertEquals("0", scalar("SELECT max(a) FROM zzw4a_v3"));
        assertEquals("zero", scalar("SELECT k FROM zzw4a_v3 ORDER BY k"));
    } finally {
        exec("DROP TABLE zzw4a_v3 CASCADE");
    }
}

@Test
void virtualColumnStillRaisesForAQueryThatReadsIt() throws Exception {
    // PG 18: only a query that actually projects or filters on the column raises. Measured:
    // SELECT g, SELECT * and WHERE g > 1 all raise 22012; count(*) does not.
    exec("CREATE TABLE zzw4a_v4 (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
    try {
        exec("INSERT INTO zzw4a_v4 (a,k) VALUES (0,'zero')");
        assertEquals("22012", stateOf("SELECT g FROM zzw4a_v4"));
        assertTrue(messageOf("SELECT g FROM zzw4a_v4").toLowerCase().contains("division by zero"));
        assertEquals("22012", stateOf("SELECT * FROM zzw4a_v4"));
        assertEquals("22012", stateOf("SELECT a FROM zzw4a_v4 WHERE g > 1"));
    } finally {
        exec("DROP TABLE zzw4a_v4 CASCADE");
    }
}

@Test
void virtualColumnStillReadableWhenItsExpressionHolds() throws Exception {
    // The narrowing must not hide a value: a query that names the column, or one that stars,
    // still gets it. PG 18 answers 20 and 10 | 20 here.
    exec("CREATE TABLE zzw4a_v5 (a int, d int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
    try {
        exec("INSERT INTO zzw4a_v5 (a) VALUES (10)");
        assertEquals("20", scalar("SELECT d FROM zzw4a_v5"));
        assertEquals("10", scalar("SELECT a FROM zzw4a_v5 WHERE d = 20"));
        assertEquals("20", scalar("SELECT z.d FROM zzw4a_v5 z ORDER BY z.d"));
        assertEquals(1, num("SELECT count(*) FROM (SELECT * FROM zzw4a_v5) s WHERE s.d = 20"));
    } finally {
        exec("DROP TABLE zzw4a_v5 CASCADE");
    }
}

    // ------------------------------------------------------------ A propagated declaration can be withdrawn

    @Test
    void aConstraintDroppedOnTheParentIsDroppedOnEveryDescendantThatCarriedIt() throws Exception {
        exec("CREATE TABLE zzw4b_ph (a int, b text) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw4b_ph_0 PARTITION OF zzw4b_ph FOR VALUES FROM (0) TO (10)");
        exec("ALTER TABLE zzw4b_ph ADD CONSTRAINT zzw4b_phk CHECK (b <> 'bad')");
        exec("ALTER TABLE zzw4b_ph DROP CONSTRAINT zzw4b_phk");
        assertEquals("42704", stateOf("ALTER TABLE zzw4b_ph DROP CONSTRAINT zzw4b_phk"));
        assertEquals("constraint \"zzw4b_phk\" of relation \"zzw4b_ph_0\" does not exist",
                messageOf("ALTER TABLE zzw4b_ph_0 DROP CONSTRAINT zzw4b_phk"));
        exec("INSERT INTO zzw4b_ph_0 VALUES (1, 'bad')");
        assertEquals(1, num("SELECT count(*)::int FROM zzw4b_ph"));

        exec("CREATE TABLE zzw4b_ih (a int, b text)");
        exec("CREATE TABLE zzw4b_ihc () INHERITS (zzw4b_ih)");
        exec("ALTER TABLE zzw4b_ih ADD CONSTRAINT zzw4b_ihk CHECK (b <> 'bad')");
        exec("ALTER TABLE zzw4b_ih DROP CONSTRAINT zzw4b_ihk");
        assertEquals(0, num("SELECT count(*)::int FROM pg_constraint"
                + " WHERE conrelid = 'zzw4b_ihc'::regclass AND contype = 'c'"));
        exec("INSERT INTO zzw4b_ihc VALUES (1, 'bad')");
        assertEquals(1, num("SELECT count(*)::int FROM zzw4b_ihc"));

        exec("CREATE TABLE zzw4b_cd (a int, b text CHECK (b <> 'bad')) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw4b_cd_0 PARTITION OF zzw4b_cd FOR VALUES FROM (0) TO (10)");
        exec("ALTER TABLE zzw4b_cd DROP CONSTRAINT zzw4b_cd_b_check");
        assertEquals(0, num("SELECT count(*)::int FROM pg_constraint"
                + " WHERE conrelid = 'zzw4b_cd_0'::regclass AND contype = 'c'"));
        exec("INSERT INTO zzw4b_cd_0 VALUES (1, 'bad')");

        exec("CREATE TABLE zzw4b_rf (id int PRIMARY KEY)");
        exec("CREATE TABLE zzw4b_fk (a int, r int) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw4b_fk_0 PARTITION OF zzw4b_fk FOR VALUES FROM (0) TO (10)");
        exec("ALTER TABLE zzw4b_fk ADD CONSTRAINT zzw4b_fkc FOREIGN KEY (r) REFERENCES zzw4b_rf(id)");
        assertEquals("23503", stateOf("INSERT INTO zzw4b_fk VALUES (1, 99)"));
        exec("ALTER TABLE zzw4b_fk DROP CONSTRAINT zzw4b_fkc");
        assertEquals(0, num("SELECT count(*)::int FROM pg_constraint"
                + " WHERE conrelid = 'zzw4b_fk_0'::regclass AND contype = 'f'"));
        exec("INSERT INTO zzw4b_fk VALUES (1, 99)");
        assertEquals(1, num("SELECT count(*)::int FROM zzw4b_fk"));

        exec("DROP TABLE zzw4b_ph");
        exec("DROP TABLE zzw4b_ihc");
        exec("DROP TABLE zzw4b_ih");
        exec("DROP TABLE zzw4b_cd");
        exec("DROP TABLE zzw4b_fk");
        exec("DROP TABLE zzw4b_rf");
    }

    @Test
    void onlyLeavesEachDescendantHoldingTheConstraintAsItsOwn() throws Exception {
        exec("CREATE TABLE zzw4b_o1 (a int, b text)");
        exec("CREATE TABLE zzw4b_o1c () INHERITS (zzw4b_o1)");
        exec("ALTER TABLE zzw4b_o1 ADD CONSTRAINT zzw4b_o1k CHECK (b <> 'bad')");
        exec("ALTER TABLE ONLY zzw4b_o1 DROP CONSTRAINT zzw4b_o1k");
        // The child goes on enforcing it...
        assertEquals("23514", stateOf("INSERT INTO zzw4b_o1c VALUES (1, 'bad')"));
        // ...and it is the child's own from now on, so the child may drop it.
        exec("ALTER TABLE zzw4b_o1c DROP CONSTRAINT zzw4b_o1k");
        exec("INSERT INTO zzw4b_o1c VALUES (1, 'bad')");
        assertEquals(1, num("SELECT count(*)::int FROM zzw4b_o1c"));
        exec("DROP TABLE zzw4b_o1c");
        exec("DROP TABLE zzw4b_o1");
    }

    @Test
    void aTableTakenOutOfAHierarchyOwnsTheConstraintsItKeeps() throws Exception {
        exec("CREATE TABLE zzw4b_j6 (a int, b text) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw4b_j6_0 PARTITION OF zzw4b_j6 FOR VALUES FROM (0) TO (10)");
        exec("ALTER TABLE zzw4b_j6 ADD CONSTRAINT zzw4b_j6k CHECK (b <> 'bad')");
        exec("ALTER TABLE zzw4b_j6 DETACH PARTITION zzw4b_j6_0");
        // The copy survives the detach, as PostgreSQL leaves it...
        assertEquals(1, num("SELECT count(*)::int FROM pg_constraint"
                + " WHERE conrelid = 'zzw4b_j6_0'::regclass AND contype = 'c'"));
        // ...and the standalone table may drop what is now its own.
        exec("ALTER TABLE zzw4b_j6_0 DROP CONSTRAINT zzw4b_j6k");
        exec("INSERT INTO zzw4b_j6_0 VALUES (1, 'bad')");
        assertEquals(1, num("SELECT count(*)::int FROM zzw4b_j6_0"));

        exec("CREATE TABLE zzw4b_nc0 (a int, b text)");
        exec("ALTER TABLE zzw4b_nc0 ADD CONSTRAINT zzw4b_nck CHECK (b <> 'bad')");
        exec("CREATE TABLE zzw4b_nc1 () INHERITS (zzw4b_nc0)");
        exec("ALTER TABLE zzw4b_nc1 NO INHERIT zzw4b_nc0");
        exec("ALTER TABLE zzw4b_nc1 DROP CONSTRAINT zzw4b_nck");
        exec("INSERT INTO zzw4b_nc1 VALUES (1, 'bad')");
        assertEquals(1, num("SELECT count(*)::int FROM zzw4b_nc1"));

        exec("DROP TABLE zzw4b_j6_0");
        exec("DROP TABLE zzw4b_j6");
        exec("DROP TABLE zzw4b_nc1");
        exec("DROP TABLE zzw4b_nc0");
    }

    @Test
    void dropNotNullOnAParentReachesItsInheritanceChildren() throws Exception {
        exec("CREATE TABLE zzw4b_j1 (a int, b text)");
        exec("CREATE TABLE zzw4b_j1c () INHERITS (zzw4b_j1)");
        exec("ALTER TABLE zzw4b_j1 ALTER COLUMN b SET NOT NULL");
        assertEquals("23502", stateOf("INSERT INTO zzw4b_j1c (a) VALUES (1)"));
        exec("ALTER TABLE zzw4b_j1 ALTER COLUMN b DROP NOT NULL");
        assertEquals("YES", scalar("SELECT is_nullable FROM information_schema.columns"
                + " WHERE table_name = 'zzw4b_j1c' AND column_name = 'b'"));
        exec("INSERT INTO zzw4b_j1c (a) VALUES (1)");
        assertEquals(1, num("SELECT count(*)::int FROM zzw4b_j1c"));

        exec("CREATE TABLE zzw4b_dn (a int, b text NOT NULL)");
        exec("CREATE TABLE zzw4b_dnc () INHERITS (zzw4b_dn)");
        exec("ALTER TABLE zzw4b_dn ALTER COLUMN b DROP NOT NULL");
        exec("INSERT INTO zzw4b_dnc (a) VALUES (1)");
        assertEquals(1, num("SELECT count(*)::int FROM zzw4b_dnc"));

        // ONLY asks for the named relation alone, and the child keeps the rule.
        exec("CREATE TABLE zzw4b_on (a int, b text NOT NULL)");
        exec("CREATE TABLE zzw4b_onc () INHERITS (zzw4b_on)");
        exec("ALTER TABLE ONLY zzw4b_on ALTER COLUMN b DROP NOT NULL");
        assertEquals("23502", stateOf("INSERT INTO zzw4b_onc (a) VALUES (1)"));
        exec("INSERT INTO zzw4b_on (a) VALUES (1)");

        exec("DROP TABLE zzw4b_j1c");
        exec("DROP TABLE zzw4b_j1");
        exec("DROP TABLE zzw4b_dnc");
        exec("DROP TABLE zzw4b_dn");
        exec("DROP TABLE zzw4b_onc");
        exec("DROP TABLE zzw4b_on");
    }

    @Test
    void aDescendantMayNotDropTheNotNullItsParentDeclares() throws Exception {
        exec("CREATE TABLE zzw4b_j3 (a int, b text NOT NULL) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw4b_j3_0 PARTITION OF zzw4b_j3 FOR VALUES FROM (0) TO (10)");
        assertEquals("42P16", stateOf("ALTER TABLE zzw4b_j3_0 ALTER COLUMN b DROP NOT NULL"));
        assertEquals("column \"b\" is marked NOT NULL in parent table",
                messageOf("ALTER TABLE zzw4b_j3_0 ALTER COLUMN b DROP NOT NULL"));
        assertEquals("23502", stateOf("INSERT INTO zzw4b_j3_0 (a) VALUES (1)"));

        exec("CREATE TABLE zzw4b_j4 (a int, b text NOT NULL)");
        exec("CREATE TABLE zzw4b_j4c () INHERITS (zzw4b_j4)");
        assertEquals("42P16", stateOf("ALTER TABLE zzw4b_j4c ALTER COLUMN b DROP NOT NULL"));
        assertEquals("cannot drop inherited constraint \"zzw4b_j4_b_not_null\""
                + " of relation \"zzw4b_j4c\"",
                messageOf("ALTER TABLE zzw4b_j4c ALTER COLUMN b DROP NOT NULL"));
        assertEquals("23502", stateOf("INSERT INTO zzw4b_j4c (a) VALUES (1)"));

        // A partition whose parent leaves the column nullable declared its own NOT NULL, and that
        // one is its own to drop.
        exec("CREATE TABLE zzw4b_q1 (a int, b text) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw4b_q1_0 PARTITION OF zzw4b_q1 FOR VALUES FROM (0) TO (10)");
        exec("ALTER TABLE zzw4b_q1_0 ALTER COLUMN b SET NOT NULL");
        exec("ALTER TABLE zzw4b_q1_0 ALTER COLUMN b DROP NOT NULL");
        exec("INSERT INTO zzw4b_q1_0 (a) VALUES (1)");
        assertEquals(1, num("SELECT count(*)::int FROM zzw4b_q1"));

        // Detaching hands the rule over, so the standalone table may then drop it.
        exec("ALTER TABLE zzw4b_j3 DETACH PARTITION zzw4b_j3_0");
        exec("ALTER TABLE zzw4b_j3_0 ALTER COLUMN b DROP NOT NULL");
        exec("INSERT INTO zzw4b_j3_0 (a) VALUES (1)");

        exec("DROP TABLE zzw4b_j3_0");
        exec("DROP TABLE zzw4b_j3");
        exec("DROP TABLE zzw4b_j4c");
        exec("DROP TABLE zzw4b_j4");
        exec("DROP TABLE zzw4b_q1");
    }

    @Test
    void aPartitionsCopyOfAParentIndexIsNamedTheWayPostgresNamesIt() throws Exception {
        exec("CREATE TABLE zzw4b_ux (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE UNIQUE INDEX zzw4b_ux_idx ON zzw4b_ux (i)");
        exec("CREATE TABLE zzw4b_ux_0 PARTITION OF zzw4b_ux FOR VALUES FROM (0) TO (10)");
        assertEquals("zzw4b_ux_0_i_idx", scalar(
                "SELECT ic.relname FROM pg_index i JOIN pg_class c ON i.indrelid = c.oid"
                + " JOIN pg_class ic ON i.indexrelid = ic.oid WHERE c.relname = 'zzw4b_ux_0'"));
        assertEquals(1, num("SELECT count(*)::int FROM pg_index i"
                + " JOIN pg_class c ON i.indrelid = c.oid WHERE c.relname = 'zzw4b_ux_0'"));
        exec("INSERT INTO zzw4b_ux VALUES (5, 'a')");
        assertEquals("duplicate key value violates unique constraint \"zzw4b_ux_0_i_idx\"",
                messageOf("INSERT INTO zzw4b_ux VALUES (5, 'b')"));

        exec("CREATE TABLE zzw4b_px (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE INDEX zzw4b_px_idx ON zzw4b_px (s)");
        exec("CREATE TABLE zzw4b_px_0 PARTITION OF zzw4b_px FOR VALUES FROM (0) TO (10)");
        assertEquals("zzw4b_px_0_s_idx", scalar(
                "SELECT ic.relname FROM pg_index i JOIN pg_class c ON i.indrelid = c.oid"
                + " JOIN pg_class ic ON i.indexrelid = ic.oid WHERE c.relname = 'zzw4b_px_0'"));

        // Two indexes over one column give the second copy the number PostgreSQL appends.
        exec("CREATE TABLE zzw4b_cx (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE INDEX zzw4b_cx_a ON zzw4b_cx (i)");
        exec("CREATE INDEX zzw4b_cx_b ON zzw4b_cx (i)");
        exec("CREATE TABLE zzw4b_cx_0 PARTITION OF zzw4b_cx FOR VALUES FROM (0) TO (10)");
        assertEquals("zzw4b_cx_0_i_idx,zzw4b_cx_0_i_idx1", scalar(
                "SELECT string_agg(ic.relname, ',' ORDER BY ic.relname) FROM pg_index i"
                + " JOIN pg_class c ON i.indrelid = c.oid"
                + " JOIN pg_class ic ON i.indexrelid = ic.oid WHERE c.relname = 'zzw4b_cx_0'"));

        exec("DROP TABLE zzw4b_ux");
        exec("DROP TABLE zzw4b_px");
        exec("DROP TABLE zzw4b_cx");
    }

    // ------------------------------------------------------------ Definition validation, round three

    @Test
    void aViewColumnDefaultStaysOnTheView() throws Exception {
        exec("CREATE TABLE zzw4c_z1 (i int, w text)");
        exec("CREATE VIEW zzw4c_z1v AS SELECT i, w FROM zzw4c_z1");
        exec("ALTER VIEW zzw4c_z1v ALTER COLUMN w SET DEFAULT 'vv'");
        // PG 18: the default is the view's, so the base relation carries none of it.
        assertNull(scalar("SELECT column_default FROM information_schema.columns"
                + " WHERE table_name='zzw4c_z1' AND column_name='w'"));
        exec("INSERT INTO zzw4c_z1 (i) VALUES (1)");
        assertNull(scalar("SELECT w FROM zzw4c_z1 WHERE i = 1"));
        // And a write through the view still takes it.
        exec("INSERT INTO zzw4c_z1v (i) VALUES (2)");
        assertEquals("vv", scalar("SELECT w FROM zzw4c_z1 WHERE i = 2"));
        assertEquals("'vv'::text", scalar("SELECT column_default FROM information_schema.columns"
                + " WHERE table_name='zzw4c_z1v' AND column_name='w'"));
        // A column the view does not publish is the column that is not there.
        assertEquals("42703", stateOf("ALTER VIEW zzw4c_z1v ALTER COLUMN nope SET DEFAULT 'q'"));
        exec("DROP VIEW zzw4c_z1v");
        exec("DROP TABLE zzw4c_z1");
    }

    @Test
    void aMaterializedViewTakesNoColumnDefault() throws Exception {
        exec("CREATE TABLE zzw4c_mt (i int, w text)");
        exec("CREATE MATERIALIZED VIEW zzw4c_mv AS SELECT i, w FROM zzw4c_mt");
        // PG 18: 42809, with the kind that refuses it on the detail line.
        assertEquals("42809",
                stateOf("ALTER MATERIALIZED VIEW zzw4c_mv ALTER COLUMN w SET DEFAULT 'q'"));
        assertEquals("This operation is not supported for materialized views.",
                detailOf("ALTER MATERIALIZED VIEW zzw4c_mv ALTER COLUMN w SET DEFAULT 'q'"));
        exec("DROP MATERIALIZED VIEW zzw4c_mv");
        exec("DROP TABLE zzw4c_mt");
    }

    @Test
    void renamingABaseColumnLeavesLiteralsAlone() throws Exception {
        exec("CREATE TABLE zzw4c_q1 (v text, x int)");
        exec("INSERT INTO zzw4c_q1 VALUES ('hello', 1)");
        exec("CREATE VIEW zzw4c_q1v AS SELECT v, 'v'::text AS lit, x FROM zzw4c_q1");
        exec("ALTER TABLE zzw4c_q1 RENAME COLUMN v TO v2");
        // PG 18: the literal 'v' is not a reference to the column, so it is untouched.
        assertEquals("v", scalar("SELECT lit FROM zzw4c_q1v"));
        assertEquals("hello", scalar("SELECT v FROM zzw4c_q1v"));
        assertTrue(scalar("SELECT pg_get_viewdef('zzw4c_q1v'::regclass)").contains("'v'::text"));
        exec("DROP VIEW zzw4c_q1v");
        exec("DROP TABLE zzw4c_q1");
    }

    @Test
    void renamingOneRelationsColumnLeavesAnothersOfTheSameNameAlone() throws Exception {
        exec("CREATE TABLE zzw4c_qa (v text)");
        exec("CREATE TABLE zzw4c_qb (v text)");
        exec("INSERT INTO zzw4c_qa VALUES ('A')");
        exec("INSERT INTO zzw4c_qb VALUES ('B')");
        exec("CREATE VIEW zzw4c_qabv AS SELECT zzw4c_qa.v AS av, zzw4c_qb.v AS bv"
                + " FROM zzw4c_qa, zzw4c_qb");
        exec("ALTER TABLE zzw4c_qa RENAME COLUMN v TO v2");
        // PG 18: A | B -- only the renamed relation's reference moved.
        assertEquals("A", scalar("SELECT av FROM zzw4c_qabv"));
        assertEquals("B", scalar("SELECT bv FROM zzw4c_qabv"));
        assertTrue(scalar("SELECT pg_get_viewdef('zzw4c_qabv'::regclass)").contains("zzw4c_qb.v"));
        exec("DROP VIEW zzw4c_qabv");
        exec("DROP TABLE zzw4c_qa");
        exec("DROP TABLE zzw4c_qb");
    }

    @Test
    void renamingABaseColumnLeavesAViewOverAViewAlone() throws Exception {
        exec("CREATE TABLE zzw4c_r1 (i int, v text)");
        exec("INSERT INTO zzw4c_r1 VALUES (1, 'a')");
        exec("CREATE VIEW zzw4c_r1v AS SELECT i, v FROM zzw4c_r1 WHERE v = 'a'");
        exec("CREATE VIEW zzw4c_r2v AS SELECT i, v FROM zzw4c_r1v");
        exec("ALTER TABLE zzw4c_r1 RENAME COLUMN v TO v2");
        // PG 18: the inner view still publishes v, so the outer one goes on reading it.
        assertEquals("a", scalar("SELECT v FROM zzw4c_r2v"));
        exec("DROP VIEW zzw4c_r2v");
        exec("DROP VIEW zzw4c_r1v");
        exec("DROP TABLE zzw4c_r1");
    }

    @Test
    void aGenerationExpressionIsTypeCheckedAgainstItsColumn() throws Exception {
        // PG 18: 42804 for each, worded as it words a DEFAULT of the wrong type.
        assertEquals("column \"b\" is of type integer but default expression is of type text",
                messageOf("CREATE TABLE zzw4c_gg1 (a int,"
                        + " b int GENERATED ALWAYS AS ('abc'::text) STORED)"));
        assertEquals("You will need to rewrite or cast the expression.",
                fieldsOf("CREATE TABLE zzw4c_gg1 (a int,"
                        + " b int GENERATED ALWAYS AS ('abc'::text) STORED)").getHint());
        assertEquals("column \"b\" is of type date but default expression is of type integer",
                messageOf("CREATE TABLE zzw4c_gg2 (a int,"
                        + " b date GENERATED ALWAYS AS (1) STORED)"));
        assertEquals("column \"b\" is of type integer but default expression is of type text",
                messageOf("CREATE TABLE zzw4c_gg3 (a text,"
                        + " b int GENERATED ALWAYS AS (a) STORED)"));
        assertEquals("column \"b\" is of type boolean but default expression is of type integer",
                messageOf("CREATE TABLE zzw4c_gg7 (a int,"
                        + " b bool GENERATED ALWAYS AS (1) STORED)"));
        // And the pairs PostgreSQL has an assignment cast for are still accepted.
        exec("CREATE TABLE zzw4c_gg4 (a int, b text GENERATED ALWAYS AS (a) STORED)");
        exec("CREATE TABLE zzw4c_gg5 (a numeric, b int GENERATED ALWAYS AS (a) STORED)");
        exec("CREATE TABLE zzw4c_gg8 (a int, b numeric GENERATED ALWAYS AS (1.5) STORED)");
        exec("CREATE TABLE zzw4c_gg9 (a int, b int GENERATED ALWAYS AS (a * 2) STORED)");
        exec("INSERT INTO zzw4c_gg9 (a) VALUES (3)");
        assertEquals(6L, num("SELECT b FROM zzw4c_gg9"));
        exec("DROP TABLE zzw4c_gg4");
        exec("DROP TABLE zzw4c_gg5");
        exec("DROP TABLE zzw4c_gg8");
        exec("DROP TABLE zzw4c_gg9");
    }

    @Test
    void anExclusionOperatorMustBeInTheIndexOperatorFamily() throws Exception {
        // PG 18: btree is the default access method and <> is not one of its search operators.
        assertEquals("operator <>(integer,integer) is not a member of operator family"
                        + " \"integer_ops\"",
                messageOf("CREATE TABLE zzw4c_e3 (a int, EXCLUDE (a WITH <>))"));
        assertEquals("The exclusion operator must be related to the index operator class"
                        + " for the constraint.",
                detailOf("CREATE TABLE zzw4c_e3 (a int, EXCLUDE (a WITH <>))"));
        assertEquals("42809", stateOf("CREATE TABLE zzw4c_e3 (a int, EXCLUDE (a WITH <>))"));
        assertEquals("operator <>(text,text) is not a member of operator family \"text_ops\"",
                messageOf("CREATE TABLE zzw4c_e3 (a varchar(10), EXCLUDE (a WITH <>))"));
        assertEquals("operator <>(timestamp with time zone,timestamp with time zone)"
                        + " is not a member of operator family \"datetime_ops\"",
                messageOf("CREATE TABLE zzw4c_e3 (a timestamptz, EXCLUDE (a WITH <>))"));
        // Equality is a member, so the same constraint over = is stored.
        exec("CREATE TABLE zzw4c_e3 (a int, EXCLUDE (a WITH =))");
        exec("CREATE TABLE zzw4c_e4 (a int)");
        assertEquals("42809",
                stateOf("ALTER TABLE zzw4c_e4 ADD CONSTRAINT zzw4c_e4_x EXCLUDE (a WITH <>)"));
        exec("DROP TABLE zzw4c_e3");
        exec("DROP TABLE zzw4c_e4");
    }

    @Test
    void aCompositeTypeIsALikeSource() throws Exception {
        exec("CREATE TYPE zzw4c_lct AS (m int, n text)");
        // PG 18: LIKE copies a row's shape, and a stand-alone composite has one.
        exec("CREATE TABLE zzw4c_lk5 (LIKE zzw4c_lct)");
        assertEquals("m,n", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name='zzw4c_lk5'"));
        assertEquals("integer", scalar("SELECT data_type FROM information_schema.columns"
                + " WHERE table_name='zzw4c_lk5' AND column_name='m'"));
        exec("DROP TABLE zzw4c_lk5");
        exec("DROP TYPE zzw4c_lct");
    }

    @Test
    void aTypedTableTakesColumnOptionsAndBelongsToItsType() throws Exception {
        exec("CREATE TYPE zzw4c_ct3 AS (x int, y text)");
        exec("CREATE TABLE zzw4c_oa OF zzw4c_ct3 (x WITH OPTIONS NOT NULL)");
        exec("CREATE TABLE zzw4c_ob OF zzw4c_ct3 (PRIMARY KEY (x), y WITH OPTIONS DEFAULT 'd')");
        assertEquals("NO", scalar("SELECT is_nullable FROM information_schema.columns"
                + " WHERE table_name='zzw4c_oa' AND column_name='x'"));
        assertEquals("'d'::text", scalar("SELECT column_default FROM information_schema.columns"
                + " WHERE table_name='zzw4c_ob' AND column_name='y'"));
        exec("INSERT INTO zzw4c_ob (x) VALUES (1)");
        assertEquals("d", scalar("SELECT y FROM zzw4c_ob WHERE x = 1"));
        // Options may only be written for a column the type declares.
        assertEquals("42703",
                stateOf("CREATE TABLE zzw4c_od OF zzw4c_ct3 (z WITH OPTIONS NOT NULL)"));
        // A typed table has exactly the type's columns, and the type outlives neither of them.
        assertEquals("cannot add column to typed table",
                messageOf("ALTER TABLE zzw4c_oa ADD COLUMN z int"));
        assertEquals("2BP01", stateOf("DROP TYPE zzw4c_ct3"));
        assertTrue(detailOf("DROP TYPE zzw4c_ct3").contains("table zzw4c_oa depends on type"));
        exec("DROP TABLE zzw4c_oa");
        exec("DROP TABLE zzw4c_ob");
        exec("DROP TYPE zzw4c_ct3");
    }

    @Test
    void setStatisticsTakesASignedIntegerAndNothingElse() throws Exception {
        exec("CREATE TABLE zzw4c_st (a int, b int)");
        // PG 18: SignedIconst takes an explicit +, and a string literal is a syntax error.
        exec("ALTER TABLE zzw4c_st ALTER COLUMN a SET STATISTICS +5");
        assertEquals(5L, num("SELECT attstattarget FROM pg_attribute"
                + " WHERE attrelid='zzw4c_st'::regclass AND attname='a'"));
        assertEquals("syntax error at or near \"'5'\"",
                messageOf("ALTER TABLE zzw4c_st ALTER COLUMN a SET STATISTICS '5'"));
        assertEquals("syntax error at or near \"1.5\"",
                messageOf("ALTER TABLE zzw4c_st ALTER COLUMN a SET STATISTICS 1.5"));
        exec("ALTER TABLE zzw4c_st ALTER COLUMN a SET STATISTICS -1");
        exec("DROP TABLE zzw4c_st");
    }

    @Test
    void tablesampleAppliesOnlyToRelationsThatHoldTheirOwnRows() throws Exception {
        exec("CREATE TABLE zzw4c_ts (a int, b int)");
        exec("INSERT INTO zzw4c_ts VALUES (1,1),(2,2)");
        exec("CREATE VIEW zzw4c_tsv AS SELECT a FROM zzw4c_ts");
        exec("CREATE SEQUENCE zzw4c_tsq");
        // PG 18: 0A000 on a view and on a sequence, and the table itself still samples.
        assertEquals("0A000",
                stateOf("SELECT count(*) FROM zzw4c_tsv TABLESAMPLE BERNOULLI (100)"));
        assertEquals("TABLESAMPLE clause can only be applied to tables and materialized views",
                messageOf("SELECT count(*) FROM zzw4c_tsv TABLESAMPLE BERNOULLI (100)"));
        assertEquals("0A000",
                stateOf("SELECT count(*) FROM zzw4c_tsq TABLESAMPLE BERNOULLI (100)"));
        assertEquals(2L, num("SELECT count(*) FROM zzw4c_ts TABLESAMPLE BERNOULLI (100)"));
        // And the two complaints about the clause itself are PostgreSQL's own words.
        assertEquals("sample percentage must be between 0 and 100",
                messageOf("SELECT count(*) FROM zzw4c_ts TABLESAMPLE BERNOULLI (150)"));
        assertEquals("sample percentage must be between 0 and 100",
                messageOf("SELECT count(*) FROM zzw4c_ts TABLESAMPLE BERNOULLI (-1)"));
        assertEquals("tablesample method nosuchmethod does not exist",
                messageOf("SELECT count(*) FROM zzw4c_ts TABLESAMPLE NOSUCHMETHOD (10)"));
        exec("DROP SEQUENCE zzw4c_tsq");
        exec("DROP VIEW zzw4c_tsv");
        exec("DROP TABLE zzw4c_ts");
    }

    // ------------------------------------------------------------ Trigger and rule objects, round two

    /**
     * A rule's own qualification is analysed where the rule is written. OLD and NEW there are the
     * rows of the ruled relation, so a column neither of them has is 42703 at CREATE RULE — not a
     * stored rule that the next write to the relation trips over.
     */
    @Test
    void createRuleResolvesTheColumnsItsQualificationNames() throws Exception {
        exec("DROP TABLE IF EXISTS zzw4d_qa CASCADE");
        exec("DROP TABLE IF EXISTS zzw4d_qb CASCADE");
        exec("CREATE TABLE zzw4d_qa (i int, j int)");
        exec("CREATE TABLE zzw4d_qb (i int)");
        assertEquals("42703", stateOf("CREATE RULE zzw4d_q5r AS ON INSERT TO zzw4d_qa"
                + " WHERE NEW.nosuchcol2 > 0 DO ALSO INSERT INTO zzw4d_qb VALUES (1)"));
        assertEquals("column new.nosuchcol2 does not exist",
                messageOf("CREATE RULE zzw4d_q5r AS ON INSERT TO zzw4d_qa"
                        + " WHERE NEW.nosuchcol2 > 0 DO ALSO INSERT INTO zzw4d_qb VALUES (1)"));
        assertEquals("42703", stateOf("CREATE RULE zzw4d_q11r AS ON UPDATE TO zzw4d_qa"
                + " WHERE OLD.nosuchcol3 > 0 DO ALSO INSERT INTO zzw4d_qb VALUES (1)"));
        assertEquals("column old.nosuchcol3 does not exist",
                messageOf("CREATE RULE zzw4d_q11r AS ON UPDATE TO zzw4d_qa"
                        + " WHERE OLD.nosuchcol3 > 0 DO ALSO INSERT INTO zzw4d_qb VALUES (1)"));
        // Neither rule was stored, so the relation can still be written to.
        assertEquals("(no rows)",
                scalar("SELECT rulename FROM pg_rules WHERE tablename = 'zzw4d_qa'"));
        exec("INSERT INTO zzw4d_qa VALUES (5, 6)");
        assertEquals("1", scalar("SELECT count(*) FROM zzw4d_qa"));
        exec("DROP TABLE IF EXISTS zzw4d_qa CASCADE");
        exec("DROP TABLE IF EXISTS zzw4d_qb CASCADE");
    }

    /**
     * PostgreSQL resolves a call by name and argument list while it analyses the rule, in the
     * action and in the qualification alike, so a name nothing answers to is 42883 at CREATE RULE.
     * A rule that does call something real is stored and fires.
     */
    @Test
    void createRuleResolvesTheFunctionsItCalls() throws Exception {
        exec("DROP TABLE IF EXISTS zzw4d_fa CASCADE");
        exec("DROP TABLE IF EXISTS zzw4d_fb CASCADE");
        exec("CREATE TABLE zzw4d_fa (i int, j int)");
        exec("CREATE TABLE zzw4d_fb (i int)");
        assertEquals("42883", stateOf("CREATE RULE zzw4d_f7r AS ON INSERT TO zzw4d_fa"
                + " DO ALSO INSERT INTO zzw4d_fb VALUES (nosuchfunc(1))"));
        assertEquals("function nosuchfunc(integer) does not exist",
                messageOf("CREATE RULE zzw4d_f7r AS ON INSERT TO zzw4d_fa"
                        + " DO ALSO INSERT INTO zzw4d_fb VALUES (nosuchfunc(1))"));
        assertEquals("42883", stateOf("CREATE RULE zzw4d_f8r AS ON INSERT TO zzw4d_fa"
                + " WHERE nosuchfunc(1) > 0 DO ALSO INSERT INTO zzw4d_fb VALUES (1)"));
        assertEquals("(no rows)",
                scalar("SELECT rulename FROM pg_rules WHERE tablename = 'zzw4d_fa'"));
        exec("INSERT INTO zzw4d_fa VALUES (7, 8)");
        assertEquals("1", scalar("SELECT count(*) FROM zzw4d_fa"));
        exec("CREATE RULE zzw4d_f9r AS ON INSERT TO zzw4d_fa WHERE NEW.i > 0"
                + " DO ALSO INSERT INTO zzw4d_fb VALUES (abs(NEW.i))");
        exec("INSERT INTO zzw4d_fa VALUES (9, 10)");
        assertEquals("1", scalar("SELECT count(*) FROM zzw4d_fb"));
        exec("DROP TABLE IF EXISTS zzw4d_fa CASCADE");
        exec("DROP TABLE IF EXISTS zzw4d_fb CASCADE");
    }

    /**
     * A constraint trigger is a constraint SET CONSTRAINTS can name. Its deferrability decides what
     * may be asked of it, and a name that is neither a constraint nor a constraint trigger is still
     * 42704.
     */
    @Test
    void setConstraintsFindsAConstraintTriggerByName() throws Exception {
        exec("DROP TABLE IF EXISTS zzw4d_t5 CASCADE");
        exec("DROP FUNCTION IF EXISTS zzw4d_f1() CASCADE");
        exec("CREATE TABLE zzw4d_t5 (i int)");
        exec("CREATE FUNCTION zzw4d_f1() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$"
                + " LANGUAGE plpgsql");
        exec("CREATE CONSTRAINT TRIGGER zzw4d_tg5c AFTER INSERT ON zzw4d_t5"
                + " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION zzw4d_f1()");
        exec("CREATE CONSTRAINT TRIGGER zzw4d_tg5d AFTER INSERT ON zzw4d_t5"
                + " FOR EACH ROW EXECUTE FUNCTION zzw4d_f1()");
        assertEquals("OK", stateOf("SET CONSTRAINTS zzw4d_tg5c IMMEDIATE"));
        assertEquals("OK", stateOf("SET CONSTRAINTS zzw4d_tg5c DEFERRED"));
        assertEquals("OK", stateOf("SET CONSTRAINTS public.zzw4d_tg5c IMMEDIATE"));
        assertEquals("OK", stateOf("SET CONSTRAINTS zzw4d_tg5d IMMEDIATE"));
        assertEquals("constraint \"zzw4d_tg5d\" is not deferrable",
                messageOf("SET CONSTRAINTS zzw4d_tg5d DEFERRED"));
        assertEquals("42809", stateOf("SET CONSTRAINTS zzw4d_tg5d DEFERRED"));
        assertEquals("42704", stateOf("SET CONSTRAINTS zzw4d_nosuch IMMEDIATE"));
        exec("DROP TABLE IF EXISTS zzw4d_t5 CASCADE");
        exec("DROP FUNCTION IF EXISTS zzw4d_f1() CASCADE");
    }

    /**
     * PostgreSQL has no way to replace a constraint trigger and says so; OR REPLACE on an ordinary
     * trigger is the form it does support, so the refusal has to tell the two apart.
     */
    @Test
    void createOrReplaceConstraintTriggerIsRefused() throws Exception {
        exec("DROP TABLE IF EXISTS zzw4d_t6 CASCADE");
        exec("DROP FUNCTION IF EXISTS zzw4d_f2() CASCADE");
        exec("CREATE TABLE zzw4d_t6 (i int)");
        exec("CREATE FUNCTION zzw4d_f2() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$"
                + " LANGUAGE plpgsql");
        assertEquals("0A000", stateOf("CREATE OR REPLACE CONSTRAINT TRIGGER zzw4d_tg6b"
                + " AFTER INSERT ON zzw4d_t6 FOR EACH ROW EXECUTE FUNCTION zzw4d_f2()"));
        assertEquals("CREATE OR REPLACE CONSTRAINT TRIGGER is not supported",
                messageOf("CREATE OR REPLACE CONSTRAINT TRIGGER zzw4d_tg6b"
                        + " AFTER INSERT ON zzw4d_t6 FOR EACH ROW EXECUTE FUNCTION zzw4d_f2()"));
        assertEquals("OK", stateOf("CREATE OR REPLACE TRIGGER zzw4d_tg6e"
                + " AFTER INSERT ON zzw4d_t6 FOR EACH ROW EXECUTE FUNCTION zzw4d_f2()"));
        exec("DROP TABLE IF EXISTS zzw4d_t6 CASCADE");
        exec("DROP FUNCTION IF EXISTS zzw4d_f2() CASCADE");
    }

    /**
     * An ErrorResponse the protocol layer raised for itself aborts the transaction block it stands
     * in, exactly as a failing statement does: ReadyForQuery answers E, the next statement is
     * refused with 25P02, and COMMIT throws the block's work away. Driven over the raw protocol
     * because a second Execute on a finished portal is a message no driver sends.
     *
     * <p>Needs com.memgres.client.RawWireClient, which is already in the test tree.
     */
    @Test
    void anExtendedProtocolErrorAbortsTheTransactionBlock() throws Exception {
        try (com.memgres.client.RawWireClient wire =
                     new com.memgres.client.RawWireClient(memgres.getPort())) {
            wire.startup("memgres", "memgres");
            java.util.function.Function<com.memgres.client.RawWireClient, java.util.List<String>>
                    readToReady = c -> {
                java.util.List<String> seen = new java.util.ArrayList<>();
                try {
                    while (true) {
                        com.memgres.client.RawWireClient.Msg m = c.read();
                        if (m == null) break;
                        seen.add(m.toString());
                        if (m.type == 'Z') break;
                    }
                } catch (java.io.IOException e) {
                    seen.add("<io>");
                }
                return seen;
            };
            wire.write(com.memgres.client.RawWireClient.query("CREATE TABLE zzw4d_ab (id int)"));
            readToReady.apply(wire);
            wire.write(com.memgres.client.RawWireClient.query("BEGIN"));
            assertEquals("[C[BEGIN], Z[T]]", readToReady.apply(wire).toString());

            wire.write(com.memgres.client.RawWireClient.frame('P',
                    com.memgres.client.RawWireClient.concat(
                            com.memgres.client.RawWireClient.cstring(""),
                            com.memgres.client.RawWireClient.cstring(
                                    "INSERT INTO zzw4d_ab VALUES (1)"),
                            com.memgres.client.RawWireClient.int16(0))));
            wire.write(com.memgres.client.RawWireClient.frame('B',
                    com.memgres.client.RawWireClient.concat(
                            com.memgres.client.RawWireClient.cstring("zzw4d_portal"),
                            com.memgres.client.RawWireClient.cstring(""),
                            com.memgres.client.RawWireClient.int16(0),
                            com.memgres.client.RawWireClient.int16(0),
                            com.memgres.client.RawWireClient.int16(0))));
            byte[] run = com.memgres.client.RawWireClient.frame('E',
                    com.memgres.client.RawWireClient.concat(
                            com.memgres.client.RawWireClient.cstring("zzw4d_portal"),
                            com.memgres.client.RawWireClient.int32(0)));
            wire.write(run);
            wire.write(run);
            wire.write(com.memgres.client.RawWireClient.sync());
            assertEquals("[1, 2, C[INSERT 0 1], E[55000] portal \"zzw4d_portal\" cannot be run,"
                    + " Z[E]]", readToReady.apply(wire).toString());

            wire.write(com.memgres.client.RawWireClient.query("CREATE TABLE zzw4d_ab2 (id int)"));
            assertEquals("[E[25P02] current transaction is aborted, commands ignored until end"
                    + " of transaction block, Z[E]]", readToReady.apply(wire).toString());

            wire.write(com.memgres.client.RawWireClient.query("COMMIT"));
            assertEquals("Z[I]", readToReady.apply(wire).get(1));
        }
        assertEquals("0", scalar("SELECT count(*) FROM zzw4d_ab"));
        assertEquals("0", scalar("SELECT count(*) FROM pg_class WHERE relname = 'zzw4d_ab2'"));
        exec("DROP TABLE IF EXISTS zzw4d_ab CASCADE");
    }

    /**
     * When the bytes of an extended-protocol COPY reach the client. The completion is written and
     * left for the Flush or Sync the client asks for, so CopyDone is answered with nothing; the
     * refusal of a COPY into a view is pushed as soon as it is written, so a client that flushes
     * and waits is told at Execute rather than blocking until Sync.
     *
     * <p>Needs com.memgres.client.RawWireClient, which is already in the test tree.
     */
    @Test
    void extendedCopyHoldsItsCompletionForSyncAndDeliversItsRefusalAtOnce() throws Exception {
        exec("DROP VIEW IF EXISTS zzw4d_vcx CASCADE");
        exec("DROP TABLE IF EXISTS zzw4d_cx CASCADE");
        exec("CREATE TABLE zzw4d_cx (a int)");
        exec("CREATE VIEW zzw4d_vcx AS SELECT a FROM zzw4d_cx");
        try (com.memgres.client.RawWireClient wire =
                     new com.memgres.client.RawWireClient(memgres.getPort())) {
            wire.startup("memgres", "memgres");
            wire.write(com.memgres.client.RawWireClient.parse("COPY zzw4d_cx FROM STDIN"));
            wire.write(com.memgres.client.RawWireClient.bind());
            wire.write(com.memgres.client.RawWireClient.execute());
            wire.write(com.memgres.client.RawWireClient.frame('H', new byte[0]));
            assertEquals("[1, 2, G, <waiting>]", wire.readUntilQuiet().toString());
            wire.write(com.memgres.client.RawWireClient.frame('d',
                    "7\n".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            wire.write(com.memgres.client.RawWireClient.frame('c', new byte[0]));
            assertEquals("[<waiting>]", wire.readUntilQuiet().toString());
            wire.write(com.memgres.client.RawWireClient.sync());
            assertEquals("[C[COPY 1], Z[I], <waiting>]", wire.readUntilQuiet().toString());

            wire.write(com.memgres.client.RawWireClient.parse("COPY zzw4d_vcx FROM STDIN"));
            wire.write(com.memgres.client.RawWireClient.bind());
            wire.write(com.memgres.client.RawWireClient.execute());
            wire.write(com.memgres.client.RawWireClient.frame('H', new byte[0]));
            assertEquals("[1, 2, G, E[42809] cannot copy to view \"zzw4d_vcx\", <waiting>]",
                    wire.readUntilQuiet().toString());
            wire.write(com.memgres.client.RawWireClient.sync());
            assertEquals("[Z[I], <waiting>]", wire.readUntilQuiet().toString());
        }
        assertEquals("1", scalar("SELECT count(*) FROM zzw4d_cx"));
        exec("DROP VIEW IF EXISTS zzw4d_vcx CASCADE");
        exec("DROP TABLE IF EXISTS zzw4d_cx CASCADE");
    }

    // ------------------------------------------------------------ A drop takes effect, and a deferred check fires once

    // Class-level helpers these tests use beyond exec/scalar/num/stateOf. The connection is the
    // usual static one opened with memgres.getJdbcUrl() + "?preferQueryMode=simple".

    /** Every value of the first column, in order, joined with a comma. */
    private static String column(String sql) throws SQLException {
        List<String> got = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) got.add(rs.getString(1));
        }
        return String.join(",", got);
    }

    /** A log table and a trigger function that writes one line per firing into it. */
    private static void logger(String prefix) throws SQLException {
        exec("CREATE TABLE " + prefix + "_log (seq serial, t text)");
        exec("CREATE FUNCTION " + prefix + "_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO " + prefix + "_log (t) VALUES (TG_TABLE_NAME || '/' || TG_WHEN"
                + " || '/' || TG_OP || '/' || TG_LEVEL); RETURN NULL; END $$");
    }

    /** Runs {@code holding} in a second session and leaves it uncommitted for the body. */
    private void whileAnotherSessionHolds(String holding, Body body) throws Exception {
        try (Connection other = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword())) {
            other.setAutoCommit(false);
            try (Statement s = other.createStatement()) {
                s.execute(holding);
            }
            body.run();
            other.rollback();
        }
    }

    private interface Body {
        void run() throws Exception;
    }

    @Test
    void cascade_delete_fires_the_referencing_table_statement_triggers_once() throws SQLException {
        logger("rsa");
        exec("CREATE TABLE rsa_parent (id int PRIMARY KEY)");
        exec("CREATE TABLE rsa_child (id int PRIMARY KEY,"
                + " p int REFERENCES rsa_parent(id) ON DELETE CASCADE)");
        exec("INSERT INTO rsa_parent VALUES (1),(2),(3)");
        exec("INSERT INTO rsa_child VALUES (10,1),(11,1),(20,2)");
        exec("CREATE TRIGGER rsa_bs BEFORE DELETE ON rsa_child FOR EACH STATEMENT"
                + " EXECUTE FUNCTION rsa_note()");
        exec("CREATE TRIGGER rsa_as AFTER DELETE ON rsa_child FOR EACH STATEMENT"
                + " EXECUTE FUNCTION rsa_note()");
        exec("CREATE TRIGGER rsa_ar AFTER DELETE ON rsa_child FOR EACH ROW"
                + " EXECUTE FUNCTION rsa_note()");

        // Two parent rows, three child rows: one BEFORE and one AFTER for the statement, and one
        // AFTER for each row the action took.
        exec("DELETE FROM rsa_parent WHERE id IN (1,2)");
        assertEquals("rsa_child/BEFORE/DELETE/STATEMENT,rsa_child/AFTER/DELETE/ROW,"
                        + "rsa_child/AFTER/DELETE/ROW,rsa_child/AFTER/DELETE/ROW,"
                        + "rsa_child/AFTER/DELETE/STATEMENT",
                column("SELECT t FROM rsa_log ORDER BY seq"));
        assertEquals(0, num("SELECT count(*) FROM rsa_child"));

        // A parent row with no children still runs the action, so they still fire.
        exec("DELETE FROM rsa_log");
        exec("DELETE FROM rsa_parent WHERE id = 3");
        assertEquals("rsa_child/BEFORE/DELETE/STATEMENT,rsa_child/AFTER/DELETE/STATEMENT",
                column("SELECT t FROM rsa_log ORDER BY seq"));

        // No parent row means no action and nothing to fire.
        exec("DELETE FROM rsa_log");
        exec("DELETE FROM rsa_parent WHERE id = 99");
        assertEquals(0, num("SELECT count(*) FROM rsa_log"));
    }

    @Test
    void update_cascade_and_delete_set_null_fire_them_as_an_update() throws SQLException {
        logger("rsb");
        exec("CREATE TABLE rsb_parent (id int PRIMARY KEY)");
        exec("CREATE TABLE rsb_child (id int PRIMARY KEY, p int REFERENCES rsb_parent(id)"
                + " ON UPDATE CASCADE ON DELETE SET NULL)");
        exec("INSERT INTO rsb_parent VALUES (1),(2)");
        exec("INSERT INTO rsb_child VALUES (10,1),(20,2)");
        exec("CREATE TRIGGER rsb_bs BEFORE UPDATE ON rsb_child FOR EACH STATEMENT"
                + " EXECUTE FUNCTION rsb_note()");
        exec("CREATE TRIGGER rsb_as AFTER UPDATE ON rsb_child FOR EACH STATEMENT"
                + " EXECUTE FUNCTION rsb_note()");

        exec("UPDATE rsb_parent SET id = id + 100 WHERE id IN (1,2)");
        assertEquals("rsb_child/BEFORE/UPDATE/STATEMENT,rsb_child/AFTER/UPDATE/STATEMENT",
                column("SELECT t FROM rsb_log ORDER BY seq"));
        assertEquals("101,102", column("SELECT p FROM rsb_child ORDER BY id"));

        exec("DELETE FROM rsb_log");
        exec("DELETE FROM rsb_parent");
        assertEquals("rsb_child/BEFORE/UPDATE/STATEMENT,rsb_child/AFTER/UPDATE/STATEMENT",
                column("SELECT t FROM rsb_log ORDER BY seq"));
        assertEquals(2, num("SELECT count(*) FROM rsb_child WHERE p IS NULL"));
    }

    @Test
    void a_self_referencing_cascade_fires_them_once_not_twice() throws SQLException {
        logger("rsc");
        exec("CREATE TABLE rsc_t (id int PRIMARY KEY,"
                + " parent int REFERENCES rsc_t(id) ON DELETE CASCADE)");
        exec("INSERT INTO rsc_t VALUES (1,NULL),(2,1),(3,2)");
        exec("CREATE TRIGGER rsc_bs BEFORE DELETE ON rsc_t FOR EACH STATEMENT"
                + " EXECUTE FUNCTION rsc_note()");
        exec("CREATE TRIGGER rsc_as AFTER DELETE ON rsc_t FOR EACH STATEMENT"
                + " EXECUTE FUNCTION rsc_note()");
        // The statement's own target and the action's referencing table are the same relation, and
        // a relation's statement-level triggers fire once for the statement.
        exec("DELETE FROM rsc_t WHERE id = 1");
        assertEquals("rsc_t/BEFORE/DELETE/STATEMENT,rsc_t/AFTER/DELETE/STATEMENT",
                column("SELECT t FROM rsc_log ORDER BY seq"));
        assertEquals(0, num("SELECT count(*) FROM rsc_t"));
    }

    @Test
    void an_after_statement_trigger_sees_the_cascaded_rows_in_its_transition_table()
            throws SQLException {
        exec("CREATE TABLE rsd_log (seq serial, t text)");
        exec("CREATE FUNCTION rsd_note() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE n int;"
                + " BEGIN SELECT count(*) INTO n FROM rsd_gone;"
                + " INSERT INTO rsd_log (t) VALUES (TG_TABLE_NAME || '/oldtable=' || n);"
                + " RETURN NULL; END $$");
        exec("CREATE TABLE rsd_parent (id int PRIMARY KEY)");
        exec("CREATE TABLE rsd_child (id int PRIMARY KEY,"
                + " p int REFERENCES rsd_parent(id) ON DELETE CASCADE)");
        exec("INSERT INTO rsd_parent VALUES (1),(2),(3)");
        exec("INSERT INTO rsd_child VALUES (10,1),(11,1),(20,2)");
        exec("CREATE TRIGGER rsd_as AFTER DELETE ON rsd_child REFERENCING OLD TABLE AS rsd_gone"
                + " FOR EACH STATEMENT EXECUTE FUNCTION rsd_note()");
        // One firing for the statement, over every row the action took from every parent row.
        exec("DELETE FROM rsd_parent WHERE id IN (1,2,3)");
        assertEquals("rsd_child/oldtable=3", column("SELECT t FROM rsd_log ORDER BY seq"));
    }

    @Test
    void no_action_writes_nothing_to_the_referencing_table_and_fires_nothing() throws SQLException {
        logger("rse");
        exec("CREATE TABLE rse_parent (id int PRIMARY KEY)");
        exec("CREATE TABLE rse_child (id int PRIMARY KEY,"
                + " p int REFERENCES rse_parent(id) ON DELETE NO ACTION)");
        exec("INSERT INTO rse_parent VALUES (1),(2)");
        exec("INSERT INTO rse_child VALUES (10,1)");
        exec("CREATE TRIGGER rse_bs BEFORE DELETE ON rse_child FOR EACH STATEMENT"
                + " EXECUTE FUNCTION rse_note()");
        exec("CREATE TRIGGER rse_as AFTER DELETE ON rse_child FOR EACH STATEMENT"
                + " EXECUTE FUNCTION rse_note()");
        exec("DELETE FROM rse_parent WHERE id = 2");
        assertEquals(0, num("SELECT count(*) FROM rse_log"));
    }

    @Test
    void they_fire_inside_an_explicit_transaction_too() throws SQLException {
        logger("rsf");
        exec("CREATE TABLE rsf_parent (id int PRIMARY KEY)");
        exec("CREATE TABLE rsf_child (id int PRIMARY KEY,"
                + " p int REFERENCES rsf_parent(id) ON DELETE CASCADE)");
        exec("CREATE TRIGGER rsf_bs BEFORE DELETE ON rsf_child FOR EACH STATEMENT"
                + " EXECUTE FUNCTION rsf_note()");
        exec("CREATE TRIGGER rsf_as AFTER DELETE ON rsf_child FOR EACH STATEMENT"
                + " EXECUTE FUNCTION rsf_note()");
        conn.setAutoCommit(false);
        try {
            exec("INSERT INTO rsf_parent VALUES (1)");
            exec("INSERT INTO rsf_child VALUES (10,1)");
            exec("DELETE FROM rsf_parent WHERE id = 1");
            // The AFTER half belongs to the statement, not to the commit, so it has already run.
            assertEquals("rsf_child/BEFORE/DELETE/STATEMENT,rsf_child/AFTER/DELETE/STATEMENT",
                    column("SELECT t FROM rsf_log ORDER BY seq"));
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }
    }

    @Test
    void drop_type_takes_a_composite_away_while_another_session_holds_uncommitted_ddl()
            throws Exception {
        exec("CREATE TYPE dta_c AS (x int)");
        whileAnotherSessionHolds("CREATE TYPE dtb_c AS (y int)", () -> {
            assertEquals("OK", stateOf("DROP TYPE dta_c"));
            assertEquals("42704", stateOf("CREATE TABLE dta_t (c dta_c)"));
            assertEquals(0, num("SELECT count(*) FROM pg_type WHERE typname = 'dta_c'"));
        });
    }

    @Test
    void drop_domain_takes_it_away_while_another_session_holds_uncommitted_ddl() throws Exception {
        exec("CREATE DOMAIN dta_d AS int");
        whileAnotherSessionHolds("CREATE DOMAIN dtb_d AS int", () -> {
            assertEquals("OK", stateOf("DROP DOMAIN dta_d"));
            assertEquals("42704", stateOf("CREATE TABLE dta_dt (c dta_d)"));
            assertEquals(0, num("SELECT count(*) FROM pg_type WHERE typname = 'dta_d'"));
        });
    }

    @Test
    void drop_type_takes_an_enum_away_while_another_session_holds_uncommitted_ddl()
            throws Exception {
        exec("CREATE TYPE dta_e AS ENUM ('a')");
        whileAnotherSessionHolds("CREATE TYPE dtb_e AS ENUM ('b')", () -> {
            assertEquals("OK", stateOf("DROP TYPE dta_e"));
            assertEquals("42704", stateOf("CREATE TABLE dta_et (c dta_e)"));
            assertEquals(0, num("SELECT count(*) FROM pg_type WHERE typname = 'dta_e'"));
        });
    }

    @Test
    void alter_type_rename_moves_an_enum_while_another_session_holds_uncommitted_ddl()
            throws Exception {
        exec("CREATE TYPE dta_r AS ENUM ('a')");
        whileAnotherSessionHolds("CREATE TYPE dtb_r AS ENUM ('b')", () -> {
            assertEquals("OK", stateOf("ALTER TYPE dta_r RENAME TO dta_r2"));
            // The type answers to its new name and to nothing else: it is one type, not two.
            assertEquals("42704", stateOf("CREATE TABLE dta_rt (c dta_r)"));
            assertEquals(0, num("SELECT count(*) FROM pg_type WHERE typname = 'dta_r'"));
            assertEquals(1, num("SELECT count(*) FROM pg_type WHERE typname = 'dta_r2'"));
        });
    }

    // ------------------------------------------------------------ Composites, snapshots and data-modifying CTEs

    @Test
    void aDomainMayStandOverACompositeType() throws Exception {
        exec("DROP DOMAIN IF EXISTS zzw4f_dc CASCADE");
        exec("DROP TYPE IF EXISTS zzw4f_c9 CASCADE");
        exec("CREATE TYPE zzw4f_c9 AS (a int, b text)");
        try {
            assertEquals("OK", stateOf("CREATE DOMAIN zzw4f_dc AS zzw4f_c9 CHECK ((VALUE).a > 3)"));
            assertEquals("1",
                    scalar("SELECT count(*)::text FROM pg_type WHERE typname = 'zzw4f_dc'"));
        } finally {
            exec("DROP DOMAIN IF EXISTS zzw4f_dc CASCADE");
            exec("DROP TYPE IF EXISTS zzw4f_c9 CASCADE");
        }
    }

    @Test
    void repeatableReadSnapshotsTheDatabaseAtItsFirstStatement() throws Exception {
        exec("DROP TABLE IF EXISTS zzw4f_late");
        try (Connection b = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword())) {
            b.setAutoCommit(false);
            try (Statement s = b.createStatement()) {
                s.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                try (ResultSet rs = s.executeQuery("SELECT 1")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
            }
            // Another session creates and fills a relation after B's snapshot was taken.
            exec("CREATE TABLE zzw4f_late (i int)");
            exec("INSERT INTO zzw4f_late VALUES (1),(2)");
            try (Statement s = b.createStatement()) {
                try (ResultSet rs = s.executeQuery(
                        "SELECT count(*) FROM pg_class WHERE relname = 'zzw4f_late'")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1), "pg_class");
                }
                try (ResultSet rs = s.executeQuery("SELECT count(*) FROM zzw4f_late")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1), "rows");
                }
            }
            b.rollback();
        } finally {
            exec("DROP TABLE IF EXISTS zzw4f_late");
        }
    }

    @Test
    void repeatableReadStillSeesARelationItCreatedItself() throws Exception {
        try (Connection b = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword())) {
            b.setAutoCommit(false);
            try (Statement s = b.createStatement()) {
                s.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                s.execute("SELECT 1");
                s.execute("CREATE TABLE zzw4f_own (i int)");
                s.execute("INSERT INTO zzw4f_own VALUES (1),(2),(3)");
                try (ResultSet rs = s.executeQuery("SELECT count(*) FROM zzw4f_own")) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1));
                }
                try (ResultSet rs = s.executeQuery(
                        "SELECT count(*) FROM pg_class WHERE relname = 'zzw4f_own'")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
            }
            b.rollback();
        }
    }

    @Test
    void twoWritingWithItemsOnOneRowLeaveOnlyTheFirstWrite() throws Exception {
        exec("DROP TABLE IF EXISTS zzw4f_cte");
        exec("CREATE TABLE zzw4f_cte (id int PRIMARY KEY, v int)");
        exec("INSERT INTO zzw4f_cte VALUES (1,0)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "WITH a AS (UPDATE zzw4f_cte SET v = 100 WHERE id = 1 RETURNING id),"
                     + " b AS (UPDATE zzw4f_cte SET v = 200 WHERE id = 1 RETURNING id)"
                     + " SELECT (SELECT count(*) FROM a) AS ca, (SELECT count(*) FROM b) AS cb")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("ca"));
            assertEquals(0, rs.getInt("cb"));
        }
        assertEquals("100", scalar("SELECT v::text FROM zzw4f_cte WHERE id = 1"));
        exec("DROP TABLE zzw4f_cte");
    }

    @Test
    void anAbortedUpdateLeavesTheRowsTupleIdentityAlone() throws Exception {
        exec("DROP TABLE IF EXISTS zzw4f_c8");
        exec("CREATE TABLE zzw4f_c8 (id int PRIMARY KEY, v int)");
        exec("INSERT INTO zzw4f_c8 VALUES (1,1)");
        assertEquals("(0,1)", scalar("SELECT ctid::text FROM zzw4f_c8 WHERE id = 1"));
        exec("BEGIN");
        exec("UPDATE zzw4f_c8 SET v = v + 1 WHERE id = 1");
        assertEquals("(0,2)", scalar("SELECT ctid::text FROM zzw4f_c8 WHERE id = 1"));
        exec("ROLLBACK");
        assertEquals("(0,1)", scalar("SELECT ctid::text FROM zzw4f_c8 WHERE id = 1"));
        assertEquals("1", scalar("SELECT v::text FROM zzw4f_c8 WHERE id = 1"));
        exec("DROP TABLE zzw4f_c8");
    }

    // ---------------------------------------------------------- VIRTUAL generated columns

    @Test
    void aVirtualColumnIsWorkedOutForTheRowsAQueryKeepsAndForNoOthers() throws Exception {
        exec("DROP TABLE IF EXISTS zzw5a_vg1 CASCADE");
        exec("CREATE TABLE zzw5a_vg1 (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO zzw5a_vg1 (a,k) VALUES (5,'five')");
        exec("INSERT INTO zzw5a_vg1 (a,k) VALUES (0,'zero')");

        // PostgreSQL puts the generation expression where the reference to the column stood, so
        // the select list's is evaluated for the rows the WHERE let through and for no others.
        assertEquals("2", scalar("SELECT g FROM zzw5a_vg1 WHERE a = 5"));
        assertEquals("(no rows)", scalar("SELECT g FROM zzw5a_vg1 WHERE false"));
        assertEquals("zero", scalar("SELECT k FROM zzw5a_vg1 WHERE a = 0"));
        assertEquals(2L, num("SELECT count(*) FROM zzw5a_vg1"));

        // Reading it of the row it raises for is the one case that raises.
        assertEquals("division by zero", messageOf("SELECT g FROM zzw5a_vg1"));
        assertEquals("22012", stateOf("SELECT * FROM zzw5a_vg1"));
        assertEquals("22012", stateOf("SELECT k FROM zzw5a_vg1 ORDER BY g"));
        assertEquals("22012", stateOf("SELECT k FROM zzw5a_vg1 WHERE g > 1"));
        assertEquals("22012", stateOf("SELECT max(k) FROM zzw5a_vg1 GROUP BY g"));

        exec("DROP TABLE zzw5a_vg1");
    }

    @Test
    void aWriteIsNotLostToAVirtualColumnTheStatementNeverNames() throws Exception {
        exec("DROP TABLE IF EXISTS zzw5a_vg2 CASCADE");
        exec("CREATE TABLE zzw5a_vg2 (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO zzw5a_vg2 (a,k) VALUES (5,'five')");
        exec("INSERT INTO zzw5a_vg2 (a,k) VALUES (0,'zero')");

        // None of these names g, so PostgreSQL never evaluates it and every one of them writes.
        assertEquals(1, update("UPDATE zzw5a_vg2 SET k = 'x' WHERE a = 5"));
        assertEquals("x", scalar("SELECT k FROM zzw5a_vg2 WHERE a = 5"));
        assertEquals(1, update("UPDATE zzw5a_vg2 SET k = k || '!' WHERE a = 0"));
        assertEquals("zero!", scalar("SELECT k FROM zzw5a_vg2 WHERE a = 0"));
        assertEquals(1, update("DELETE FROM zzw5a_vg2 WHERE k = 'zero!'"));
        assertEquals(1L, num("SELECT count(*) FROM zzw5a_vg2"));
        assertEquals("OK", stateOf("INSERT INTO zzw5a_vg2 (a,k) VALUES (0,'r1') RETURNING k"));
        assertEquals("r1", scalar("SELECT k FROM zzw5a_vg2 WHERE a = 0"));

        // An assignment that does read it reads it of the rows the qualification kept.
        assertEquals(1, update("UPDATE zzw5a_vg2 SET k = 'z' || g WHERE a = 5"));
        assertEquals("z2", scalar("SELECT k FROM zzw5a_vg2 WHERE a = 5"));

        exec("DROP TABLE zzw5a_vg2");
    }

    @Test
    void aDerivedTableReadsOfTheRelationWhatTheQueryAroundItReads() throws Exception {
        exec("DROP TABLE IF EXISTS zzw5a_vg3 CASCADE");
        exec("CREATE TABLE zzw5a_vg3 (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO zzw5a_vg3 (a,k) VALUES (5,'five')");
        exec("INSERT INTO zzw5a_vg3 (a,k) VALUES (0,'zero')");

        // PostgreSQL pulls a derived table up into the query that reads it, so the * inside one
        // asks for what the query around it asks of it and no more.
        assertEquals("five", scalar("SELECT k FROM (SELECT * FROM zzw5a_vg3) s ORDER BY k"));
        assertEquals(2L, num("SELECT count(*) FROM (SELECT * FROM zzw5a_vg3) s"));
        assertEquals(2L, num("SELECT count(*) FROM (SELECT * FROM (SELECT * FROM zzw5a_vg3) u) s"));
        assertEquals("five", scalar("WITH c AS (SELECT * FROM zzw5a_vg3) SELECT k FROM c ORDER BY k"));
        assertEquals(2L, num("WITH c AS (SELECT * FROM zzw5a_vg3) SELECT count(*) FROM c"));
        // The query around this one does read every column, so the expression is evaluated.
        assertEquals("22012", stateOf("SELECT * FROM (SELECT * FROM zzw5a_vg3) s"));

        exec("DROP TABLE zzw5a_vg3");
    }

    // ---------------------------------------------------------- what a rule's NEW carries

    @Test
    void aRuleReadsTheDefaultOfAColumnTheInsertLeftOut() throws Exception {
        exec("DROP TABLE IF EXISTS zzw5a_rd1 CASCADE");
        exec("DROP TABLE IF EXISTS zzw5a_rd1log CASCADE");
        exec("CREATE TABLE zzw5a_rd1 (a text DEFAULT 'dflt', b int)");
        exec("CREATE TABLE zzw5a_rd1log (m text)");
        exec("CREATE RULE zzw5a_rd1r AS ON INSERT TO zzw5a_rd1"
                + " DO ALSO INSERT INTO zzw5a_rd1log VALUES (NEW.a)");
        exec("INSERT INTO zzw5a_rd1 (b) VALUES (7)");

        // PostgreSQL fills in the defaults while it rewrites the statement, which is before the
        // rules are applied, so NEW carries the default and not a null.
        assertEquals("dflt", scalar("SELECT a FROM zzw5a_rd1"));
        assertEquals("dflt", scalar("SELECT m FROM zzw5a_rd1log"));

        exec("DROP TABLE zzw5a_rd1 CASCADE");
        exec("DROP TABLE zzw5a_rd1log CASCADE");
    }

    @Test
    void aRuleQualificationReadsTheDefaultTheSameWay() throws Exception {
        exec("DROP TABLE IF EXISTS zzw5a_rd2 CASCADE");
        exec("DROP TABLE IF EXISTS zzw5a_rd2log CASCADE");
        exec("CREATE TABLE zzw5a_rd2 (id int, a text DEFAULT 'dv')");
        exec("CREATE TABLE zzw5a_rd2log (m text)");
        exec("CREATE RULE zzw5a_rd2r AS ON INSERT TO zzw5a_rd2 WHERE NEW.a = 'dv'"
                + " DO ALSO INSERT INTO zzw5a_rd2log VALUES ('sawdefault')");
        exec("INSERT INTO zzw5a_rd2 (id) VALUES (1)");
        exec("INSERT INTO zzw5a_rd2 (id, a) VALUES (2, 'other')");

        assertEquals("dv", scalar("SELECT a FROM zzw5a_rd2 WHERE id = 1"));
        assertEquals(1L, num("SELECT count(*) FROM zzw5a_rd2log"));
        assertEquals("sawdefault", scalar("SELECT m FROM zzw5a_rd2log"));

        exec("DROP TABLE zzw5a_rd2 CASCADE");
        exec("DROP TABLE zzw5a_rd2log CASCADE");
    }

    @Test
    void theDefaultKeywordInAValuesListIsTheColumnsDefaultUnderARuleToo() throws Exception {
        exec("DROP TABLE IF EXISTS zzw5a_rd5 CASCADE");
        exec("DROP TABLE IF EXISTS zzw5a_rd5log CASCADE");
        exec("CREATE TABLE zzw5a_rd5 (a text DEFAULT 'dflt', b int)");
        exec("CREATE TABLE zzw5a_rd5log (m text)");
        exec("CREATE RULE zzw5a_rd5r AS ON INSERT TO zzw5a_rd5"
                + " DO ALSO INSERT INTO zzw5a_rd5log VALUES (NEW.a)");

        // DEFAULT is not a value and evaluates to nothing: it asks for the column's default.
        assertEquals("OK", stateOf("INSERT INTO zzw5a_rd5 (a,b) VALUES (DEFAULT, 8)"));
        assertEquals("OK", stateOf("INSERT INTO zzw5a_rd5 VALUES (DEFAULT, 9)"));
        assertEquals(2L, num("SELECT count(*) FROM zzw5a_rd5 WHERE a = 'dflt'"));
        assertEquals(2L, num("SELECT count(*) FROM zzw5a_rd5log WHERE m = 'dflt'"));

        exec("DROP TABLE zzw5a_rd5 CASCADE");
        exec("DROP TABLE zzw5a_rd5log CASCADE");
    }

    @Test
    void aColumnTheSystemComputesReadsItsValueInNewAndOneWithNoDefaultReadsNull() throws Exception {
        exec("DROP TABLE IF EXISTS zzw5a_rd3 CASCADE");
        exec("DROP TABLE IF EXISTS zzw5a_rd3log CASCADE");
        exec("CREATE TABLE zzw5a_rd3 (a int, s int GENERATED ALWAYS AS (a*2) STORED, t text)");
        exec("CREATE TABLE zzw5a_rd3log (m text)");
        exec("CREATE RULE zzw5a_rd3r AS ON INSERT TO zzw5a_rd3 DO ALSO INSERT INTO zzw5a_rd3log"
                + " VALUES (coalesce(NEW.s::text, 'null') || '/' || coalesce(NEW.t, 'null'))");
        exec("INSERT INTO zzw5a_rd3 (a) VALUES (4)");

        // A stored generated column is worked out where the rule reads it, from the row the
        // statement is writing, so NEW carries the value the row is about to hold. A column
        // nothing was written to and nothing computes reads null there.
        assertEquals("8", scalar("SELECT s FROM zzw5a_rd3"));
        assertEquals("8/null", scalar("SELECT m FROM zzw5a_rd3log"));

        exec("DROP TABLE zzw5a_rd3 CASCADE");
        exec("DROP TABLE zzw5a_rd3log CASCADE");
    }

    @Test
    void aDefaultDrawnFromASequenceIsDrawnAgainForNew() throws Exception {
        exec("DROP TABLE IF EXISTS zzw5a_rd4 CASCADE");
        exec("DROP TABLE IF EXISTS zzw5a_rd4log CASCADE");
        exec("CREATE TABLE zzw5a_rd4 (id serial, b int)");
        exec("CREATE TABLE zzw5a_rd4log (m int)");
        exec("CREATE RULE zzw5a_rd4r AS ON INSERT TO zzw5a_rd4"
                + " DO ALSO INSERT INTO zzw5a_rd4log VALUES (NEW.id)");
        exec("INSERT INTO zzw5a_rd4 (b) VALUES (1)");

        // PostgreSQL evaluates a default expression once for every place the statement it rewrote
        // holds one, so the rule's NEW draws a value of its own.
        assertEquals("1", scalar("SELECT id FROM zzw5a_rd4"));
        assertEquals("2", scalar("SELECT m FROM zzw5a_rd4log"));

        exec("DROP TABLE zzw5a_rd4 CASCADE");
        exec("DROP TABLE zzw5a_rd4log CASCADE");
    }

    // ---------------------------------------------------------- an index and a constraint a relation holds because a parent has it

    @Test
    void anIndexOnAPartitionedTableNamesEachPartitionsCopyAfterThePartition() throws Exception {
        exec("CREATE TABLE zzw5b_uy (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzw5b_uy_0 PARTITION OF zzw5b_uy FOR VALUES FROM (0) TO (10)");
        exec("CREATE UNIQUE INDEX zzw5b_uy_idx ON zzw5b_uy (i)");
        // PG 18 names a partition's copy of the index after the partition and the columns it
        // reads, not after the parent's index, and there is exactly one of them.
        assertEquals("1", scalar("SELECT count(*) FROM pg_index i"
                + " JOIN pg_class c ON i.indrelid = c.oid WHERE c.relname = 'zzw5b_uy_0'"));
        assertEquals("zzw5b_uy_0_i_idx", scalar("SELECT ic.relname FROM pg_index i"
                + " JOIN pg_class c ON i.indrelid = c.oid"
                + " JOIN pg_class ic ON i.indexrelid = ic.oid"
                + " WHERE c.relname = 'zzw5b_uy_0'"));
        assertEquals("1", scalar("SELECT count(*) FROM pg_index i"
                + " JOIN pg_class c ON i.indrelid = c.oid"
                + " WHERE c.relname = 'zzw5b_uy_0' AND i.indisunique"));
        exec("INSERT INTO zzw5b_uy VALUES (3, 'a')");
        assertEquals("23505", stateOf("INSERT INTO zzw5b_uy VALUES (3, 'b')"));
        assertTrue(messageOf("INSERT INTO zzw5b_uy VALUES (3, 'b')")
                        .contains("\"zzw5b_uy_0_i_idx\""),
                "the duplicate key names the partition's own index");
        exec("DROP TABLE zzw5b_uy");

        exec("CREATE TABLE zzw5b_qy (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzw5b_qy_0 PARTITION OF zzw5b_qy FOR VALUES FROM (0) TO (10)");
        exec("CREATE INDEX zzw5b_qy_idx ON zzw5b_qy (s)");
        assertEquals("zzw5b_qy_0_s_idx",
                scalar("SELECT indexname FROM pg_indexes WHERE tablename = 'zzw5b_qy_0'"));
        exec("DROP TABLE zzw5b_qy");
    }

    @Test
    void attachPartitionIndexesTheIncomingTableTheWayTheHierarchyIsIndexed() throws Exception {
        exec("CREATE TABLE zzw5b_at (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE INDEX zzw5b_at_idx ON zzw5b_at (s)");
        exec("CREATE TABLE zzw5b_at_0 (i int, s text)");
        exec("ALTER TABLE zzw5b_at ATTACH PARTITION zzw5b_at_0 FOR VALUES FROM (0) TO (10)");
        // PG 18 gives the attached table a copy of the parent's index, named for itself.
        assertEquals("zzw5b_at_0_s_idx", scalar("SELECT ic.relname FROM pg_index i"
                + " JOIN pg_class c ON i.indrelid = c.oid"
                + " JOIN pg_class ic ON i.indexrelid = ic.oid"
                + " WHERE c.relname = 'zzw5b_at_0'"));
        assertEquals("0", scalar("SELECT count(*) FROM pg_index i"
                + " JOIN pg_class c ON i.indrelid = c.oid"
                + " WHERE c.relname = 'zzw5b_at_0' AND i.indisunique"));
        exec("DROP TABLE zzw5b_at");
    }

    @Test
    void aMatchingIndexTheRelationAlreadyCarriesIsTheCopy() throws Exception {
        // PG 18 attaches an equivalent index the table already has instead of building a second
        // one over the same rows -- on ATTACH and on CREATE INDEX alike.
        exec("CREATE TABLE zzw5b_a2 (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE INDEX zzw5b_a2_idx ON zzw5b_a2 (s)");
        exec("CREATE TABLE zzw5b_a2_0 (i int, s text)");
        exec("CREATE INDEX zzw5b_a2_own ON zzw5b_a2_0 (s)");
        exec("ALTER TABLE zzw5b_a2 ATTACH PARTITION zzw5b_a2_0 FOR VALUES FROM (0) TO (10)");
        assertEquals("1", scalar("SELECT count(*) FROM pg_index i"
                + " JOIN pg_class c ON i.indrelid = c.oid WHERE c.relname = 'zzw5b_a2_0'"));
        assertEquals("zzw5b_a2_own", scalar("SELECT ic.relname FROM pg_index i"
                + " JOIN pg_class c ON i.indrelid = c.oid"
                + " JOIN pg_class ic ON i.indexrelid = ic.oid"
                + " WHERE c.relname = 'zzw5b_a2_0'"));
        exec("DROP TABLE zzw5b_a2");

        exec("CREATE TABLE zzw5b_m (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzw5b_m_0 PARTITION OF zzw5b_m FOR VALUES FROM (0) TO (10)");
        exec("CREATE INDEX zzw5b_m_own ON zzw5b_m_0 (s)");
        exec("CREATE INDEX zzw5b_m_idx ON zzw5b_m (s)");
        assertEquals("zzw5b_m_own", scalar("SELECT ic.relname FROM pg_index i"
                + " JOIN pg_class c ON i.indrelid = c.oid"
                + " JOIN pg_class ic ON i.indexrelid = ic.oid"
                + " WHERE c.relname = 'zzw5b_m_0'"));
        assertEquals("1", scalar("SELECT count(*) FROM pg_index i"
                + " JOIN pg_class c ON i.indrelid = c.oid WHERE c.relname = 'zzw5b_m_0'"));
        exec("DROP TABLE zzw5b_m");

        // A non-unique index does not answer for a unique one, so the copy is still made.
        exec("CREATE TABLE zzw5b_n (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE UNIQUE INDEX zzw5b_n_idx ON zzw5b_n (i)");
        exec("CREATE TABLE zzw5b_n_0 (i int, s text)");
        exec("CREATE INDEX zzw5b_n_own ON zzw5b_n_0 (i)");
        exec("ALTER TABLE zzw5b_n ATTACH PARTITION zzw5b_n_0 FOR VALUES FROM (0) TO (10)");
        assertEquals("2", scalar("SELECT count(*) FROM pg_index i"
                + " JOIN pg_class c ON i.indrelid = c.oid WHERE c.relname = 'zzw5b_n_0'"));
        exec("INSERT INTO zzw5b_n VALUES (3, 'a')");
        assertTrue(messageOf("INSERT INTO zzw5b_n VALUES (3, 'b')")
                        .contains("\"zzw5b_n_0_i_idx\""),
                "the unique copy is the one a duplicate key is reported against");
        exec("DROP TABLE zzw5b_n");
    }

    @Test
    void aConstraintTakenFromAParentIsNotTheRelationsOwn() throws Exception {
        exec("CREATE TABLE zzw5b_cl (a int CHECK (a > 0), b int NOT NULL, c int PRIMARY KEY)");
        exec("CREATE TABLE zzw5b_clc () INHERITS (zzw5b_cl)");
        // PG 18: zzw5b_cl_a_check | f | 1 and both NOT NULLs f | 1 on the child; the parent's own
        // rows stay t | 0.
        assertEquals("3", scalar("SELECT count(*) FROM pg_constraint c"
                + " JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzw5b_clc'"
                + " AND c.conislocal = false AND c.coninhcount = 1"));
        assertEquals("0", scalar("SELECT count(*) FROM pg_constraint c"
                + " JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzw5b_clc'"
                + " AND c.conislocal = true"));
        assertEquals("0", scalar("SELECT count(*) FROM pg_constraint c"
                + " JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzw5b_cl'"
                + " AND (c.conislocal = false OR c.coninhcount <> 0)"));
        exec("DROP TABLE zzw5b_clc");
        exec("DROP TABLE zzw5b_cl");

        exec("CREATE TABLE zzw5b_pl (a int NOT NULL, b text, c int, PRIMARY KEY (a))"
                + " PARTITION BY RANGE (a)");
        exec("CREATE TABLE zzw5b_pl_0 PARTITION OF zzw5b_pl FOR VALUES FROM (0) TO (10)");
        exec("ALTER TABLE zzw5b_pl ADD CONSTRAINT zzw5b_plk CHECK (b <> 'bad')");
        // PG 18: the partition's pkey, its NOT NULL and the CHECK the parent declared are all
        // f | 1 -- the partition obeys them, it did not declare them.
        assertEquals("3", scalar("SELECT count(*) FROM pg_constraint c"
                + " JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzw5b_pl_0'"
                + " AND c.conislocal = false AND c.coninhcount = 1"));
        assertEquals("0", scalar("SELECT count(*) FROM pg_constraint c"
                + " JOIN pg_class cl ON c.conrelid = cl.oid WHERE cl.relname = 'zzw5b_pl_0'"
                + " AND c.conislocal = true"));
        exec("DROP TABLE zzw5b_pl");
    }
    // ---------------------------------------------------------- a definition is judged where it is written

    /**
     * A materialized view holds the rows of its last refresh, so PostgreSQL samples it exactly as
     * it samples a table. A view holds none of its own, and PG's own message names both kinds.
     */
    @Test
    void tablesampleReadsAMaterializedViewsStoredRows() throws Exception {
        exec("CREATE TABLE zzc_ts_t (a int, v text)");
        exec("INSERT INTO zzc_ts_t VALUES (1, 'x'), (2, 'y')");
        exec("CREATE MATERIALIZED VIEW zzc_ts_m AS SELECT a, v FROM zzc_ts_t");
        exec("CREATE MATERIALIZED VIEW zzc_ts_n AS SELECT a FROM zzc_ts_t WITH NO DATA");
        exec("CREATE VIEW zzc_ts_v AS SELECT a, v FROM zzc_ts_t");
        try {
            assertEquals("2", scalar("SELECT count(*) FROM zzc_ts_m TABLESAMPLE SYSTEM (100)"));
            assertEquals("2", scalar("SELECT count(*) FROM zzc_ts_m TABLESAMPLE BERNOULLI (100)"));
            assertEquals("0", scalar("SELECT count(*) FROM zzc_ts_m TABLESAMPLE SYSTEM (0)"));
            assertEquals("2", scalar("SELECT count(*) FROM zzc_ts_m s TABLESAMPLE SYSTEM (100)"));
            assertEquals("2", scalar("SELECT count(*) FROM public.zzc_ts_m TABLESAMPLE SYSTEM (100)"));
            assertEquals("x", scalar("SELECT v FROM zzc_ts_m TABLESAMPLE SYSTEM (100) ORDER BY a"));
            // A view is computed, not stored: it has no pages of its own to take a fraction of.
            assertEquals("0A000", stateOf("SELECT count(*) FROM zzc_ts_v TABLESAMPLE SYSTEM (100)"));
            assertTrue(messageOf("SELECT count(*) FROM zzc_ts_v TABLESAMPLE SYSTEM (100)")
                    .contains("TABLESAMPLE clause can only be applied to tables and materialized views"));
            // One that has never been populated holds nothing to sample yet.
            assertEquals("55000", stateOf("SELECT count(*) FROM zzc_ts_n TABLESAMPLE SYSTEM (100)"));
        } finally {
            exec("DROP VIEW zzc_ts_v");
            exec("DROP MATERIALIZED VIEW zzc_ts_n");
            exec("DROP MATERIALIZED VIEW zzc_ts_m");
            exec("DROP TABLE zzc_ts_t");
        }
    }

    /**
     * PostgreSQL judges a DEFAULT where the column is defined: it looks for an assignment cast
     * from the expression's type to the column's and refuses the pair when there is none, rather
     * than storing a table every INSERT that omits the column would fail on.
     */
    @Test
    void aColumnDefaultIsTypeCheckedWhereTheColumnIsDefined() throws Exception {
        assertEquals("42804", stateOf("CREATE TABLE zzc_df1 (a text, b int DEFAULT 'abc'::text)"));
        assertTrue(messageOf("CREATE TABLE zzc_df1 (a text, b int DEFAULT 'abc'::text)")
                .contains("column \"b\" is of type integer but default expression is of type text"));
        assertEquals("42804", stateOf("CREATE TABLE zzc_df2 (a int, b date DEFAULT 1)"));
        assertTrue(messageOf("CREATE TABLE zzc_df2 (a int, b date DEFAULT 1)")
                .contains("column \"b\" is of type date but default expression is of type integer"));
        // A literal that carries its own type is a type mismatch, not bad input syntax.
        assertEquals("42804", stateOf("CREATE TABLE zzc_df3 (a int, b int DEFAULT true)"));
        assertEquals("42804", stateOf("CREATE TABLE zzc_df4 (a int, b uuid DEFAULT 3)"));
        assertEquals("42804", stateOf("CREATE TABLE zzc_df5 (a int, b interval DEFAULT 3)"));
        // Nothing the refusals named was created.
        assertEquals("0", scalar("SELECT count(*) FROM information_schema.tables"
                + " WHERE table_name LIKE 'zzc\\_df%'"));
        // The pairs PostgreSQL has an assignment cast for stand, and whether the value fits is
        // the insert's business: 2147483648 is a bigint the first row to take it fails on.
        exec("CREATE TABLE zzc_df6 (a text DEFAULT 5, b numeric DEFAULT 1, c int DEFAULT 1.5,"
                + " d int DEFAULT '5', e timestamp DEFAULT now(), f int DEFAULT 2147483648,"
                + " g int DEFAULT NULL, h money DEFAULT 3, i int[] DEFAULT '{1,2}')");
        try {
            assertEquals("5", scalar("SELECT a FROM (SELECT * FROM zzc_df6 UNION ALL SELECT * FROM zzc_df6) z"
                    + " WHERE false UNION ALL SELECT '5'"));
        } finally {
            exec("DROP TABLE zzc_df6");
        }
    }

    /**
     * A typed table's shape is its composite type's. PostgreSQL refuses to drop a column from one
     * before it even looks the column up, so a name that is not there gets the same refusal.
     */
    @Test
    void aTypedTableKeepsEveryColumnItsTypeDeclares() throws Exception {
        exec("CREATE TYPE zzc_ct AS (x int, y text)");
        exec("CREATE TABLE zzc_of OF zzc_ct (x WITH OPTIONS NOT NULL, y WITH OPTIONS DEFAULT 'dd')");
        try {
            assertEquals("42809", stateOf("ALTER TABLE zzc_of DROP COLUMN y"));
            assertTrue(messageOf("ALTER TABLE zzc_of DROP COLUMN y")
                    .contains("cannot drop column from typed table"));
            assertEquals("42809", stateOf("ALTER TABLE zzc_of DROP COLUMN IF EXISTS y"));
            assertEquals("42809", stateOf("ALTER TABLE zzc_of DROP COLUMN nosuch"));
            assertEquals("42809", stateOf("ALTER TABLE zzc_of DROP COLUMN IF EXISTS nosuch"));
            assertEquals("42809", stateOf("ALTER TABLE zzc_of DROP COLUMN y CASCADE"));
            // The table still has the shape it was declared with.
            assertEquals("2", scalar("SELECT count(*) FROM information_schema.columns"
                    + " WHERE table_name = 'zzc_of'"));
            assertEquals("'dd'::text", scalar("SELECT column_default FROM information_schema.columns"
                    + " WHERE table_name = 'zzc_of' AND column_name = 'y'"));
            assertEquals("NO", scalar("SELECT is_nullable FROM information_schema.columns"
                    + " WHERE table_name = 'zzc_of' AND column_name = 'x'"));
        } finally {
            exec("DROP TABLE zzc_of");
            exec("DROP TYPE zzc_ct");
        }
    }

    /**
     * A default written on a view column is filed against the view's own relation, which is the
     * catalogue information_schema is derived from and the one pg_dump reads.
     */
    @Test
    void aViewColumnDefaultIsFiledAgainstTheView() throws Exception {
        exec("CREATE TABLE zzc_ad (i int DEFAULT 7, w text)");
        exec("CREATE VIEW zzc_adv AS SELECT i, w FROM zzc_ad");
        try {
            exec("ALTER VIEW zzc_adv ALTER COLUMN w SET DEFAULT 'vv'");
            assertEquals("2|'vv'::text", scalar("SELECT adnum || '|' || pg_get_expr(adbin, adrelid)"
                    + " FROM pg_attrdef WHERE adrelid = 'zzc_adv'::regclass"));
            // The relation underneath keeps its own default and gains nothing from the view's.
            assertEquals("1|7", scalar("SELECT adnum || '|' || pg_get_expr(adbin, adrelid)"
                    + " FROM pg_attrdef WHERE adrelid = 'zzc_ad'::regclass"));
            assertEquals("'vv'::text", scalar("SELECT column_default FROM information_schema.columns"
                    + " WHERE table_name = 'zzc_adv' AND column_name = 'w'"));
            exec("ALTER VIEW zzc_adv ALTER COLUMN w DROP DEFAULT");
            assertEquals("0", scalar("SELECT count(*) FROM pg_attrdef"
                    + " WHERE adrelid = 'zzc_adv'::regclass"));
        } finally {
            exec("DROP VIEW zzc_adv");
            exec("DROP TABLE zzc_ad");
        }
    }

    /**
     * pg_get_viewdef prints the analysed query, not the text it was written as: columns bare
     * where the query reads one relation, an unknown literal as the constant it resolved to, and
     * a line per relation in the FROM list.
     */
    @Test
    void aViewDefinitionReadsAsPostgresPrintsIt() throws Exception {
        exec("CREATE TABLE zzc_vd (v text, w text)");
        exec("CREATE TABLE zzc_vd2 (x int)");
        exec("CREATE VIEW zzc_vds AS SELECT * FROM zzc_vd");
        exec("CREATE VIEW zzc_vdw AS SELECT upper(v) AS uv, w AS wv, 'v' AS lit"
                + " FROM zzc_vd WHERE v = 'v' ORDER BY w");
        exec("CREATE VIEW zzc_vdj AS SELECT a.v, b.x FROM zzc_vd a, zzc_vd2 b WHERE a.v = 'q'");
        exec("CREATE VIEW zzc_vdi AS SELECT x FROM zzc_vd2 WHERE x = '4'");
        try {
            assertEquals(" SELECT v,\n    w\n   FROM zzc_vd;",
                    scalar("SELECT pg_get_viewdef('zzc_vds'::regclass, true)"));
            assertEquals(" SELECT upper(v) AS uv,\n    w AS wv,\n    'v'::text AS lit\n"
                            + "   FROM zzc_vd\n  WHERE v = 'v'::text\n  ORDER BY w;",
                    scalar("SELECT pg_get_viewdef('zzc_vdw'::regclass, true)"));
            assertEquals(" SELECT a.v,\n    b.x\n   FROM zzc_vd a,\n    zzc_vd2 b\n"
                            + "  WHERE a.v = 'q'::text;",
                    scalar("SELECT pg_get_viewdef('zzc_vdj'::regclass, true)"));
            // Against an integer column the constant reads back as itself, so it prints bare.
            assertEquals(" SELECT x\n   FROM zzc_vd2\n  WHERE x = 4;",
                    scalar("SELECT pg_get_viewdef('zzc_vdi'::regclass, true)"));
            // Renaming a base column leaves the view reading it under its new name.
            exec("ALTER TABLE zzc_vd RENAME COLUMN v TO v2");
            assertEquals(" SELECT upper(v2) AS uv,\n    w AS wv,\n    'v'::text AS lit\n"
                            + "   FROM zzc_vd\n  WHERE v2 = 'v'::text\n  ORDER BY w;",
                    scalar("SELECT pg_get_viewdef('zzc_vdw'::regclass, true)"));
        } finally {
            exec("DROP VIEW zzc_vdi");
            exec("DROP VIEW zzc_vdj");
            exec("DROP VIEW zzc_vdw");
            exec("DROP VIEW zzc_vds");
            exec("DROP TABLE zzc_vd2");
            exec("DROP TABLE zzc_vd");
        }
    }

    /**
     * PostgreSQL names what depends on a type in the order it recorded the dependencies:
     * relations in the order they were created, and one relation's columns from the last back to
     * the first.
     */
    @Test
    void whatDependsOnATypeIsNamedInTheOrderItWasMade() throws Exception {
        exec("CREATE TYPE zzc_dt AS (x int, y text)");
        exec("CREATE TABLE zzc_dz OF zzc_dt");
        exec("CREATE TABLE zzc_da OF zzc_dt");
        exec("CREATE TABLE zzc_dm (q zzc_dt, r zzc_dt)");
        try {
            assertEquals("2BP01", stateOf("DROP TYPE zzc_dt"));
            assertEquals("table zzc_dz depends on type zzc_dt\n"
                            + "table zzc_da depends on type zzc_dt\n"
                            + "column r of table zzc_dm depends on type zzc_dt\n"
                            + "column q of table zzc_dm depends on type zzc_dt",
                    detailOf("DROP TYPE zzc_dt"));
        } finally {
            exec("DROP TABLE zzc_dm");
            exec("DROP TABLE zzc_da");
            exec("DROP TABLE zzc_dz");
            exec("DROP TYPE zzc_dt");
        }
    }
    // ---------------------------------------------------------- a rule is analysed while it is written

    @Test
    void ruleNamingAFunctionThatDoesNotExistIsRefusedWhereverItStands() throws Exception {
        exec("DROP TABLE IF EXISTS zzw5d_wa CASCADE");
        exec("DROP TABLE IF EXISTS zzw5d_wb CASCADE");
        exec("CREATE TABLE zzw5d_wa (i int, j int)");
        exec("CREATE TABLE zzw5d_wb (i int)");

        String qualification = "CREATE RULE zzw5d_w1r AS ON INSERT TO zzw5d_wa"
                + " WHERE nosuchfunc2(NEW.i) DO ALSO INSERT INTO zzw5d_wb VALUES (1)";
        assertEquals("42883", stateOf(qualification));
        assertTrue(messageOf(qualification).contains("function nosuchfunc2(integer) does not exist"),
                messageOf(qualification));
        assertEquals("No function matches the given name and argument types."
                + " You might need to add explicit type casts.", hintOf(qualification));

        assertEquals("42883", stateOf("CREATE RULE zzw5d_w3r AS ON INSERT TO zzw5d_wa"
                + " DO ALSO INSERT INTO zzw5d_wb SELECT nosuchfunc3(i) FROM zzw5d_wb"));
        assertEquals("42883", stateOf("CREATE RULE zzw5d_w4r AS ON INSERT TO zzw5d_wa"
                + " DO ALSO UPDATE zzw5d_wb SET i = nosuchfunc4(i)"));
        assertEquals("42883", stateOf("CREATE RULE zzw5d_w5r AS ON INSERT TO zzw5d_wa"
                + " DO ALSO DELETE FROM zzw5d_wb WHERE nosuchfunc5(i) > 0"));
        assertEquals("42883", stateOf("CREATE RULE zzw5d_w6r AS ON INSERT TO zzw5d_wa"
                + " DO ALSO INSERT INTO zzw5d_wb WITH q AS (SELECT nosuchfunc6(i) AS v"
                + " FROM zzw5d_wb) SELECT v FROM q"));

        assertEquals("0", scalar("SELECT count(*)::text FROM pg_rules WHERE tablename = 'zzw5d_wa'"));
        exec("INSERT INTO zzw5d_wa VALUES (5, 6)");
        assertEquals("1", scalar("SELECT count(*)::text FROM zzw5d_wa"));

        exec("DROP TABLE zzw5d_wa CASCADE");
        exec("DROP TABLE zzw5d_wb CASCADE");
    }

    @Test
    void ruleActionNamingAColumnItsTargetDoesNotHoldIsRefused() throws Exception {
        exec("DROP TABLE IF EXISTS zzw5d_xa CASCADE");
        exec("DROP TABLE IF EXISTS zzw5d_xb CASCADE");
        exec("CREATE TABLE zzw5d_xa (i int, j int)");
        exec("CREATE TABLE zzw5d_xb (i int)");

        String setTarget = "CREATE RULE zzw5d_x1r AS ON INSERT TO zzw5d_xa"
                + " DO ALSO UPDATE zzw5d_xb SET nosuchtarget = 1";
        assertEquals("42703", stateOf(setTarget));
        assertTrue(messageOf(setTarget)
                .contains("column \"nosuchtarget\" of relation \"zzw5d_xb\" does not exist"),
                messageOf(setTarget));

        String columnList = "CREATE RULE zzw5d_x2r AS ON INSERT TO zzw5d_xa"
                + " DO ALSO INSERT INTO zzw5d_xb (nosuchcol) VALUES (1)";
        assertEquals("42703", stateOf(columnList));
        assertTrue(messageOf(columnList)
                .contains("column \"nosuchcol\" of relation \"zzw5d_xb\" does not exist"),
                messageOf(columnList));

        // An alias on the UPDATE and a schema qualifier on the INSERT change nothing: the message
        // names the relation, not the way the action wrote it.
        assertTrue(messageOf("CREATE RULE zzw5d_x3r AS ON INSERT TO zzw5d_xa"
                + " DO ALSO UPDATE zzw5d_xb AS z SET nosuchtarget = 1")
                .contains("column \"nosuchtarget\" of relation \"zzw5d_xb\" does not exist"));
        assertTrue(messageOf("CREATE RULE zzw5d_x4r AS ON INSERT TO zzw5d_xa"
                + " DO ALSO INSERT INTO public.zzw5d_xb (nosuchcol) VALUES (1)")
                .contains("column \"nosuchcol\" of relation \"zzw5d_xb\" does not exist"));

        assertEquals("0", scalar("SELECT count(*)::text FROM pg_rules WHERE tablename = 'zzw5d_xa'"));
        exec("INSERT INTO zzw5d_xa VALUES (1, 2)");
        assertEquals("1", scalar("SELECT count(*)::text FROM zzw5d_xa"));

        exec("DROP TABLE zzw5d_xa CASCADE");
        exec("DROP TABLE zzw5d_xb CASCADE");
    }

    @Test
    void aRuleActionIsAnalysedInTheOrderPostgresAnalysesIt() throws Exception {
        exec("DROP TABLE IF EXISTS zzw5d_ya CASCADE");
        exec("DROP TABLE IF EXISTS zzw5d_yb CASCADE");
        exec("CREATE TABLE zzw5d_ya (i int, j int)");
        exec("CREATE TABLE zzw5d_yb (i int)");

        // The relation outranks everything the action says about it.
        assertEquals("42P01", stateOf("CREATE RULE zzw5d_y1r AS ON INSERT TO zzw5d_ya"
                + " DO ALSO INSERT INTO zzw5d_nosuchrel (nosuchcol) VALUES (1)"));
        // An INSERT's column list is matched before anything it is handed is read.
        assertTrue(messageOf("CREATE RULE zzw5d_y2r AS ON INSERT TO zzw5d_ya"
                + " DO ALSO INSERT INTO zzw5d_yb (nosuchcol) VALUES (nosuchfuncb(1))")
                .contains("column \"nosuchcol\" of relation \"zzw5d_yb\" does not exist"));
        assertTrue(messageOf("CREATE RULE zzw5d_y3r AS ON INSERT TO zzw5d_ya"
                + " DO ALSO INSERT INTO zzw5d_yb (nosuchcol) VALUES (NEW.nosuchx)")
                .contains("column \"nosuchcol\" of relation \"zzw5d_yb\" does not exist"));
        // An UPDATE's assignments are read before its targets are matched.
        assertTrue(messageOf("CREATE RULE zzw5d_y4r AS ON INSERT TO zzw5d_ya"
                + " DO ALSO UPDATE zzw5d_yb SET nosuchtarget = nosuchfunca(i)")
                .contains("function nosuchfunca(integer) does not exist"));
        assertTrue(messageOf("CREATE RULE zzw5d_y5r AS ON INSERT TO zzw5d_ya"
                + " DO ALSO UPDATE zzw5d_yb SET nosuchtarget = NEW.nosuchx")
                .contains("column new.nosuchx does not exist"));
        // A call's arguments are resolved before the call itself.
        assertTrue(messageOf("CREATE RULE zzw5d_y6r AS ON INSERT TO zzw5d_ya"
                + " DO ALSO INSERT INTO zzw5d_yb VALUES (nosuchouter(nosuchinner(1)))")
                .contains("function nosuchinner(integer) does not exist"));
        // The rule's own qualification is read before any of its actions.
        assertTrue(messageOf("CREATE RULE zzw5d_y7r AS ON INSERT TO zzw5d_ya"
                + " WHERE NEW.nosuchw > 0 DO ALSO INSERT INTO zzw5d_yb (nosuchcol) VALUES (1)")
                .contains("column new.nosuchw does not exist"));
        // An aggregate is resolved by its argument list like any other call.
        assertTrue(messageOf("CREATE RULE zzw5d_y8r AS ON INSERT TO zzw5d_ya"
                + " DO ALSO INSERT INTO zzw5d_yb VALUES (abs(NEW.i, NEW.j))")
                .contains("function abs(integer, integer) does not exist"));

        assertEquals("0", scalar("SELECT count(*)::text FROM pg_rules WHERE tablename = 'zzw5d_ya'"));
        exec("DROP TABLE zzw5d_ya CASCADE");
        exec("DROP TABLE zzw5d_yb CASCADE");
    }

    @Test
    void droppingARelationTakesItsOwnRulesWithIt() throws Exception {
        exec("DROP TABLE IF EXISTS zzw5d_ra CASCADE");
        exec("DROP TABLE IF EXISTS zzw5d_rb CASCADE");
        exec("CREATE TABLE zzw5d_ra (i int)");
        exec("CREATE TABLE zzw5d_rb (i int)");
        exec("CREATE RULE zzw5d_r1r AS ON INSERT TO zzw5d_ra"
                + " DO ALSO INSERT INTO zzw5d_rb VALUES (NEW.i)");
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_rules WHERE tablename = 'zzw5d_ra'"));

        exec("DROP TABLE zzw5d_ra CASCADE");
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_rules"
                + " WHERE tablename IN ('zzw5d_ra','zzw5d_rb')"));
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_class"
                + " WHERE relname IN ('zzw5d_ra','zzw5d_rb')"));

        // A relation created under the name afterwards starts with no rules and relhasrules false.
        exec("CREATE TABLE zzw5d_ra (i int)");
        assertEquals("false", scalar("SELECT relhasrules::text FROM pg_class WHERE relname = 'zzw5d_ra'"));
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_rules WHERE tablename = 'zzw5d_ra'"));

        exec("DROP TABLE zzw5d_ra CASCADE");
        exec("DROP TABLE zzw5d_rb CASCADE");
    }

    @Test
    void commitWithNoTransactionInProgressWarns() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("COMMIT");
            SQLWarning w = s.getWarnings();
            assertNotNull(w, "COMMIT outside a transaction block warns");
            assertEquals("25P01", w.getSQLState());
            assertTrue(w.getMessage().contains("there is no transaction in progress"),
                    w.getMessage());
        }
        // A COMMIT that does close a block says nothing.
        try (Statement s = conn.createStatement()) {
            s.execute("BEGIN");
        }
        try (Statement s = conn.createStatement()) {
            s.execute("COMMIT");
            assertNull(s.getWarnings());
        }
    }

    // A command tag is only visible on the wire, so this one speaks raw v3 to the embedded server.
    @Test
    void commitOfAnAbortedBlockAnswersWithTheRollbackTag() throws Exception {
        List<String> tags = rawSimpleQueries(List.of(
                "BEGIN", "SELECT 1/0", "SELECT 1", "COMMIT",
                "BEGIN", "SELECT 1", "COMMIT",
                "BEGIN", "SELECT 1/0", "ROLLBACK"));
        assertEquals(List.of(
                "CommandComplete(BEGIN) ReadyForQuery(T)",
                "ErrorResponse[22012] ReadyForQuery(E)",
                "ErrorResponse[25P02] ReadyForQuery(E)",
                "CommandComplete(ROLLBACK) ReadyForQuery(I)",
                "CommandComplete(BEGIN) ReadyForQuery(T)",
                "CommandComplete(SELECT 1) ReadyForQuery(T)",
                "CommandComplete(COMMIT) ReadyForQuery(I)",
                "CommandComplete(BEGIN) ReadyForQuery(T)",
                "ErrorResponse[22012] ReadyForQuery(E)",
                "CommandComplete(ROLLBACK) ReadyForQuery(I)"), tags);
    }

    /** One simple query per entry, answered as its message types and command tag. */
    private static List<String> rawSimpleQueries(List<String> sqls) throws IOException {
        List<String> out = new ArrayList<>();
        try (Socket socket = new Socket("localhost", memgres.getPort())) {
            DataOutputStream to = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream from = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            ByteArrayOutputStream startup = new ByteArrayOutputStream();
            DataOutputStream sd = new DataOutputStream(startup);
            sd.writeInt(196608);
            cstring(sd, "user"); cstring(sd, memgres.getUser());
            cstring(sd, "database"); cstring(sd, "test");
            sd.writeByte(0);
            to.writeInt(startup.size() + 4);
            to.write(startup.toByteArray());
            to.flush();
            while (true) {
                int type = from.read();
                byte[] body = new byte[from.readInt() - 4];
                from.readFully(body);
                if (type == 'R' && intAt(body) == 3) {
                    ByteArrayOutputStream pw = new ByteArrayOutputStream();
                    cstring(new DataOutputStream(pw), memgres.getPassword());
                    to.writeByte('p');
                    to.writeInt(pw.size() + 4);
                    to.write(pw.toByteArray());
                    to.flush();
                } else if (type == 'Z') {
                    break;
                }
            }
            for (String sql : sqls) {
                byte[] q = (sql + "\0").getBytes(StandardCharsets.UTF_8);
                to.writeByte('Q');
                to.writeInt(q.length + 4);
                to.write(q);
                to.flush();
                StringBuilder line = new StringBuilder();
                while (true) {
                    int type = from.read();
                    byte[] body = new byte[from.readInt() - 4];
                    from.readFully(body);
                    if (type == 'C') {
                        line.append("CommandComplete(").append(cstring(body, 0)).append(") ");
                    } else if (type == 'E') {
                        line.append("ErrorResponse[").append(field(body, 'C')).append("] ");
                    } else if (type == 'Z') {
                        line.append("ReadyForQuery(").append((char) body[0]).append(')');
                        break;
                    }
                }
                out.add(line.toString());
            }
        }
        return out;
    }

    private static int intAt(byte[] b) {
        return ((b[0] & 0xff) << 24) | ((b[1] & 0xff) << 16) | ((b[2] & 0xff) << 8) | (b[3] & 0xff);
    }

    private static String field(byte[] payload, char want) {
        int i = 0;
        while (i < payload.length && payload[i] != 0) {
            char code = (char) payload[i];
            String value = cstring(payload, i + 1);
            if (code == want) return value;
            i += 1 + value.getBytes(StandardCharsets.UTF_8).length + 1;
        }
        return "?";
    }

    private static String cstring(byte[] b, int from) {
        int end = from;
        while (end < b.length && b[end] != 0) end++;
        return new String(b, from, end - from, StandardCharsets.UTF_8);
    }

    private static void cstring(DataOutputStream d, String s) throws IOException {
        d.write(s.getBytes(StandardCharsets.UTF_8));
        d.writeByte(0);
    }
    // ---------------------------------------------------------- a composite type's attributes, and the schema a type lives in

    /**
     * A composite attribute keeps the modifier it was declared with, and one that is dropped keeps
     * its number: PostgreSQL leaves the row behind under a name nobody could have written, so the
     * next attribute added takes a number of its own. Measured against PostgreSQL 18.
     */
    @Test
    void aCompositeAttributeKeepsItsModifierAndADroppedOneKeepsItsNumber() throws Exception {
        exec("DROP TYPE IF EXISTS zzw5e_ha CASCADE");
        exec("CREATE TYPE zzw5e_ha AS (x int)");
        exec("ALTER TYPE zzw5e_ha ADD ATTRIBUTE y text");
        exec("ALTER TYPE zzw5e_ha RENAME ATTRIBUTE y TO y2");
        exec("ALTER TYPE zzw5e_ha ALTER ATTRIBUTE y2 TYPE varchar(5)");
        assertEquals("x:integer,y2:character varying(5)",
                scalar("SELECT string_agg(a.attname || ':'"
                        + " || format_type(a.atttypid, a.atttypmod), ',' ORDER BY a.attnum)"
                        + " FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid"
                        + " WHERE c.relname = 'zzw5e_ha' AND a.attnum > 0"));
        exec("ALTER TYPE zzw5e_ha DROP ATTRIBUTE y2");
        exec("ALTER TYPE zzw5e_ha ADD ATTRIBUTE z int");
        assertEquals("x,........pg.dropped.2........,z",
                scalar("SELECT string_agg(a.attname, ',' ORDER BY a.attnum)"
                        + " FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid"
                        + " WHERE c.relname = 'zzw5e_ha' AND a.attnum > 0"));
        assertEquals("x,z",
                scalar("SELECT string_agg(a.attname, ',' ORDER BY a.attnum)"
                        + " FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid"
                        + " WHERE c.relname = 'zzw5e_ha' AND a.attnum > 0 AND NOT a.attisdropped"));
        // The gap counts towards the relation's attribute count, as it does in PostgreSQL.
        assertEquals("3", scalar("SELECT relnatts::text FROM pg_class WHERE relname = 'zzw5e_ha'"));
        assertEquals("x:1,z:3",
                scalar("SELECT string_agg(a.attribute_name || ':' || a.ordinal_position::text,"
                        + " ',' ORDER BY a.ordinal_position) FROM information_schema.attributes a"
                        + " WHERE a.udt_name = 'zzw5e_ha'"));
        // The type itself is made of what is left, so a value of it has two fields.
        assertEquals("7", scalar("SELECT (ROW(7, 8)::zzw5e_ha).x::text"));
        assertEquals("8", scalar("SELECT (ROW(7, 8)::zzw5e_ha).z::text"));
        exec("DROP TYPE zzw5e_ha");
    }

    /**
     * A composite attribute declared with a modifier reports it, whether the modifier was written
     * on CREATE TYPE or added later. Measured against PostgreSQL 18.
     */
    @Test
    void aCompositeAttributeDeclaredWithAModifierReportsIt() throws Exception {
        exec("DROP TYPE IF EXISTS zzw5e_hc CASCADE");
        exec("CREATE TYPE zzw5e_hc AS (x int, y varchar(5), w numeric(8,2), u char(4))");
        assertEquals("x|integer|-1,y|character varying(5)|9,w|numeric(8,2)|524294,u|character(4)|8",
                scalar("SELECT string_agg(a.attname || '|'"
                        + " || format_type(a.atttypid, a.atttypmod) || '|' || a.atttypmod::text,"
                        + " ',' ORDER BY a.attnum) FROM pg_attribute a"
                        + " JOIN pg_class c ON c.oid = a.attrelid"
                        + " WHERE c.relname = 'zzw5e_hc' AND a.attnum > 0"));
        exec("DROP TYPE zzw5e_hc");
    }

    /**
     * SET SCHEMA moves a type out of the schema its bare name was answered from, and an
     * unqualified name is the search path's to answer: the old word reaches nothing, and the type
     * is reachable where it was moved to. Measured against PostgreSQL 18 for a domain, an enum and
     * a composite alike.
     */
    @Test
    void aTypeMovedToAnotherSchemaIsNoLongerReachableByItsBareName() throws Exception {
        exec("DROP SCHEMA IF EXISTS zzw5e_s2 CASCADE");
        exec("DROP DOMAIN IF EXISTS zzw5e_kr CASCADE");
        exec("DROP TYPE IF EXISTS zzw5e_ks CASCADE");
        exec("DROP TYPE IF EXISTS zzw5e_kc CASCADE");
        exec("CREATE SCHEMA zzw5e_s2");
        exec("CREATE DOMAIN zzw5e_kr AS int");
        exec("CREATE TYPE zzw5e_ks AS ENUM ('a')");
        exec("CREATE TYPE zzw5e_kc AS (q int)");
        exec("ALTER DOMAIN zzw5e_kr SET SCHEMA zzw5e_s2");
        exec("ALTER TYPE zzw5e_ks SET SCHEMA zzw5e_s2");
        exec("ALTER TYPE zzw5e_kc SET SCHEMA zzw5e_s2");
        assertEquals("zzw5e_s2", scalar("SELECT n.nspname FROM pg_type t"
                + " JOIN pg_namespace n ON n.oid = t.typnamespace WHERE t.typname = 'zzw5e_kr'"));
        assertEquals("42704", stateOf("CREATE TABLE zzw5e_t9 (c zzw5e_kr)"));
        assertEquals("type \"zzw5e_kr\" does not exist",
                messageOf("CREATE TABLE zzw5e_t9 (c zzw5e_kr)"));
        assertEquals("42704", stateOf("CREATE TABLE zzw5e_ta (c zzw5e_ks)"));
        assertEquals("type \"zzw5e_ks\" does not exist",
                messageOf("CREATE TABLE zzw5e_ta (c zzw5e_ks)"));
        assertEquals("42704", stateOf("CREATE TABLE zzw5e_tb (c zzw5e_kc)"));
        assertEquals("type \"zzw5e_kc\" does not exist",
                messageOf("CREATE TABLE zzw5e_tb (c zzw5e_kc)"));
        // Written where it now lives, all three are found.
        exec("CREATE TABLE zzw5e_tc (c zzw5e_s2.zzw5e_kc, d zzw5e_s2.zzw5e_kr,"
                + " e zzw5e_s2.zzw5e_ks)");
        exec("DROP TABLE zzw5e_tc");
        exec("DROP SCHEMA zzw5e_s2 CASCADE");
    }}
