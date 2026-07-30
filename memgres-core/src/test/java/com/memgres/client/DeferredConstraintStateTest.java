package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A DEFERRABLE constraint is still a constraint: postponing the check to COMMIT changes when it
 * runs, never whether it runs.
 *
 * <p>The gap these tests close was the worst kind — a transaction that inserted a duplicate under
 * a deferred UNIQUE and then rolled back left the constraint unenforced, for every session, so the
 * next insert of the same key was accepted and the table kept two identical rows. The cause was in
 * the index: a unique index stored one row per key, so the transient duplicate a deferred check
 * allows overwrote the committed row's entry, and undoing the duplicate then removed the key
 * outright.
 *
 * <p>The rest pins the shapes around it: the referenced side of a NO ACTION foreign key waits for
 * COMMIT the way the referencing side does while RESTRICT still refuses the statement, a
 * subtransaction takes its postponed checks with it when it is rolled back, and every ordinary
 * behaviour — a deferred violation surfacing at COMMIT, SET CONSTRAINTS moving it back to the
 * statement, plain constraints unaffected — keeps working.
 */
class DeferredConstraintStateTest {

    static Memgres memgres;
    static Connection conn;
    static Connection other;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        other = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        other.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (other != null) other.close();
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        exec(conn, sql);
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
        }
    }

    private static String scalar(String sql) throws SQLException {
        return scalar(conn, sql);
    }

    private static String scalar(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
                assertTrue(rs.next(), "expected a row from: " + sql);
                return rs.getString(1);
            }
        }
    }

    private static List<String> column(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) out.add(rs.getString(1));
            }
        }
        return out;
    }

    /** Assert the statement is refused with this SQLSTATE and a message containing this text. */
    private static void refused(String sql, String sqlState, String messagePart) throws SQLException {
        refused(conn, sql, sqlState, messagePart);
    }

    private static void refused(Connection c, String sql, String sqlState, String messagePart) {
        SQLException e = assertThrows(SQLException.class, () -> exec(c, sql), "expected a failure: " + sql);
        assertEquals(sqlState, e.getSQLState(), "SQLSTATE for: " + sql + " (" + e.getMessage() + ")");
        assertTrue(e.getMessage() != null && e.getMessage().contains(messagePart),
                "message for " + sql + " was: " + e.getMessage());
    }

    // ------------------------------------------------------------------
    // The finding: an aborted transaction must not disarm the constraint
    // ------------------------------------------------------------------

    @Test
    void rollingBackAPendingUniqueCheckLeavesTheConstraintEnforced() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_u CASCADE");
        exec("CREATE TABLE dcs_u (i int PRIMARY KEY, j int,"
                + " CONSTRAINT dcs_uu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_u VALUES (1,1)");

        exec("BEGIN");
        exec("INSERT INTO dcs_u VALUES (2,1)");   // deferred: accepted for now
        exec("ROLLBACK");

        refused("INSERT INTO dcs_u VALUES (3,1)", "23505",
                "duplicate key value violates unique constraint \"dcs_uu\"");
        assertEquals(List.of("1"), column("SELECT j::text FROM dcs_u ORDER BY i"));

        // Every other session sees the same refusal: the index, not a session flag, holds the key.
        refused(other, "INSERT INTO dcs_u VALUES (4,1)", "23505",
                "duplicate key value violates unique constraint \"dcs_uu\"");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_u"));
        exec("DROP TABLE dcs_u");
    }

    @Test
    void theSameHoldsForAPrimaryKeyAndForARepeatedCycle() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_p CASCADE");
        exec("CREATE TABLE dcs_p (i int, CONSTRAINT dcs_pp PRIMARY KEY (i) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_p VALUES (1)");
        for (int cycle = 0; cycle < 3; cycle++) {
            exec("BEGIN");
            exec("INSERT INTO dcs_p VALUES (1)");
            exec("ROLLBACK");
            refused("INSERT INTO dcs_p VALUES (1)", "23505",
                    "duplicate key value violates unique constraint \"dcs_pp\"");
        }
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_p"));
        exec("DROP TABLE dcs_p");
    }

    @Test
    void aFailedCommitAlsoLeavesTheConstraintEnforced() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_c CASCADE");
        exec("CREATE TABLE dcs_c (i int PRIMARY KEY, j int,"
                + " CONSTRAINT dcs_cu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_c VALUES (1,1)");
        exec("BEGIN");
        exec("INSERT INTO dcs_c VALUES (2,1)");
        refused("COMMIT", "23505", "duplicate key value violates unique constraint \"dcs_cu\"");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_c"));

        exec("BEGIN");
        exec("SET CONSTRAINTS dcs_cu IMMEDIATE");
        refused("INSERT INTO dcs_c VALUES (3,1)", "23505",
                "duplicate key value violates unique constraint \"dcs_cu\"");
        exec("ROLLBACK");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_c"));
        exec("DROP TABLE dcs_c");
    }

    @Test
    void anUpdateToADuplicateThatIsRolledBackFreesNothing() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_up CASCADE");
        exec("CREATE TABLE dcs_up (i int PRIMARY KEY, j int,"
                + " CONSTRAINT dcs_upu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_up VALUES (1,1),(2,2)");
        exec("BEGIN");
        exec("UPDATE dcs_up SET j = 1 WHERE i = 2");
        exec("ROLLBACK");
        refused("UPDATE dcs_up SET j = 1 WHERE i = 2", "23505",
                "duplicate key value violates unique constraint \"dcs_upu\"");
        assertEquals(List.of("1", "2"), column("SELECT j::text FROM dcs_up ORDER BY i"));
        exec("DROP TABLE dcs_up");
    }

    @Test
    void twoConstraintsOnOneTableAreBothStillEnforced() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_two CASCADE");
        exec("CREATE TABLE dcs_two (i int PRIMARY KEY, j int, k int,"
                + " CONSTRAINT dcs_twa UNIQUE (j) DEFERRABLE INITIALLY DEFERRED,"
                + " CONSTRAINT dcs_twb UNIQUE (k) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_two VALUES (1,1,1)");
        exec("BEGIN");
        exec("INSERT INTO dcs_two VALUES (2,1,2)");   // only dcs_twa is left pending
        exec("ROLLBACK");
        refused("INSERT INTO dcs_two VALUES (3,1,3)", "23505",
                "duplicate key value violates unique constraint \"dcs_twa\"");
        refused("INSERT INTO dcs_two VALUES (4,4,1)", "23505",
                "duplicate key value violates unique constraint \"dcs_twb\"");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_two"));
        exec("DROP TABLE dcs_two");
    }

    @Test
    void theSameShapeOnTwoTablesStaysIndependent() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_ta CASCADE");
        exec("DROP TABLE IF EXISTS dcs_tb CASCADE");
        exec("CREATE TABLE dcs_ta (i int PRIMARY KEY, j int,"
                + " CONSTRAINT dcs_tau UNIQUE (j) DEFERRABLE INITIALLY DEFERRED)");
        exec("CREATE TABLE dcs_tb (i int PRIMARY KEY, j int,"
                + " CONSTRAINT dcs_tbu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_ta VALUES (1,1)");
        exec("INSERT INTO dcs_tb VALUES (1,1)");
        exec("BEGIN");
        exec("INSERT INTO dcs_ta VALUES (2,1)");
        exec("INSERT INTO dcs_tb VALUES (2,1)");
        exec("ROLLBACK");
        refused("INSERT INTO dcs_ta VALUES (3,1)", "23505", "\"dcs_tau\"");
        refused("INSERT INTO dcs_tb VALUES (3,1)", "23505", "\"dcs_tbu\"");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_ta"));
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_tb"));
        exec("DROP TABLE dcs_ta");
        exec("DROP TABLE dcs_tb");
    }

    @Test
    void deferrableInitiallyImmediateDeferredByHandBehavesTheSame() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_ii CASCADE");
        exec("CREATE TABLE dcs_ii (i int PRIMARY KEY, j int,"
                + " CONSTRAINT dcs_iiu UNIQUE (j) DEFERRABLE INITIALLY IMMEDIATE)");
        exec("INSERT INTO dcs_ii VALUES (1,1)");

        // Without SET CONSTRAINTS it fires at the statement.
        exec("BEGIN");
        refused("INSERT INTO dcs_ii VALUES (2,1)", "23505", "\"dcs_iiu\"");
        exec("ROLLBACK");

        // With it, the check waits, and rolling back still leaves the key held.
        exec("BEGIN");
        exec("SET CONSTRAINTS dcs_iiu DEFERRED");
        exec("INSERT INTO dcs_ii VALUES (3,1)");
        exec("ROLLBACK");
        refused("INSERT INTO dcs_ii VALUES (4,1)", "23505", "\"dcs_iiu\"");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_ii"));
        exec("DROP TABLE dcs_ii");
    }

    @Test
    void rollbackToSavepointDiscardsOnlyTheSubtransactionsPendingChecks() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_sp CASCADE");
        exec("CREATE TABLE dcs_sp (i int PRIMARY KEY, j int,"
                + " CONSTRAINT dcs_spu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_sp VALUES (1,1)");

        // The duplicate is taken back with the subtransaction, so COMMIT is clean.
        exec("BEGIN");
        exec("SAVEPOINT sp1");
        exec("INSERT INTO dcs_sp VALUES (2,1)");
        exec("ROLLBACK TO SAVEPOINT sp1");
        exec("INSERT INTO dcs_sp VALUES (3,7)");
        exec("COMMIT");
        assertEquals(List.of("1", "7"), column("SELECT j::text FROM dcs_sp ORDER BY i"));

        // A duplicate written before the savepoint survives the rollback and still fails COMMIT.
        exec("BEGIN");
        exec("INSERT INTO dcs_sp VALUES (4,1)");
        exec("SAVEPOINT sp2");
        exec("INSERT INTO dcs_sp VALUES (5,9)");
        exec("ROLLBACK TO SAVEPOINT sp2");
        refused("COMMIT", "23505", "\"dcs_spu\"");
        assertEquals("2", scalar("SELECT count(*)::text FROM dcs_sp"));

        refused("INSERT INTO dcs_sp VALUES (6,1)", "23505", "\"dcs_spu\"");
        exec("DROP TABLE dcs_sp");
    }

    @Test
    void aSubtransactionTakesItsPendingForeignKeyCheckWithIt() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_fkc CASCADE");
        exec("DROP TABLE IF EXISTS dcs_fkp CASCADE");
        exec("CREATE TABLE dcs_fkp (i int PRIMARY KEY)");
        exec("CREATE TABLE dcs_fkc (i int PRIMARY KEY, p int,"
                + " CONSTRAINT dcs_fk FOREIGN KEY (p) REFERENCES dcs_fkp(i) DEFERRABLE INITIALLY DEFERRED)");
        exec("BEGIN");
        exec("SAVEPOINT sp1");
        exec("INSERT INTO dcs_fkc VALUES (1,99)");
        exec("ROLLBACK TO SAVEPOINT sp1");
        exec("COMMIT");
        assertEquals("0", scalar("SELECT count(*)::text FROM dcs_fkc"));

        // Released rather than rolled back, the same insert still fails at COMMIT.
        exec("BEGIN");
        exec("SAVEPOINT sp2");
        exec("INSERT INTO dcs_fkc VALUES (1,99)");
        exec("RELEASE SAVEPOINT sp2");
        refused("COMMIT", "23503", "violates foreign key constraint \"dcs_fk\"");
        assertEquals("0", scalar("SELECT count(*)::text FROM dcs_fkc"));
        exec("DROP TABLE dcs_fkc");
        exec("DROP TABLE dcs_fkp");
    }

    // ------------------------------------------------------------------
    // The referenced side of a foreign key
    // ------------------------------------------------------------------

    @Test
    void deletingAReferencedRowUnderADeferredNoActionKeyWaitsForCommit() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_rc CASCADE");
        exec("DROP TABLE IF EXISTS dcs_rp CASCADE");
        exec("CREATE TABLE dcs_rp (i int PRIMARY KEY)");
        exec("CREATE TABLE dcs_rc (i int PRIMARY KEY, p int,"
                + " CONSTRAINT dcs_rf FOREIGN KEY (p) REFERENCES dcs_rp(i) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_rp VALUES (1)");
        exec("INSERT INTO dcs_rc VALUES (1,1)");

        // The delete goes through; the key has to be back, or the child gone, by COMMIT.
        exec("BEGIN");
        exec("DELETE FROM dcs_rp WHERE i = 1");
        assertEquals("0", scalar("SELECT count(*)::text FROM dcs_rp"));
        refused("COMMIT", "23503",
                "update or delete on table \"dcs_rp\" violates foreign key constraint \"dcs_rf\"");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_rp"));

        // Putting the key back inside the transaction makes it pass.
        exec("BEGIN");
        exec("DELETE FROM dcs_rp WHERE i = 1");
        exec("INSERT INTO dcs_rp VALUES (1)");
        exec("COMMIT");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_rp"));

        // So does moving the child out of the way.
        exec("BEGIN");
        exec("DELETE FROM dcs_rp WHERE i = 1");
        exec("DELETE FROM dcs_rc WHERE p = 1");
        exec("COMMIT");
        assertEquals("0", scalar("SELECT count(*)::text FROM dcs_rp"));
        assertEquals("0", scalar("SELECT count(*)::text FROM dcs_rc"));
        exec("DROP TABLE dcs_rc");
        exec("DROP TABLE dcs_rp");
    }

    @Test
    void setConstraintsImmediateRaisesTheReferencedSideCheckAtOnce() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_ic CASCADE");
        exec("DROP TABLE IF EXISTS dcs_ip CASCADE");
        exec("CREATE TABLE dcs_ip (i int PRIMARY KEY)");
        exec("CREATE TABLE dcs_ic (i int PRIMARY KEY, p int,"
                + " CONSTRAINT dcs_if FOREIGN KEY (p) REFERENCES dcs_ip(i) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_ip VALUES (1)");
        exec("INSERT INTO dcs_ic VALUES (1,1)");
        exec("BEGIN");
        exec("DELETE FROM dcs_ip WHERE i = 1");
        refused("SET CONSTRAINTS dcs_if IMMEDIATE", "23503",
                "update or delete on table \"dcs_ip\" violates foreign key constraint \"dcs_if\"");
        exec("ROLLBACK");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_ip"));

        // ALL IMMEDIATE does the same, and afterwards the delete fires at the statement.
        exec("BEGIN");
        exec("SET CONSTRAINTS ALL IMMEDIATE");
        refused("DELETE FROM dcs_ip WHERE i = 1", "23503",
                "update or delete on table \"dcs_ip\" violates foreign key constraint \"dcs_if\"");
        exec("ROLLBACK");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_ip"));
        exec("DROP TABLE dcs_ic");
        exec("DROP TABLE dcs_ip");
    }

    @Test
    void changingAReferencedKeyAndPuttingItBackIsAccepted() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_kc CASCADE");
        exec("DROP TABLE IF EXISTS dcs_kp CASCADE");
        exec("CREATE TABLE dcs_kp (i int PRIMARY KEY)");
        exec("CREATE TABLE dcs_kc (i int PRIMARY KEY, p int,"
                + " CONSTRAINT dcs_kf FOREIGN KEY (p) REFERENCES dcs_kp(i) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_kp VALUES (1)");
        exec("INSERT INTO dcs_kc VALUES (1,1)");
        exec("BEGIN");
        exec("UPDATE dcs_kp SET i = 2 WHERE i = 1");
        exec("UPDATE dcs_kp SET i = 1 WHERE i = 2");
        exec("COMMIT");
        assertEquals(List.of("1"), column("SELECT i::text FROM dcs_kp ORDER BY i"));

        exec("BEGIN");
        exec("UPDATE dcs_kp SET i = 3 WHERE i = 1");
        refused("COMMIT", "23503",
                "update or delete on table \"dcs_kp\" violates foreign key constraint \"dcs_kf\"");
        assertEquals(List.of("1"), column("SELECT i::text FROM dcs_kp ORDER BY i"));
        exec("DROP TABLE dcs_kc");
        exec("DROP TABLE dcs_kp");
    }

    @Test
    void restrictRefusesTheStatementEvenWhenTheConstraintIsDeferred() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_sc CASCADE");
        exec("DROP TABLE IF EXISTS dcs_sp2 CASCADE");
        exec("CREATE TABLE dcs_sp2 (i int PRIMARY KEY)");
        exec("CREATE TABLE dcs_sc (i int PRIMARY KEY, p int,"
                + " CONSTRAINT dcs_sf FOREIGN KEY (p) REFERENCES dcs_sp2(i)"
                + " ON DELETE RESTRICT ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_sp2 VALUES (1)");
        exec("INSERT INTO dcs_sc VALUES (1,1)");
        exec("BEGIN");
        refused("DELETE FROM dcs_sp2 WHERE i = 1", "23001",
                "violates RESTRICT setting of foreign key constraint \"dcs_sf\"");
        exec("ROLLBACK");
        refused("UPDATE dcs_sp2 SET i = 2 WHERE i = 1", "23001",
                "violates RESTRICT setting of foreign key constraint \"dcs_sf\"");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_sp2"));
        exec("DROP TABLE dcs_sc");
        exec("DROP TABLE dcs_sp2");
    }

    @Test
    void cascadeAndSetNullStillActAtTheStatement() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_cc CASCADE");
        exec("DROP TABLE IF EXISTS dcs_cn CASCADE");
        exec("DROP TABLE IF EXISTS dcs_cp CASCADE");
        exec("CREATE TABLE dcs_cp (i int PRIMARY KEY)");
        exec("CREATE TABLE dcs_cc (i int PRIMARY KEY, p int,"
                + " CONSTRAINT dcs_cf FOREIGN KEY (p) REFERENCES dcs_cp(i)"
                + " ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)");
        exec("CREATE TABLE dcs_cn (i int PRIMARY KEY, p int,"
                + " CONSTRAINT dcs_nf FOREIGN KEY (p) REFERENCES dcs_cp(i)"
                + " ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_cp VALUES (1)");
        exec("INSERT INTO dcs_cc VALUES (1,1)");
        exec("INSERT INTO dcs_cn VALUES (1,1)");
        exec("BEGIN");
        exec("DELETE FROM dcs_cp WHERE i = 1");
        assertEquals("0", scalar("SELECT count(*)::text FROM dcs_cc"));
        assertEquals("0", scalar("SELECT count(*)::text FROM dcs_cn WHERE p IS NOT NULL"));
        exec("COMMIT");
        assertEquals("0", scalar("SELECT count(*)::text FROM dcs_cc"));
        exec("DROP TABLE dcs_cc");
        exec("DROP TABLE dcs_cn");
        exec("DROP TABLE dcs_cp");
    }

    @Test
    void aNonDeferrableKeyIsUnaffectedByAnAbortedTransaction() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_nc CASCADE");
        exec("DROP TABLE IF EXISTS dcs_np CASCADE");
        exec("CREATE TABLE dcs_np (i int PRIMARY KEY)");
        exec("CREATE TABLE dcs_nc (i int PRIMARY KEY, p int REFERENCES dcs_np(i))");
        exec("INSERT INTO dcs_np VALUES (1)");
        exec("INSERT INTO dcs_nc VALUES (1,1)");
        exec("BEGIN");
        refused("DELETE FROM dcs_np WHERE i = 1", "23503",
                "update or delete on table \"dcs_np\" violates foreign key constraint");
        exec("ROLLBACK");
        refused("DELETE FROM dcs_np WHERE i = 1", "23503",
                "update or delete on table \"dcs_np\" violates foreign key constraint");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_np"));

        exec("DROP TABLE IF EXISTS dcs_nu CASCADE");
        exec("CREATE TABLE dcs_nu (i int PRIMARY KEY, j int UNIQUE)");
        exec("INSERT INTO dcs_nu VALUES (1,1)");
        exec("BEGIN");
        exec("INSERT INTO dcs_nu VALUES (2,2)");
        exec("ROLLBACK");
        refused("INSERT INTO dcs_nu VALUES (3,1)", "23505", "duplicate key value");
        exec("INSERT INTO dcs_nu VALUES (4,4)");
        assertEquals("2", scalar("SELECT count(*)::text FROM dcs_nu"));
        exec("DROP TABLE dcs_nu");
        exec("DROP TABLE dcs_nc");
        exec("DROP TABLE dcs_np");
    }

    // ------------------------------------------------------------------
    // What deferring is for, and must keep doing
    // ------------------------------------------------------------------

    @Test
    void theLegitimateUsesOfDeferralStillWork() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_sw CASCADE");
        exec("CREATE TABLE dcs_sw (i int PRIMARY KEY, j int,"
                + " CONSTRAINT dcs_swu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_sw VALUES (1,1),(2,2)");
        exec("BEGIN");
        exec("UPDATE dcs_sw SET j = 2 WHERE i = 1");
        exec("UPDATE dcs_sw SET j = 1 WHERE i = 2");
        exec("COMMIT");
        assertEquals(List.of("2", "1"), column("SELECT j::text FROM dcs_sw ORDER BY i"));

        // A duplicate written and then removed before COMMIT is fine.
        exec("BEGIN");
        exec("INSERT INTO dcs_sw VALUES (3,2)");
        exec("DELETE FROM dcs_sw WHERE i = 1");
        exec("COMMIT");
        assertEquals("2", scalar("SELECT count(*)::text FROM dcs_sw"));
        refused("INSERT INTO dcs_sw VALUES (4,2)", "23505", "\"dcs_swu\"");
        exec("DROP TABLE dcs_sw");

        // A child row written before its parent is the point of a deferred foreign key.
        exec("DROP TABLE IF EXISTS dcs_oc CASCADE");
        exec("DROP TABLE IF EXISTS dcs_op CASCADE");
        exec("CREATE TABLE dcs_op (i int PRIMARY KEY)");
        exec("CREATE TABLE dcs_oc (i int PRIMARY KEY, p int,"
                + " CONSTRAINT dcs_of FOREIGN KEY (p) REFERENCES dcs_op(i) DEFERRABLE INITIALLY DEFERRED)");
        exec("BEGIN");
        exec("INSERT INTO dcs_oc VALUES (1,5)");
        exec("INSERT INTO dcs_op VALUES (5)");
        exec("COMMIT");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_oc"));
        exec("DROP TABLE dcs_oc");
        exec("DROP TABLE dcs_op");
    }

    @Test
    void setConstraintsAcceptsBareSchemaQualifiedAndAllForms() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_sn CASCADE");
        exec("CREATE TABLE dcs_sn (i int PRIMARY KEY, j int,"
                + " CONSTRAINT dcs_snu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_sn VALUES (1,1)");

        exec("BEGIN");
        exec("SET CONSTRAINTS dcs_snu IMMEDIATE");
        refused("INSERT INTO dcs_sn VALUES (2,1)", "23505", "\"dcs_snu\"");
        exec("ROLLBACK");

        exec("BEGIN");
        exec("SET CONSTRAINTS public.dcs_snu IMMEDIATE");
        refused("INSERT INTO dcs_sn VALUES (3,1)", "23505", "\"dcs_snu\"");
        exec("ROLLBACK");

        exec("BEGIN");
        exec("SET CONSTRAINTS ALL DEFERRED");
        exec("INSERT INTO dcs_sn VALUES (4,1)");
        refused("SET CONSTRAINTS ALL IMMEDIATE", "23505", "\"dcs_snu\"");
        exec("ROLLBACK");

        refused("INSERT INTO dcs_sn VALUES (5,1)", "23505", "\"dcs_snu\"");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_sn"));

        refused("SET CONSTRAINTS dcs_no_such_constraint DEFERRED", "42704",
                "constraint \"dcs_no_such_constraint\" does not exist");
        exec("DROP TABLE dcs_sn");
    }

    @Test
    void nullKeysUnderADeferredUniqueRemainDistinct() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_nn CASCADE");
        exec("CREATE TABLE dcs_nn (i int PRIMARY KEY, j int,"
                + " CONSTRAINT dcs_nnu UNIQUE (j) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_nn VALUES (1,NULL),(2,NULL)");
        exec("BEGIN");
        exec("INSERT INTO dcs_nn VALUES (3,NULL)");
        exec("ROLLBACK");
        exec("INSERT INTO dcs_nn VALUES (4,NULL)");
        assertEquals("3", scalar("SELECT count(*)::text FROM dcs_nn"));
        exec("DROP TABLE dcs_nn");
    }

    @Test
    void aMultiColumnDeferredKeyIsHeldPerKeyNotPerColumn() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_mc CASCADE");
        exec("CREATE TABLE dcs_mc (a int, b int,"
                + " CONSTRAINT dcs_mcp PRIMARY KEY (a,b) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_mc VALUES (1,1)");
        exec("BEGIN");
        exec("INSERT INTO dcs_mc VALUES (1,1)");
        exec("ROLLBACK");
        refused("INSERT INTO dcs_mc VALUES (1,1)", "23505", "\"dcs_mcp\"");
        exec("INSERT INTO dcs_mc VALUES (1,2)");
        assertEquals("2", scalar("SELECT count(*)::text FROM dcs_mc"));
        exec("DROP TABLE dcs_mc");
    }

    // ------------------------------------------------------------------
    // Declaring deferrability
    // ------------------------------------------------------------------

    @Test
    void aCheckConstraintCannotBeMarkedDeferrable() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_ck CASCADE");
        refused("CREATE TABLE dcs_ck (i int, CONSTRAINT dcs_ckc CHECK (i > 0) DEFERRABLE INITIALLY DEFERRED)",
                "0A000", "CHECK constraints cannot be marked DEFERRABLE");
        refused("CREATE TABLE dcs_ck (i int, CONSTRAINT dcs_ckc CHECK (i > 0) DEFERRABLE)",
                "0A000", "CHECK constraints cannot be marked DEFERRABLE");
        refused("CREATE TABLE dcs_ck (i int, CONSTRAINT dcs_ckc CHECK (i > 0) INITIALLY DEFERRED)",
                "0A000", "CHECK constraints cannot be marked DEFERRABLE");
        refused("CREATE TABLE dcs_ck (i int CHECK (i > 0) DEFERRABLE)",
                "42601", "misplaced DEFERRABLE clause");
        refused("CREATE TABLE dcs_ck (i int CHECK (i > 0) NOT DEFERRABLE)",
                "42601", "misplaced NOT DEFERRABLE clause");
        refused("CREATE TABLE dcs_ck (i int CHECK (i > 0) INITIALLY IMMEDIATE)",
                "42601", "misplaced INITIALLY IMMEDIATE clause");

        // What a CHECK already is, it may still say.
        exec("CREATE TABLE dcs_ck (i int, CONSTRAINT dcs_ckc CHECK (i > 0) NOT DEFERRABLE INITIALLY IMMEDIATE)");
        refused("ALTER TABLE dcs_ck ADD CONSTRAINT dcs_ckd CHECK (i < 100) DEFERRABLE INITIALLY DEFERRED",
                "0A000", "CHECK constraints cannot be marked DEFERRABLE");
        exec("ALTER TABLE dcs_ck ADD CONSTRAINT dcs_cke CHECK (i < 100) NOT DEFERRABLE");
        exec("INSERT INTO dcs_ck VALUES (5)");
        refused("INSERT INTO dcs_ck VALUES (-5)", "23514", "violates check constraint \"dcs_ckc\"");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_ck"));
        exec("DROP TABLE dcs_ck");
    }

    @Test
    void theDeferrabilityClausesAreAcceptedInEveryCombination() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_dc CASCADE");
        exec("CREATE TABLE dcs_dc (i int, j int, k int, m int,"
                + " CONSTRAINT dcs_dcp PRIMARY KEY (i) NOT DEFERRABLE,"
                + " CONSTRAINT dcs_dca UNIQUE (j) DEFERRABLE INITIALLY IMMEDIATE,"
                + " CONSTRAINT dcs_dcb UNIQUE (k) NOT DEFERRABLE INITIALLY IMMEDIATE,"
                + " CONSTRAINT dcs_dcc UNIQUE (m) INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_dc VALUES (1,1,1,1)");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_dc"));

        // The bare INITIALLY DEFERRED carries DEFERRABLE with it.
        assertEquals("YES", scalar("SELECT is_deferrable FROM information_schema.table_constraints"
                + " WHERE constraint_name = 'dcs_dcc'"));
        assertEquals("YES", scalar("SELECT initially_deferred FROM information_schema.table_constraints"
                + " WHERE constraint_name = 'dcs_dcc'"));
        assertEquals("NO", scalar("SELECT is_deferrable FROM information_schema.table_constraints"
                + " WHERE constraint_name = 'dcs_dcb'"));
        assertEquals("YES", scalar("SELECT is_deferrable FROM information_schema.table_constraints"
                + " WHERE constraint_name = 'dcs_dca'"));
        assertEquals("NO", scalar("SELECT initially_deferred FROM information_schema.table_constraints"
                + " WHERE constraint_name = 'dcs_dca'"));

        // pg_constraint says the same thing.
        assertEquals("true", scalar("SELECT condeferred::text FROM pg_constraint"
                + " WHERE conname = 'dcs_dcc'"));
        assertEquals("false", scalar("SELECT condeferrable::text FROM pg_constraint"
                + " WHERE conname = 'dcs_dcb'"));

        exec("BEGIN");
        exec("INSERT INTO dcs_dc VALUES (2,2,2,1)");   // dcs_dcc is deferred
        refused("COMMIT", "23505", "\"dcs_dcc\"");
        assertEquals("1", scalar("SELECT count(*)::text FROM dcs_dc"));
        exec("DROP TABLE dcs_dc");
    }

    @Test
    void aColumnLevelKeyStillTakesItsDeferrability() throws Exception {
        exec("DROP TABLE IF EXISTS dcs_clc CASCADE");
        exec("DROP TABLE IF EXISTS dcs_clp CASCADE");
        exec("CREATE TABLE dcs_clp (i int PRIMARY KEY NOT DEFERRABLE)");
        exec("CREATE TABLE dcs_clc (i int PRIMARY KEY,"
                + " j int UNIQUE DEFERRABLE INITIALLY DEFERRED,"
                + " p int REFERENCES dcs_clp(i) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO dcs_clp VALUES (1)");
        exec("BEGIN");
        exec("INSERT INTO dcs_clc VALUES (1,1,9)");    // both deferred keys unsatisfied
        refused("COMMIT", "23503", "violates foreign key constraint");
        assertEquals("0", scalar("SELECT count(*)::text FROM dcs_clc"));
        exec("INSERT INTO dcs_clc VALUES (1,1,1)");
        refused("INSERT INTO dcs_clc VALUES (2,1,1)", "23505", "duplicate key value");
        exec("DROP TABLE dcs_clc");
        exec("DROP TABLE dcs_clp");
    }
}
