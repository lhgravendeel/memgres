package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A statement outside a transaction is a transaction of its own, so one that fails partway leaves
 * nothing behind.
 *
 * <p>memgres recorded undo entries only while a transaction was open, so nothing an autocommit
 * statement wrote could be taken back: a multi-row UPDATE that divided by zero on its third row
 * kept the first two, and a query whose data-modifying WITH item had already written kept the write
 * even though the statement was refused. Both are now undone by the statement's own scope.
 *
 * <p>The referential tests here are the residue of the ON DELETE SET DEFAULT re-check. That check
 * refused a self-referential DELETE PostgreSQL performs, never ran for ON UPDATE, and could not see
 * a default row a CASCADE further up was about to remove.
 */
class StatementAtomicityTest {

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
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static long count(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    // =========================================================================
    // A failed statement leaves nothing behind
    // =========================================================================

    @Test
    void aMultiRowUpdateThatFailsPartwayChangesNothing() throws Exception {
        exec("DROP TABLE IF EXISTS sat_t CASCADE");
        exec("CREATE TABLE sat_t (id int PRIMARY KEY, v int)");
        exec("INSERT INTO sat_t VALUES (1,1),(2,2),(3,0),(4,4)");

        assertEquals("22012", stateOf("UPDATE sat_t SET v = 100 / v"));

        assertEquals(0, count("SELECT count(*) FROM sat_t WHERE v = 100"),
                "the rows updated before the failure must be put back");
        assertEquals(4, count("SELECT count(*) FROM sat_t"));
        assertEquals(1, count("SELECT count(*) FROM sat_t WHERE id = 1 AND v = 1"));
    }

    @Test
    void aStatementThatTripsAConstraintPartwayChangesNothing() throws Exception {
        exec("DROP TABLE IF EXISTS sat_c CASCADE");
        exec("CREATE TABLE sat_c (id int PRIMARY KEY, v int CHECK (v < 100))");
        exec("INSERT INTO sat_c VALUES (1,1),(2,2),(3,99)");

        assertEquals("23514", stateOf("UPDATE sat_c SET v = v + 10"));
        assertEquals(1, count("SELECT count(*) FROM sat_c WHERE v > 5"),
                "only the row that already held 99 is above 5");
    }

    @Test
    void aRefusedStatementAppliesNoneOfItsWithItemsWrites() throws Exception {
        exec("DROP TABLE IF EXISTS sat_log CASCADE");
        exec("CREATE TABLE sat_log (id int PRIMARY KEY)");

        assertEquals("42883", stateOf(
                "WITH w AS (INSERT INTO sat_log VALUES (1) RETURNING id) "
                        + "SELECT sat_nosuchfn(id) FROM w"));
        assertEquals(0, count("SELECT count(*) FROM sat_log"), "the INSERT must not stand");

        assertEquals("42703", stateOf(
                "WITH w AS (INSERT INTO sat_log VALUES (1) RETURNING id) SELECT w.nosuchcol FROM w"));
        assertEquals(0, count("SELECT count(*) FROM sat_log"));

        exec("INSERT INTO sat_log VALUES (1),(2),(3)");
        assertEquals("42883", stateOf(
                "WITH w AS (UPDATE sat_log SET id = id + 50 RETURNING id) "
                        + "SELECT sat_nosuchfn(1) FROM w"));
        assertEquals(0, count("SELECT count(*) FROM sat_log WHERE id >= 50"),
                "the UPDATE must not stand");

        assertEquals("42883", stateOf(
                "WITH w AS (DELETE FROM sat_log RETURNING id) SELECT sat_nosuchfn(1) FROM w"));
        assertEquals(3, count("SELECT count(*) FROM sat_log"), "the DELETE must not stand");
    }

    @Test
    void aStatementThatSucceedsStillWrites() throws Exception {
        exec("DROP TABLE IF EXISTS sat_log CASCADE");
        exec("CREATE TABLE sat_log (id int PRIMARY KEY)");

        assertEquals("OK", stateOf(
                "WITH w AS (INSERT INTO sat_log VALUES (7) RETURNING id) SELECT count(*) FROM w"));
        assertEquals(1, count("SELECT count(*) FROM sat_log"));
    }

    @Test
    void anExplicitTransactionStillRollsBackAsAWhole() throws Exception {
        exec("DROP TABLE IF EXISTS sat_tx CASCADE");
        exec("CREATE TABLE sat_tx (id int PRIMARY KEY)");
        conn.setAutoCommit(false);
        try {
            exec("INSERT INTO sat_tx VALUES (1)");
            exec("INSERT INTO sat_tx VALUES (2)");
            assertEquals("22012", stateOf("INSERT INTO sat_tx VALUES (1/0)"),
                    "the failing statement is undone on its own");
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }
        assertEquals(0, count("SELECT count(*) FROM sat_tx"),
                "and the rollback still takes the two that succeeded");
    }

    @Test
    void aCommittedTransactionKeepsItsWork() throws Exception {
        exec("DROP TABLE IF EXISTS sat_tx CASCADE");
        exec("CREATE TABLE sat_tx (id int PRIMARY KEY)");
        conn.setAutoCommit(false);
        try {
            exec("INSERT INTO sat_tx VALUES (1)");
            exec("INSERT INTO sat_tx VALUES (2)");
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }
        assertEquals(2, count("SELECT count(*) FROM sat_tx"));
    }

    // =========================================================================
    // The ON DELETE SET DEFAULT residuals
    // =========================================================================

    @Test
    void aSelfReferentialDeleteThatNeedsNoDefaultIsPerformed() throws Exception {
        exec("DROP TABLE IF EXISTS sat_self CASCADE");
        exec("CREATE TABLE sat_self (id int PRIMARY KEY, "
                + "parent int DEFAULT 9 REFERENCES sat_self(id) ON DELETE SET DEFAULT)");
        exec("INSERT INTO sat_self VALUES (9,NULL)");
        exec("INSERT INTO sat_self VALUES (1,9),(2,1)");
        exec("DELETE FROM sat_self WHERE id = 1");

        // Row 2 is itself removed, so nothing is left needing the default.
        assertEquals("OK", stateOf("DELETE FROM sat_self WHERE id IN (2, 9)"));
        assertEquals(0, count("SELECT count(*) FROM sat_self"));
    }

    @Test
    void onUpdateSetDefaultChecksTheKeyItWrites() throws Exception {
        exec("DROP TABLE IF EXISTS sat_uc CASCADE");
        exec("DROP TABLE IF EXISTS sat_up CASCADE");
        exec("CREATE TABLE sat_up (id int PRIMARY KEY)");
        exec("INSERT INTO sat_up VALUES (1),(9)");
        exec("CREATE TABLE sat_uc (id int PRIMARY KEY, "
                + "pid int DEFAULT 9 REFERENCES sat_up(id) ON UPDATE SET DEFAULT)");
        exec("INSERT INTO sat_uc VALUES (10,1)");
        exec("UPDATE sat_up SET id = 5 WHERE id = 1");

        assertEquals("23503", stateOf("UPDATE sat_up SET id = id + 100"));
        assertEquals(0, count("SELECT count(*) FROM sat_uc c "
                        + "WHERE NOT EXISTS (SELECT 1 FROM sat_up p WHERE p.id = c.pid)"),
                "no key may be left pointing at a row that is gone");
    }

    @Test
    void aDefaultRowARemovalCascadeIsAboutToTakeIsSeen() throws Exception {
        exec("DROP TABLE IF EXISTS sat_gc CASCADE");
        exec("DROP TABLE IF EXISTS sat_mid CASCADE");
        exec("DROP TABLE IF EXISTS sat_top CASCADE");
        exec("CREATE TABLE sat_top (id int PRIMARY KEY)");
        exec("INSERT INTO sat_top VALUES (1),(9)");
        exec("CREATE TABLE sat_mid (id int PRIMARY KEY, "
                + "tid int REFERENCES sat_top(id) ON DELETE CASCADE)");
        exec("INSERT INTO sat_mid VALUES (100,1),(109,9)");
        exec("CREATE TABLE sat_gc (id int PRIMARY KEY, "
                + "mid int DEFAULT 109 REFERENCES sat_mid(id) ON DELETE SET DEFAULT)");
        exec("INSERT INTO sat_gc VALUES (1000,100)");

        assertEquals("23503", stateOf("DELETE FROM sat_top WHERE id IN (1,9)"),
                "109 disappears through the cascade, so the default may not point at it");
        assertEquals(0, count("SELECT count(*) FROM sat_gc g "
                + "WHERE NOT EXISTS (SELECT 1 FROM sat_mid p WHERE p.id = g.mid)"));
        assertEquals(2, count("SELECT count(*) FROM sat_top"), "and nothing was deleted");
    }

    @Test
    void theOrdinaryReferentialShapesAreUntouched() throws Exception {
        exec("DROP TABLE IF EXISTS sat_dc CASCADE");
        exec("DROP TABLE IF EXISTS sat_dp CASCADE");
        exec("CREATE TABLE sat_dp (id int PRIMARY KEY)");
        exec("INSERT INTO sat_dp VALUES (1),(2),(9)");
        exec("CREATE TABLE sat_dc (id int PRIMARY KEY, "
                + "pid int DEFAULT 9 REFERENCES sat_dp(id) ON DELETE SET DEFAULT)");
        exec("INSERT INTO sat_dc VALUES (11,2)");

        exec("DELETE FROM sat_dp WHERE id = 2");
        assertEquals(1, count("SELECT count(*) FROM sat_dc WHERE pid = 9"),
                "the default is written when its own row survives");

        exec("DROP TABLE IF EXISTS sat_cc CASCADE");
        exec("DROP TABLE IF EXISTS sat_cp CASCADE");
        exec("CREATE TABLE sat_cp (id int PRIMARY KEY)");
        exec("INSERT INTO sat_cp VALUES (1),(2)");
        exec("CREATE TABLE sat_cc (id int PRIMARY KEY, "
                + "pid int REFERENCES sat_cp(id) ON DELETE CASCADE)");
        exec("INSERT INTO sat_cc VALUES (10,1),(11,2)");
        exec("DELETE FROM sat_cp WHERE id = 1");
        assertEquals(1, count("SELECT count(*) FROM sat_cc"), "CASCADE still cascades");
    }
}
