package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * statement_timeout actually cancels a runaway statement (SQLSTATE 57014, PG's wording),
 * plus the neighbouring behaviour that must not change: 0 meaning no limit, every spelling
 * PG accepts, and a statement that finishes inside the limit being untouched.
 *
 * <p>Verified against PostgreSQL 18: every assertion here matches what PG returns.
 */
class StatementTimeoutTest {

    /**
     * Large enough that neither engine can finish it inside the tight limits used below.
     *
     * <p>It has to be a statement that reads the rows: counting a series on its own is arithmetic
     * on its ends now, and two hundred million of them are counted as fast as three are. A
     * predicate is evaluated per row, which is the work the limit is there to interrupt.
     */
    private static final String RUNAWAY =
            "SELECT count(*) FROM generate_series(1, 200000000) g WHERE g > 0";

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (memgres != null) memgres.close();
    }

    private Connection newConn() throws SQLException {
        return DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    // ---- the timeout fires ----

    @Test
    void runawaySelectIsCanceled() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("SET statement_timeout = '50ms'");
            long start = System.currentTimeMillis();
            SQLException e = assertThrows(SQLException.class, () -> s.execute(RUNAWAY));
            long elapsed = System.currentTimeMillis() - start;
            assertEquals("57014", e.getSQLState());
            assertEquals("ERROR: canceling statement due to statement timeout", e.getMessage());
            assertTrue(elapsed < 10000, "50ms limit should fail fast, took " + elapsed + "ms");
        }
    }

    @Test
    void runawayRecursiveCteIsCanceled() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("SET statement_timeout = '50ms'");
            SQLException e = assertThrows(SQLException.class, () -> s.execute(
                    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 200000000)"
                            + " SELECT count(*) FROM r"));
            assertEquals("57014", e.getSQLState());
        }
    }

    @Test
    void runawayInsertIsCanceledAndInsertsNothing() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE sto_ins (n int)");
            s.execute("SET statement_timeout = '50ms'");
            SQLException e = assertThrows(SQLException.class, () -> s.execute(
                    "INSERT INTO sto_ins SELECT g FROM generate_series(1, 200000000) g"));
            assertEquals("57014", e.getSQLState());
            s.execute("SET statement_timeout = 0");
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM sto_ins")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getLong(1));
            }
            s.execute("DROP TABLE sto_ins");
        }
    }

    @Test
    void timeoutAbortsTheOpenTransactionAndRollbackRecovers() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("SET statement_timeout = '50ms'");
            s.execute("BEGIN");
            assertEquals("57014", assertThrows(SQLException.class, () -> s.execute(RUNAWAY)).getSQLState());

            // PG leaves the transaction aborted: nothing but ROLLBACK is accepted.
            SQLException aborted = assertThrows(SQLException.class, () -> s.execute("SELECT 1"));
            assertEquals("25P02", aborted.getSQLState());

            s.execute("ROLLBACK");
            s.execute("SET statement_timeout = 0");
            try (ResultSet rs = s.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    void sessionSurvivesRepeatedTimeouts() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("SET statement_timeout = '50ms'");
            for (int i = 0; i < 3; i++) {
                assertEquals("57014",
                        assertThrows(SQLException.class, () -> s.execute(RUNAWAY)).getSQLState());
            }
            // The limit is per statement, so a quick one still succeeds with it in force.
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM generate_series(1, 100)")) {
                assertTrue(rs.next());
                assertEquals(100, rs.getLong(1));
            }
        }
    }

    @Test
    void setLocalTimeoutAppliesInsideTheTransactionOnly() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("BEGIN");
            s.execute("SET LOCAL statement_timeout = '50ms'");
            assertEquals("57014",
                    assertThrows(SQLException.class, () -> s.execute(RUNAWAY)).getSQLState());
            s.execute("ROLLBACK");
            try (ResultSet rs = s.executeQuery("SHOW statement_timeout")) {
                assertTrue(rs.next());
                assertEquals("0", rs.getString(1));
            }
        }
    }

    // ---- the timeout does not fire ----

    @Test
    void zeroMeansNoLimit() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("SET statement_timeout = 0");
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM generate_series(1, 20000)")) {
                assertTrue(rs.next());
                assertEquals(20000, rs.getLong(1));
            }
        }
    }

    @Test
    void statementInsideTheLimitIsUnaffected() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("SET statement_timeout = '30s'");
            try (ResultSet rs = s.executeQuery("SELECT sum(g) FROM generate_series(1, 2000) g")) {
                assertTrue(rs.next());
                assertEquals(2001000L, rs.getLong(1));
            }
            try (ResultSet rs = s.executeQuery(
                    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 500)"
                            + " SELECT count(*) FROM r")) {
                assertTrue(rs.next());
                assertEquals(500, rs.getLong(1));
            }
        }
    }

    @Test
    void transactionCommandsAreNotLimited() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("SET statement_timeout = '1ms'");
            s.execute("BEGIN");
            s.execute("SAVEPOINT sto_sp");
            s.execute("RELEASE SAVEPOINT sto_sp");
            s.execute("COMMIT");
            s.execute("BEGIN");
            s.execute("ROLLBACK");
        }
    }

    // ---- spellings ----

    @Test
    void acceptsEveryUnitSpellingPostgresDoes() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            assertShows(s, "SET statement_timeout = '2s'", "2s");
            assertShows(s, "SET statement_timeout = 2000", "2s");
            assertShows(s, "SET statement_timeout = '500ms'", "500ms");
            assertShows(s, "SET statement_timeout = 500", "500ms");
            assertShows(s, "SET statement_timeout = '1min'", "1min");
            assertShows(s, "SET statement_timeout = '1h'", "1h");
            assertShows(s, "SET SESSION statement_timeout = '3s'", "3s");
            assertShows(s, "SET statement_timeout = 0", "0");
            assertShows(s, "SET statement_timeout = '0'", "0");
            assertShows(s, "SET statement_timeout TO DEFAULT", "0");

            s.execute("SET statement_timeout = '250ms'");
            try (ResultSet rs = s.executeQuery("SELECT current_setting('statement_timeout')")) {
                assertTrue(rs.next());
                assertEquals("250ms", rs.getString(1));
            }
            assertShows(s, "RESET statement_timeout", "0");
        }
    }

    @Test
    void setConfigSetsTheTimeout() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                    "SELECT set_config('statement_timeout', '750ms', false)")) {
                assertTrue(rs.next());
                assertEquals("750ms", rs.getString(1));
            }
            try (ResultSet rs = s.executeQuery("SHOW statement_timeout")) {
                assertTrue(rs.next());
                assertEquals("750ms", rs.getString(1));
            }
        }
    }

    // ---- lock_timeout ----

    @Test
    void lockTimeoutReportsPostgresWording() throws Exception {
        try (Connection holder = newConn(); Connection waiter = newConn()) {
            holder.setAutoCommit(true);
            try (Statement s = holder.createStatement()) {
                s.execute("CREATE TABLE sto_lock (id int PRIMARY KEY, v text)");
                s.execute("INSERT INTO sto_lock VALUES (1, 'held')");
            }
            holder.setAutoCommit(false);
            holder.createStatement().executeQuery("SELECT * FROM sto_lock WHERE id = 1 FOR UPDATE");

            waiter.setAutoCommit(false);
            try (Statement s = waiter.createStatement()) {
                s.execute("SET lock_timeout = '100ms'");
                long start = System.currentTimeMillis();
                SQLException e = assertThrows(SQLException.class, () ->
                        s.executeQuery("SELECT * FROM sto_lock WHERE id = 1 FOR UPDATE"));
                long elapsed = System.currentTimeMillis() - start;
                assertEquals("55P03", e.getSQLState());
                assertTrue(e.getMessage().startsWith("ERROR: canceling statement due to lock timeout"),
                        "unexpected message: " + e.getMessage());
                assertTrue(elapsed < 3000, "100ms lock_timeout should fail fast, took " + elapsed + "ms");
            }
            waiter.rollback();
            holder.rollback();
            holder.setAutoCommit(true);
            holder.createStatement().execute("DROP TABLE sto_lock");
        }
    }

    // ---- idle_in_transaction_session_timeout ----

    @Test
    void idleInTransactionTimeoutDropsTheConnection() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("SET idle_in_transaction_session_timeout = '150ms'");
            s.execute("BEGIN");
            Thread.sleep(1200);
            assertThrows(SQLException.class, () -> s.execute("SELECT 1"),
                    "connection should have been terminated while idle in transaction");
        }
    }

    @Test
    void idleOutsideATransactionIsNotTerminated() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("SET idle_in_transaction_session_timeout = '150ms'");
            Thread.sleep(600);
            try (ResultSet rs = s.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    private static void assertShows(Statement s, String setSql, String expected) throws SQLException {
        s.execute(setSql);
        try (ResultSet rs = s.executeQuery("SHOW statement_timeout")) {
            assertTrue(rs.next());
            assertEquals(expected, rs.getString(1), "after: " + setSql);
        }
    }
}
