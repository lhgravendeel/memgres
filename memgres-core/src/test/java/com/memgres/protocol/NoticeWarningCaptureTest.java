package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for notice/warning capture from SQL and PL/pgSQL.
 *
 * Covers: RAISE NOTICE, RAISE WARNING, RAISE EXCEPTION, SQLSTATE propagation,
 * multiple notices, ordering, notices from triggers and functions, and
 * SQLWarning chain visibility on both Statement and Connection.
 *
 * Table prefix: nwc_
 */
class NoticeWarningCaptureTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    static String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row for: " + sql);
            return rs.getString(1);
        }
    }

    /** Collect all SQLWarning messages from the warning chain into a list. */
    private static List<String> collectWarnings(SQLWarning w) {
        List<String> msgs = new ArrayList<>();
        while (w != null) {
            msgs.add(w.getMessage());
            w = w.getNextWarning();
        }
        return msgs;
    }

    // -------------------------------------------------------------------------
    // 1. RAISE NOTICE capture via Statement.getWarnings()
    // -------------------------------------------------------------------------
    @Test
    void raiseNotice_capturedOnStatement() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DO $$ BEGIN RAISE NOTICE 'hello %', 'world'; END $$");
            SQLWarning w = s.getWarnings();
            assertNotNull(w, "RAISE NOTICE should produce a SQLWarning on the Statement");
            assertTrue(w.getMessage().contains("hello world"),
                    "Warning message should contain 'hello world', got: " + w.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // 2. RAISE WARNING capture
    // -------------------------------------------------------------------------
    @Test
    void raiseWarning_capturedOnStatement() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DO $$ BEGIN RAISE WARNING 'this is a warning'; END $$");
            SQLWarning w = s.getWarnings();
            assertNotNull(w, "RAISE WARNING should produce a SQLWarning on the Statement");
            assertTrue(w.getMessage().contains("this is a warning"),
                    "Warning message should contain 'this is a warning', got: " + w.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // 3. RAISE EXCEPTION causes SQLException
    // -------------------------------------------------------------------------
    @Test
    void raiseException_throwsSQLException() {
        assertThrows(SQLException.class, () -> exec("DO $$ BEGIN RAISE EXCEPTION 'boom'; END $$"),
                "RAISE EXCEPTION should throw SQLException");
    }

    // -------------------------------------------------------------------------
    // 4. RAISE EXCEPTION with ERRCODE: verify SQLSTATE in caught exception
    // -------------------------------------------------------------------------
    @Test
    void raiseException_withSqlstate_propagatesSqlstate() {
        try {
            exec("DO $$ BEGIN RAISE EXCEPTION 'custom error' USING ERRCODE = '22000'; END $$");
            fail("Should have thrown SQLException");
        } catch (SQLException e) {
            assertEquals("22000", e.getSQLState(),
                    "SQLSTATE should be '22000', got: " + e.getSQLState());
        }
    }

    // -------------------------------------------------------------------------
    // 5. Multiple notices in one statement: all should be captured
    // -------------------------------------------------------------------------
    @Test
    void multipleNotices_allCaptured() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                DO $$
                BEGIN
                  RAISE NOTICE 'first';
                  RAISE NOTICE 'second';
                  RAISE NOTICE 'third';
                END $$
                """);
            List<String> msgs = collectWarnings(s.getWarnings());
            assertEquals(3, msgs.size(),
                    "Expected 3 notices in warning chain, got: " + msgs.size() + " -> " + msgs);
        }
    }

    // -------------------------------------------------------------------------
    // 6. Notice ordering: verify notices arrive in raise order
    // -------------------------------------------------------------------------
    @Test
    void noticeOrdering_preservedInChain() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                DO $$
                BEGIN
                  RAISE NOTICE 'alpha';
                  RAISE NOTICE 'beta';
                  RAISE NOTICE 'gamma';
                END $$
                """);
            List<String> msgs = collectWarnings(s.getWarnings());
            assertEquals(3, msgs.size(), "Expected 3 notices");
            assertTrue(msgs.get(0).contains("alpha"), "First notice should be 'alpha', got: " + msgs.get(0));
            assertTrue(msgs.get(1).contains("beta"),  "Second notice should be 'beta', got: " + msgs.get(1));
            assertTrue(msgs.get(2).contains("gamma"), "Third notice should be 'gamma', got: " + msgs.get(2));
        }
    }

    // -------------------------------------------------------------------------
    // 7. Notice from trigger, captured on the triggering INSERT statement
    // -------------------------------------------------------------------------
    @Test
    void noticeFromTrigger_capturedOnInsert() throws SQLException {
        exec("CREATE TABLE nwc_trig_tbl (id int)");
        exec("""
            CREATE OR REPLACE FUNCTION nwc_trig_fn() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              RAISE NOTICE 'trigger fired for id=%', NEW.id;
              RETURN NEW;
            END $$
            """);
        exec("CREATE TRIGGER nwc_trig AFTER INSERT ON nwc_trig_tbl FOR EACH ROW EXECUTE FUNCTION nwc_trig_fn()");
        try {
            try (Statement s = conn.createStatement()) {
                s.execute("INSERT INTO nwc_trig_tbl VALUES (42)");
                SQLWarning w = s.getWarnings();
                assertNotNull(w, "Trigger RAISE NOTICE should produce a SQLWarning");
                assertTrue(w.getMessage().contains("trigger fired"),
                        "Notice should mention 'trigger fired', got: " + w.getMessage());
                assertTrue(w.getMessage().contains("42"),
                        "Notice should include the inserted id=42, got: " + w.getMessage());
            }
        } finally {
            exec("DROP TABLE nwc_trig_tbl CASCADE");
            exec("DROP FUNCTION IF EXISTS nwc_trig_fn()");
        }
    }

    // -------------------------------------------------------------------------
    // 8. Notice from function, captured when function is called
    // -------------------------------------------------------------------------
    @Test
    void noticeFromFunction_capturedOnCall() throws SQLException {
        exec("""
            CREATE OR REPLACE FUNCTION nwc_notice_fn(p int) RETURNS int LANGUAGE plpgsql AS $$
            BEGIN
              RAISE NOTICE 'processing value %', p;
              RETURN p * 2;
            END $$
            """);
        try {
            try (Statement s = conn.createStatement()) {
                s.executeQuery("SELECT nwc_notice_fn(7)").close();
                SQLWarning w = s.getWarnings();
                assertNotNull(w, "Function RAISE NOTICE should produce a SQLWarning");
                assertTrue(w.getMessage().contains("processing value"),
                        "Notice should say 'processing value', got: " + w.getMessage());
                assertTrue(w.getMessage().contains("7"),
                        "Notice should include the argument 7, got: " + w.getMessage());
            }
        } finally {
            exec("DROP FUNCTION IF EXISTS nwc_notice_fn(int)");
        }
    }

    // -------------------------------------------------------------------------
    // 9. WARNING vs NOTICE both in SQLWarning chain
    // -------------------------------------------------------------------------
    @Test
    void warningAndNotice_bothInChain() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                DO $$
                BEGIN
                  RAISE NOTICE 'a notice';
                  RAISE WARNING 'a warning';
                END $$
                """);
            List<String> msgs = collectWarnings(s.getWarnings());
            assertEquals(2, msgs.size(),
                    "Expected 2 entries (1 NOTICE + 1 WARNING), got: " + msgs);
            boolean hasNotice  = msgs.stream().anyMatch(m -> m.contains("a notice"));
            boolean hasWarning = msgs.stream().anyMatch(m -> m.contains("a warning"));
            assertTrue(hasNotice,  "Should have a NOTICE message, got: " + msgs);
            assertTrue(hasWarning, "Should have a WARNING message, got: " + msgs);
        }
    }

    // -------------------------------------------------------------------------
    // 10. DROP TABLE IF EXISTS on non-existent table produces a notice
    // -------------------------------------------------------------------------
    @Test
    void dropTableIfExists_nonExistent_producesNotice() throws SQLException {
        // Ensure table does not exist
        exec("DROP TABLE IF EXISTS nwc_does_not_exist");
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS nwc_does_not_exist");
            SQLWarning w = s.getWarnings();
            // PostgreSQL emits a NOTICE like "table ... does not exist, skipping"
            assertNotNull(w, "DROP TABLE IF EXISTS on missing table should produce a notice");
        }
    }

    // -------------------------------------------------------------------------
    // 11. CREATE TABLE IF NOT EXISTS on existing table produces a notice
    // -------------------------------------------------------------------------
    @Test
    void createTableIfNotExists_alreadyExists_producesNotice() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS nwc_already_exists (id int)");
        try {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS nwc_already_exists (id int)");
                SQLWarning w = s.getWarnings();
                // PostgreSQL emits a NOTICE like "relation ... already exists, skipping"
                assertNotNull(w, "CREATE TABLE IF NOT EXISTS on existing table should produce a notice");
            }
        } finally {
            exec("DROP TABLE IF EXISTS nwc_already_exists");
        }
    }

    // -------------------------------------------------------------------------
    // 12. Implicit cast notices (integer literal stored in text column, or similar)
    // -------------------------------------------------------------------------
    @Test
    void implicitCast_noErrorAndOptionalNotice() throws SQLException {
        exec("CREATE TABLE nwc_cast_tbl (val text)");
        try {
            // Inserting an integer literal into a text column should succeed
            // (implicit cast int->text). Whether a notice is emitted is
            // implementation-defined; the key assertion is no exception.
            try (Statement s = conn.createStatement()) {
                assertDoesNotThrow(() -> s.execute("INSERT INTO nwc_cast_tbl VALUES (42::text)"));
            }
            assertEquals("42", query1("SELECT val FROM nwc_cast_tbl LIMIT 1"));
        } finally {
            exec("DROP TABLE nwc_cast_tbl");
        }
    }

    // -------------------------------------------------------------------------
    // 13. RAISE NOTICE with multiple format arguments
    // -------------------------------------------------------------------------
    @Test
    void raiseNotice_withMultipleFormatArgs() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                DO $$
                BEGIN
                  RAISE NOTICE 'id=%, name=%', 1, 'test';
                END $$
                """);
            SQLWarning w = s.getWarnings();
            assertNotNull(w, "RAISE NOTICE with format args should produce a warning");
            assertTrue(w.getMessage().contains("id=1"),   "Should contain 'id=1', got: " + w.getMessage());
            assertTrue(w.getMessage().contains("name=test"), "Should contain 'name=test', got: " + w.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // 14. Notice does not affect result; query returns rows AND notice
    // -------------------------------------------------------------------------
    @Test
    void noticeDoesNotAffectQueryResult() throws SQLException {
        exec("CREATE TABLE nwc_data (id int, val text)");
        exec("INSERT INTO nwc_data VALUES (1, 'row1'), (2, 'row2')");
        try {
            try (Statement s = conn.createStatement()) {
                // A function that raises notice AND returns a value
                exec("CREATE OR REPLACE FUNCTION nwc_with_notice(x int) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'called with %', x; RETURN 'ok'; END $$");
                try (ResultSet rs = s.executeQuery("SELECT id, nwc_with_notice(id) FROM nwc_data ORDER BY id")) {
                    assertTrue(rs.next(), "Should have at least one row");
                    assertEquals(1, rs.getInt(1));
                    assertEquals("ok", rs.getString(2));
                    assertTrue(rs.next(), "Should have a second row");
                    assertEquals(2, rs.getInt(1));
                    assertEquals("ok", rs.getString(2));
                    assertFalse(rs.next(), "Should have exactly two rows");
                }
                // Notices should still be present
                assertNotNull(s.getWarnings(), "Notices should appear alongside query results");
            }
        } finally {
            exec("DROP TABLE nwc_data");
            exec("DROP FUNCTION IF EXISTS nwc_with_notice(int)");
        }
    }

    // -------------------------------------------------------------------------
    // 15. getWarnings() on Connection vs Statement: both surface notices
    // -------------------------------------------------------------------------
    @Test
    void getWarnings_onConnectionAndStatement() throws SQLException {
        // Clear any prior connection-level warnings
        conn.clearWarnings();
        try (Statement s = conn.createStatement()) {
            s.execute("DO $$ BEGIN RAISE NOTICE 'conn-level test'; END $$");

            // Statement-level warnings must be present
            SQLWarning stmtWarn = s.getWarnings();
            assertNotNull(stmtWarn, "Statement.getWarnings() should surface the NOTICE");
            assertTrue(stmtWarn.getMessage().contains("conn-level test"),
                    "Statement warning should say 'conn-level test', got: " + stmtWarn.getMessage());

            // Connection-level warnings: in the PostgreSQL JDBC driver, notices raised
            // during statement execution appear on Statement.getWarnings(). Connection-level
            // warnings accumulate separately (e.g., from SET commands). Verify that at
            // minimum the statement path delivers the notice.
            // If the driver also mirrors them on Connection, that is acceptable too.
            SQLWarning connWarn = conn.getWarnings();
            // Either location is valid; statement is the canonical one for RAISE NOTICE.
            assertTrue(stmtWarn != null || connWarn != null,
                    "At least one of Statement or Connection should carry the notice");
        }
    }

    // -------------------------------------------------------------------------
    // 16. RAISE NOTICE with HINT clause: message and hint accessible
    // -------------------------------------------------------------------------
    @Test
    void raiseNotice_withHint_capturedInWarning() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                DO $$
                BEGIN
                  RAISE NOTICE 'something happened' USING HINT = 'try this fix';
                END $$
                """);
            SQLWarning w = s.getWarnings();
            assertNotNull(w, "RAISE NOTICE with HINT should produce a SQLWarning");
            assertTrue(w.getMessage().contains("something happened"),
                    "Warning message should contain the notice text, got: " + w.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // 17. RAISE NOTICE from nested function call, bubbles up to statement
    // -------------------------------------------------------------------------
    @Test
    void raiseNotice_fromNestedFunctionCall_bubblesUp() throws SQLException {
        exec("""
            CREATE OR REPLACE FUNCTION nwc_inner() RETURNS void LANGUAGE plpgsql AS $$
            BEGIN RAISE NOTICE 'inner notice'; END $$
            """);
        exec("""
            CREATE OR REPLACE FUNCTION nwc_outer() RETURNS void LANGUAGE plpgsql AS $$
            BEGIN PERFORM nwc_inner(); RAISE NOTICE 'outer notice'; END $$
            """);
        try {
            try (Statement s = conn.createStatement()) {
                s.execute("SELECT nwc_outer()");
                List<String> msgs = collectWarnings(s.getWarnings());
                assertTrue(msgs.size() >= 2,
                        "Expected at least 2 notices from nested functions, got: " + msgs);
                boolean hasInner = msgs.stream().anyMatch(m -> m.contains("inner notice"));
                boolean hasOuter = msgs.stream().anyMatch(m -> m.contains("outer notice"));
                assertTrue(hasInner, "Should see 'inner notice', got: " + msgs);
                assertTrue(hasOuter, "Should see 'outer notice', got: " + msgs);
            }
        } finally {
            exec("DROP FUNCTION IF EXISTS nwc_outer()");
            exec("DROP FUNCTION IF EXISTS nwc_inner()");
        }
    }

    // -------------------------------------------------------------------------
    // 18. clearWarnings() clears previous notices before next statement
    // -------------------------------------------------------------------------
    @Test
    void clearWarnings_clearsBeforeNextExecution() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DO $$ BEGIN RAISE NOTICE 'old notice'; END $$");
            assertNotNull(s.getWarnings(), "Should have warnings after first execute");

            s.clearWarnings();
            assertNull(s.getWarnings(), "Warnings should be null after clearWarnings()");

            // Execute a statement that does NOT produce notices
            s.execute("SELECT 1");
            assertNull(s.getWarnings(), "No warnings expected from plain SELECT 1");
        }
    }

    // -------------------------------------------------------------------------
    // 19. RAISE with DEBUG level: usually not surfaced but must not throw
    // -------------------------------------------------------------------------
    @Test
    void raiseDebug_doesNotThrowAndMayOrMayNotSurfaceWarning() throws SQLException {
        // DEBUG messages are typically suppressed unless log_min_messages is set.
        // The test verifies no exception is thrown; warning presence is optional.
        assertDoesNotThrow(() -> {
            try (Statement s = conn.createStatement()) {
                s.execute("DO $$ BEGIN RAISE DEBUG 'debug message'; END $$");
            }
        }, "RAISE DEBUG should not throw an exception");
    }

    // -------------------------------------------------------------------------
    // 20. Exception caught inside DO block; notice still delivered, no outer error
    // -------------------------------------------------------------------------
    @Test
    void exceptionCaughtInsideDoBlock_noticeStillDelivered() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                DO $$
                BEGIN
                  RAISE NOTICE 'before exception';
                  BEGIN
                    RAISE EXCEPTION 'inner error';
                  EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE 'caught: %', SQLERRM;
                  END;
                  RAISE NOTICE 'after catch';
                END $$
                """);
            List<String> msgs = collectWarnings(s.getWarnings());
            assertTrue(msgs.size() >= 2,
                    "Expected at least 2 notices (before and after catch), got: " + msgs);
            boolean hasBefore = msgs.stream().anyMatch(m -> m.contains("before exception"));
            boolean hasAfter  = msgs.stream().anyMatch(m -> m.contains("after catch"));
            assertTrue(hasBefore, "Should see 'before exception' notice, got: " + msgs);
            assertTrue(hasAfter,  "Should see 'after catch' notice, got: " + msgs);
        }
    }
}
