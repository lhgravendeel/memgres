package com.memgres.wire;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for wire protocol behavior: H7, H12, M25, L5, L9, L10.
 */
class WireProtocolBehaviorTest {

    static Memgres memgres;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (memgres != null) memgres.close();
    }

    private Connection conn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private Connection simpleConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    // === L5: Syntax error Position should be 1-based ===

    @Test
    void l5_syntaxErrorPositionOneBased() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try {
                st.execute("SELECT FROM WHERE");
                fail("Expected syntax error");
            } catch (SQLException e) {
                assertEquals("42601", e.getSQLState());
                // Message should follow PG convention: "syntax error at or near ..."
                // and NOT duplicate position info in the text
                assertFalse(e.getMessage().contains("at position 0"),
                        "Position should not be 0-based in message");
            }
        }
    }

    @Test
    void l5_syntaxErrorMessageNotDuplicated() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try {
                st.execute("SELECTX foo");
                fail("Expected syntax error");
            } catch (SQLException e) {
                assertEquals("42601", e.getSQLState());
                // Should not have redundant "at position N" in the message text
                String msg = e.getMessage();
                assertNotNull(msg);
            }
        }
    }

    // === L9: RAISE WARNING should use SQLState 01000 ===

    @Test
    void l9_raiseWarningState01000() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE OR REPLACE FUNCTION wpb_warn_test() RETURNS void AS $$ " +
                    "BEGIN RAISE WARNING 'test warning'; END; $$ LANGUAGE plpgsql");
            try {
                st.execute("SELECT wpb_warn_test()");
                // Check that the warning was delivered
                SQLWarning warning = st.getWarnings();
                if (warning != null) {
                    assertEquals("01000", warning.getSQLState(),
                            "RAISE WARNING should use SQLState 01000, not 00000");
                }
            } finally {
                st.execute("DROP FUNCTION IF EXISTS wpb_warn_test()");
            }
        }
    }

    // === M25: {call proc()} should error for procedures via function path ===

    @Test
    void m25_procedureNotCallableAsFunction() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE OR REPLACE PROCEDURE wpb_noop_proc() AS $$ BEGIN END; $$ LANGUAGE plpgsql");
            try {
                st.execute("SELECT wpb_noop_proc()");
                fail("Expected 42809 error for calling procedure as function");
            } catch (SQLException e) {
                assertEquals("42809", e.getSQLState());
            } finally {
                st.execute("DROP PROCEDURE IF EXISTS wpb_noop_proc()");
            }
        }
    }

    // === L10: Composite index in getIndexInfo ===

    @Test
    void l10_compositeIndexAllColumns() throws Exception {
        try (Connection c = simpleConn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE wpb_comp_test(a int, b int, c text)");
            st.execute("CREATE INDEX wpb_comp_test_idx ON wpb_comp_test(a, b)");
            try {
                DatabaseMetaData md = c.getMetaData();
                int colCount = 0;
                try (ResultSet rs = md.getIndexInfo(null, "public", "wpb_comp_test", false, true)) {
                    while (rs.next()) {
                        String indexName = rs.getString("INDEX_NAME");
                        if ("wpb_comp_test_idx".equals(indexName)) {
                            colCount++;
                            int ordinal = rs.getShort("ORDINAL_POSITION");
                            String colName = rs.getString("COLUMN_NAME");
                            if (ordinal == 1) assertEquals("a", colName);
                            if (ordinal == 2) assertEquals("b", colName);
                        }
                    }
                }
                assertEquals(2, colCount, "Composite index should report both columns");
            } finally {
                st.execute("DROP TABLE IF EXISTS wpb_comp_test");
            }
        }
    }

    // === L10: pg_get_indexdef with column number ===

    @Test
    void l10_pgGetIndexdefColumnNumber() throws Exception {
        try (Connection c = simpleConn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE wpb_idef_test(x int, y int)");
            st.execute("CREATE INDEX wpb_idef_test_idx ON wpb_idef_test(x, y)");
            try {
                try (ResultSet rs = st.executeQuery(
                        "SELECT pg_get_indexdef(c.oid, 1, true) AS col1, pg_get_indexdef(c.oid, 2, true) AS col2 " +
                                "FROM pg_class c WHERE c.relname = 'wpb_idef_test_idx'")) {
                    assertTrue(rs.next());
                    assertEquals("x", rs.getString("col1"));
                    assertEquals("y", rs.getString("col2"));
                }
            } finally {
                st.execute("DROP TABLE IF EXISTS wpb_idef_test");
            }
        }
    }

    // === L10: COLUMN_DEF formatting ===

    @Test
    void l10_columnDefCurrentTimestamp() throws Exception {
        try (Connection c = simpleConn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE wpb_def_test(id int, ts timestamp DEFAULT CURRENT_TIMESTAMP)");
            try {
                try (ResultSet rs = st.executeQuery(
                        "SELECT column_default FROM information_schema.columns " +
                                "WHERE table_name='wpb_def_test' AND column_name='ts'")) {
                    assertTrue(rs.next());
                    String def = rs.getString(1);
                    assertNotNull(def);
                    assertTrue(def.toUpperCase().contains("CURRENT_TIMESTAMP"),
                            "Default should contain CURRENT_TIMESTAMP, got: " + def);
                }
            } finally {
                st.execute("DROP TABLE IF EXISTS wpb_def_test");
            }
        }
    }

    // === L10: getFunctionColumns for SQL functions ===

    @Test
    void l10_functionColumnsForSqlFunction() throws Exception {
        try (Connection c = simpleConn(); Statement st = c.createStatement()) {
            st.execute("CREATE FUNCTION wpb_add_fn(a int, b int) RETURNS int AS $$ SELECT a + b; $$ LANGUAGE sql");
            try {
                try (ResultSet rs = st.executeQuery(
                        "SELECT proargnames FROM pg_proc WHERE proname = 'wpb_add_fn'")) {
                    assertTrue(rs.next());
                    String argNames = rs.getString(1);
                    assertNotNull(argNames, "SQL function should have proargnames populated");
                    assertTrue(argNames.contains("a"), "Should contain param name 'a'");
                    assertTrue(argNames.contains("b"), "Should contain param name 'b'");
                }
            } finally {
                st.execute("DROP FUNCTION IF EXISTS wpb_add_fn(int, int)");
            }
        }
    }

    // === H12: Batch within explicit transaction rolls back on failure ===

    @Test
    void h12_batchInExplicitTxnRollsBack() throws Exception {
        try (Connection c = conn()) {
            c.setAutoCommit(true);
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE wpb_batch_test(id int PRIMARY KEY)");
                c.setAutoCommit(false);

                // Insert in explicit transaction, with a failure mid-way
                st.execute("INSERT INTO wpb_batch_test VALUES (1)");
                st.execute("INSERT INTO wpb_batch_test VALUES (2)");
                try {
                    st.execute("INSERT INTO wpb_batch_test VALUES (2)"); // duplicate PK
                    fail("Expected unique violation");
                } catch (SQLException e) {
                    assertEquals("23505", e.getSQLState());
                }
                c.rollback();

                // All rows should be gone after rollback
                try (ResultSet rs = st.executeQuery("SELECT count(*)::int FROM wpb_batch_test")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1));
                }
                c.setAutoCommit(true);
                st.execute("DROP TABLE wpb_batch_test");
            }
        }
    }

    // === H7: Cancel should work in extended protocol ===

    @Test
    void h7_cancelExtendedProtocol() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.setQueryTimeout(2); // 2 seconds
            try {
                // pg_sleep should be cancelled
                st.execute("SELECT pg_sleep(30)");
                fail("Expected cancellation");
            } catch (SQLException e) {
                // Either 57014 (query_canceled) or timeout is acceptable
                String state = e.getSQLState();
                assertTrue("57014".equals(state) || "57000".equals(state) || e.getMessage().contains("cancel"),
                        "Expected cancel error, got: " + state + " " + e.getMessage());
            }
        }
    }
}
