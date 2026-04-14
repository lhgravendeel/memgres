package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 13 failures from pg-catalog-prepared-statements-cursors.sql where
 * Memgres diverges from PostgreSQL 18 behavior.
 *
 * Stmt 7   - pg_prepared_statements column count: PG=8, Memgres=0
 * Stmt 35  - custom_plans after 2 EXECUTEs: PG=0, Memgres=2
 * Stmt 38  - custom_plans after 6 EXECUTEs: PG=0, Memgres=6
 * Stmt 70  - pg_cursors baseline count: PG=1, Memgres=0
 * Stmt 71  - pg_cursors column count: PG=6, Memgres=0
 * Stmt 74  - cursor is_scrollable default: PG=true, Memgres=false
 * Stmt 98  - pg_cursors count with 3 declared: PG=4, Memgres=3
 * Stmt 111 - pg_cursors count after CLOSE ALL: PG=1, Memgres=0
 * Stmt 137 - pg_cursors count after ROLLBACK: PG=1, Memgres=0
 * Stmt 141 - pg_cursors count with holdable after COMMIT: PG=2, Memgres=1
 * Stmt 143 - pg_cursors count after DISCARD ALL: PG=1, Memgres=0
 * Stmt 171 - cursor statement starts with SELECT (should be false): PG=f, Memgres=t
 * Stmt 172 - cursor statement contains DECLARE (no_declare should be false): PG=f, Memgres=t
 */
class PgCatalogPreparedStmtsCursorsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            s.execute("DEALLOCATE ALL");
            s.execute("DROP TABLE IF EXISTS cat_test CASCADE");
            s.execute("CREATE TABLE cat_test ("
                    + "id   integer PRIMARY KEY, "
                    + "name text NOT NULL, "
                    + "val  numeric(10,2))");
            s.execute("INSERT INTO cat_test VALUES "
                    + "(1, 'alpha', 10.50), (2, 'beta', 20.75), (3, 'gamma', 30.00)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DEALLOCATE ALL");
                s.execute("DROP TABLE IF EXISTS cat_test CASCADE");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            return rs.getString(1);
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            return rs.getInt(1);
        }
    }

    /**
     * Stmt 7: pg_prepared_statements should have 8 columns in information_schema
     * (name, statement, prepare_time, parameter_types, result_types, from_sql,
     * generic_plans, custom_plans). Memgres returns 0.
     */
    @Test
    void testStmt7_pgPreparedStatementsColumnCount() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("PREPARE cat_inv AS SELECT 1");
            try {
                int colCount = queryInt(
                        "SELECT count(*)::integer AS col_count "
                                + "FROM information_schema.columns "
                                + "WHERE table_schema = 'pg_catalog' "
                                + "AND table_name = 'pg_prepared_statements'");
                assertEquals(8, colCount,
                        "pg_prepared_statements should have 8 columns in information_schema");
            } finally {
                s.execute("DEALLOCATE cat_inv");
            }
        }
    }

    /**
     * Stmt 35: After 2 EXECUTEs of a simple query, custom_plans should remain 0
     * (PG uses generic plans for simple queries). Memgres returns 2.
     */
    @Test
    void testStmt35_customPlansAfterTwoExecutes() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("PREPARE cat_exec_cnt AS SELECT 42");
            try {
                s.execute("EXECUTE cat_exec_cnt");
                s.execute("EXECUTE cat_exec_cnt");
                int customPlans = queryInt(
                        "SELECT custom_plans FROM pg_prepared_statements "
                                + "WHERE name = 'cat_exec_cnt'");
                assertEquals(0, customPlans,
                        "custom_plans should remain 0 after EXECUTEs of a simple query "
                                + "(PG uses generic plans)");
            } finally {
                s.execute("DEALLOCATE cat_exec_cnt");
            }
        }
    }

    /**
     * Stmt 38: After 6 EXECUTEs (total, including previous), custom_plans should
     * remain 0. Memgres returns 6.
     */
    @Test
    void testStmt38_customPlansAfterSixExecutes() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("PREPARE cat_exec_cnt AS SELECT 42");
            try {
                // Execute 6 times total
                for (int i = 0; i < 6; i++) {
                    s.execute("EXECUTE cat_exec_cnt");
                }
                int customPlans = queryInt(
                        "SELECT custom_plans FROM pg_prepared_statements "
                                + "WHERE name = 'cat_exec_cnt'");
                assertEquals(0, customPlans,
                        "custom_plans should remain 0 after 6 EXECUTEs of a simple query "
                                + "(PG uses generic plans)");
            } finally {
                s.execute("DEALLOCATE cat_exec_cnt");
            }
        }
    }

    /**
     * Stmt 70: pg_cursors baseline count should be 1 (PG has an implicit portal
     * cursor). Memgres returns 0.
     */
    @Test
    void testStmt70_pgCursorsBaselineCount() throws SQLException {
        int count = queryInt("SELECT count(*)::integer AS count FROM pg_cursors");
        assertEquals(1, count,
                "pg_cursors should have 1 entry for the implicit portal cursor");
    }

    /**
     * Stmt 71: pg_cursors should have 6 columns in information_schema (name,
     * statement, is_holdable, is_binary, is_scrollable, creation_time).
     * Memgres returns 0.
     */
    @Test
    void testStmt71_pgCursorsColumnCount() throws SQLException {
        int colCount = queryInt(
                "SELECT count(*)::integer AS col_count "
                        + "FROM information_schema.columns "
                        + "WHERE table_schema = 'pg_catalog' "
                        + "AND table_name = 'pg_cursors'");
        assertEquals(6, colCount,
                "pg_cursors should have 6 columns in information_schema");
    }

    /**
     * Stmt 74: A cursor declared without SCROLL should still report
     * is_scrollable=true in PG (default behavior). Memgres returns false.
     */
    @Test
    void testStmt74_cursorIsScrollableDefault() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE cat_cur CURSOR FOR SELECT id FROM cat_test ORDER BY id");
            try (ResultSet rs = s.executeQuery(
                    "SELECT name, is_holdable::text, is_binary::text, is_scrollable::text "
                            + "FROM pg_cursors WHERE name = 'cat_cur'")) {
                assertTrue(rs.next(), "Expected a row for cursor cat_cur in pg_cursors");
                assertEquals("cat_cur", rs.getString(1));
                assertEquals("false", rs.getString(2), "is_holdable");
                assertEquals("false", rs.getString(3), "is_binary");
                assertEquals("true", rs.getString(4),
                        "is_scrollable should be true by default in PG 18 (only explicit NO SCROLL reports false)");
            }
            s.execute("CLOSE cat_cur");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    /**
     * Stmt 98: With 3 declared cursors in a transaction, pg_cursors should show
     * count=4 (3 declared + 1 implicit portal). Memgres returns 3.
     */
    @Test
    void testStmt98_multipleCursorsCountIncludesImplicit() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE cat_m1 CURSOR FOR SELECT 1");
            s.execute("DECLARE cat_m2 CURSOR FOR SELECT 2");
            s.execute("DECLARE cat_m3 CURSOR FOR SELECT 3");
            int count = queryInt("SELECT count(*)::integer AS count FROM pg_cursors");
            assertEquals(4, count,
                    "pg_cursors should show 4 entries (3 declared + 1 implicit portal)");
            s.execute("CLOSE ALL");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    /**
     * Stmt 111: After CLOSE ALL in a transaction, pg_cursors should still have
     * count=1 (the implicit portal cursor remains). Memgres returns 0.
     */
    @Test
    void testStmt111_closeAllLeavesImplicitPortal() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE cat_cl1 CURSOR FOR SELECT 1");
            s.execute("DECLARE cat_cl2 CURSOR FOR SELECT 2");
            s.execute("CLOSE ALL");
            int count = queryInt("SELECT count(*)::integer AS count FROM pg_cursors");
            assertEquals(1, count,
                    "After CLOSE ALL, implicit portal cursor should remain (count=1)");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    /**
     * Stmt 137: After ROLLBACK of cursors (including holdable), pg_cursors should
     * have count=1 (the implicit portal cursor remains). Memgres returns 0.
     */
    @Test
    void testStmt137_rollbackLeavesImplicitPortal() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE cat_lc_rb1 CURSOR WITH HOLD FOR SELECT 1");
            s.execute("DECLARE cat_lc_rb2 CURSOR FOR SELECT 2");
        }
        conn.rollback();
        conn.setAutoCommit(true);

        int count = queryInt("SELECT count(*)::integer AS count FROM pg_cursors");
        assertEquals(1, count,
                "After ROLLBACK, implicit portal cursor should remain (count=1)");
    }

    /**
     * Stmt 141: After declaring a holdable cursor and committing, pg_cursors should
     * show count=2 (1 holdable + 1 implicit portal). Memgres returns 1.
     */
    @Test
    void testStmt141_holdableCursorPlusImplicitPortal() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE cat_da CURSOR WITH HOLD FOR SELECT 1");
        }
        conn.commit();
        conn.setAutoCommit(true);

        try {
            int count = queryInt("SELECT count(*)::integer AS count FROM pg_cursors");
            assertEquals(2, count,
                    "pg_cursors should show 2 entries (1 holdable + 1 implicit portal)");
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("DISCARD ALL");
            } catch (SQLException ignored) {
            }
        }
    }

    /**
     * Stmt 143: After DISCARD ALL removes holdable cursors, pg_cursors should
     * still have count=1 (the implicit portal cursor). Memgres returns 0.
     */
    @Test
    void testStmt143_discardAllLeavesImplicitPortal() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE cat_da CURSOR WITH HOLD FOR SELECT 1");
        }
        conn.commit();
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("DISCARD ALL");
        }

        int count = queryInt("SELECT count(*)::integer AS count FROM pg_cursors");
        assertEquals(1, count,
                "After DISCARD ALL, implicit portal cursor should remain (count=1)");
    }

    /**
     * Stmt 171: The statement column in pg_cursors should contain the full
     * DECLARE statement, so upper(statement) should NOT start with 'SELECT'.
     * PG returns false; Memgres returns true (it stores only the query portion).
     */
    @Test
    void testStmt171_cursorStatementDoesNotStartWithSelect() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE cat_qonly SCROLL CURSOR WITH HOLD FOR "
                    + "SELECT id, name FROM cat_test ORDER BY id");
            String startsWithSelect = query1(
                    "SELECT (upper(statement) LIKE 'SELECT%') AS starts_with_select "
                            + "FROM pg_cursors WHERE name = 'cat_qonly'");
            assertEquals("f", startsWithSelect,
                    "pg_cursors.statement should include the full DECLARE, "
                            + "not start with SELECT");
            s.execute("CLOSE cat_qonly");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    /**
     * Stmt 172: The statement column in pg_cursors should contain 'DECLARE',
     * so the expression (upper(statement) NOT LIKE '%DECLARE%') should be false.
     * PG returns false; Memgres returns true (it stores only the query portion).
     */
    @Test
    void testStmt172_cursorStatementContainsDeclare() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("DECLARE cat_qonly SCROLL CURSOR WITH HOLD FOR "
                    + "SELECT id, name FROM cat_test ORDER BY id");
            String noDeclare = query1(
                    "SELECT (upper(statement) NOT LIKE '%DECLARE%') AS no_declare "
                            + "FROM pg_cursors WHERE name = 'cat_qonly'");
            assertEquals("f", noDeclare,
                    "pg_cursors.statement should contain DECLARE keyword");
            s.execute("CLOSE cat_qonly");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }
}
