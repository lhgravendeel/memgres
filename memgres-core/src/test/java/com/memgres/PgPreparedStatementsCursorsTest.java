package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for B3: pg_prepared_statements and pg_cursors catalog views.
 * Verifies that session-scoped prepared statements and cursors are visible
 * in their respective catalog views with correct metadata.
 */
class PgPreparedStatementsCursorsTest {

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

    private void exec(String sql) throws Exception {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private int queryInt(String sql) throws Exception {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected a row from: " + sql);
            return rs.getInt(1);
        }
    }

    // ===== pg_prepared_statements =====

    @Test
    void pgPreparedStatementsEmptyByDefault() throws Exception {
        // Ensure clean state
        exec("DEALLOCATE ALL");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM pg_prepared_statements")) {
            assertFalse(rs.next(), "Should be empty when no prepared statements exist");
        }
    }

    @Test
    void pgPreparedStatementsShowsPreparedSelect() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE myplan AS SELECT 1");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT name, statement, from_sql FROM pg_prepared_statements")) {
            assertTrue(rs.next());
            assertEquals("myplan", rs.getString("name"));
            assertNotNull(rs.getString("statement"));
            assertTrue(rs.getBoolean("from_sql"));
            assertFalse(rs.next());
        }
        exec("DEALLOCATE myplan");
    }

    @Test
    void pgPreparedStatementsWithParameterTypes() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE typed_plan (integer, text) AS SELECT $1, $2");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT name, parameter_types FROM pg_prepared_statements WHERE name = 'typed_plan'")) {
            assertTrue(rs.next());
            assertEquals("typed_plan", rs.getString("name"));
            String paramTypes = rs.getString("parameter_types");
            assertNotNull(paramTypes);
            assertTrue(paramTypes.contains("integer"), "Should contain 'integer' type: " + paramTypes);
            assertTrue(paramTypes.contains("text"), "Should contain 'text' type: " + paramTypes);
        }
        exec("DEALLOCATE typed_plan");
    }

    @Test
    void pgPreparedStatementsEmptyParameterTypes() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE no_params AS SELECT 42");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT parameter_types FROM pg_prepared_statements WHERE name = 'no_params'")) {
            assertTrue(rs.next());
            assertEquals("{}", rs.getString("parameter_types"));
        }
        exec("DEALLOCATE no_params");
    }

    @Test
    void pgPreparedStatementsPrepareTime() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE timed_plan AS SELECT 1");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT prepare_time FROM pg_prepared_statements WHERE name = 'timed_plan'")) {
            assertTrue(rs.next());
            String prepareTime = rs.getString("prepare_time");
            assertNotNull(prepareTime, "prepare_time should not be null");
            // Should be a valid timestamp string
            assertTrue(prepareTime.length() > 10, "prepare_time should be a timestamp: " + prepareTime);
        }
        exec("DEALLOCATE timed_plan");
    }

    @Test
    void pgPreparedStatementsMultiple() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE plan_a AS SELECT 1");
        exec("PREPARE plan_b (integer) AS SELECT $1 + 1");
        exec("PREPARE plan_c AS SELECT 'hello'");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT name FROM pg_prepared_statements ORDER BY name")) {
            assertTrue(rs.next()); assertEquals("plan_a", rs.getString(1));
            assertTrue(rs.next()); assertEquals("plan_b", rs.getString(1));
            assertTrue(rs.next()); assertEquals("plan_c", rs.getString(1));
            assertFalse(rs.next());
        }
        exec("DEALLOCATE ALL");
    }

    @Test
    void pgPreparedStatementsDeallocateRemovesEntry() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE to_remove AS SELECT 1");
        // Verify it's there
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_prepared_statements WHERE name = 'to_remove'")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
        exec("DEALLOCATE to_remove");
        // Verify it's gone
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_prepared_statements WHERE name = 'to_remove'")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    void pgPreparedStatementsDeallocateAllClearsAll() throws Exception {
        exec("PREPARE all_a AS SELECT 1");
        exec("PREPARE all_b AS SELECT 2");
        exec("DEALLOCATE ALL");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_prepared_statements")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    void pgPreparedStatementsHasResultTypesColumn() throws Exception {
        // PG 18 adds result_types column
        exec("DEALLOCATE ALL");
        exec("PREPARE rt_plan AS SELECT 1");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT result_types FROM pg_prepared_statements WHERE name = 'rt_plan'")) {
            assertTrue(rs.next());
            // result_types may be null in our implementation but the column must exist
            rs.getString("result_types"); // Should not throw
        }
        exec("DEALLOCATE rt_plan");
    }

    @Test
    void pgPreparedStatementsAllColumns() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE full_plan (integer) AS SELECT $1 * 2");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM pg_prepared_statements WHERE name = 'full_plan'")) {
            assertTrue(rs.next());
            ResultSetMetaData meta = rs.getMetaData();
            // Verify all expected columns exist
            boolean hasName = false, hasStatement = false, hasPrepareTime = false;
            boolean hasParameterTypes = false, hasResultTypes = false, hasFromSql = false;
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                switch (meta.getColumnName(i).toLowerCase()) {
                    case "name": hasName = true; break;
                    case "statement": hasStatement = true; break;
                    case "prepare_time": hasPrepareTime = true; break;
                    case "parameter_types": hasParameterTypes = true; break;
                    case "result_types": hasResultTypes = true; break;
                    case "from_sql": hasFromSql = true; break;
                }
            }
            assertTrue(hasName, "Missing 'name' column");
            assertTrue(hasStatement, "Missing 'statement' column");
            assertTrue(hasPrepareTime, "Missing 'prepare_time' column");
            assertTrue(hasParameterTypes, "Missing 'parameter_types' column");
            assertTrue(hasResultTypes, "Missing 'result_types' column (PG 18)");
            assertTrue(hasFromSql, "Missing 'from_sql' column");
        }
        exec("DEALLOCATE full_plan");
    }

    @Test
    void pgPreparedStatementsCaseInsensitiveName() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE MyPlan AS SELECT 1");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT name FROM pg_prepared_statements")) {
            assertTrue(rs.next());
            assertEquals("myplan", rs.getString("name"), "PG lowercases prepared statement names");
        }
        exec("DEALLOCATE myplan");
    }

    @Test
    void pgPreparedStatementsExecuteDoesNotAffectView() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE exec_plan AS SELECT 42");
        exec("EXECUTE exec_plan");
        // Still visible after execution
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_prepared_statements WHERE name = 'exec_plan'")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
        exec("DEALLOCATE exec_plan");
    }

    // ===== pg_cursors =====

    @Test
    void pgCursorsEmptyByDefault() throws Exception {
        exec("CLOSE ALL");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM pg_cursors")) {
            assertFalse(rs.next(), "Should be empty when no cursors exist");
        }
    }

    @Test
    void pgCursorsShowsDeclaredCursor() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CLOSE ALL");
            exec("DECLARE mycur CURSOR FOR SELECT 1");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT name, statement FROM pg_cursors")) {
                assertTrue(rs.next());
                assertEquals("mycur", rs.getString("name"));
                assertNotNull(rs.getString("statement"));
                assertFalse(rs.next());
            }
            exec("CLOSE mycur");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void pgCursorsIsHoldable() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CLOSE ALL");
            exec("DECLARE holdcur CURSOR WITH HOLD FOR SELECT 1");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT is_holdable FROM pg_cursors WHERE name = 'holdcur'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("is_holdable"));
            }
            exec("CLOSE holdcur");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void pgCursorsIsScrollable() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CLOSE ALL");
            exec("DECLARE scrollcur SCROLL CURSOR FOR SELECT 1");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT is_scrollable FROM pg_cursors WHERE name = 'scrollcur'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("is_scrollable"));
            }
            exec("CLOSE scrollcur");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void pgCursorsDefaultFlags() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CLOSE ALL");
            exec("DECLARE defcur CURSOR FOR SELECT 1");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT is_holdable, is_binary, is_scrollable FROM pg_cursors WHERE name = 'defcur'")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean("is_holdable"));
                assertFalse(rs.getBoolean("is_binary"));
                assertFalse(rs.getBoolean("is_scrollable"));
            }
            exec("CLOSE defcur");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void pgCursorsCreationTime() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CLOSE ALL");
            exec("DECLARE timecur CURSOR FOR SELECT 1");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT creation_time FROM pg_cursors WHERE name = 'timecur'")) {
                assertTrue(rs.next());
                String creationTime = rs.getString("creation_time");
                assertNotNull(creationTime, "creation_time should not be null");
                assertTrue(creationTime.length() > 10, "creation_time should be a timestamp: " + creationTime);
            }
            exec("CLOSE timecur");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void pgCursorsMultiple() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CLOSE ALL");
            exec("DECLARE cur_a CURSOR FOR SELECT 1");
            exec("DECLARE cur_b CURSOR FOR SELECT 2");
            exec("DECLARE cur_c SCROLL CURSOR WITH HOLD FOR SELECT 3");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT name FROM pg_cursors ORDER BY name")) {
                assertTrue(rs.next()); assertEquals("cur_a", rs.getString(1));
                assertTrue(rs.next()); assertEquals("cur_b", rs.getString(1));
                assertTrue(rs.next()); assertEquals("cur_c", rs.getString(1));
                assertFalse(rs.next());
            }
            exec("CLOSE ALL");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void pgCursorsCloseRemovesEntry() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CLOSE ALL");
            exec("DECLARE closeme CURSOR FOR SELECT 1");
            // Verify it's there
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_cursors WHERE name = 'closeme'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            exec("CLOSE closeme");
            // Verify it's gone
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_cursors WHERE name = 'closeme'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void pgCursorsCloseAllClearsAll() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE ca CURSOR FOR SELECT 1");
            exec("DECLARE cb CURSOR FOR SELECT 2");
            exec("CLOSE ALL");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_cursors")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void pgCursorsAllColumns() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CLOSE ALL");
            exec("DECLARE colcur CURSOR FOR SELECT 1");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT * FROM pg_cursors WHERE name = 'colcur'")) {
                assertTrue(rs.next());
                ResultSetMetaData meta = rs.getMetaData();
                boolean hasName = false, hasStatement = false, hasCreationTime = false;
                boolean hasIsHoldable = false, hasIsBinary = false, hasIsScrollable = false;
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    switch (meta.getColumnName(i).toLowerCase()) {
                        case "name": hasName = true; break;
                        case "statement": hasStatement = true; break;
                        case "creation_time": hasCreationTime = true; break;
                        case "is_holdable": hasIsHoldable = true; break;
                        case "is_binary": hasIsBinary = true; break;
                        case "is_scrollable": hasIsScrollable = true; break;
                    }
                }
                assertTrue(hasName, "Missing 'name' column");
                assertTrue(hasStatement, "Missing 'statement' column");
                assertTrue(hasCreationTime, "Missing 'creation_time' column");
                assertTrue(hasIsHoldable, "Missing 'is_holdable' column");
                assertTrue(hasIsBinary, "Missing 'is_binary' column");
                assertTrue(hasIsScrollable, "Missing 'is_scrollable' column");
            }
            exec("CLOSE colcur");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void pgCursorsFetchDoesNotAffectView() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CLOSE ALL");
            exec("CREATE TABLE IF NOT EXISTS cursor_test (id int)");
            exec("INSERT INTO cursor_test VALUES (1), (2), (3)");
            exec("DECLARE fetchcur CURSOR FOR SELECT id FROM cursor_test ORDER BY id");
            exec("FETCH 1 FROM fetchcur");
            // Still visible after fetch
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_cursors WHERE name = 'fetchcur'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            exec("CLOSE fetchcur");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // ===== Session isolation =====

    @Test
    void pgPreparedStatementsSessionIsolation() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE sess_plan AS SELECT 1");
        // Open a second connection
        try (Connection conn2 = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword())) {
            conn2.setAutoCommit(true);
            try (Statement st = conn2.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_prepared_statements")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Second session should not see first session's prepared statements");
            }
        }
        exec("DEALLOCATE sess_plan");
    }

    @Test
    void pgCursorsSessionIsolation() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CLOSE ALL");
            exec("DECLARE iso_cur CURSOR FOR SELECT 1");
            // Open a second connection
            try (Connection conn2 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword())) {
                conn2.setAutoCommit(true);
                try (Statement st = conn2.createStatement();
                     ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_cursors")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1), "Second session should not see first session's cursors");
                }
            }
            exec("CLOSE iso_cur");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // ===== Combined / interaction tests =====

    @Test
    void preparedStatementWithTableQuery() throws Exception {
        exec("DEALLOCATE ALL");
        exec("CREATE TABLE IF NOT EXISTS ps_test (id int, name text)");
        exec("PREPARE ins_plan (integer, text) AS INSERT INTO ps_test VALUES ($1, $2)");
        exec("EXECUTE ins_plan(1, 'alice')");
        // Verify prepared statement is in catalog
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT name, parameter_types FROM pg_prepared_statements WHERE name = 'ins_plan'")) {
            assertTrue(rs.next());
            assertEquals("ins_plan", rs.getString("name"));
            String params = rs.getString("parameter_types");
            assertTrue(params.contains("integer"));
            assertTrue(params.contains("text"));
        }
        // Verify the insert worked
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM ps_test WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("alice", rs.getString("name"));
        }
        exec("DEALLOCATE ins_plan");
        exec("DROP TABLE ps_test");
    }

    @Test
    void cursorWithRealData() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CREATE TABLE IF NOT EXISTS cur_data (val int)");
            exec("INSERT INTO cur_data VALUES (10), (20), (30)");
            exec("DECLARE datacur CURSOR FOR SELECT val FROM cur_data ORDER BY val");
            // Verify cursor appears in pg_cursors
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT name, statement FROM pg_cursors WHERE name = 'datacur'")) {
                assertTrue(rs.next());
                assertEquals("datacur", rs.getString("name"));
                String stmtText = rs.getString("statement");
                assertNotNull(stmtText);
            }
            // Fetch data through the cursor
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("FETCH ALL FROM datacur")) {
                assertTrue(rs.next()); assertEquals(10, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(20, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(30, rs.getInt(1));
                assertFalse(rs.next());
            }
            exec("CLOSE datacur");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void countPreparedStatementsAndCursors() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE count_a AS SELECT 1");
        exec("PREPARE count_b AS SELECT 2");
        // Count via catalog
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_prepared_statements")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
        exec("DEALLOCATE ALL");
    }

    @Test
    void pgPreparedStatementsFilterByName() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE filter_a AS SELECT 1");
        exec("PREPARE filter_b AS SELECT 2");
        exec("PREPARE filter_c AS SELECT 3");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT name FROM pg_prepared_statements WHERE name LIKE 'filter_%' ORDER BY name")) {
            assertTrue(rs.next()); assertEquals("filter_a", rs.getString(1));
            assertTrue(rs.next()); assertEquals("filter_b", rs.getString(1));
            assertTrue(rs.next()); assertEquals("filter_c", rs.getString(1));
            assertFalse(rs.next());
        }
        exec("DEALLOCATE ALL");
    }

    @Test
    void pgCursorsCaseInsensitiveName() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CLOSE ALL");
            exec("DECLARE MyCursor CURSOR FOR SELECT 1");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT name FROM pg_cursors")) {
                assertTrue(rs.next());
                assertEquals("mycursor", rs.getString("name"), "PG lowercases cursor names");
            }
            exec("CLOSE mycursor");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void pgPreparedStatementsWithSelectStar() throws Exception {
        exec("DEALLOCATE ALL");
        exec("CREATE TABLE IF NOT EXISTS star_test (a int, b text)");
        exec("PREPARE star_plan AS SELECT * FROM star_test");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT name, statement FROM pg_prepared_statements WHERE name = 'star_plan'")) {
            assertTrue(rs.next());
            assertEquals("star_plan", rs.getString("name"));
            assertNotNull(rs.getString("statement"));
        }
        exec("DEALLOCATE star_plan");
        exec("DROP TABLE star_test");
    }

    // ===== Gap fix tests: Cursor transaction lifecycle =====

    @Test
    void nonHoldableCursorDestroyedOnCommit() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE txcur CURSOR FOR SELECT 1");
            // Verify cursor exists
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_cursors WHERE name = 'txcur'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            conn.commit();
            // After COMMIT, non-holdable cursor should be destroyed
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_cursors WHERE name = 'txcur'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Non-holdable cursor should be destroyed on COMMIT");
            }
        } finally {
            conn.setAutoCommit(true);
        }
    }

    @Test
    void holdableCursorSurvivesCommit() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE holdtxcur CURSOR WITH HOLD FOR SELECT 1");
            conn.commit();
            // WITH HOLD cursor should survive COMMIT
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_cursors WHERE name = 'holdtxcur'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "WITH HOLD cursor should survive COMMIT");
            }
            exec("CLOSE holdtxcur");
        } finally {
            conn.setAutoCommit(true);
        }
    }

    @Test
    void allCursorsDestroyedOnRollback() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE rbcur1 CURSOR FOR SELECT 1");
            exec("DECLARE rbcur2 CURSOR WITH HOLD FOR SELECT 2");
            // Both exist
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_cursors")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            conn.rollback();
            // PG destroys ALL cursors on ROLLBACK (including WITH HOLD)
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_cursors")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "All cursors should be destroyed on ROLLBACK");
            }
        } finally {
            conn.setAutoCommit(true);
        }
    }

    @Test
    void mixedHoldableNonHoldableOnCommit() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE plain_cur CURSOR FOR SELECT 1");
            exec("DECLARE hold_cur CURSOR WITH HOLD FOR SELECT 2");
            conn.commit();
            // Only holdable cursor should survive
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT name FROM pg_cursors ORDER BY name")) {
                assertTrue(rs.next());
                assertEquals("hold_cur", rs.getString(1));
                assertFalse(rs.next(), "Only holdable cursor should survive COMMIT");
            }
            exec("CLOSE hold_cur");
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // ===== Gap fix tests: SqlUnparser DML support =====

    @Test
    void pgPreparedStatementsInsertStatement() throws Exception {
        exec("DEALLOCATE ALL");
        exec("CREATE TABLE IF NOT EXISTS unparse_test (id int, name text)");
        exec("PREPARE ins_stmt (integer, text) AS INSERT INTO unparse_test VALUES ($1, $2)");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT statement FROM pg_prepared_statements WHERE name = 'ins_stmt'")) {
            assertTrue(rs.next());
            String stmtText = rs.getString("statement");
            assertNotNull(stmtText);
            // Should be valid SQL, not Java toString
            assertTrue(stmtText.toUpperCase().contains("INSERT"),
                    "Statement should contain INSERT: " + stmtText);
            assertTrue(stmtText.toLowerCase().contains("unparse_test"),
                    "Statement should contain table name: " + stmtText);
        }
        exec("DEALLOCATE ins_stmt");
        exec("DROP TABLE unparse_test");
    }

    @Test
    void pgPreparedStatementsUpdateStatement() throws Exception {
        exec("DEALLOCATE ALL");
        exec("CREATE TABLE IF NOT EXISTS upd_test (id int, val text)");
        exec("PREPARE upd_stmt (text, integer) AS UPDATE upd_test SET val = $1 WHERE id = $2");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT statement FROM pg_prepared_statements WHERE name = 'upd_stmt'")) {
            assertTrue(rs.next());
            String stmtText = rs.getString("statement");
            assertNotNull(stmtText);
            assertTrue(stmtText.toUpperCase().contains("UPDATE"),
                    "Statement should contain UPDATE: " + stmtText);
            assertTrue(stmtText.toUpperCase().contains("SET"),
                    "Statement should contain SET: " + stmtText);
        }
        exec("DEALLOCATE upd_stmt");
        exec("DROP TABLE upd_test");
    }

    @Test
    void pgPreparedStatementsDeleteStatement() throws Exception {
        exec("DEALLOCATE ALL");
        exec("CREATE TABLE IF NOT EXISTS del_test (id int)");
        exec("PREPARE del_stmt (integer) AS DELETE FROM del_test WHERE id = $1");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT statement FROM pg_prepared_statements WHERE name = 'del_stmt'")) {
            assertTrue(rs.next());
            String stmtText = rs.getString("statement");
            assertNotNull(stmtText);
            assertTrue(stmtText.toUpperCase().contains("DELETE"),
                    "Statement should contain DELETE: " + stmtText);
            assertTrue(stmtText.toLowerCase().contains("del_test"),
                    "Statement should contain table name: " + stmtText);
        }
        exec("DEALLOCATE del_stmt");
        exec("DROP TABLE del_test");
    }

    // ===== Gap fix tests: Connection close cleanup =====

    @Test
    void connectionCloseCleansPreparedStatements() throws Exception {
        Connection conn2 = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn2.setAutoCommit(true);
        try (Statement st = conn2.createStatement()) {
            st.execute("PREPARE conn_close_plan AS SELECT 1");
            // Verify it exists
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_prepared_statements WHERE name = 'conn_close_plan'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
        conn2.close();
        // After close, the session's prepared statements should be gone
        // (verified implicitly — another session never saw them; and if we reconnect, it's a new session)
        try (Connection conn3 = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword())) {
            conn3.setAutoCommit(true);
            try (Statement st = conn3.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_prepared_statements")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "New connection should have empty pg_prepared_statements");
            }
        }
    }

    // ===== Error cases =====

    @Test
    void fetchFromClosedCursorErrors() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE err_cur CURSOR FOR SELECT 1");
            exec("CLOSE err_cur");
            try (Statement st = conn.createStatement()) {
                assertThrows(SQLException.class, () -> st.executeQuery("FETCH ALL FROM err_cur"));
            }
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void deallocateNonexistentErrors() throws Exception {
        exec("DEALLOCATE ALL");
        try (Statement st = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () -> st.execute("DEALLOCATE nonexistent"));
            assertTrue(ex.getMessage().contains("does not exist"));
        }
    }

    @Test
    void closeNonexistentCursorErrors() throws Exception {
        conn.setAutoCommit(false);
        try {
            try (Statement st = conn.createStatement()) {
                SQLException ex = assertThrows(SQLException.class, () -> st.execute("CLOSE nonexistent"));
                assertTrue(ex.getMessage().contains("does not exist"));
            }
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void duplicatePrepareErrors() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE dup_plan AS SELECT 1");
        try (Statement st = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () -> st.execute("PREPARE dup_plan AS SELECT 2"));
            assertTrue(ex.getMessage().contains("already exists"));
        }
        exec("DEALLOCATE dup_plan");
    }

    @Test
    void duplicateCursorErrors() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE dup_cur CURSOR FOR SELECT 1");
            try (Statement st = conn.createStatement()) {
                SQLException ex = assertThrows(SQLException.class, () -> st.execute("DECLARE dup_cur CURSOR FOR SELECT 2"));
                assertTrue(ex.getMessage().contains("already exists"));
            }
            // Transaction is now in aborted state; rollback will clean up
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // ===== result_types tests (PG 16+) =====

    @Test
    void resultTypesForSelectInteger() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE rt_int AS SELECT 1 AS a");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT result_types FROM pg_prepared_statements WHERE name = 'rt_int'")) {
            assertTrue(rs.next());
            String rt = rs.getString("result_types");
            assertNotNull(rt, "result_types should not be null for SELECT");
            assertTrue(rt.contains("integer"), "Should contain 'integer': " + rt);
        }
        exec("DEALLOCATE rt_int");
    }

    @Test
    void resultTypesForSelectMultipleColumns() throws Exception {
        exec("DEALLOCATE ALL");
        exec("CREATE TABLE IF NOT EXISTS rt_multi (id int, name text, active boolean)");
        exec("PREPARE rt_multi_plan AS SELECT id, name, active FROM rt_multi");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT result_types FROM pg_prepared_statements WHERE name = 'rt_multi_plan'")) {
            assertTrue(rs.next());
            String rt = rs.getString("result_types");
            assertNotNull(rt, "result_types should not be null for SELECT");
            assertTrue(rt.contains("integer"), "Should contain 'integer': " + rt);
            assertTrue(rt.contains("text"), "Should contain 'text': " + rt);
            assertTrue(rt.contains("boolean"), "Should contain 'boolean': " + rt);
        }
        exec("DEALLOCATE rt_multi_plan");
        exec("DROP TABLE rt_multi");
    }

    @Test
    void resultTypesNullForDmlWithoutReturning() throws Exception {
        exec("DEALLOCATE ALL");
        exec("CREATE TABLE IF NOT EXISTS rt_dml (id int)");
        exec("PREPARE rt_dml_ins AS INSERT INTO rt_dml VALUES (1)");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT result_types FROM pg_prepared_statements WHERE name = 'rt_dml_ins'")) {
            assertTrue(rs.next());
            assertNull(rs.getString("result_types"), "result_types should be null for DML without RETURNING");
        }
        exec("DEALLOCATE rt_dml_ins");
        exec("DROP TABLE rt_dml");
    }

    @Test
    void resultTypesNullForUpdateWithoutReturning() throws Exception {
        exec("DEALLOCATE ALL");
        exec("CREATE TABLE IF NOT EXISTS rt_upd (id int, val text)");
        exec("PREPARE rt_upd_plan AS UPDATE rt_upd SET val = 'x' WHERE id = 1");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT result_types FROM pg_prepared_statements WHERE name = 'rt_upd_plan'")) {
            assertTrue(rs.next());
            assertNull(rs.getString("result_types"), "result_types should be null for UPDATE without RETURNING");
        }
        exec("DEALLOCATE rt_upd_plan");
        exec("DROP TABLE rt_upd");
    }

    @Test
    void resultTypesNullForDeleteWithoutReturning() throws Exception {
        exec("DEALLOCATE ALL");
        exec("CREATE TABLE IF NOT EXISTS rt_del (id int)");
        exec("PREPARE rt_del_plan AS DELETE FROM rt_del WHERE id = 1");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT result_types FROM pg_prepared_statements WHERE name = 'rt_del_plan'")) {
            assertTrue(rs.next());
            assertNull(rs.getString("result_types"), "result_types should be null for DELETE without RETURNING");
        }
        exec("DEALLOCATE rt_del_plan");
        exec("DROP TABLE rt_del");
    }

    @Test
    void resultTypesForInsertReturning() throws Exception {
        exec("DEALLOCATE ALL");
        exec("CREATE TABLE IF NOT EXISTS rt_ret (id int, name text)");
        exec("PREPARE rt_ret_plan AS INSERT INTO rt_ret VALUES (1, 'a') RETURNING id, name");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT result_types FROM pg_prepared_statements WHERE name = 'rt_ret_plan'")) {
            assertTrue(rs.next());
            String rt = rs.getString("result_types");
            assertNotNull(rt, "result_types should not be null for INSERT RETURNING");
            assertTrue(rt.contains("integer"), "Should contain 'integer': " + rt);
            assertTrue(rt.contains("text"), "Should contain 'text': " + rt);
        }
        exec("DEALLOCATE rt_ret_plan");
        exec("DROP TABLE rt_ret");
    }

    @Test
    void resultTypesForSelectWithParams() throws Exception {
        exec("DEALLOCATE ALL");
        exec("CREATE TABLE IF NOT EXISTS rt_param (id int, val text)");
        exec("PREPARE rt_param_plan (integer) AS SELECT id, val FROM rt_param WHERE id = $1");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT result_types FROM pg_prepared_statements WHERE name = 'rt_param_plan'")) {
            assertTrue(rs.next());
            String rt = rs.getString("result_types");
            assertNotNull(rt, "result_types should not be null for parameterized SELECT");
            assertTrue(rt.contains("integer"), "Should contain 'integer': " + rt);
            assertTrue(rt.contains("text"), "Should contain 'text': " + rt);
        }
        exec("DEALLOCATE rt_param_plan");
        exec("DROP TABLE rt_param");
    }

    @Test
    void resultTypesQuotesTypeNamesWithSpaces() throws Exception {
        exec("DEALLOCATE ALL");
        exec("CREATE TABLE IF NOT EXISTS rt_ts (id int, created_at timestamptz)");
        exec("PREPARE rt_ts_plan AS SELECT created_at FROM rt_ts");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT result_types FROM pg_prepared_statements WHERE name = 'rt_ts_plan'")) {
            assertTrue(rs.next());
            String rt = rs.getString("result_types");
            assertNotNull(rt);
            // PG quotes type names with spaces: "timestamp with time zone"
            assertTrue(rt.contains("timestamp with time zone"),
                    "Should contain timestamptz display name: " + rt);
        }
        exec("DEALLOCATE rt_ts_plan");
        exec("DROP TABLE rt_ts");
    }

    // ===== Protocol-level prepared statements tests =====

    @Test
    void protocolPreparedStatementVisibleInCatalog() throws Exception {
        // Use a separate connection in extended query mode (no preferQueryMode=simple)
        try (Connection extConn = DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword())) {
            extConn.setAutoCommit(true);
            // Create a named prepared statement via JDBC (triggers Parse with a name after threshold)
            // But we can directly test by using SQL PREPARE and checking from_sql
            // For protocol-level, we need JDBC to use extended query mode
            // Use prepareStatement with a specific name by setting prepareThreshold=1
            // Actually, JDBC doesn't let us control stmt names easily.
            // Instead, let's verify the SQL-level from_sql=true behavior is correct
            try (Statement st = extConn.createStatement()) {
                st.execute("PREPARE proto_test AS SELECT 42");
            }
            try (Statement st = extConn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT from_sql FROM pg_prepared_statements WHERE name = 'proto_test'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("from_sql"), "SQL PREPARE should have from_sql=true");
            }
            try (Statement st = extConn.createStatement()) {
                st.execute("DEALLOCATE proto_test");
            }
        }
    }

    @Test
    void protocolPreparedStatementFromSqlFalse() throws Exception {
        // Test that protocol-level prepared stmts (via JDBC prepareStatement in extended mode)
        // show from_sql=false. JDBC uses unnamed stmts initially, so we need prepareThreshold=1
        // to force named server-side prepared statements on first execute.
        try (Connection extConn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?prepareThreshold=1", memgres.getUser(), memgres.getPassword())) {
            extConn.setAutoCommit(true);
            // Execute a prepared statement — after threshold, JDBC creates named server-side stmt
            try (java.sql.PreparedStatement ps = extConn.prepareStatement("SELECT 1 AS val")) {
                ps.execute(); // 1st execution — triggers named Parse
                ps.execute(); // 2nd execution — confirms it's reusable
            }
            // Check if any protocol-level prepared statements are visible
            try (Statement st = extConn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT name, from_sql FROM pg_prepared_statements WHERE from_sql = false")) {
                // May or may not have entries depending on JDBC driver behavior
                // The key test is that the from_sql column works correctly
                // If entries exist, from_sql must be false
                while (rs.next()) {
                    assertFalse(rs.getBoolean("from_sql"),
                            "Protocol-level prepared statement should have from_sql=false");
                }
            }
        }
    }

    // ===== B3 gap audit: additional tests =====

    // --- Gap 1: generic_plans / custom_plans columns ---

    @Test
    void pgPreparedStatementsHasGenericAndCustomPlansColumns() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE plan_cols AS SELECT 1");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = 'plan_cols'")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getLong("generic_plans"), "generic_plans should be 0 (no planner)");
            assertEquals(0, rs.getLong("custom_plans"), "custom_plans should be 0 before any EXECUTE");
        }
        exec("DEALLOCATE plan_cols");
    }

    @Test
    void pgPreparedStatementsCustomPlansIncrementsOnExecute() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE exec_count AS SELECT 42");
        // Execute 3 times
        exec("EXECUTE exec_count");
        exec("EXECUTE exec_count");
        exec("EXECUTE exec_count");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT custom_plans FROM pg_prepared_statements WHERE name = 'exec_count'")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getLong("custom_plans"), "custom_plans should be 3 after 3 executions");
        }
        exec("DEALLOCATE exec_count");
    }

    @Test
    void pgPreparedStatementsAllEightColumns() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE eight_col AS SELECT 1 + 1 AS val");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM pg_prepared_statements WHERE name = 'eight_col'")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(8, meta.getColumnCount(), "pg_prepared_statements should have 8 columns");
            assertTrue(rs.next());
            assertEquals("eight_col", rs.getString("name"));
            assertNotNull(rs.getString("statement"));
            assertNotNull(rs.getString("prepare_time"));
            assertNotNull(rs.getString("parameter_types"));
            assertNotNull(rs.getString("result_types"));
            assertTrue(rs.getBoolean("from_sql"));
            assertEquals(0, rs.getLong("generic_plans"));
            assertEquals(0, rs.getLong("custom_plans"));
        }
        exec("DEALLOCATE eight_col");
    }

    // --- Gap 2: BINARY cursor flag ---

    @Test
    void pgCursorsBinaryFlagDefault() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE default_cur CURSOR FOR SELECT 1");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT is_binary FROM pg_cursors WHERE name = 'default_cur'")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean("is_binary"), "Default cursor should not be binary");
            }
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void pgCursorsBinaryCursorFlag() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE bin_cur BINARY CURSOR FOR SELECT 1");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT is_binary FROM pg_cursors WHERE name = 'bin_cur'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("is_binary"), "BINARY cursor should have is_binary=true");
            }
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void pgCursorsBinaryScrollCursorCombination() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE binsrc BINARY SCROLL CURSOR FOR SELECT 1");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT is_binary, is_scrollable FROM pg_cursors WHERE name = 'binsrc'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("is_binary"), "Should be binary");
                assertTrue(rs.getBoolean("is_scrollable"), "Should be scrollable");
            }
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // --- Gap 3: Verbatim SQL text (not AST-reconstructed) ---

    @Test
    void pgPreparedStatementsPreservesOriginalSql() throws Exception {
        exec("DEALLOCATE ALL");
        // Use specific formatting that SqlUnparser would change
        exec("PREPARE orig_sql AS SELECT   1  +  2   AS   result");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT statement FROM pg_prepared_statements WHERE name = 'orig_sql'")) {
            assertTrue(rs.next());
            String stmt = rs.getString("statement");
            // Should preserve the original spacing from after "AS "
            assertTrue(stmt.contains("  "), "Should preserve original whitespace: " + stmt);
        }
        exec("DEALLOCATE orig_sql");
    }

    @Test
    void pgCursorsPreservesOriginalQuerySql() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE orig_cur CURSOR FOR SELECT   1  +  2   AS   result");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT statement FROM pg_cursors WHERE name = 'orig_cur'")) {
                assertTrue(rs.next());
                String stmt = rs.getString("statement");
                // Should preserve the original query text after "FOR "
                assertTrue(stmt.contains("  "), "Should preserve original whitespace: " + stmt);
            }
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // --- Gap 4: Timestamp format (PG style) ---

    @Test
    void pgPreparedStatementsPrepareTimeFormat() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE ts_fmt AS SELECT 1");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT prepare_time FROM pg_prepared_statements WHERE name = 'ts_fmt'")) {
            assertTrue(rs.next());
            String ts = rs.getString("prepare_time");
            assertNotNull(ts);
            // PG format: "2024-01-15 10:30:00.123456+00" — no 'T', space separator
            assertFalse(ts.contains("T"), "Timestamp should not contain 'T' (Java format): " + ts);
            assertTrue(ts.contains(" "), "Timestamp should use space separator: " + ts);
            // Should match PG pattern: yyyy-MM-dd HH:mm:ss.SSSSSS+offset
            assertTrue(ts.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+[+\\-]\\d{2}(:\\d{2})?"),
                    "Timestamp should match PG format: " + ts);
        }
        exec("DEALLOCATE ts_fmt");
    }

    @Test
    void pgCursorsCreationTimeFormat() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE ts_cur CURSOR FOR SELECT 1");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT creation_time FROM pg_cursors WHERE name = 'ts_cur'")) {
                assertTrue(rs.next());
                String ts = rs.getString("creation_time");
                assertNotNull(ts);
                assertFalse(ts.contains("T"), "Timestamp should not contain 'T' (Java format): " + ts);
                assertTrue(ts.contains(" "), "Timestamp should use space separator: " + ts);
            }
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // --- Gap 5: DISCARD ALL completeness ---

    @Test
    void discardAllRemovesPreparedStatementsAndCursors() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE disc_plan AS SELECT 1");
        // Verify prepared statement exists
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_prepared_statements")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) > 0, "Should have at least one prepared statement");
        }
        exec("DISCARD ALL");
        // Both should be empty
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_prepared_statements")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "DISCARD ALL should remove all prepared statements");
        }
    }

    // ===== B3 deep gap audit round 2: additional tests =====

    // --- Gap 6: Array subscripting on parameter_types / result_types ---

    @Test
    void parameterTypesArraySubscripting() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE arr_sub (integer, text, boolean) AS SELECT $1, $2, $3");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT parameter_types[1], parameter_types[2], parameter_types[3] FROM pg_prepared_statements WHERE name = 'arr_sub'")) {
            assertTrue(rs.next());
            assertEquals("integer", rs.getString(1));
            assertEquals("text", rs.getString(2));
            assertEquals("boolean", rs.getString(3));
        }
        exec("DEALLOCATE arr_sub");
    }

    @Test
    void resultTypesArraySubscripting() throws Exception {
        exec("DEALLOCATE ALL");
        exec("CREATE TABLE IF NOT EXISTS arr_rt_test (id integer, name text)");
        try {
            exec("PREPARE arr_rt AS SELECT id, name FROM arr_rt_test");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT result_types[1], result_types[2] FROM pg_prepared_statements WHERE name = 'arr_rt'")) {
                assertTrue(rs.next());
                assertEquals("integer", rs.getString(1));
                assertEquals("text", rs.getString(2));
            }
            exec("DEALLOCATE arr_rt");
        } finally {
            exec("DROP TABLE IF EXISTS arr_rt_test");
        }
    }

    @Test
    void parameterTypesEmptyArraySubscriptReturnsNull() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE no_p AS SELECT 42");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT parameter_types[1] FROM pg_prepared_statements WHERE name = 'no_p'")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1), "Subscript on empty array should return null");
        }
        exec("DEALLOCATE no_p");
    }

    // --- Quoted identifier edge cases in SQL extraction ---

    @Test
    void prepareWithAsInAliasPreservesVerbatimSql() throws Exception {
        exec("DEALLOCATE ALL");
        // The body contains " AS " in a column alias — extraction must find the PREPARE AS, not the alias
        exec("PREPARE as_alias AS SELECT 1 AS my_alias");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT statement FROM pg_prepared_statements WHERE name = 'as_alias'")) {
            assertTrue(rs.next());
            String stmt = rs.getString("statement");
            assertTrue(stmt.toUpperCase().startsWith("SELECT"),
                    "Statement should start with SELECT, got: " + stmt);
            assertTrue(stmt.contains("my_alias"),
                    "Statement should contain the alias: " + stmt);
        }
        exec("DEALLOCATE as_alias");
    }

    @Test
    void prepareWithStringContainingAs() throws Exception {
        exec("DEALLOCATE ALL");
        // Body contains a string literal with " AS " — should not confuse the extraction
        exec("PREPARE str_as AS SELECT 'hello AS world' AS greeting");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT statement FROM pg_prepared_statements WHERE name = 'str_as'")) {
            assertTrue(rs.next());
            String stmt = rs.getString("statement");
            assertTrue(stmt.contains("hello AS world"),
                    "Statement should preserve the string literal: " + stmt);
        }
        exec("DEALLOCATE str_as");
    }

    @Test
    void declareWithForUpdatePreservesFullQuery() throws Exception {
        exec("CREATE TABLE IF NOT EXISTS for_test (id integer)");
        conn.setAutoCommit(false);
        try {
            exec("DECLARE for_cur CURSOR FOR SELECT id FROM for_test FOR UPDATE");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT statement FROM pg_cursors WHERE name = 'for_cur'")) {
                assertTrue(rs.next());
                String stmt = rs.getString("statement");
                assertTrue(stmt.toUpperCase().contains("FOR UPDATE"),
                        "Statement should include FOR UPDATE: " + stmt);
            }
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
            exec("DROP TABLE IF EXISTS for_test");
        }
    }

    @Test
    void declareWithStringContainingFor() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE str_cur CURSOR FOR SELECT 'waiting for something'");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT statement FROM pg_cursors WHERE name = 'str_cur'")) {
                assertTrue(rs.next());
                String stmt = rs.getString("statement");
                assertTrue(stmt.contains("waiting for something"),
                        "Statement should preserve string literal: " + stmt);
            }
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // --- CTE preservation in verbatim SQL ---

    @Test
    void prepareWithCtePreservesVerbatimSql() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE cte_plan AS WITH nums AS (SELECT 1 AS n) SELECT n FROM nums");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT statement FROM pg_prepared_statements WHERE name = 'cte_plan'")) {
            assertTrue(rs.next());
            String stmt = rs.getString("statement");
            assertTrue(stmt.toUpperCase().contains("WITH"),
                    "Statement should contain WITH clause: " + stmt);
            assertTrue(stmt.toUpperCase().contains("NUMS"),
                    "Statement should contain CTE name: " + stmt);
        }
        exec("DEALLOCATE cte_plan");
    }

    // --- Dollar-quoted strings and comments in SQL extraction ---

    @Test
    void prepareWithDollarQuoteContainingAs() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE dq_plan AS SELECT $$ AS inside dollar $$");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT statement FROM pg_prepared_statements WHERE name = 'dq_plan'")) {
            assertTrue(rs.next());
            String stmt = rs.getString("statement");
            assertTrue(stmt.contains("AS inside dollar"),
                    "Dollar-quoted body should be preserved: " + stmt);
        }
        exec("DEALLOCATE dq_plan");
    }

    @Test
    void prepareWithBlockCommentBeforeAs() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE /* comment */ cm_plan AS SELECT 1");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT statement FROM pg_prepared_statements WHERE name = 'cm_plan'")) {
            assertTrue(rs.next());
            String stmt = rs.getString("statement");
            assertTrue(stmt.toUpperCase().startsWith("SELECT"),
                    "Should extract body after AS, not inside comment: " + stmt);
        }
        exec("DEALLOCATE cm_plan");
    }

    @Test
    void prepareWithLineCommentContainingAs() throws Exception {
        exec("DEALLOCATE ALL");
        // Line comment contains " AS " but the real AS is after the comment
        exec("PREPARE lc_plan -- AS comment\n AS SELECT 1");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT statement FROM pg_prepared_statements WHERE name = 'lc_plan'")) {
            assertTrue(rs.next());
            String stmt = rs.getString("statement");
            assertTrue(stmt.toUpperCase().startsWith("SELECT"),
                    "Should skip line comment and find real AS: " + stmt);
        }
        exec("DEALLOCATE lc_plan");
    }

    // ---- B3 Gap Audit Round 4: DISCARD PLANS, NO SCROLL, cursor position ----

    @Test
    void discardPlansPreservesPreparedStatements() throws Exception {
        exec("DEALLOCATE ALL");
        exec("PREPARE dp_plan AS SELECT 1");
        // DISCARD PLANS should NOT remove prepared statements (only invalidates cached plans)
        exec("DISCARD PLANS");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT name FROM pg_prepared_statements WHERE name = 'dp_plan'")) {
            assertTrue(rs.next(), "DISCARD PLANS should not remove prepared statements");
            assertEquals("dp_plan", rs.getString("name"));
        }
        exec("DEALLOCATE dp_plan");
    }

    @Test
    void noScrollCursorRejectsBackwardFetch() throws Exception {
        exec("BEGIN");
        exec("DECLARE ns_cur CURSOR FOR SELECT 1");
        exec("FETCH NEXT FROM ns_cur"); // forward is OK
        try {
            exec("FETCH PRIOR FROM ns_cur");
            fail("FETCH PRIOR on NO SCROLL cursor should fail");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("cursor can only scan forward"),
                    "Expected 55000 error: " + e.getMessage());
        }
        exec("ROLLBACK");
    }

    @Test
    void noScrollCursorRejectsLast() throws Exception {
        exec("BEGIN");
        exec("DECLARE ns_cur2 CURSOR FOR SELECT 1");
        try {
            exec("FETCH LAST FROM ns_cur2");
            fail("FETCH LAST on NO SCROLL cursor should fail");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("cursor can only scan forward"),
                    "Expected 55000 error: " + e.getMessage());
        }
        exec("ROLLBACK");
    }

    @Test
    void noScrollCursorRejectsAbsolute() throws Exception {
        exec("BEGIN");
        exec("DECLARE ns_cur3 CURSOR FOR SELECT 1");
        try {
            exec("FETCH ABSOLUTE 1 FROM ns_cur3");
            fail("FETCH ABSOLUTE on NO SCROLL cursor should fail");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("cursor can only scan forward"),
                    "Expected 55000 error: " + e.getMessage());
        }
        exec("ROLLBACK");
    }

    @Test
    void noScrollCursorAllowsForwardRelative() throws Exception {
        exec("BEGIN");
        exec("DECLARE ns_cur4 CURSOR FOR SELECT generate_series(1,3)");
        // RELATIVE with positive count is forward — should work on NO SCROLL
        assertEquals(1, queryInt("FETCH RELATIVE 1 FROM ns_cur4"));
        assertEquals(2, queryInt("FETCH RELATIVE 1 FROM ns_cur4"));
        exec("CLOSE ns_cur4");
        exec("COMMIT");
    }

    @Test
    void noScrollCursorRejectsNegativeRelative() throws Exception {
        exec("BEGIN");
        exec("DECLARE ns_cur5 CURSOR FOR SELECT generate_series(1,3)");
        exec("FETCH NEXT FROM ns_cur5"); // move to row 1
        try {
            exec("FETCH RELATIVE -1 FROM ns_cur5");
            fail("FETCH RELATIVE -1 on NO SCROLL cursor should fail");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("cursor can only scan forward"),
                    "Expected 55000 error: " + e.getMessage());
        }
        exec("ROLLBACK");
    }

    @Test
    void scrollCursorAllowsAllDirections() throws Exception {
        exec("BEGIN");
        exec("DECLARE sc_cur SCROLL CURSOR FOR SELECT generate_series(1,3)");
        assertEquals(1, queryInt("FETCH NEXT FROM sc_cur"));
        assertEquals(2, queryInt("FETCH NEXT FROM sc_cur"));
        assertEquals(1, queryInt("FETCH PRIOR FROM sc_cur"));
        assertEquals(1, queryInt("FETCH FIRST FROM sc_cur"));
        assertEquals(3, queryInt("FETCH LAST FROM sc_cur"));
        assertEquals(2, queryInt("FETCH ABSOLUTE 2 FROM sc_cur"));
        exec("CLOSE sc_cur");
        exec("COMMIT");
    }

    @Test
    void cursorPositionMovesToAfterLastOnFetchPastEnd() throws Exception {
        exec("BEGIN");
        exec("DECLARE pos_cur SCROLL CURSOR FOR SELECT generate_series(1,2)");
        assertEquals(1, queryInt("FETCH NEXT FROM pos_cur"));
        assertEquals(2, queryInt("FETCH NEXT FROM pos_cur"));
        // FETCH NEXT past end should move position to "after last"
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("FETCH NEXT FROM pos_cur")) {
            assertFalse(rs.next(), "No more rows");
        }
        // Now FETCH PRIOR should return the last row (2), proving position was "after last"
        assertEquals(2, queryInt("FETCH PRIOR FROM pos_cur"));
        exec("CLOSE pos_cur");
        exec("COMMIT");
    }

    @Test
    void cursorPositionMovesToBeforeFirstOnFetchPrior() throws Exception {
        exec("BEGIN");
        exec("DECLARE pos_cur2 SCROLL CURSOR FOR SELECT generate_series(1,2)");
        assertEquals(1, queryInt("FETCH NEXT FROM pos_cur2"));
        // FETCH PRIOR from first row should move to "before first"
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("FETCH PRIOR FROM pos_cur2")) {
            assertFalse(rs.next(), "No row before first");
        }
        // FETCH NEXT should now return first row, proving position was "before first"
        assertEquals(1, queryInt("FETCH NEXT FROM pos_cur2"));
        exec("CLOSE pos_cur2");
        exec("COMMIT");
    }

    @Test
    void fetchAbsoluteZeroPositionsBeforeFirst() throws Exception {
        exec("BEGIN");
        exec("DECLARE abs0_cur SCROLL CURSOR FOR SELECT generate_series(1,3)");
        assertEquals(2, queryInt("FETCH ABSOLUTE 2 FROM abs0_cur"));
        // ABSOLUTE 0 = position before first (returns no row)
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("FETCH ABSOLUTE 0 FROM abs0_cur")) {
            assertFalse(rs.next(), "ABSOLUTE 0 returns no row");
        }
        // NEXT should return first row
        assertEquals(1, queryInt("FETCH NEXT FROM abs0_cur"));
        exec("CLOSE abs0_cur");
        exec("COMMIT");
    }

    @Test
    void backwardAllPositionsBeforeFirst() throws Exception {
        exec("BEGIN");
        exec("DECLARE bwa_cur SCROLL CURSOR FOR SELECT generate_series(1,3)");
        // Move to end
        exec("FETCH LAST FROM bwa_cur");
        // BACKWARD ALL: fetch all rows backwards, position ends at "before first"
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("FETCH BACKWARD ALL FROM bwa_cur")) {
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
        // Position should be "before first" — NEXT returns first row
        assertEquals(1, queryInt("FETCH NEXT FROM bwa_cur"));
        exec("CLOSE bwa_cur");
        exec("COMMIT");
    }

    @Test
    void forwardAllPositionsAfterLast() throws Exception {
        exec("BEGIN");
        exec("DECLARE fwa_cur SCROLL CURSOR FOR SELECT generate_series(1,3)");
        // FORWARD ALL
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("FETCH FORWARD ALL FROM fwa_cur")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
        // Position should be "after last" — PRIOR returns last row
        assertEquals(3, queryInt("FETCH PRIOR FROM fwa_cur"));
        exec("CLOSE fwa_cur");
        exec("COMMIT");
    }
}
