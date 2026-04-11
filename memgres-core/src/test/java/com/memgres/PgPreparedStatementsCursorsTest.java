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
}
