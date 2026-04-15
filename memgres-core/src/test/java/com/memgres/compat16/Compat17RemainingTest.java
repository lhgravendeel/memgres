package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the 19 remaining PG 18 vs Memgres differences.
 * All tests should FAIL initially, then pass after fixes.
 */
class Compat17RemainingTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement stmt = conn.createStatement()) {
            // Graph for cycle detection tests
            stmt.execute("CREATE TABLE rcte17_graph (src integer, dst integer)");
            stmt.execute("INSERT INTO rcte17_graph VALUES (1, 2), (2, 3), (3, 4), (4, 2), (1, 5), (5, 6)");

            // Tree for SEARCH subquery test
            stmt.execute("CREATE TABLE rcte17_tree (child_id integer, parent_id integer, label text)");
            stmt.execute("INSERT INTO rcte17_tree VALUES "
                    + "(1, NULL, 'root'), (2, 1, 'left'), (3, 1, 'right'), "
                    + "(4, 2, 'left-left'), (5, 2, 'left-right'), (6, 3, 'right-left')");

            // FK tables for error response test
            stmt.execute("CREATE SCHEMA IF NOT EXISTS errfield17_test");
            stmt.execute("SET search_path = errfield17_test, public");
            stmt.execute("CREATE TABLE errfield17_test.errfield_parent (id integer PRIMARY KEY)");
            stmt.execute("CREATE TABLE errfield17_test.errfield_child (id integer PRIMARY KEY, parent_id integer REFERENCES errfield17_test.errfield_parent(id))");
            stmt.execute("INSERT INTO errfield17_test.errfield_parent VALUES (1)");
            stmt.execute("INSERT INTO errfield17_test.errfield_child VALUES (1, 1)");

            // errfield_diag function for FK test
            stmt.execute(
                "CREATE FUNCTION errfield17_diag(sql_text text) RETURNS TABLE(\n" +
                "  err_sqlstate text, detail text\n" +
                ") LANGUAGE plpgsql AS $$\n" +
                "DECLARE v_state text; v_detail text;\n" +
                "BEGIN\n" +
                "  EXECUTE sql_text;\n" +
                "  err_sqlstate := 'OK'; detail := '';\n" +
                "  RETURN NEXT;\n" +
                "EXCEPTION WHEN OTHERS THEN\n" +
                "  GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_detail = PG_EXCEPTION_DETAIL;\n" +
                "  err_sqlstate := v_state; detail := v_detail;\n" +
                "  RETURN NEXT;\n" +
                "END; $$");

            // Large object test functions
            stmt.execute(
                "CREATE OR REPLACE FUNCTION lo_fd_test17() RETURNS text AS $$\n" +
                "DECLARE loid oid; fd integer; result bytea;\n" +
                "BEGIN\n" +
                "  loid := lo_from_bytea(0, 'Test data'::bytea);\n" +
                "  fd := lo_open(loid, x'20000'::int);\n" +
                "  result := loread(fd, 9);\n" +
                "  PERFORM lo_close(fd);\n" +
                "  PERFORM lo_unlink(loid);\n" +
                "  RETURN convert_from(result, 'UTF8');\n" +
                "END; $$ LANGUAGE plpgsql");

            stmt.execute(
                "CREATE OR REPLACE FUNCTION lo_seek_test17() RETURNS text AS $$\n" +
                "DECLARE loid oid; fd integer; pos integer; result bytea;\n" +
                "BEGIN\n" +
                "  loid := lo_from_bytea(0, 'ABCDEFGH'::bytea);\n" +
                "  fd := lo_open(loid, x'20000'::int);\n" +
                "  PERFORM lo_lseek(fd, 5, 0);\n" +
                "  pos := lo_tell(fd);\n" +
                "  result := loread(fd, 3);\n" +
                "  PERFORM lo_close(fd);\n" +
                "  PERFORM lo_unlink(loid);\n" +
                "  RETURN pos::text || ':' || convert_from(result, 'UTF8');\n" +
                "END; $$ LANGUAGE plpgsql");

            // Variadic test function
            stmt.execute("CREATE FUNCTION var17_with_prefix(prefix text, VARIADIC args text[]) "
                    + "RETURNS text LANGUAGE plpgsql AS $$ "
                    + "BEGIN RETURN prefix || ':' || array_to_string(args, ','); END; $$");

            // Window test data
            stmt.execute("CREATE TABLE wf17_data (id integer PRIMARY KEY, dept text, salary integer)");
            stmt.execute("INSERT INTO wf17_data VALUES " +
                    "(1, 'eng', 80000), (2, 'eng', 90000), (3, 'eng', 100000), " +
                    "(4, 'sales', 60000), (5, 'sales', 70000), (6, 'sales', 80000), " +
                    "(7, 'hr', 55000), (8, 'hr', 65000)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ============================================================
    // MULTIRANGE || OPERATOR (10 tests) — PG does NOT have || for multiranges
    // ============================================================

    @Test
    @DisplayName("int4multirange || int4multirange should error")
    void testMultirangeConcatInt4() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1,4)}'::int4multirange || '{[6,9)}'::int4multirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("int4multirange || int4multirange overlapping should error")
    void testMultirangeConcatInt4Overlapping() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1,5)}'::int4multirange || '{[3,8)}'::int4multirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("int4multirange || int4multirange adjacent should error")
    void testMultirangeConcatInt4Adjacent() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1,3)}'::int4multirange || '{[3,6)}'::int4multirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("int4multirange || int4range should error")
    void testMultirangeConcatWithRange() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1,4)}'::int4multirange || '[6,9)'::int4range AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("empty int4multirange || int4multirange should error")
    void testMultirangeConcatEmpty() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{}'::int4multirange || '{[1,5)}'::int4multirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("int8multirange || int8multirange should error")
    void testMultirangeConcatInt8() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1,5)}'::int8multirange || '{[3,10)}'::int8multirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("nummultirange || nummultirange should error")
    void testMultirangeConcatNum() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1.5,4.0)}'::nummultirange || '{[3.0,7.5)}'::nummultirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("datemultirange || datemultirange should error")
    void testMultirangeConcatDate() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[2024-01-01,2024-03-01)}'::datemultirange || '{[2024-06-01,2024-09-01)}'::datemultirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("tsmultirange || tsmultirange should error")
    void testMultirangeConcatTs() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[\"2024-01-01\",\"2024-01-02\")}'::tsmultirange || '{[\"2024-01-02\",\"2024-01-03\")}'::tsmultirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("int4multirange || combined with * should error on ||")
    void testMultirangeConcatCombined() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1,5)}'::int4multirange || '{}'::int4multirange AS union_result, "
                        + "'{[1,5)}'::int4multirange * '{}'::int4multirange AS inter_result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    // ============================================================
    // RECURSIVE CTE CYCLE DETECTION (5 tests)
    // ============================================================

    @Test
    @DisplayName("SEARCH clause in subquery context should parse")
    void testSearchInSubquery() throws Exception {
        String sql = "SELECT (SELECT string_agg(id::text, ',' ORDER BY ordcol) FROM ("
                + " WITH RECURSIVE tree(id, label) AS ("
                + "   SELECT child_id, label FROM rcte17_tree WHERE parent_id IS NULL"
                + "   UNION ALL"
                + "   SELECT t.child_id, t.label FROM rcte17_tree t JOIN tree tr ON t.parent_id = tr.id"
                + " ) SEARCH BREADTH FIRST BY id SET ordcol"
                + " SELECT id, ordcol FROM tree"
                + ") bf) AS bf_order";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("bf_order"));
        }
    }

    @Test
    @DisplayName("CYCLE detection should mark cycle rows as true")
    void testCycleDetectionMarksTrue() throws Exception {
        String sql = "WITH RECURSIVE traverse(node) AS ("
                + "  SELECT 1"
                + "  UNION ALL"
                + "  SELECT g.dst FROM rcte17_graph g JOIN traverse t ON g.src = t.node"
                + ") CYCLE node SET is_cycle USING path "
                + "SELECT bool_or(is_cycle) AS has_cycle_rows FROM traverse";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("has_cycle_rows"),
                    "bool_or(is_cycle) should be true — at least one row should be marked as cycle");
        }
    }

    @Test
    @DisplayName("CYCLE detection with custom column names should mark true")
    void testCycleDetectionCustomColumns() throws Exception {
        String sql = "WITH RECURSIVE traverse(node) AS ("
                + "  SELECT 1"
                + "  UNION ALL"
                + "  SELECT g.dst FROM rcte17_graph g JOIN traverse t ON g.src = t.node"
                + ") CYCLE node SET found_loop USING visited_path "
                + "SELECT bool_or(found_loop) AS has_loop FROM traverse";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("has_loop"),
                    "bool_or(found_loop) should be true — cycle exists in graph");
        }
    }

    @Test
    @DisplayName("CYCLE+SEARCH BREADTH should return 7 total rows")
    void testCycleSearchBreadthCount() throws Exception {
        String sql = "WITH RECURSIVE traverse(node) AS ("
                + "  SELECT 1"
                + "  UNION ALL"
                + "  SELECT g.dst FROM rcte17_graph g JOIN traverse t ON g.src = t.node"
                + ") SEARCH BREADTH FIRST BY node SET ordcol "
                + "CYCLE node SET is_cycle USING path "
                + "SELECT count(*)::integer AS cnt FROM traverse";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals(7, rs.getInt("cnt"),
                    "SEARCH BREADTH + CYCLE should produce 7 total rows (6 non-cycle + 1 cycle)");
        }
    }

    @Test
    @DisplayName("CYCLE+SEARCH DEPTH non-cycle count should be 6")
    void testCycleSearchDepthNonCycleCount() throws Exception {
        String sql = "WITH RECURSIVE traverse(node) AS ("
                + "  SELECT 1"
                + "  UNION ALL"
                + "  SELECT g.dst FROM rcte17_graph g JOIN traverse t ON g.src = t.node"
                + ") SEARCH DEPTH FIRST BY node SET ordcol "
                + "CYCLE node SET is_cycle USING path "
                + "SELECT count(*)::integer AS non_cycle_cnt FROM traverse WHERE NOT is_cycle";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals(6, rs.getInt("non_cycle_cnt"),
                    "6 non-cycle rows in depth-first traversal");
        }
    }

    // ============================================================
    // LARGE OBJECT FUNCTIONS (2 tests)
    // ============================================================

    @Test
    @DisplayName("lo_open + loread should return data via fd")
    void testLoFdRead() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT lo_fd_test17() AS result")) {
            assertTrue(rs.next());
            assertEquals("Test data", rs.getString("result"),
                    "loread via fd should return 'Test data'");
        }
    }

    @Test
    @DisplayName("lo_tell should report current position after lo_lseek")
    void testLoTell() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT lo_seek_test17() AS result")) {
            assertTrue(rs.next());
            assertEquals("5:FGH", rs.getString("result"),
                    "lo_tell should return 5 after lseek to 5, loread should return 'FGH'");
        }
    }

    // ============================================================
    // ERROR RESPONSE FIELDS (1 test)
    // ============================================================

    @Test
    @DisplayName("FK violation DELETE should include detail field")
    void testFkDeleteDetail() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                "SELECT err_sqlstate, (detail IS NOT NULL AND detail <> '') AS has_detail "
                + "FROM errfield17_diag('DELETE FROM errfield17_test.errfield_parent WHERE id = 1')")) {
            assertTrue(rs.next());
            assertEquals("23503", rs.getString("err_sqlstate"));
            assertTrue(rs.getBoolean("has_detail"),
                    "FK violation on DELETE should include a detail field");
        }
    }

    // ============================================================
    // JSONPATH QUOTING (1 test)
    // ============================================================

    @Test
    @DisplayName("jsonpath should quote object keys")
    void testJsonpathQuotesKeys() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT '$.store.book[*].author'::jsonpath AS jp")) {
            assertTrue(rs.next());
            assertEquals("$.\"store\".\"book\"[*].\"author\"", rs.getString("jp"),
                    "jsonpath output should quote object keys like PG");
        }
    }

    // ============================================================
    // VARIADIC RESOLUTION (1 test)
    // ============================================================

    @Test
    @DisplayName("single unknown-typed arg should not resolve to VARIADIC function")
    void testVariadicSingleArgReject() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT var17_with_prefix('hello') AS result");
            }
        });
        // PG returns "function var_with_prefix(unknown) does not exist" (42883)
        assertEquals("42883", ex.getSQLState(),
                "Should get 'function does not exist' error, got: " + ex.getMessage());
    }

    // ============================================================
    // WINDOW NAMED CLAUSE ORDERING (1 test)
    // ============================================================

    @Test
    @DisplayName("named WINDOW with deterministic ORDER BY should produce correct ranks")
    void testNamedWindowDeterministicOrdering() throws Exception {
        // Use ORDER BY salary, id to make ordering deterministic
        // This avoids the non-deterministic tiebreaker issue between PG and Memgres
        String sql = "SELECT id, dept, rank() OVER dept_w AS dept_rank, "
                + "rank() OVER global_w AS global_rank "
                + "FROM wf17_data "
                + "WINDOW dept_w AS (PARTITION BY dept ORDER BY salary), "
                + "global_w AS (ORDER BY salary) "
                + "ORDER BY salary, id";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            // Rows 1-4 have salaries 55000, 60000, 65000, 70000 (unique)
            for (int i = 0; i < 4; i++) assertTrue(rs.next());
            // Row 5: id=1 (eng, 80000) — deterministic with ORDER BY salary, id
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("eng", rs.getString("dept"));
            assertEquals(1, rs.getInt("dept_rank"), "id=1 should be rank 1 in eng");
            assertEquals(5, rs.getInt("global_rank"), "salary 80000 should be rank 5 globally");
            // Row 6: id=6 (sales, 80000)
            assertTrue(rs.next());
            assertEquals(6, rs.getInt("id"));
            assertEquals("sales", rs.getString("dept"));
            assertEquals(3, rs.getInt("dept_rank"), "id=6 should be rank 3 in sales");
            assertEquals(5, rs.getInt("global_rank"), "salary 80000 should also be rank 5");
        }
    }
}
