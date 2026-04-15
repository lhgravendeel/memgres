package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the 5 remaining PG 18 vs Memgres differences (round 9).
 * 1. lowrite fd persistence
 * 2-4. CTE CYCLE row emission (count off by 1)
 * 5. VARIADIC with explicit empty array
 */
class Compat18RemainingTest {
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
            // Graph for cycle tests: 1->2->3->1 (cycle), 3->4->5 (branch)
            stmt.execute("CREATE TABLE rcte18_graph (src integer, dst integer)");
            stmt.execute("INSERT INTO rcte18_graph VALUES (1, 2), (2, 3), (3, 1), (3, 4), (4, 5)");

            // VARIADIC function
            stmt.execute("CREATE FUNCTION var18_concat_all(VARIADIC parts text[]) RETURNS text "
                    + "LANGUAGE plpgsql AS $$ DECLARE result text := ''; p text; BEGIN "
                    + "FOREACH p IN ARRAY parts LOOP result := result || p; END LOOP; "
                    + "RETURN result; END; $$");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // 1. lowrite fd persistence — write via fd, close, reopen, read back
    // =========================================================================
    @Test
    @DisplayName("lowrite persists data through close/reopen cycle")
    void testLowritePersistence() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION lo18_fd_test() RETURNS text LANGUAGE plpgsql AS $$ "
                    + "DECLARE loid oid; fd integer; data bytea; BEGIN "
                    + "loid := lo_creat(-1); "
                    + "fd := lo_open(loid, x'20000'::integer); "
                    + "PERFORM lowrite(fd, 'Test data'::bytea); "
                    + "PERFORM lo_close(fd); "
                    + "fd := lo_open(loid, x'40000'::integer); "
                    + "data := loread(fd, 9); "
                    + "PERFORM lo_close(fd); "
                    + "PERFORM lo_unlink(loid); "
                    + "RETURN convert_from(data, 'UTF8'); "
                    + "END; $$");
            try (ResultSet rs = stmt.executeQuery("SELECT lo18_fd_test() AS result")) {
                assertTrue(rs.next());
                assertEquals("Test data", rs.getString("result"));
            }
        }
    }

    // =========================================================================
    // 2. CTE CYCLE total count — should be 6 (5 non-cycle + 1 cycle row)
    // =========================================================================
    @Test
    @DisplayName("CTE CYCLE emits cycle row — total count is 6")
    void testCteCycleTotalCount() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "WITH RECURSIVE traverse(node) AS ( "
                 + "SELECT 1 UNION ALL "
                 + "SELECT g.dst FROM rcte18_graph g JOIN traverse t ON g.src = t.node "
                 + ") CYCLE node SET is_cycle USING path "
                 + "SELECT count(*)::integer AS cnt FROM traverse")) {
            assertTrue(rs.next());
            assertEquals(6, rs.getInt("cnt"),
                    "PG emits 6 rows: seed(1), 2, 3, 1(cycle), 4, 5");
        }
    }

    // =========================================================================
    // 3. CTE CYCLE non-cycle count — should be 5
    // =========================================================================
    @Test
    @DisplayName("CTE CYCLE non-cycle count is 5")
    void testCteCycleNonCycleCount() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "WITH RECURSIVE traverse(node) AS ( "
                 + "SELECT 1 UNION ALL "
                 + "SELECT g.dst FROM rcte18_graph g JOIN traverse t ON g.src = t.node "
                 + ") CYCLE node SET is_cycle USING path "
                 + "SELECT count(*)::integer AS cnt FROM traverse WHERE NOT is_cycle")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("cnt"),
                    "5 non-cycle rows: seed(1), 2, 3, 4, 5");
        }
    }

    // =========================================================================
    // 4. CTE SEARCH BREADTH FIRST + CYCLE total count — should be 6
    // =========================================================================
    @Test
    @DisplayName("CTE SEARCH BREADTH FIRST + CYCLE total count is 6")
    void testCteBreadthFirstCycleTotalCount() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "WITH RECURSIVE traverse(node) AS ( "
                 + "SELECT 1 UNION ALL "
                 + "SELECT g.dst FROM rcte18_graph g JOIN traverse t ON g.src = t.node "
                 + ") SEARCH BREADTH FIRST BY node SET ordcol "
                 + "CYCLE node SET is_cycle USING path "
                 + "SELECT count(*)::integer AS cnt FROM traverse")) {
            assertTrue(rs.next());
            assertEquals(6, rs.getInt("cnt"),
                    "PG emits 6 rows with SEARCH BREADTH FIRST + CYCLE");
        }
    }

    // =========================================================================
    // 5. CTE SEARCH DEPTH FIRST + CYCLE non-cycle count — should be 5
    // =========================================================================
    @Test
    @DisplayName("CTE SEARCH DEPTH FIRST + CYCLE non-cycle count is 5")
    void testCteDepthFirstCycleNonCycleCount() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "WITH RECURSIVE traverse(node) AS ( "
                 + "SELECT 1 UNION ALL "
                 + "SELECT g.dst FROM rcte18_graph g JOIN traverse t ON g.src = t.node "
                 + ") SEARCH DEPTH FIRST BY node SET ordcol "
                 + "CYCLE node SET is_cycle USING path "
                 + "SELECT count(*)::integer AS non_cycle_cnt FROM traverse WHERE NOT is_cycle")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("non_cycle_cnt"),
                    "5 non-cycle rows with SEARCH DEPTH FIRST + CYCLE");
        }
    }

    // =========================================================================
    // 6. VARIADIC with explicit empty array
    // =========================================================================
    @Test
    @DisplayName("VARIADIC function with empty array resolves correctly")
    void testVariadicEmptyArray() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT var18_concat_all(VARIADIC ARRAY[]::text[]) AS result")) {
            assertTrue(rs.next());
            assertEquals("", rs.getString("result"));
        }
    }
}
