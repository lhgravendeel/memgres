package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class RecursiveCteCompatTest {
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
            stmt.execute("CREATE TABLE rcte_tree (child_id integer, parent_id integer, label text)");
            stmt.execute("INSERT INTO rcte_tree VALUES "
                    + "(1, NULL, 'root'), (2, 1, 'left'), (3, 1, 'right'), "
                    + "(4, 2, 'left-left'), (5, 2, 'left-right'), (6, 3, 'right-left')");

            stmt.execute("CREATE TABLE rcte_graph (src integer, dst integer)");
            stmt.execute("INSERT INTO rcte_graph VALUES "
                    + "(1, 2), (2, 3), (3, 4), (4, 2), (1, 5), (5, 6)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    @DisplayName("SEARCH BREADTH FIRST BY should order recursive CTE results")
    void testSearchBreadthFirstOrdering() throws Exception {
        // PG supports SEARCH BREADTH FIRST BY; Memgres errors with
        // "Expected keyword SELECT near 'SEARCH'"
        String sql = "WITH RECURSIVE tree(id, label) AS ("
                + "  SELECT child_id, label FROM rcte_tree WHERE parent_id IS NULL"
                + "  UNION ALL"
                + "  SELECT t.child_id, t.label FROM rcte_tree t JOIN tree tr ON t.parent_id = tr.id"
                + ") SEARCH BREADTH FIRST BY id SET ordcol "
                + "SELECT id, label FROM tree ORDER BY ordcol";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row");
            assertEquals(1, rs.getInt("id"), "First row should be root node with id=1");
            int count = 1;
            while (rs.next()) {
                count++;
            }
            assertEquals(6, count, "Expected 6 rows from breadth-first traversal");
        }
    }

    @Test
    @DisplayName("SEARCH DEPTH FIRST BY should order recursive CTE results")
    void testSearchDepthFirstOrdering() throws Exception {
        // PG supports SEARCH DEPTH FIRST BY; Memgres errors with
        // "Expected keyword SELECT near 'SEARCH'"
        String sql = "WITH RECURSIVE tree(id, label) AS ("
                + "  SELECT child_id, label FROM rcte_tree WHERE parent_id IS NULL"
                + "  UNION ALL"
                + "  SELECT t.child_id, t.label FROM rcte_tree t JOIN tree tr ON t.parent_id = tr.id"
                + ") SEARCH DEPTH FIRST BY id SET ordcol "
                + "SELECT id, label FROM tree ORDER BY ordcol";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row");
            assertEquals(1, rs.getInt("id"), "First row should be root node with id=1");
            int count = 1;
            while (rs.next()) {
                count++;
            }
            assertEquals(6, count, "Expected 6 rows from depth-first traversal");
        }
    }

    @Test
    @DisplayName("CYCLE detection with SEARCH BREADTH FIRST should limit traversal")
    void testCycleDetectionBreadthFirst() throws Exception {
        // PG returns 6 rows with cycle detection; Memgres returns 1667
        // (no cycle detection, runs until recursion limit)
        String sql = "WITH RECURSIVE traverse(node) AS ("
                + "  SELECT 1"
                + "  UNION ALL"
                + "  SELECT g.dst FROM rcte_graph g JOIN traverse t ON g.src = t.node"
                + ") SEARCH BREADTH FIRST BY node SET ordcol "
                + "CYCLE node SET is_cycle USING path "
                + "SELECT count(*)::integer AS cnt FROM traverse";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected a result row");
            assertEquals(6, rs.getInt("cnt"),
                    "Cycle detection should limit breadth-first traversal to 6 rows");
        }
    }

    @Test
    @DisplayName("CYCLE detection with SEARCH DEPTH FIRST should identify non-cycle rows")
    void testCycleDetectionDepthFirst() throws Exception {
        // PG returns non_cycle_cnt=5; Memgres returns 669
        // (no cycle detection, runs until recursion limit)
        String sql = "WITH RECURSIVE traverse(node) AS ("
                + "  SELECT 1"
                + "  UNION ALL"
                + "  SELECT g.dst FROM rcte_graph g JOIN traverse t ON g.src = t.node"
                + ") SEARCH DEPTH FIRST BY node SET ordcol "
                + "CYCLE node SET is_cycle USING path "
                + "SELECT count(*)::integer AS non_cycle_cnt FROM traverse WHERE NOT is_cycle";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected a result row");
            assertEquals(5, rs.getInt("non_cycle_cnt"),
                    "Cycle detection should yield 5 non-cycle rows in depth-first traversal");
        }
    }
}
