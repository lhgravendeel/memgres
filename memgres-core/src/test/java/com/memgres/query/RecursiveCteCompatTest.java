package com.memgres.query;

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

            // Second graph for cycle emission tests: 1->2->3->1 (cycle), 3->4->5 (branch)
            stmt.execute("CREATE TABLE rcte_graph2 (src integer, dst integer)");
            stmt.execute("INSERT INTO rcte_graph2 VALUES (1, 2), (2, 3), (3, 1), (3, 4), (4, 5)");
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
            assertEquals(7, rs.getInt("cnt"),
                    "Cycle detection should produce 7 rows (6 non-cycle + 1 cycle)");
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
            assertEquals(6, rs.getInt("non_cycle_cnt"),
                    "Cycle detection should yield 6 non-cycle rows in depth-first traversal");
        }
    }

    @Test
    @DisplayName("SEARCH clause in subquery context should parse")
    void testSearchInSubquery() throws Exception {
        String sql = "SELECT (SELECT string_agg(id::text, ',' ORDER BY ordcol) FROM ("
                + " WITH RECURSIVE tree(id, label) AS ("
                + "   SELECT child_id, label FROM rcte_tree WHERE parent_id IS NULL"
                + "   UNION ALL"
                + "   SELECT t.child_id, t.label FROM rcte_tree t JOIN tree tr ON t.parent_id = tr.id"
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
                + "  SELECT g.dst FROM rcte_graph g JOIN traverse t ON g.src = t.node"
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
                + "  SELECT g.dst FROM rcte_graph g JOIN traverse t ON g.src = t.node"
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
    @DisplayName("CTE CYCLE emits cycle row — total count is 6 (3-node cycle graph)")
    void testCteCycleTotalCount() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "WITH RECURSIVE traverse(node) AS ( "
                 + "SELECT 1 UNION ALL "
                 + "SELECT g.dst FROM rcte_graph2 g JOIN traverse t ON g.src = t.node "
                 + ") CYCLE node SET is_cycle USING path "
                 + "SELECT count(*)::integer AS cnt FROM traverse")) {
            assertTrue(rs.next());
            assertEquals(6, rs.getInt("cnt"),
                    "PG emits 6 rows: seed(1), 2, 3, 1(cycle), 4, 5");
        }
    }

    @Test
    @DisplayName("CTE CYCLE non-cycle count is 5 (3-node cycle graph)")
    void testCteCycleNonCycleCount() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "WITH RECURSIVE traverse(node) AS ( "
                 + "SELECT 1 UNION ALL "
                 + "SELECT g.dst FROM rcte_graph2 g JOIN traverse t ON g.src = t.node "
                 + ") CYCLE node SET is_cycle USING path "
                 + "SELECT count(*)::integer AS cnt FROM traverse WHERE NOT is_cycle")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("cnt"),
                    "5 non-cycle rows: seed(1), 2, 3, 4, 5");
        }
    }

    @Test
    @DisplayName("CTE SEARCH BREADTH FIRST + CYCLE total count is 6 (3-node cycle graph)")
    void testCteBreadthFirstCycleTotalCount() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "WITH RECURSIVE traverse(node) AS ( "
                 + "SELECT 1 UNION ALL "
                 + "SELECT g.dst FROM rcte_graph2 g JOIN traverse t ON g.src = t.node "
                 + ") SEARCH BREADTH FIRST BY node SET ordcol "
                 + "CYCLE node SET is_cycle USING path "
                 + "SELECT count(*)::integer AS cnt FROM traverse")) {
            assertTrue(rs.next());
            assertEquals(6, rs.getInt("cnt"),
                    "PG emits 6 rows with SEARCH BREADTH FIRST + CYCLE");
        }
    }

    @Test
    @DisplayName("CTE SEARCH DEPTH FIRST + CYCLE non-cycle count is 5 (3-node cycle graph)")
    void testCteDepthFirstCycleNonCycleCount() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "WITH RECURSIVE traverse(node) AS ( "
                 + "SELECT 1 UNION ALL "
                 + "SELECT g.dst FROM rcte_graph2 g JOIN traverse t ON g.src = t.node "
                 + ") SEARCH DEPTH FIRST BY node SET ordcol "
                 + "CYCLE node SET is_cycle USING path "
                 + "SELECT count(*)::integer AS non_cycle_cnt FROM traverse WHERE NOT is_cycle")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("non_cycle_cnt"),
                    "5 non-cycle rows with SEARCH DEPTH FIRST + CYCLE");
        }
    }
}
