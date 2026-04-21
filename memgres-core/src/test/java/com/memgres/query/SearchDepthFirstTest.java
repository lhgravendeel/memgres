package com.memgres.query;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diff #17: SEARCH DEPTH FIRST BY id SET ordcol, data differs from PG.
 * The ordcol-based ordering doesn't match PG's depth-first traversal.
 */
class SearchDepthFirstTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // Exact query from 21_recursive_cte_srf_tableexpr.sql
    // SEARCH DEPTH FIRST with a non-recursive graph CTE should succeed and return rows in DFS order.
    @Test void search_depth_first_ordering() throws SQLException {
        exec("CREATE TABLE edges(src int, dst int)");
        exec("INSERT INTO edges VALUES (1,2),(1,3),(2,4),(3,4),(4,5),(5,1)");
        try {
            List<String> ids = new ArrayList<>();
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("""
                     WITH RECURSIVE graph(src, dst) AS (
                       SELECT src, dst FROM edges
                     ),
                     search_graph(id, path) AS (
                       SELECT 1, ARRAY[1]
                       UNION ALL
                       SELECT e.dst, sg.path || e.dst
                       FROM search_graph sg
                       JOIN graph e ON e.src = sg.id
                       WHERE cardinality(sg.path) < 4
                     )
                     SEARCH DEPTH FIRST BY id SET ordcol
                     SELECT id, path FROM search_graph ORDER BY ordcol
                     """)) {
                while (rs.next()) ids.add(rs.getString(1));
            }
            assertEquals(7, ids.size(), "SEARCH DEPTH FIRST should return 7 rows");
            assertEquals("1", ids.get(0), "First row should be id=1 (root)");
        } finally {
            exec("DROP TABLE edges");
        }
    }
}
