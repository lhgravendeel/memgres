package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Issue #5: SEARCH DEPTH FIRST ordering wrong.
 * ordcol values don't reflect correct depth-first traversal.
 * Extended query protocol version.
 */
class ExtendedProtocolSearchDfsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    @Test void search_depth_first_ordering() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ext_edges(src int, dst int)");
            s.execute("INSERT INTO ext_edges VALUES (1,2),(1,3),(2,4),(3,4),(4,5),(5,1)");
        }
        try {
            List<Integer> ids = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement("""
                    WITH RECURSIVE graph(src, dst) AS (
                      SELECT src, dst FROM ext_edges
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
                    """);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) ids.add(rs.getInt(1));
            }
            assertEquals(7, ids.size(), "SEARCH DEPTH FIRST should return 7 rows");
            assertEquals(1, ids.get(0), "First row should be id=1 (root)");
        } finally {
            try (Statement s = conn.createStatement()) { s.execute("DROP TABLE ext_edges"); }
        }
    }
}
