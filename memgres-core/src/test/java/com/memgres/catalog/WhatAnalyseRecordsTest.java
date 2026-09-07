package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What ANALYZE records about a partitioned table.
 *
 * <p>A partitioned table holds no rows of its own: analysing it is analysing the partitions the
 * rows are really in, and PostgreSQL records each of them as analysed. Left out, every partition
 * went on reporting that it had never been looked at -- which is what a planner reads to decide it
 * knows nothing about a relation.
 *
 * <p>And what the table has is what its partitions hold, which is the count PostgreSQL reports for
 * it. Counted from its own storage, a partitioned table that had just been analysed reported no
 * rows at all.
 */
class WhatAnalyseRecordsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int width = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 1; i <= width; i++) {
                    if (i > 1) line.append('|');
                    line.append(rs.getString(i));
                }
                out.add(line.toString());
            }
        }
        return out;
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** Analysing a partitioned table analyses what actually holds its rows. */
    @Test
    void whatAnalysingAPartitionedTableRecords() throws SQLException {
        exec("CREATE TABLE zwn_p (a int) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zwn_p1 PARTITION OF zwn_p FOR VALUES FROM (0) TO (50)");
        exec("CREATE TABLE zwn_p2 PARTITION OF zwn_p FOR VALUES FROM (50) TO (100)");
        exec("INSERT INTO zwn_p SELECT g FROM generate_series(1,60) g");
        // Never analysed is -1 for all three.
        assertEquals(java.util.Arrays.asList("zwn_p|-1", "zwn_p1|-1", "zwn_p2|-1"),
                reltuples());
        exec("ANALYZE zwn_p");
        // The parent reports what its partitions hold between them.
        assertEquals(java.util.Arrays.asList("zwn_p|60", "zwn_p1|49", "zwn_p2|11"),
                reltuples());
        exec("DROP TABLE zwn_p");
    }

    /** An ordinary table still reports what it holds. */
    @Test
    void whatAnalysingAPlainTableRecords() throws SQLException {
        exec("CREATE TABLE zwn_t (a int)");
        exec("INSERT INTO zwn_t SELECT g FROM generate_series(1,7) g");
        assertEquals(java.util.Arrays.asList("zwn_t|-1"),
                rows("SELECT relname::text || '|' || reltuples::int::text FROM pg_class"
                        + " WHERE relname = 'zwn_t'"));
        exec("ANALYZE zwn_t");
        assertEquals(java.util.Arrays.asList("zwn_t|7"),
                rows("SELECT relname::text || '|' || reltuples::int::text FROM pg_class"
                        + " WHERE relname = 'zwn_t'"));
        exec("DROP TABLE zwn_t");
    }

    private static List<String> reltuples() throws SQLException {
        return rows("SELECT relname::text || '|' || reltuples::int::text FROM pg_class"
                + " WHERE relname LIKE 'zwn_p%' ORDER BY relname");
    }
}
