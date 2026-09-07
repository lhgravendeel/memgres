package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What dropping an index on a partitioned table takes with it.
 *
 * <p>An index on a partitioned table reads no rows of its own: every partition carries a copy
 * attached to it, and the copy exists because the parent does. Dropping only the parent left each
 * partition indexed by something nothing owned, with its name still taken -- and dropping the copy
 * on its own, which PostgreSQL refuses, would have left the parent covering a partition it no
 * longer indexed.
 */
class WhatDroppingAPartitionedIndexTakesTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE TABLE zwi_p (a int, b int) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zwi_p1 PARTITION OF zwi_p FOR VALUES FROM (0) TO (10)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE zwi_p");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static List<String> indexes() throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT indexname FROM pg_indexes"
                     + " WHERE tablename IN ('zwi_p','zwi_p1') ORDER BY indexname")) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The copies go with the index they were made for, and not before it. */
    @Test
    void whatGoesWithTheParent() throws SQLException {
        exec("CREATE INDEX zwi_idx ON zwi_p (b)");
        assertEquals(java.util.Arrays.asList("zwi_idx", "zwi_p1_b_idx"), indexes());
        // The copy is not the reader's to drop.
        assertEquals("2BP01", stateOf("DROP INDEX zwi_p1_b_idx"));
        exec("DROP INDEX zwi_idx");
        assertEquals(java.util.Collections.emptyList(), indexes());
    }

    /** A rolled-back drop puts both back. */
    @Test
    void whatARolledBackDropPutsBack() throws SQLException {
        exec("CREATE INDEX zwi_idx2 ON zwi_p (b)");
        exec("BEGIN");
        exec("DROP INDEX zwi_idx2");
        assertEquals(java.util.Collections.emptyList(), indexes());
        exec("ROLLBACK");
        assertEquals(java.util.Arrays.asList("zwi_idx2", "zwi_p1_b_idx"), indexes());
        exec("DROP INDEX zwi_idx2");
    }
}
