package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that sequence CACHE combined with CYCLE wraps correctly at the
 * MAXVALUE boundary (rather than pre-allocating past MAXVALUE).
 */
class SequenceCacheCycleTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    @Test
    void sequenceCacheCycle_shouldWrapAtBoundary() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS seq_cache_cycle");
        exec("CREATE SEQUENCE seq_cache_cycle START 1 MAXVALUE 5 CACHE 3 CYCLE");

        long[] vals = new long[6];
        for (int i = 0; i < 6; i++) {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT nextval('seq_cache_cycle')")) {
                rs.next();
                vals[i] = rs.getLong(1);
            }
        }

        // Values should be 1, 2, 3, 4, 5, 1 (cycle back)
        assertEquals(1, vals[0], "val[0]");
        assertEquals(2, vals[1], "val[1]");
        assertEquals(3, vals[2], "val[2]");
        assertEquals(4, vals[3], "val[3]");
        assertEquals(5, vals[4], "val[4]");
        assertEquals(1, vals[5], "val[5] should cycle back to 1");

        exec("DROP SEQUENCE seq_cache_cycle");
    }
}
