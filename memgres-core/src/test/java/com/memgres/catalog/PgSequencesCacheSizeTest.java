package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that pg_sequences exposes the cache_size column with the sequence's
 * configured CACHE value.
 */
class PgSequencesCacheSizeTest {

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
    void pgSequences_shouldHaveCacheSizeColumn() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS seq_cache_size");
        exec("CREATE SEQUENCE seq_cache_size START 1 CACHE 10");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT cache_size FROM pg_sequences WHERE sequencename = 'seq_cache_size'")) {
            assertTrue(rs.next(), "pg_sequences should have a row for seq_cache_size");
            assertEquals(10, rs.getLong(1), "cache_size should be 10");
        }

        exec("DROP SEQUENCE seq_cache_size");
    }
}
