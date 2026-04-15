package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 11 compatibility tests for remaining PG 18 divergences.
 * Covers: collation validation, pg_sequences cache_size, sequence CACHE+CYCLE,
 * RESET dotted params, boolean GUC error message, cursor NO SCROLL enforcement.
 */
class Round11CompatTest {

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

    // =========================================================================
    // 1. Collation validation: unknown collation should be rejected with 42704
    // =========================================================================

    @Test
    void collation_unknownName_shouldError42704() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("SELECT 'a' COLLATE \"nonexistent_collation\""));
        assertEquals("42704", ex.getSQLState(),
                "Unknown collation should produce 42704; got: " + ex.getMessage());
    }

    @Test
    void collation_enUS_utf8_shouldError42704() {
        // en_US.utf8 is not guaranteed to exist — PG rejects it when not installed
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("SELECT 'a' COLLATE \"en_US.utf8\""));
        assertEquals("42704", ex.getSQLState(),
                "en_US.utf8 collation should not be accepted blindly; got: " + ex.getMessage());
    }

    @Test
    void collation_C_shouldWork() throws SQLException {
        // C/POSIX are always available
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'hello' COLLATE \"C\"")) {
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
        }
    }

    @Test
    void collation_default_shouldWork() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'hello' COLLATE \"default\"")) {
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
        }
    }

    // =========================================================================
    // 2. pg_sequences should have cache_size column
    // =========================================================================

    @Test
    void pgSequences_shouldHaveCacheSizeColumn() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS seq_r11_cache");
        exec("CREATE SEQUENCE seq_r11_cache START 1 CACHE 10");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT cache_size FROM pg_sequences WHERE sequencename = 'seq_r11_cache'")) {
            assertTrue(rs.next(), "pg_sequences should have a row for seq_r11_cache");
            assertEquals(10, rs.getLong(1), "cache_size should be 10");
        }

        exec("DROP SEQUENCE seq_r11_cache");
    }

    // =========================================================================
    // 3. Sequence CACHE + CYCLE should wrap correctly
    // =========================================================================

    @Test
    void sequenceCacheCycle_shouldWrapAtBoundary() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS seq_r11_cycle");
        exec("CREATE SEQUENCE seq_r11_cycle START 1 MAXVALUE 5 CACHE 3 CYCLE");

        long[] vals = new long[6];
        for (int i = 0; i < 6; i++) {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT nextval('seq_r11_cycle')")) {
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

        exec("DROP SEQUENCE seq_r11_cycle");
    }

    // =========================================================================
    // 4. RESET should handle dotted (namespaced) params
    // =========================================================================

    @Test
    void resetDottedParam_shouldSucceed() throws SQLException {
        exec("SET myapp.custom_setting = 'value'");
        // This should not throw
        exec("RESET myapp.custom_setting");
    }

    // =========================================================================
    // 5. Boolean GUC error message should match PG wording
    // =========================================================================

    @Test
    void booleanGuc_invalidValue_shouldSayRequiresBoolean() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("SET enable_seqscan = 'maybe'"));
        assertTrue(ex.getMessage().contains("requires a Boolean value"),
                "Error should say 'requires a Boolean value'; got: " + ex.getMessage());
    }

    // =========================================================================
    // 6. FETCH PRIOR on NO SCROLL cursor should error with 55000
    // =========================================================================

    @Test
    void fetchPrior_noScrollCursor_shouldError55000() throws SQLException {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE r11_ns NO SCROLL CURSOR FOR SELECT 1");
            SQLException ex = assertThrows(SQLException.class, () ->
                    exec("FETCH PRIOR FROM r11_ns"));
            assertEquals("55000", ex.getSQLState(),
                    "FETCH PRIOR on NO SCROLL cursor should produce 55000; got: " + ex.getMessage());
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }
}
