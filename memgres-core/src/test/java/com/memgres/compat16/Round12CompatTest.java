package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 12 compatibility tests:
 * 1. CREATE INDEX COLLATE should validate collation names
 * 2. FOREACH SLICE array→text formatting should produce clean PG array literals
 */
class Round12CompatTest {

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
    // 1. CREATE INDEX with invalid COLLATE should be rejected
    // =========================================================================

    @Test
    void createIndex_invalidCollation_shouldError42704() throws SQLException {
        exec("DROP TABLE IF EXISTS r12_idx_test");
        exec("CREATE TABLE r12_idx_test (word text)");

        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE INDEX r12_idx ON r12_idx_test (word COLLATE \"en_US.utf8\")"));
        assertEquals("42704", ex.getSQLState(),
                "CREATE INDEX with unknown collation should produce 42704; got: " + ex.getMessage());

        exec("DROP TABLE r12_idx_test");
    }

    @Test
    void createIndex_validCollation_shouldWork() throws SQLException {
        exec("DROP TABLE IF EXISTS r12_idx_valid");
        exec("CREATE TABLE r12_idx_valid (word text)");
        // C is always valid
        exec("CREATE INDEX r12_idx_c ON r12_idx_valid (word COLLATE \"C\")");
        exec("DROP TABLE r12_idx_valid");
    }

    // =========================================================================
    // 2. FOREACH SLICE array→text should produce clean PG array literals
    // =========================================================================

    @Test
    void foreachSlice_arrayToText_shouldFormatCorrectly() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r12_foreach_slice()");
        exec("CREATE FUNCTION r12_foreach_slice() RETURNS text LANGUAGE plpgsql AS $$\n"
                + "DECLARE\n"
                + "  arr integer[] := ARRAY[[1,2],[3,4],[5,6]];\n"
                + "  slice integer[];\n"
                + "  result text := '';\n"
                + "BEGIN\n"
                + "  FOREACH slice SLICE 1 IN ARRAY arr LOOP\n"
                + "    result := result || slice::text || ';';\n"
                + "  END LOOP;\n"
                + "  RETURN result;\n"
                + "END;\n"
                + "$$");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT r12_foreach_slice()")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertEquals("{1,2};{3,4};{5,6};", result,
                    "FOREACH SLICE 1 should produce clean array literals; got: " + result);
        }

        exec("DROP FUNCTION r12_foreach_slice()");
    }
}
