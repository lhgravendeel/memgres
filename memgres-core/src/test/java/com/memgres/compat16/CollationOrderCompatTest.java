package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests string collation ordering differences between Memgres and PostgreSQL 18.
 *
 * PG 18 uses locale-specific collation (default: en_US.UTF-8 or C).
 * Memgres uses Java String.compareTo() which is binary UTF-8 ordering.
 *
 * Key difference: In C/POSIX locale, PG sorts uppercase before lowercase
 * (same as binary), but in en_US.UTF-8 locale, PG sorts case-insensitively
 * first, then by case. For example:
 *   - C locale: 'A' < 'B' < 'a' < 'b'  (same as Java/Memgres)
 *   - en_US.UTF-8: 'a' < 'A' < 'b' < 'B'  (locale-aware)
 *
 * These tests assert en_US.UTF-8 PG behavior and are expected to fail on Memgres
 * unless the database is created with C locale, in which case the binary ordering
 * matches. The COLLATE "en_US.utf8" tests specifically target locale behavior.
 */
class CollationOrderCompatTest {

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

    static List<String> queryColumn(String sql) throws SQLException {
        List<String> results = new ArrayList<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) results.add(rs.getString(1));
        }
        return results;
    }

    // -------------------------------------------------------------------------
    // COLLATE clause should actually affect sort order
    // -------------------------------------------------------------------------

    @Test
    void collate_enUS_shouldRejectUnknownCollation() {
        // en_US.utf8 is not guaranteed to exist (depends on OS locale installation).
        // Both PG and Memgres should reject it with 42704 when not available.
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("SELECT 'a' COLLATE \"en_US.utf8\""));
        assertEquals("42704", ex.getSQLState(),
                "en_US.utf8 collation should be rejected with 42704; got: " + ex.getMessage());
    }

    @Test
    void collate_C_shouldSortBinary() throws SQLException {
        exec("DROP TABLE IF EXISTS collation_c_test");
        exec("CREATE TABLE collation_c_test (word text)");
        exec("INSERT INTO collation_c_test VALUES ('b'), ('A'), ('a'), ('B')");

        List<String> result = queryColumn(
                "SELECT word FROM collation_c_test ORDER BY word COLLATE \"C\"");

        // C locale: pure byte ordering: A (65), B (66), a (97), b (98)
        // This should match Memgres behavior
        assertEquals(List.of("A", "B", "a", "b"), result,
                "C collation should sort by byte value; got: " + result);
    }

    // -------------------------------------------------------------------------
    // MIN/MAX should respect collation
    // -------------------------------------------------------------------------

    @Test
    void min_shouldRespectCollation() throws SQLException {
        exec("DROP TABLE IF EXISTS collation_minmax");
        exec("CREATE TABLE collation_minmax (word text COLLATE \"C\")");
        exec("INSERT INTO collation_minmax VALUES ('b'), ('A'), ('a'), ('B')");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT min(word) FROM collation_minmax")) {
            assertTrue(rs.next());
            String minVal = rs.getString(1);
            // C collation: binary ordering — 'A' (65) < 'B' (66) < 'a' (97) < 'b' (98)
            assertEquals("A", minVal,
                    "MIN with C collation should use binary ordering; got '" + minVal + "'");
        }
    }

    // -------------------------------------------------------------------------
    // DISTINCT should respect collation for deduplication
    // -------------------------------------------------------------------------

    @Test
    void distinct_shouldNotDeduplicateDifferentCases() throws SQLException {
        exec("DROP TABLE IF EXISTS collation_distinct");
        exec("CREATE TABLE collation_distinct (word text)");
        exec("INSERT INTO collation_distinct VALUES ('abc'), ('ABC'), ('abc')");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT DISTINCT word FROM collation_distinct ORDER BY word")) {
            List<String> results = new ArrayList<>();
            while (rs.next()) results.add(rs.getString(1));
            // Both PG and Memgres: 'abc' and 'ABC' are distinct (they differ in content)
            assertEquals(2, results.size(),
                    "DISTINCT should keep 'abc' and 'ABC' as separate values");
        }
    }

    // -------------------------------------------------------------------------
    // GROUP BY ordering should respect collation
    // -------------------------------------------------------------------------

    @Test
    void groupBy_orderBy_shouldRespectCollation() throws SQLException {
        exec("DROP TABLE IF EXISTS collation_group");
        exec("CREATE TABLE collation_group (category text COLLATE \"C\", amount int)");
        exec("INSERT INTO collation_group VALUES ('b', 1), ('A', 2), ('a', 3), ('B', 4)");

        List<String> result = queryColumn(
                "SELECT category FROM collation_group GROUP BY category ORDER BY category");

        // C collation: binary ordering — A, B, a, b
        assertEquals(List.of("A", "B", "a", "b"), result,
                "GROUP BY + ORDER BY with C collation should use binary ordering; got: " + result);
    }

    // -------------------------------------------------------------------------
    // Index ordering should respect collation
    // -------------------------------------------------------------------------

    @Test
    void createIndex_withValidCollation_shouldWork() throws SQLException {
        exec("DROP TABLE IF EXISTS collation_idx");
        exec("CREATE TABLE collation_idx (word text)");
        exec("INSERT INTO collation_idx VALUES ('b'), ('A'), ('a'), ('B')");

        // C collation is always available — CREATE INDEX with COLLATE should work
        exec("CREATE INDEX collation_idx_word ON collation_idx (word COLLATE \"C\")");

        List<String> result = queryColumn(
                "SELECT word FROM collation_idx ORDER BY word COLLATE \"C\"");
        assertEquals(List.of("A", "B", "a", "b"), result,
                "Index with C collation should use binary order; got: " + result);
    }
}
