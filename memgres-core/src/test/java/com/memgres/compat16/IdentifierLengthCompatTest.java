package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests identifier length handling differences between Memgres and PostgreSQL 18.
 *
 * PG 18: Identifiers are silently truncated to NAMEDATALEN-1 = 63 bytes.
 * When an identifier exceeds 63 chars, PG truncates it and may issue a NOTICE.
 * Two identifiers that differ only after the 63rd character are treated as the same.
 *
 * Memgres: No identifier length limit is enforced. Identifiers can be arbitrarily
 * long and are stored/compared in full. This means:
 *   - A 100-char table name works in Memgres but is truncated in PG
 *   - Two names differing at position 64+ are distinct in Memgres but collide in PG
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres.
 */
class IdentifierLengthCompatTest {

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

    // 63 chars exactly (max PG identifier length)
    static final String NAME_63 = "a".repeat(63);
    // 64 chars - will be truncated to 63 in PG
    static final String NAME_64 = "a".repeat(64);
    // Two names that differ only at position 64
    static final String NAME_LONG_A = "x".repeat(63) + "a_suffix";
    static final String NAME_LONG_B = "x".repeat(63) + "b_suffix";

    @BeforeEach
    void cleanup() throws SQLException {
        exec("DROP TABLE IF EXISTS \"" + NAME_63 + "\"");
        exec("DROP TABLE IF EXISTS \"" + NAME_64 + "\"");
        exec("DROP TABLE IF EXISTS \"" + NAME_LONG_A + "\"");
        exec("DROP TABLE IF EXISTS \"" + NAME_LONG_B + "\"");
        // Also drop the truncated form
        exec("DROP TABLE IF EXISTS \"" + "x".repeat(63) + "\"");
    }

    // -------------------------------------------------------------------------
    // Table names should be truncated to 63 characters
    // -------------------------------------------------------------------------

    @Test
    void tableName_shouldBeTruncatedTo63Chars() throws SQLException {
        String longName = "t" + "a".repeat(80); // 81 chars total
        exec("CREATE TABLE \"" + longName + "\" (id int)");

        // In PG, the table name is truncated to 63 chars
        String truncated = longName.substring(0, 63);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
                             + "AND tablename LIKE 'ta%'")) {
            assertTrue(rs.next(), "Should find the created table");
            String actual = rs.getString(1);
            assertEquals(truncated, actual,
                    "Table name should be truncated to 63 chars. "
                            + "Expected length " + truncated.length() + ", got length " + actual.length());
        }

        // Cleanup
        exec("DROP TABLE IF EXISTS \"" + truncated + "\"");
    }

    // -------------------------------------------------------------------------
    // Two names that differ only after 63 chars should collide
    // -------------------------------------------------------------------------

    @Test
    void twoLongNames_differingAfter63_shouldCollide() throws SQLException {
        // Create table with NAME_LONG_A
        exec("CREATE TABLE \"" + NAME_LONG_A + "\" (id int)");

        // In PG, NAME_LONG_B truncates to same 63-char prefix -> 42P07 duplicate
        // In Memgres, full names are distinct -> succeeds
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE \"" + NAME_LONG_B + "\" (id int)");
            fail("Creating table with name that differs only after char 63 should fail "
                    + "with 42P07 (duplicate_table) due to truncation");
        } catch (SQLException e) {
            assertEquals("42P07", e.getSQLState(),
                    "Names differing only after 63 chars should collide. Got: "
                            + e.getSQLState() + " - " + e.getMessage());
        }

        // Cleanup
        exec("DROP TABLE IF EXISTS \"" + NAME_LONG_A.substring(0, 63) + "\"");
    }

    // -------------------------------------------------------------------------
    // Column names should also be truncated
    // -------------------------------------------------------------------------

    @Test
    void columnName_shouldBeTruncatedTo63Chars() throws SQLException {
        String longCol = "c" + "o".repeat(80); // 81 chars
        String truncCol = longCol.substring(0, 63);

        exec("CREATE TABLE idlen_col_test (\"" + longCol + "\" int)");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM idlen_col_test")) {
            ResultSetMetaData md = rs.getMetaData();
            String actual = md.getColumnName(1);
            assertEquals(truncCol, actual,
                    "Column name should be truncated to 63 chars. Got length " + actual.length());
        }

        exec("DROP TABLE idlen_col_test");
    }

    // -------------------------------------------------------------------------
    // 63-char name should work without truncation
    // -------------------------------------------------------------------------

    @Test
    void exactly63Chars_shouldWorkWithoutTruncation() throws SQLException {
        exec("CREATE TABLE \"" + NAME_63 + "\" (id int)");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT tablename FROM pg_tables WHERE tablename = '" + NAME_63 + "'")) {
            assertTrue(rs.next(), "63-char table name should be preserved exactly");
            assertEquals(NAME_63, rs.getString(1));
        }

        exec("DROP TABLE \"" + NAME_63 + "\"");
    }

    // -------------------------------------------------------------------------
    // Index names should be truncated
    // -------------------------------------------------------------------------

    @Test
    void indexName_shouldBeTruncatedTo63Chars() throws SQLException {
        exec("CREATE TABLE idlen_idx_test (id int)");
        String longIdx = "idx_" + "x".repeat(80);
        exec("CREATE INDEX \"" + longIdx + "\" ON idlen_idx_test (id)");

        String truncIdx = longIdx.substring(0, 63);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT indexname FROM pg_indexes WHERE tablename = 'idlen_idx_test'")) {
            assertTrue(rs.next());
            String actual = rs.getString(1);
            assertEquals(truncIdx, actual,
                    "Index name should be truncated to 63 chars. Got length " + actual.length());
        }

        exec("DROP TABLE idlen_idx_test");
    }
}
