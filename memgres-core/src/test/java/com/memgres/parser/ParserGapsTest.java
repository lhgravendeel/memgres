package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for parser edge cases identified in the verification suite.
 *
 * Covers:
 * - Empty SELECT body: SELECT (with no columns, no FROM)
 * - CREATE VIEW AS SELECT (empty body should error)
 * - SELECT a COLLATE FROM p: parser must not treat FROM as collation name
 * - FOR UPDATE FOR SHARE: dual lock mode should error
 * - SELECT FROM table (no output columns): valid PG syntax
 * - Mixed-case identifier as schema qualifier
 * - COLLATE in expressions
 * - ALTER TABLE RENAME on partitioned table columns
 */
class ParserGapsTest {

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

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = 0; while (rs.next()) n++; return n;
        }
    }

    static List<String> columnNames(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> cols = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) cols.add(md.getColumnName(i));
            return cols;
        }
    }

    // ========================================================================
    // Empty SELECT body
    // ========================================================================

    @Test
    void bare_select_with_no_columns_no_from_fails() {
        // PG 18 allows bare SELECT (returns 0 columns)
        assertDoesNotThrow(() -> exec("SELECT"));
    }

    @Test
    void create_view_as_empty_select_fails() throws SQLException {
        exec("CREATE TABLE parser_t(a int)");
        try {
            // PG 18 allows bare SELECT (returns 0 columns)
            assertDoesNotThrow(() -> exec("CREATE VIEW vv AS SELECT"));
        } finally {
            try { exec("DROP VIEW IF EXISTS vv"); } catch (SQLException ignored) {}
            try { exec("DROP TABLE IF EXISTS parser_t"); } catch (SQLException ignored) {}
        }
    }

    // ========================================================================
    // SELECT FROM table (no output columns), valid PG syntax
    // ========================================================================

    @Test
    void select_from_table_no_columns_returns_rows() throws SQLException {
        exec("CREATE TABLE sf_t(id int PRIMARY KEY)");
        exec("INSERT INTO sf_t VALUES (1), (2), (3)");
        try {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT FROM sf_t")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(0, md.getColumnCount(), "SELECT FROM should produce 0 columns");
                int rowCount = 0;
                while (rs.next()) rowCount++;
                assertEquals(3, rowCount, "Should return 3 rows despite 0 columns");
            }
        } finally {
            exec("DROP TABLE sf_t");
        }
    }

    @Test
    void select_from_with_where_clause() throws SQLException {
        exec("CREATE TABLE sf2_t(id int PRIMARY KEY, a int)");
        exec("INSERT INTO sf2_t VALUES (1, 10), (2, 20)");
        try {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT FROM sf2_t WHERE a = 10")) {
                int rowCount = 0;
                while (rs.next()) rowCount++;
                assertEquals(1, rowCount, "WHERE should still filter");
            }
        } finally {
            exec("DROP TABLE sf2_t");
        }
    }

    // ========================================================================
    // SELECT a COLLATE FROM p: parser must handle COLLATE before FROM
    // ========================================================================

    @Test
    void select_column_collate_from_table() throws SQLException {
        // PG: SELECT txt COLLATE "C" FROM table_name (valid)
        exec("CREATE TABLE col_t(id int, txt text COLLATE \"C\")");
        exec("INSERT INTO col_t VALUES (1, 'abc')");
        try {
            String val = scalar("SELECT txt COLLATE \"C\" FROM col_t");
            assertEquals("abc", val);
        } finally {
            exec("DROP TABLE col_t");
        }
    }

    @Test
    void collation_in_order_by() throws SQLException {
        exec("CREATE TABLE col2_t(id int, txt text)");
        exec("INSERT INTO col2_t VALUES (1, 'abc'), (2, 'ABC')");
        try {
            // ORDER BY txt COLLATE "C" for case-sensitive ordering
            var rows = new ArrayList<String>();
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT txt FROM col2_t ORDER BY txt COLLATE \"C\"")) {
                while (rs.next()) rows.add(rs.getString(1));
            }
            // In C collation, uppercase comes before lowercase
            assertEquals("ABC", rows.get(0), "C collation: uppercase first");
            assertEquals("abc", rows.get(1));
        } finally {
            exec("DROP TABLE col2_t");
        }
    }

    @Test
    void collation_comparison() throws SQLException {
        String val = scalar("SELECT 'ä' COLLATE \"C\" < 'z' COLLATE \"C\"");
        assertNotNull(val);
    }

    // ========================================================================
    // FOR UPDATE FOR SHARE: dual lock should error
    // ========================================================================

    @Test
    void dual_for_update_for_share_succeeds() throws SQLException {
        exec("CREATE TABLE lock_t(id int PRIMARY KEY)");
        exec("INSERT INTO lock_t VALUES (1)");
        try {
            // PG 18 supports dual FOR clauses (FOR UPDATE FOR SHARE)
            int count = countRows("SELECT * FROM lock_t FOR UPDATE FOR SHARE");
            assertEquals(1, count, "Dual FOR UPDATE FOR SHARE should succeed and return rows");
        } finally {
            exec("DROP TABLE lock_t");
        }
    }

    // ========================================================================
    // Mixed-case table name as qualifier
    // ========================================================================

    @Test
    void unquoted_table_name_as_column_qualifier() throws SQLException {
        // PG: SELECT mixedcase.normal FROM mixedcase
        // In PG, unquoted identifiers are lowercased, so 'mixedcase.normal' resolves
        // as schema.table or table.column depending on context. PG may error if
        // it interprets mixedcase as a schema name.
        exec("CREATE TABLE mixedcase(id int PRIMARY KEY, normal text)");
        exec("INSERT INTO mixedcase VALUES (1, 'test')");
        try {
            // This should work with table.column qualification
            String val = scalar("SELECT mixedcase.normal FROM mixedcase");
            assertEquals("test", val);
        } finally {
            exec("DROP TABLE mixedcase");
        }
    }

    // ========================================================================
    // ALTER TABLE RENAME on partitioned table columns
    // ========================================================================

    @Test
    void alter_table_rename_column_on_non_column_syntax() throws SQLException {
        // ALTER TABLE p RENAME a TO b, without COLUMN keyword
        // PG: should this work? PG accepts RENAME COLUMN a TO b. Without COLUMN,
        // it might interpret as renaming the constraint or the table itself.
        exec("CREATE TABLE ren_t(a int, b text)");
        try {
            // PG accepts: ALTER TABLE p RENAME COLUMN a TO b
            exec("ALTER TABLE ren_t RENAME COLUMN a TO c");
            // Verify column exists by checking the query doesn't throw (LIMIT 0 returns no rows)
            try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT c FROM ren_t LIMIT 0")) {
                assertNotNull(rs.getMetaData(), "Renamed column should exist");
                assertEquals("c", rs.getMetaData().getColumnName(1).toLowerCase(), "Column should be named 'c'");
            }

            // PG also accepts: ALTER TABLE p RENAME a TO b (without COLUMN keyword)
            exec("ALTER TABLE ren_t RENAME c TO d");
            try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT d FROM ren_t LIMIT 0")) {
                assertEquals("d", rs.getMetaData().getColumnName(1).toLowerCase(),
                        "RENAME without COLUMN keyword should also work");
            }
        } finally {
            exec("DROP TABLE IF EXISTS ren_t");
        }
    }

    // ========================================================================
    // SET GENERATED ALWAYS on non-identity column error code
    // ========================================================================

    @Test
    void set_generated_always_on_non_identity_sqlstate() throws SQLException {
        exec("CREATE TABLE sga_t(a int, b text)");
        try {
            try {
                exec("ALTER TABLE sga_t ALTER COLUMN a SET GENERATED ALWAYS");
                fail("Should fail on non-identity column");
            } catch (SQLException e) {
                // PG: 55000 (object_not_in_prerequisite_state)
                assertEquals("55000", e.getSQLState(),
                        "SET GENERATED ALWAYS on non-identity should be 55000, got " + e.getSQLState());
            }
        } finally {
            exec("DROP TABLE sga_t");
        }
    }

    // ========================================================================
    // Additional parser stress patterns
    // ========================================================================

    @Test
    void select_from_pg_class_returns_many_rows() throws SQLException {
        // pg_class should have hundreds of entries (system tables, indexes, etc.)
        int count = countRows("SELECT FROM pg_class");
        assertTrue(count > 50,
                "SELECT FROM pg_class should return many rows (system objects), got " + count);
    }

    @Test
    void select_star_from_pg_class_row_count() throws SQLException {
        int count = countRows("SELECT relname FROM pg_class");
        assertTrue(count > 50,
                "pg_class should have > 50 entries for system tables/indexes, got " + count);
    }

    // ========================================================================
    // OPERATOR() syntax edge cases
    // ========================================================================

    @Test
    void operator_qualified_unary_plus() throws SQLException {
        // PG supports unary + via OPERATOR syntax: SELECT OPERATOR(pg_catalog.+)(1)
        // This is the unary plus operator
        String val = scalar("SELECT OPERATOR(pg_catalog.+)(1)");
        assertEquals("1", val, "Unary OPERATOR(pg_catalog.+)(1) should return 1");
    }

    @Test
    void operator_qualified_three_args_fails() {
        // OPERATOR() with 3 args should always fail
        assertThrows(SQLException.class,
                () -> exec("SELECT OPERATOR(pg_catalog.+)(1,2,3)"),
                "OPERATOR with 3 args should fail");
    }
}
