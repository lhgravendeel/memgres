package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ALTER TABLE ... DROP col (without the COLUMN keyword).
 *
 * PostgreSQL allows both forms:
 *   ALTER TABLE t DROP COLUMN col;
 *   ALTER TABLE t DROP col;
 *
 * The shorthand form (without COLUMN) is valid per the PG docs and used
 * in real-world migration scripts.
 */
class AlterTableDropColumnShorthandTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getInt(1);
        }
    }

    // =========================================================================
    // Basic: DROP col (no COLUMN keyword)
    // =========================================================================

    @Test
    void testDropColumnShorthand() throws SQLException {
        exec("CREATE TABLE dc_basic (id serial PRIMARY KEY, name text, obsolete_col text)");
        exec("INSERT INTO dc_basic (name, obsolete_col) VALUES ('keep', 'remove')");
        exec("ALTER TABLE dc_basic DROP obsolete_col");
        assertEquals("keep", query1("SELECT name FROM dc_basic"));
        // Column should no longer exist
        assertThrows(SQLException.class, () ->
                query1("SELECT obsolete_col FROM dc_basic"));
    }

    @Test
    void testDropColumnShorthandWithExplicitKeyword() throws SQLException {
        // Verify the explicit COLUMN keyword still works (baseline)
        exec("CREATE TABLE dc_explicit (id serial PRIMARY KEY, a text, b text)");
        exec("INSERT INTO dc_explicit (a, b) VALUES ('x', 'y')");
        exec("ALTER TABLE dc_explicit DROP COLUMN b");
        assertThrows(SQLException.class, () -> query1("SELECT b FROM dc_explicit"));
    }

    // =========================================================================
    // Drop multiple columns in one statement (shorthand)
    // =========================================================================

    @Test
    void testDropMultipleColumnsShorthand() throws SQLException {
        exec("CREATE TABLE dc_multi (id serial PRIMARY KEY, keep_col text, drop_a text, drop_b text)");
        exec("INSERT INTO dc_multi (keep_col, drop_a, drop_b) VALUES ('kept', 'gone_a', 'gone_b')");
        exec("ALTER TABLE dc_multi DROP drop_a, DROP drop_b");
        assertEquals("kept", query1("SELECT keep_col FROM dc_multi"));
        assertThrows(SQLException.class, () -> query1("SELECT drop_a FROM dc_multi"));
        assertThrows(SQLException.class, () -> query1("SELECT drop_b FROM dc_multi"));
    }

    @Test
    void testDropMultipleMixedSyntax() throws SQLException {
        // Mix shorthand and explicit COLUMN keyword in one statement
        exec("CREATE TABLE dc_mixed (id serial PRIMARY KEY, a text, b text, c text)");
        exec("INSERT INTO dc_mixed (a, b, c) VALUES ('x', 'y', 'z')");
        exec("ALTER TABLE dc_mixed DROP a, DROP COLUMN b");
        assertThrows(SQLException.class, () -> query1("SELECT a FROM dc_mixed"));
        assertThrows(SQLException.class, () -> query1("SELECT b FROM dc_mixed"));
        // c should remain
        exec("INSERT INTO dc_mixed (c) VALUES ('still here')");
        assertEquals("still here", query1("SELECT c FROM dc_mixed WHERE c = 'still here'"));
    }

    // =========================================================================
    // Drop with CASCADE / RESTRICT
    // =========================================================================

    @Test
    void testDropColumnShorthandCascade() throws SQLException {
        exec("CREATE TABLE dc_cascade (id serial PRIMARY KEY, data text, extra text)");
        exec("ALTER TABLE dc_cascade DROP extra CASCADE");
    }

    @Test
    void testDropColumnShorthandRestrict() throws SQLException {
        exec("CREATE TABLE dc_restrict (id serial PRIMARY KEY, data text, extra text)");
        exec("ALTER TABLE dc_restrict DROP extra RESTRICT");
    }

    // =========================================================================
    // Drop with IF EXISTS (shorthand)
    // =========================================================================

    @Test
    void testDropColumnShorthandIfExists() throws SQLException {
        exec("CREATE TABLE dc_ifex (id serial PRIMARY KEY, real_col text)");
        // Column exists; should succeed
        exec("ALTER TABLE dc_ifex DROP IF EXISTS real_col");
        // Column does not exist; should not error
        exec("ALTER TABLE dc_ifex DROP IF EXISTS nonexistent_col");
    }

    // =========================================================================
    // Drop column that has a default
    // =========================================================================

    @Test
    void testDropColumnWithDefault() throws SQLException {
        exec("CREATE TABLE dc_default (id serial PRIMARY KEY, status text DEFAULT 'active', deprecated text DEFAULT 'old')");
        exec("ALTER TABLE dc_default DROP deprecated");
        exec("INSERT INTO dc_default DEFAULT VALUES");
        assertEquals("active", query1("SELECT status FROM dc_default"));
    }

    // =========================================================================
    // Drop column that has a NOT NULL constraint
    // =========================================================================

    @Test
    void testDropNotNullColumn() throws SQLException {
        exec("CREATE TABLE dc_notnull (id serial PRIMARY KEY, required_col text NOT NULL, optional_col text)");
        exec("ALTER TABLE dc_notnull DROP required_col");
        exec("INSERT INTO dc_notnull (optional_col) VALUES ('ok')");
        assertEquals("ok", query1("SELECT optional_col FROM dc_notnull"));
    }

    // =========================================================================
    // Drop column on schema-qualified table
    // =========================================================================

    @Test
    void testDropColumnShorthandSchemaQualified() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS dc_schema");
        exec("CREATE TABLE dc_schema.records (id serial PRIMARY KEY, keep_me text, drop_me text)");
        exec("ALTER TABLE dc_schema.records DROP drop_me");
        exec("INSERT INTO dc_schema.records (keep_me) VALUES ('kept')");
        assertEquals("kept", query1("SELECT keep_me FROM dc_schema.records"));
    }

    // =========================================================================
    // Drop column preserves data in remaining columns
    // =========================================================================

    @Test
    void testDropColumnPreservesData() throws SQLException {
        exec("CREATE TABLE dc_preserve (id serial PRIMARY KEY, name text, age int, obsolete text)");
        exec("INSERT INTO dc_preserve (name, age, obsolete) VALUES ('Alice', 30, 'x')");
        exec("INSERT INTO dc_preserve (name, age, obsolete) VALUES ('Bob', 25, 'y')");
        exec("ALTER TABLE dc_preserve DROP obsolete");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM dc_preserve"));
        assertEquals("Alice", query1("SELECT name FROM dc_preserve WHERE age = 30"));
    }

    // =========================================================================
    // Drop the only non-PK column
    // =========================================================================

    @Test
    void testDropOnlyNonPkColumn() throws SQLException {
        exec("CREATE TABLE dc_lonely (id serial PRIMARY KEY, only_other text)");
        exec("ALTER TABLE dc_lonely DROP only_other");
        exec("INSERT INTO dc_lonely DEFAULT VALUES");
        assertEquals("1", query1("SELECT id FROM dc_lonely"));
    }
}
