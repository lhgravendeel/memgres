package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for E'' escape string literals used as DEFAULT values and in expressions.
 *
 * PostgreSQL supports E'...' escape strings where backslash sequences are interpreted.
 * E'' is an empty escape string, commonly used in generated migrations as
 * a column default (DEFAULT E'').
 *
 * Also covers E-strings with actual escape sequences in defaults,
 * and E-strings combined with other column features.
 */
class EscapeStringDefaultTest {

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

    // =========================================================================
    // DEFAULT E'' (empty escape string)
    // =========================================================================

    @Test
    void testDefaultEmptyEscapeString() throws SQLException {
        exec("CREATE TABLE estr_empty (id serial PRIMARY KEY, label text NOT NULL DEFAULT E'')");
        exec("INSERT INTO estr_empty DEFAULT VALUES");
        assertEquals("", query1("SELECT label FROM estr_empty WHERE id = 1"));
    }

    @Test
    void testDefaultEmptyEscapeStringWithOtherColumns() throws SQLException {
        exec("""
            CREATE TABLE estr_multi (
                id text NOT NULL,
                title text NOT NULL,
                author_name text NOT NULL DEFAULT E'',
                verified boolean NOT NULL DEFAULT false,
                demo boolean NOT NULL DEFAULT false,
                CONSTRAINT estr_multi_pkey PRIMARY KEY (id)
            )
        """);
        exec("INSERT INTO estr_multi (id, title) VALUES ('1', 'Test')");
        assertEquals("", query1("SELECT author_name FROM estr_multi WHERE id = '1'"));
    }

    @Test
    void testDefaultEscapeStringWithNewline() throws SQLException {
        exec("CREATE TABLE estr_nl (id serial PRIMARY KEY, sep text DEFAULT E'\\n')");
        exec("INSERT INTO estr_nl DEFAULT VALUES");
        assertNotNull(query1("SELECT sep FROM estr_nl WHERE id = 1"));
    }

    @Test
    void testDefaultEscapeStringWithTab() throws SQLException {
        exec("CREATE TABLE estr_tab (id serial PRIMARY KEY, delimiter text DEFAULT E'\\t')");
        exec("INSERT INTO estr_tab DEFAULT VALUES");
        assertNotNull(query1("SELECT delimiter FROM estr_tab WHERE id = 1"));
    }

    @Test
    void testDefaultEscapeStringWithBackslash() throws SQLException {
        exec("CREATE TABLE estr_bs (id serial PRIMARY KEY, path text DEFAULT E'C:\\\\temp')");
        exec("INSERT INTO estr_bs DEFAULT VALUES");
        assertEquals("C:\\temp", query1("SELECT path FROM estr_bs WHERE id = 1"));
    }

    // =========================================================================
    // E'' in INSERT VALUES
    // =========================================================================

    @Test
    void testInsertWithEmptyEscapeString() throws SQLException {
        exec("CREATE TABLE estr_ins (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO estr_ins (val) VALUES (E'')");
        assertEquals("", query1("SELECT val FROM estr_ins WHERE id = 1"));
    }

    @Test
    void testInsertWithEscapeStringNewline() throws SQLException {
        exec("CREATE TABLE estr_ins2 (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO estr_ins2 (val) VALUES (E'line1\\nline2')");
        assertNotNull(query1("SELECT val FROM estr_ins2 WHERE id = 1"));
    }

    // =========================================================================
    // E'' combined with NOT NULL and other constraints
    // =========================================================================

    @Test
    void testEscapeStringDefaultNotNullWithTimestamp() throws SQLException {
        // Multiple columns with E'' defaults + TIMESTAMP(3) + booleans
        exec("""
            CREATE TABLE orm_style (
                "urlId" TEXT NOT NULL,
                "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                "updatedAt" TIMESTAMP(3) NOT NULL,
                "title" TEXT NOT NULL,
                "description" TEXT,
                "authorName" TEXT NOT NULL DEFAULT E'',
                "verified" BOOLEAN NOT NULL DEFAULT false,
                CONSTRAINT orm_style_pkey PRIMARY KEY ("urlId")
            )
        """);
        exec("INSERT INTO orm_style (\"urlId\", \"updatedAt\", \"title\") VALUES ('u1', now(), 'Test')");
        assertEquals("", query1("SELECT \"authorName\" FROM orm_style WHERE \"urlId\" = 'u1'"));
    }

    // =========================================================================
    // E'' in UPDATE SET
    // =========================================================================

    @Test
    void testUpdateSetToEmptyEscapeString() throws SQLException {
        exec("CREATE TABLE estr_upd (id serial PRIMARY KEY, val text DEFAULT 'initial')");
        exec("INSERT INTO estr_upd DEFAULT VALUES");
        exec("UPDATE estr_upd SET val = E'' WHERE id = 1");
        assertEquals("", query1("SELECT val FROM estr_upd WHERE id = 1"));
    }

    // =========================================================================
    // E-string in comparison
    // =========================================================================

    @Test
    void testWhereCompareEmptyEscapeString() throws SQLException {
        exec("CREATE TABLE estr_cmp (id serial PRIMARY KEY, val text DEFAULT E'')");
        exec("INSERT INTO estr_cmp DEFAULT VALUES");
        assertEquals("1", query1("SELECT COUNT(*) FROM estr_cmp WHERE val = E''"));
    }
}
