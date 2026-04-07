package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SQL statements that are prefixed with -- line comments
 * before the actual SQL command.
 *
 * The PG JDBC driver sends the full statement text (including comments)
 * to the server via the extended query protocol. The server must correctly
 * identify whether the actual SQL (after stripping comments) is a
 * result-producing statement (SELECT, RETURNING) or not (DDL/DML).
 *
 * If the server only looks at the first token to decide whether to send
 * RowDescription, it will incorrectly classify comment-prefixed SELECTs
 * as non-result statements and omit the RowDescription message, causing
 * the JDBC driver to throw:
 *   "Received resultset tuples, but no field structure for them"
 *
 * This is exactly what pg_dump output looks like: every statement is
 * preceded by comment lines describing the object.
 */
class CommentPrefixedSelectTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SEQUENCE counter_seq START WITH 1");
            s.execute("CREATE TABLE items (id serial PRIMARY KEY, name text)");
            s.execute("INSERT INTO items (name) VALUES ('first'), ('second')");
        }
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
    // SELECT with single-line comment prefix
    // =========================================================================

    @Test
    void testSelectWithSingleLineComment() throws SQLException {
        exec("-- This is a comment\nSELECT 1");
    }

    @Test
    void testSelectWithSingleLineCommentAndBlankLine() throws SQLException {
        exec("-- This is a comment\n\nSELECT 1");
    }

    @Test
    void testSelectWithMultipleCommentLines() throws SQLException {
        exec("-- Line 1\n-- Line 2\n-- Line 3\nSELECT 1");
    }

    // =========================================================================
    // SELECT setval with pg_dump-style comment prefix
    // =========================================================================

    @Test
    void testSetvalWithCommentPrefix() throws SQLException {
        exec("--\n-- Name: counter_seq; Type: SEQUENCE SET; Schema: public\n--\n\nSELECT setval('counter_seq', 42, true)");
    }

    @Test
    void testPgCatalogSetvalWithCommentPrefix() throws SQLException {
        exec("--\n-- Name: counter_seq; Type: SEQUENCE SET; Schema: public\n--\n\nSELECT pg_catalog.setval('counter_seq', 100, true)");
        assertEquals("101", query1("SELECT nextval('counter_seq')"));
    }

    @Test
    void testSetvalWithSchemaQualifiedSeqAndComment() throws SQLException {
        exec("CREATE SEQUENCE public.named_seq START WITH 1");
        exec("--\n-- Set sequence value\n--\n\nSELECT pg_catalog.setval('public.named_seq', 50, true)");
        assertEquals("51", query1("SELECT nextval('public.named_seq')"));
    }

    // =========================================================================
    // SELECT query with comment prefix
    // =========================================================================

    @Test
    void testSelectQueryWithCommentPrefix() throws SQLException {
        String result = query1("-- fetch count\nSELECT COUNT(*) FROM items");
        assertEquals("2", result);
    }

    @Test
    void testSelectWithPgDumpStyleHeader() throws SQLException {
        // Exact pg_dump pattern: two dashes, description, two dashes, blank line, SQL
        String result = query1("--\n-- Name: items; Type: TABLE DATA\n--\n\nSELECT COUNT(*) FROM items");
        assertEquals("2", result);
    }

    // =========================================================================
    // SELECT via execute() (not executeQuery), must still handle result set
    // =========================================================================

    @Test
    void testSelectViaExecuteWithComment() throws SQLException {
        // Statement.execute() returns true if there's a result set
        try (Statement s = conn.createStatement()) {
            boolean hasResult = s.execute("-- comment\nSELECT 1 AS val");
            assertTrue(hasResult);
            try (ResultSet rs = s.getResultSet()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    void testSetvalViaExecuteWithComment() throws SQLException {
        try (Statement s = conn.createStatement()) {
            boolean hasResult = s.execute("-- set sequence\nSELECT setval('counter_seq', 200, true)");
            assertTrue(hasResult);
            try (ResultSet rs = s.getResultSet()) {
                assertTrue(rs.next());
                assertEquals(200, rs.getLong(1));
            }
        }
    }

    // =========================================================================
    // Multiple setvals in sequence (each with comment prefix)
    // =========================================================================

    @Test
    void testMultipleSetvalWithComments() throws SQLException {
        exec("CREATE SEQUENCE seq_a START WITH 1");
        exec("CREATE SEQUENCE seq_b START WITH 1");
        exec("CREATE SEQUENCE seq_c START WITH 1");

        exec("--\n-- Name: seq_a; Type: SEQUENCE SET\n--\n\nSELECT pg_catalog.setval('seq_a', 10, true)");
        exec("--\n-- Name: seq_b; Type: SEQUENCE SET\n--\n\nSELECT pg_catalog.setval('seq_b', 20, true)");
        exec("--\n-- Name: seq_c; Type: SEQUENCE SET\n--\n\nSELECT pg_catalog.setval('seq_c', 30, false)");

        assertEquals("11", query1("SELECT nextval('seq_a')"));
        assertEquals("21", query1("SELECT nextval('seq_b')"));
        assertEquals("30", query1("SELECT nextval('seq_c')"));
    }

    // =========================================================================
    // Connection remains usable after comment-prefixed SELECT
    // =========================================================================

    @Test
    void testConnectionUsableAfterCommentSelect() throws SQLException {
        exec("-- comment\nSELECT 1");
        // Must still work
        exec("CREATE TABLE after_comment_select (id serial PRIMARY KEY)");
        exec("INSERT INTO after_comment_select DEFAULT VALUES");
        assertEquals("1", query1("SELECT COUNT(*) FROM after_comment_select"));
    }

    // =========================================================================
    // Block comment prefix (/* ... */) before SELECT
    // =========================================================================

    @Test
    void testSelectWithBlockCommentPrefix() throws SQLException {
        exec("/* block comment */\nSELECT 1");
    }

    @Test
    void testSetvalWithBlockCommentPrefix() throws SQLException {
        exec("/* Set the sequence */ SELECT setval('counter_seq', 500, true)");
        assertEquals("501", query1("SELECT nextval('counter_seq')"));
    }

    // =========================================================================
    // DDL with comment prefix (baseline; should already work)
    // =========================================================================

    @Test
    void testCreateTableWithCommentPrefix() throws SQLException {
        exec("-- Create a table\nCREATE TABLE commented_create (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO commented_create (val) VALUES ('ok')");
        assertEquals("ok", query1("SELECT val FROM commented_create"));
    }

    @Test
    void testInsertWithCommentPrefix() throws SQLException {
        exec("CREATE TABLE commented_insert (id serial PRIMARY KEY, val text)");
        exec("-- Insert data\nINSERT INTO commented_insert (val) VALUES ('commented')");
        assertEquals("commented", query1("SELECT val FROM commented_insert"));
    }

    // =========================================================================
    // pg_dump full pattern: comment + SET + comment + CREATE + comment + setval
    // =========================================================================

    @Test
    void testPgDumpSequenceLifecycleWithComments() throws SQLException {
        exec("CREATE TABLE pgd_tbl (id bigint NOT NULL, name text)");
        exec("--\n-- Name: pgd_tbl_id_seq; Type: SEQUENCE\n--\n\nCREATE SEQUENCE pgd_tbl_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1");
        exec("--\n-- Name: pgd_tbl id; Type: DEFAULT\n--\n\nALTER TABLE ONLY pgd_tbl ALTER COLUMN id SET DEFAULT nextval('pgd_tbl_id_seq'::regclass)");
        exec("--\n-- Data for pgd_tbl\n--\n\nINSERT INTO pgd_tbl (id, name) VALUES (1, 'Alice')");
        exec("INSERT INTO pgd_tbl (id, name) VALUES (2, 'Bob')");
        exec("--\n-- Name: pgd_tbl_id_seq; Type: SEQUENCE SET\n--\n\nSELECT pg_catalog.setval('pgd_tbl_id_seq', 2, true)");
        exec("--\n-- Name: pgd_tbl pgd_tbl_pkey; Type: CONSTRAINT\n--\n\nALTER TABLE ONLY pgd_tbl ADD CONSTRAINT pgd_tbl_pkey PRIMARY KEY (id)");

        // Verify
        assertEquals("2", query1("SELECT COUNT(*) FROM pgd_tbl"));
        assertEquals("3", query1("SELECT nextval('pgd_tbl_id_seq')"));
    }
}
