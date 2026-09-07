package com.memgres.json;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The clauses the SQL/JSON forms accept, and what a relation's partition tree is.
 *
 * <p>{@code JSON(...)} is a constructor with clauses of its own, not an ordinary call: it takes
 * {@code FORMAT JSON} and a uniqueness clause inside its parentheses. Read as a call, the word
 * WITH ended the argument list and the form could not be written at all. A RETURNING clause may
 * carry {@code FORMAT JSON} in the same way, wherever one may be written.
 *
 * <p>A serialised document is text. Read from the value, one that opens with a brace looked like
 * an array written out and was reported as an array of text.
 *
 * <p>And a relation that is neither partitioned nor a partition is in no partition tree at all:
 * PostgreSQL answers with no rows. Answered with the relation itself, a caller counting the rows
 * to ask whether a relation is partitioned was always told yes. A relation below the root still
 * names what stands above it.
 */
class WhatTheJsonFormsAcceptTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int width = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 1; i <= width; i++) {
                    if (i > 1) line.append('|');
                    line.append(rs.getString(i));
                }
                out.add(line.toString());
            }
        }
        return out;
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The constructor's own clauses stand inside its parentheses. */
    @Test
    void whatTheConstructorAccepts() throws SQLException {
        assertEquals("{\"a\":1}", one("SELECT JSON('{\"a\":1}')::text"));
        assertEquals("22030", stateOf("SELECT JSON('{\"a\":1,\"a\":2}' WITH UNIQUE KEYS)"));
        assertEquals("{\"a\":1,\"a\":2}",
                one("SELECT JSON('{\"a\":1,\"a\":2}' WITHOUT UNIQUE KEYS)::text"));
        assertEquals("{\"a\":1}", one("SELECT JSON('{\"a\":1}' WITH UNIQUE)::text"));
        assertEquals("{\"a\":1}", one("SELECT JSON('{\"a\":1}' WITHOUT UNIQUE)::text"));
    }

    /** A RETURNING clause may say the result is written as JSON. */
    @Test
    void whatARetuningClauseAccepts() throws SQLException {
        assertEquals("[1, 2]", one("SELECT JSON_QUERY(jsonb '{\"a\":[1,2]}', '$.a'"
                + " RETURNING text FORMAT JSON)"));
        assertEquals("[1, 2]", one("SELECT JSON_QUERY(jsonb '{\"a\":[1,2]}', '$.a'"
                + " RETURNING jsonb FORMAT JSON)::text"));
        assertEquals("{\"a\" : 1}", one("SELECT JSON_OBJECT('a': 1 RETURNING text FORMAT JSON)"));
        assertEquals("[1, 2]", one("SELECT JSON_ARRAY(1,2 RETURNING text FORMAT JSON)::text"));
    }

    /** A serialised document is text, whatever the document looks like. */
    @Test
    void whatASerialisedDocumentIs() throws SQLException {
        assertEquals("text", one("SELECT pg_typeof(JSON_SERIALIZE('{\"a\":1}'))::text"));
        assertEquals("text", one("SELECT pg_typeof(JSON_SERIALIZE('[1,2]'))::text"));
        assertEquals("{\"a\":1}", one("SELECT JSON_SERIALIZE('{\"a\":1}')"));
    }

    /** A relation outside every partition tree is in none. */
    @Test
    void whatIsInAPartitionTree() throws SQLException {
        exec("CREATE TABLE zwj_plain (i int)");
        assertEquals("0", one("SELECT count(*)::text FROM pg_partition_tree('zwj_plain')"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_partition_tree('pg_class')"));
        exec("DROP TABLE zwj_plain");
        exec("CREATE TABLE zwj_p (a int) PARTITION BY RANGE (a)");
        exec("CREATE TABLE zwj_p1 PARTITION OF zwj_p FOR VALUES FROM (0) TO (10)"
                + " PARTITION BY RANGE (a)");
        exec("CREATE TABLE zwj_p1a PARTITION OF zwj_p1 FOR VALUES FROM (0) TO (5)");
        exec("CREATE TABLE zwj_p2 PARTITION OF zwj_p FOR VALUES FROM (10) TO (20)");
        assertEquals(java.util.Arrays.asList("zwj_p|<null>|false|0", "zwj_p1|zwj_p|false|1",
                        "zwj_p2|zwj_p|true|1", "zwj_p1a|zwj_p1|true|2"),
                rows("SELECT relid::regclass::text, coalesce(parentrelid::regclass::text,'<null>'),"
                        + " isleaf::text, level::text FROM pg_partition_tree('zwj_p')"
                        + " ORDER BY level, 1"));
        // Starting below the root, the first row still names what stands above it.
        assertEquals(java.util.Arrays.asList("zwj_p1|zwj_p|false|0", "zwj_p1a|zwj_p1|true|1"),
                rows("SELECT relid::regclass::text, coalesce(parentrelid::regclass::text,'<null>'),"
                        + " isleaf::text, level::text FROM pg_partition_tree('zwj_p1')"
                        + " ORDER BY level, 1"));
        exec("DROP TABLE zwj_p");
    }

    /** A qualifier naming a schema that is not there is what is missing. */
    @Test
    void whichNameADropTriggerReports() {
        assertEquals("3F000", stateOf("DROP TRIGGER zwj_trg ON zwj_nosuchschema.zwj_tt"));
        assertEquals("42P01", stateOf("DROP TRIGGER zwj_trg ON zwj_nosuchtable"));
    }
}
