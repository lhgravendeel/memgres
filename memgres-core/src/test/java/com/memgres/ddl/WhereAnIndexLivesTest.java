package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Where an index lives, and where a rolled-back ALTER leaves a sequence.
 *
 * <p>An index lives in the schema of the relation it indexes, not the one the session happens to
 * be writing in: an unqualified relation found further along the search path takes its index with
 * it. Filed under the default schema, the index was recorded in a schema that did not hold the
 * relation, and its name was taken there instead.
 *
 * <p>RESTART is an ALTER like any other, and a rolled-back one never happened. Only the sequence's
 * settings were put back, so a rolled-back RESTART left it standing where the RESTART had moved it
 * and the next value came from a transaction that had been undone.
 */
class WhereAnIndexLivesTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** The index goes where the relation is. */
    @Test
    void whichSchemaHoldsTheIndex() throws SQLException {
        exec("CREATE SCHEMA zwj_s");
        exec("CREATE TABLE zwj_s.zwj_t (id int)");
        exec("SET search_path TO public, zwj_s");
        exec("CREATE INDEX zwj_idx ON zwj_t (id)");
        assertEquals("zwj_s|zwj_idx|zwj_t",
                one("SELECT schemaname || '|' || indexname || '|' || tablename"
                        + " FROM pg_indexes WHERE indexname = 'zwj_idx'"));
        exec("SET search_path TO public");
        exec("DROP SCHEMA zwj_s CASCADE");
    }

    /** A rolled-back RESTART leaves the sequence where it stood. */
    @Test
    void whereARolledBackRestartLeavesIt() throws SQLException {
        exec("CREATE SEQUENCE zwj_q");
        assertEquals("1", one("SELECT nextval('zwj_q')::text"));
        exec("BEGIN");
        exec("ALTER SEQUENCE zwj_q RESTART WITH 900");
        exec("ROLLBACK");
        assertEquals("2", one("SELECT nextval('zwj_q')::text"));
        // One that commits does move it.
        exec("ALTER SEQUENCE zwj_q RESTART WITH 900");
        assertEquals("900", one("SELECT nextval('zwj_q')::text"));
        exec("DROP SEQUENCE zwj_q");
    }
}
