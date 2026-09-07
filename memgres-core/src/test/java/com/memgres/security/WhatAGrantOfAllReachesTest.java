package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a grant of ALL reaches, and what it does not.
 *
 * <p>ALL stands for every privilege of the kind, and the right to hand one on is a different
 * privilege from holding it: a grant of ALL without WITH GRANT OPTION answered yes to
 * "SELECT WITH GRANT OPTION", which the grantee had not been given.
 *
 * <p>And {@code ON ALL SEQUENCES IN SCHEMA} names every sequence the schema holds, the way the
 * clause for tables does. Recorded against the words themselves, the grant became a privilege on
 * a relation called "all sequences in schema s": nothing could be asked about any sequence, and
 * the role could not be dropped while the entry stood.
 */
class WhatAGrantOfAllReachesTest {

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

    /** Holding a privilege is not holding the right to hand it on. */
    @Test
    void whatAllGrants() throws SQLException {
        exec("CREATE TABLE zwk_t (a int)");
        exec("CREATE ROLE zwk_r");
        exec("GRANT ALL ON zwk_t TO zwk_r");
        assertEquals("true", one("SELECT has_table_privilege('zwk_r','zwk_t','SELECT')::text"));
        assertEquals("false",
                one("SELECT has_table_privilege('zwk_r','zwk_t','SELECT WITH GRANT OPTION')::text"));
        exec("GRANT SELECT ON zwk_t TO zwk_r WITH GRANT OPTION");
        assertEquals("true",
                one("SELECT has_table_privilege('zwk_r','zwk_t','SELECT WITH GRANT OPTION')::text"));
        assertEquals("false",
                one("SELECT has_table_privilege('zwk_r','zwk_t','INSERT WITH GRANT OPTION')::text"));
        exec("DROP TABLE zwk_t");
        exec("DROP ROLE zwk_r");
    }

    /** The clause names the objects, so each of them takes the grant. */
    @Test
    void whatAllSequencesInSchemaGrants() throws SQLException {
        exec("CREATE SCHEMA zwk_s");
        exec("CREATE ROLE zwk_sr");
        exec("CREATE SEQUENCE zwk_s.zwk_q");
        exec("CREATE FUNCTION zwk_s.zwk_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$");
        exec("GRANT USAGE ON ALL SEQUENCES IN SCHEMA zwk_s TO zwk_sr");
        exec("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA zwk_s TO zwk_sr");
        assertEquals("true",
                one("SELECT has_sequence_privilege('zwk_sr','zwk_s.zwk_q','USAGE')::text"));
        assertEquals("true",
                one("SELECT has_function_privilege('zwk_sr','zwk_s.zwk_f()','EXECUTE')::text"));
        exec("REVOKE USAGE ON ALL SEQUENCES IN SCHEMA zwk_s FROM zwk_sr");
        assertEquals("false",
                one("SELECT has_sequence_privilege('zwk_sr','zwk_s.zwk_q','USAGE')::text"));
        // And nothing hangs off the role, so it can go.
        exec("REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA zwk_s FROM zwk_sr");
        exec("DROP SCHEMA zwk_s CASCADE");
        exec("DROP ROLE zwk_sr");
    }
}
