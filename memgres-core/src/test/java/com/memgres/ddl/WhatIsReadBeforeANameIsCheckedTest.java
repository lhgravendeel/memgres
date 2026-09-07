package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What CREATE TABLE reads before it asks whether the name is free, and how long a name may be.
 *
 * <p>PostgreSQL reads a definition through before it looks at the name, so a statement that is
 * wrong in both ways is reported as wrong rather than as a collision. Asked the other way round,
 * a definition with a bad identity option or a key over a column that is not there was reported
 * as a name already taken, and the fault in what was written was never mentioned. IF NOT EXISTS
 * is the exception: it stops at the name, and what follows is never read at all.
 *
 * <p>A name holds sixty-three bytes, and the limit is in bytes rather than characters -- a name of
 * forty two-byte characters is eighty bytes, and PostgreSQL keeps thirty-one of them. Counted in
 * characters, such a name was kept whole, so the catalogue reported a name longer than any server
 * can hold. A character is never split to reach the limit exactly.
 *
 * <p>And a default privilege belongs to the role that set it aside, so REVOKE takes back that
 * role's and no other's.
 */
class WhatIsReadBeforeANameIsCheckedTest {

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

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** The definition is read first, so its own fault is the one reported. */
    @Test
    void whichFaultIsReportedFirst() throws SQLException {
        exec("CREATE TABLE zwb_t (i int, j int)");
        assertEquals("22023",
                stateOf("CREATE TABLE zwb_t (i int GENERATED ALWAYS AS IDENTITY (CACHE 0), j int)"));
        assertTrue(messageOf("CREATE TABLE zwb_t (i int GENERATED ALWAYS AS IDENTITY"
                + " (INCREMENT BY 0), j int)").contains("INCREMENT must not be zero"));
        assertEquals("42703", stateOf("CREATE TABLE zwb_t (a int, UNIQUE (nosuchcol))"));
        assertEquals("42703", stateOf("CREATE TABLE zwb_t (a int, PRIMARY KEY (nosuchcol))"));
        assertEquals("42704", stateOf("CREATE TABLE zwb_t (a nosuchtype)"));
        assertEquals("42701", stateOf("CREATE TABLE zwb_t (a int, a int)"));
        // A definition with nothing wrong in it is the collision it was.
        assertEquals("42P07", stateOf("CREATE TABLE zwb_t (a int)"));
        // IF NOT EXISTS stops at the name and never reads what follows.
        assertNull(stateOf("CREATE TABLE IF NOT EXISTS zwb_t (a nosuchtype)"));
        assertNull(stateOf("CREATE TABLE IF NOT EXISTS zwb_t (a int, a int)"));
        exec("DROP TABLE zwb_t");
    }

    /** A name is sixty-three bytes, not sixty-three characters. */
    @Test
    void howMuchOfANameIsKept() throws SQLException {
        exec("CREATE TABLE zwb_w (\"ÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜÜ\" int)");
        assertEquals("62|31",
                one("SELECT octet_length(column_name)::text || '|' || length(column_name)::text"
                        + " FROM information_schema.columns WHERE table_name='zwb_w'"));
        exec("DROP TABLE zwb_w");
        // A cast to name is cut the same way, and never in the middle of a character.
        assertEquals("62|31", one("SELECT octet_length(repeat('Ü',40)::name)::text || '|'"
                + " || length(repeat('Ü',40)::name)::text"));
        assertEquals("62", one("SELECT octet_length((repeat('a',62)||'Ü')::name)::text"));
        // Sixty-three single-byte characters still fit whole.
        assertEquals("63|63", one("SELECT octet_length(repeat('a',80)::name)::text || '|'"
                + " || length(repeat('a',80)::name)::text"));
    }

    /** A role takes back the default it set aside, and nobody else's. */
    @Test
    void whoseDefaultARevokeTakesBack() throws SQLException {
        exec("CREATE ROLE zwb_a");
        exec("CREATE ROLE zwb_b");
        exec("CREATE ROLE zwb_g");
        exec("CREATE SCHEMA zwb_s");
        exec("ALTER DEFAULT PRIVILEGES FOR ROLE zwb_a IN SCHEMA zwb_s"
                + " GRANT SELECT ON TABLES TO zwb_g");
        exec("ALTER DEFAULT PRIVILEGES FOR ROLE zwb_b IN SCHEMA zwb_s"
                + " GRANT SELECT ON TABLES TO zwb_g");
        assertEquals("2", one("SELECT count(*)::text FROM pg_default_acl d"
                + " JOIN pg_namespace n ON n.oid=d.defaclnamespace WHERE n.nspname='zwb_s'"));
        // The session's own role set neither of these aside, so it takes neither back.
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA zwb_s REVOKE SELECT ON TABLES FROM zwb_g");
        assertEquals("2", one("SELECT count(*)::text FROM pg_default_acl d"
                + " JOIN pg_namespace n ON n.oid=d.defaclnamespace WHERE n.nspname='zwb_s'"));
        exec("ALTER DEFAULT PRIVILEGES FOR ROLE zwb_a IN SCHEMA zwb_s"
                + " REVOKE SELECT ON TABLES FROM zwb_g");
        assertEquals("1", one("SELECT count(*)::text FROM pg_default_acl d"
                + " JOIN pg_namespace n ON n.oid=d.defaclnamespace WHERE n.nspname='zwb_s'"));
        exec("ALTER DEFAULT PRIVILEGES FOR ROLE zwb_b IN SCHEMA zwb_s"
                + " REVOKE SELECT ON TABLES FROM zwb_g");
        assertEquals("0", one("SELECT count(*)::text FROM pg_default_acl d"
                + " JOIN pg_namespace n ON n.oid=d.defaclnamespace WHERE n.nspname='zwb_s'"));
        exec("DROP SCHEMA zwb_s");
        exec("DROP ROLE zwb_a");
        exec("DROP ROLE zwb_b");
        exec("DROP ROLE zwb_g");
    }
}
