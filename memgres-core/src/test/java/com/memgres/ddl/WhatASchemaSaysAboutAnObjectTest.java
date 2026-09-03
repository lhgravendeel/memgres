package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which schema an object is in, and what that settles.
 *
 * <p>A sequence a column owns goes when the column goes, so it has to be somewhere the column's
 * own schema can take it with: PostgreSQL refuses to link one across schemas. Linked anyway --
 * because the schema a three-part name wrote was dropped and the relation looked for in every
 * schema -- a sequence in public was answered for by {@code pg_get_serial_sequence} for a column of
 * a table somewhere else.
 *
 * <p>A relation is visible when the search path reaches it by its bare name, which is the path the
 * session set and not the path with public added to the end.
 *
 * <p>And a conversion has no behaviour here, but its name is remembered: dropping one that was
 * never created is refused rather than reported as done.
 */
class WhatASchemaSaysAboutAnObjectTest {

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

    /** A sequence is linked only to a column of a table in its own schema. */
    @Test
    void whereASequenceMayBeLinked() throws SQLException {
        exec("CREATE SCHEMA zws_o");
        exec("CREATE TABLE zws_o.t (a int)");
        assertEquals("55000", stateOf("CREATE SEQUENCE zws_s1 OWNED BY zws_o.t.a"));
        assertTrue(messageOf("CREATE SEQUENCE zws_s2 OWNED BY zws_o.t.a")
                .contains("sequence must be in same schema as table it is linked to"));
        assertNull(one("SELECT pg_get_serial_sequence('zws_o.t','a')"));
        // In the table's own schema it is linked.
        exec("CREATE SEQUENCE zws_o.zws_ok OWNED BY zws_o.t.a");
        assertEquals("zws_o.zws_ok", one("SELECT pg_get_serial_sequence('zws_o.t','a')"));
        exec("DROP SCHEMA zws_o CASCADE");
    }

    /** A relation is visible when the path the session set reaches it. */
    @Test
    void whichRelationsAreVisible() throws SQLException {
        exec("CREATE SCHEMA zws_h");
        exec("CREATE TABLE zws_h.t (a int)");
        exec("CREATE TABLE zws_x (i int)");
        exec("SET search_path TO zws_h");
        try {
            assertEquals("false", one("SELECT pg_table_is_visible('public.zws_x'::regclass)::text"));
            assertEquals("true", one("SELECT pg_table_is_visible('zws_h.t'::regclass)::text"));
        } finally {
            exec("RESET search_path");
        }
        assertEquals("true", one("SELECT pg_table_is_visible('public.zws_x'::regclass)::text"));
        exec("DROP TABLE zws_x");
        exec("DROP SCHEMA zws_h CASCADE");
    }

    /** A conversion that was never created is not there to drop. */
    @Test
    void whichConversionsAreThere() throws SQLException {
        assertEquals("42704", stateOf("DROP CONVERSION zws_nosuch"));
        assertTrue(messageOf("DROP CONVERSION zws_nosuch")
                .contains("conversion \"zws_nosuch\" does not exist"));
        assertNull(stateOf("DROP CONVERSION IF EXISTS zws_nosuch"));
        exec("CREATE CONVERSION zws_c FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8");
        assertNull(stateOf("DROP CONVERSION zws_c"));
        assertEquals("42704", stateOf("DROP CONVERSION zws_c"));
    }

    /** A role holds one setting per parameter, and a reset takes it away. */
    @Test
    void whatSettingsARoleCarries() throws SQLException {
        exec("CREATE ROLE zws_r");
        assertNull(rolconfig());
        exec("ALTER ROLE zws_r SET search_path = public");
        assertEquals("{search_path=public}", rolconfig());
        exec("ALTER ROLE zws_r RESET search_path");
        assertNull(rolconfig());
        // A list is one value, written with the commas that made it one.
        exec("ALTER ROLE zws_r SET search_path = a, b");
        assertEquals("{\"search_path=a, b\"}", rolconfig());
        exec("ALTER ROLE zws_r SET work_mem = '8MB'");
        assertEquals("{\"search_path=a, b\",work_mem=8MB}", rolconfig());
        exec("ALTER ROLE zws_r RESET ALL");
        assertNull(rolconfig());
        exec("DROP ROLE zws_r");
    }

    private static String rolconfig() throws SQLException {
        return one("SELECT rolconfig::text FROM pg_roles WHERE rolname='zws_r'");
    }

    /** A handler may ask where it is running. */
    @Test
    void whatAHandlerMayAskFor() {
        assertNull(stateOf("DO $$ declare v text; begin perform 1/0;"
                + " exception when others then get stacked diagnostics v = pg_context; end $$"));
        assertNull(stateOf("DO $$ declare v text; begin perform 1/0; exception when others then"
                + " get stacked diagnostics v = pg_exception_context; end $$"));
        // What belongs to a handler is still not offered outside one.
        assertEquals("42601", stateOf("DO $$ declare v text; begin"
                + " get current diagnostics v = pg_exception_context; end $$"));
    }

    /** A row with a null field settles a quantified comparison to unknown, not to false. */
    @Test
    void whatARowWithNothingInItComparesAs() throws SQLException {
        assertNull(one("SELECT ((NULL::int, 1) = ANY (SELECT 1, 1))::text"));
        assertEquals("false", one("SELECT ((NULL::int, 1) = ANY (SELECT 1, 2))::text"));
        assertNull(one("SELECT ((NULL::int, 1) = ANY (SELECT 1, 2 UNION ALL SELECT 1, 1))::text"));
        assertNull(one("SELECT ((NULL::int, 1) <> ALL (SELECT 1, 1))::text"));
        // A row that agrees or disagrees on every field settles as it always did.
        assertEquals("true", one("SELECT ((1, 1) = ANY (SELECT 1, 1))::text"));
        assertEquals("false", one("SELECT ((1, 1) = ANY (SELECT 2, 2))::text"));
    }
}
