package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: Event Triggers — completely unimplemented in Memgres.
 *
 * PG 9.3+ event triggers fire on DDL events. Memgres has no parser support,
 * no pg_event_trigger catalog, no dispatch path, and no pg_event_trigger_*
 * helper functions. Tests here pin down the PG 18 observable behavior.
 *
 * Expected to fail today. Do not modify Memgres code in this turn.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class Round14EventTriggersTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getInt(1);
        }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. pg_event_trigger catalog exists
    // =========================================================================

    @Test
    @Order(1)
    void pg_event_trigger_catalog_exists() throws SQLException {
        // Must be queryable; empty by default (no event triggers created yet).
        assertEquals(0, scalarInt("SELECT count(*)::int FROM pg_event_trigger"));
    }

    @Test
    void pg_event_trigger_expected_columns() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT column_name FROM information_schema.columns "
                             + "WHERE table_schema = 'pg_catalog' AND table_name = 'pg_event_trigger' "
                             + "ORDER BY column_name")) {
            int n = 0;
            while (rs.next()) n++;
            // PG defines evtname, evtevent, evtowner, evtfoid, evtenabled, evttags, oid
            assertTrue(n >= 6, "pg_event_trigger must expose PG columns; got " + n);
        }
    }

    // =========================================================================
    // B. CREATE EVENT TRIGGER parsing
    // =========================================================================

    @Test
    void create_event_trigger_ddl_command_start() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r14_et_fn1() RETURNS event_trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN END $$");
        // Must accept CREATE EVENT TRIGGER statement.
        exec("CREATE EVENT TRIGGER r14_et1 ON ddl_command_start EXECUTE FUNCTION r14_et_fn1()");
        assertEquals(1, scalarInt("SELECT count(*)::int FROM pg_event_trigger WHERE evtname = 'r14_et1'"));
    }

    @Test
    void create_event_trigger_ddl_command_end() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r14_et_fn2() RETURNS event_trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN END $$");
        exec("CREATE EVENT TRIGGER r14_et2 ON ddl_command_end EXECUTE FUNCTION r14_et_fn2()");
        assertEquals("ddl_command_end",
                scalarString("SELECT evtevent FROM pg_event_trigger WHERE evtname = 'r14_et2'"));
    }

    @Test
    void create_event_trigger_sql_drop() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r14_et_fn3() RETURNS event_trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN END $$");
        exec("CREATE EVENT TRIGGER r14_et3 ON sql_drop EXECUTE FUNCTION r14_et_fn3()");
        assertEquals("sql_drop",
                scalarString("SELECT evtevent FROM pg_event_trigger WHERE evtname = 'r14_et3'"));
    }

    @Test
    void create_event_trigger_table_rewrite() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r14_et_fn4() RETURNS event_trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN END $$");
        exec("CREATE EVENT TRIGGER r14_et4 ON table_rewrite EXECUTE FUNCTION r14_et_fn4()");
        assertEquals("table_rewrite",
                scalarString("SELECT evtevent FROM pg_event_trigger WHERE evtname = 'r14_et4'"));
    }

    @Test
    void create_event_trigger_when_tag_filter() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r14_et_fn5() RETURNS event_trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN END $$");
        // WHEN TAG IN (...) filter
        exec("CREATE EVENT TRIGGER r14_et5 ON ddl_command_start "
                + "WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE') "
                + "EXECUTE FUNCTION r14_et_fn5()");
        assertEquals(1, scalarInt("SELECT count(*)::int FROM pg_event_trigger WHERE evtname = 'r14_et5'"));
    }

    // =========================================================================
    // C. Event trigger firing (fires on DDL)
    // =========================================================================

    @Test
    void event_trigger_fires_on_create_table() throws SQLException {
        exec("CREATE TABLE r14_et_log (tag text, evt text, t timestamptz DEFAULT now())");
        exec("CREATE OR REPLACE FUNCTION r14_et_fire_fn() RETURNS event_trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN "
                + "  INSERT INTO r14_et_log(tag, evt) VALUES (tg_tag, tg_event); "
                + "END $$");
        exec("CREATE EVENT TRIGGER r14_et_fire ON ddl_command_start "
                + "EXECUTE FUNCTION r14_et_fire_fn()");
        exec("CREATE TABLE r14_et_target (id int)");
        // Event trigger should have fired; r14_et_log should contain a row.
        assertTrue(scalarInt("SELECT count(*)::int FROM r14_et_log") >= 1,
                "event trigger should have fired on CREATE TABLE");
    }

    // =========================================================================
    // D. pg_event_trigger_ddl_commands() helper
    // =========================================================================

    @Test
    void pg_event_trigger_ddl_commands_exists() throws SQLException {
        // Helper function must exist; can't be called outside an event trigger body.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_event_trigger_ddl_commands'")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void pg_event_trigger_dropped_objects_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_event_trigger_dropped_objects'")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void pg_event_trigger_table_rewrite_oid_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_proc WHERE proname = 'pg_event_trigger_table_rewrite_oid'")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    // =========================================================================
    // E. ALTER / DROP EVENT TRIGGER
    // =========================================================================

    @Test
    void alter_event_trigger_disable() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r14_alt_fn() RETURNS event_trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN END $$");
        exec("CREATE EVENT TRIGGER r14_alt ON ddl_command_start EXECUTE FUNCTION r14_alt_fn()");
        exec("ALTER EVENT TRIGGER r14_alt DISABLE");
        assertEquals("D",
                scalarString("SELECT evtenabled::text FROM pg_event_trigger WHERE evtname = 'r14_alt'"));
    }

    @Test
    void alter_event_trigger_enable() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r14_alt2_fn() RETURNS event_trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN END $$");
        exec("CREATE EVENT TRIGGER r14_alt2 ON ddl_command_start EXECUTE FUNCTION r14_alt2_fn()");
        exec("ALTER EVENT TRIGGER r14_alt2 DISABLE");
        exec("ALTER EVENT TRIGGER r14_alt2 ENABLE");
        assertEquals("O",
                scalarString("SELECT evtenabled::text FROM pg_event_trigger WHERE evtname = 'r14_alt2'"));
    }

    @Test
    void alter_event_trigger_rename() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r14_ren_fn() RETURNS event_trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN END $$");
        exec("CREATE EVENT TRIGGER r14_ren_old ON ddl_command_start EXECUTE FUNCTION r14_ren_fn()");
        exec("ALTER EVENT TRIGGER r14_ren_old RENAME TO r14_ren_new");
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_event_trigger WHERE evtname = 'r14_ren_new'"));
    }

    @Test
    void drop_event_trigger_works() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r14_drop_fn() RETURNS event_trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN END $$");
        exec("CREATE EVENT TRIGGER r14_drop_et ON ddl_command_start EXECUTE FUNCTION r14_drop_fn()");
        exec("DROP EVENT TRIGGER r14_drop_et");
        assertEquals(0, scalarInt(
                "SELECT count(*)::int FROM pg_event_trigger WHERE evtname = 'r14_drop_et'"));
    }

    @Test
    void drop_event_trigger_if_exists() throws SQLException {
        // Must not error even though no such trigger exists.
        exec("DROP EVENT TRIGGER IF EXISTS r14_no_such_trigger");
    }

    // =========================================================================
    // F. event_trigger return type
    // =========================================================================

    @Test
    void event_trigger_return_type_recognized() throws SQLException {
        // PG recognizes event_trigger as a special pseudo-type.
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_type WHERE typname = 'event_trigger'"));
    }

    // =========================================================================
    // G. Invalid event name rejected
    // =========================================================================

    @Test
    void create_event_trigger_invalid_event_errors() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r14_bad_fn() RETURNS event_trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN END $$");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("CREATE EVENT TRIGGER r14_bad ON not_an_event EXECUTE FUNCTION r14_bad_fn()"));
        assertNotNull(ex.getMessage());
    }
}
