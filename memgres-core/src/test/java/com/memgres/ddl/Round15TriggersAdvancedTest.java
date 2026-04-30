package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category C (triggers): trigger features that must fire
 * observable side effects.
 *
 * Covers:
 *  - WHEN clause evaluated at fire time
 *  - TRUNCATE trigger event wired
 *  - INSTEAD OF on view rewrites DML
 *  - TG_* variables (TG_NAME, TG_LEVEL, TG_TABLE_SCHEMA, TG_RELID, TG_DEPTH)
 *  - Transition tables (REFERENCING OLD/NEW TABLE AS)
 *  - CONSTRAINT TRIGGER deferral
 *  - UPDATE OF column-list
 *  - DISABLE / ENABLE TRIGGER
 */
class Round15TriggersAdvancedTest {

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
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. WHEN clause evaluated at fire time
    // =========================================================================

    @Test
    void trigger_when_clause_filters_executions() throws SQLException {
        exec("CREATE TABLE r15_t_when (id int, v int)");
        exec("CREATE TABLE r15_t_when_log (id int)");
        exec("CREATE OR REPLACE FUNCTION r15_fn_when() RETURNS trigger AS $$"
                + " BEGIN INSERT INTO r15_t_when_log VALUES (NEW.id); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_when AFTER INSERT ON r15_t_when "
                + "FOR EACH ROW WHEN (NEW.v > 10) EXECUTE FUNCTION r15_fn_when()");

        exec("INSERT INTO r15_t_when VALUES (1, 5)");   // WHEN false
        exec("INSERT INTO r15_t_when VALUES (2, 20)");  // WHEN true

        int n = scalarInt("SELECT count(*)::int FROM r15_t_when_log");
        assertEquals(1, n, "WHEN clause must filter — only id=2 should log");
    }

    // =========================================================================
    // B. TRUNCATE trigger event
    // =========================================================================

    @Test
    void truncate_trigger_fires() throws SQLException {
        exec("CREATE TABLE r15_t_tr (id int)");
        exec("CREATE TABLE r15_t_tr_log (ev text)");
        exec("CREATE OR REPLACE FUNCTION r15_fn_tr() RETURNS trigger AS $$"
                + " BEGIN INSERT INTO r15_t_tr_log VALUES (TG_OP); RETURN NULL; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_tr AFTER TRUNCATE ON r15_t_tr "
                + "FOR EACH STATEMENT EXECUTE FUNCTION r15_fn_tr()");

        exec("INSERT INTO r15_t_tr VALUES (1),(2),(3)");
        exec("TRUNCATE r15_t_tr");

        int n = scalarInt("SELECT count(*)::int FROM r15_t_tr_log WHERE ev='TRUNCATE'");
        assertEquals(1, n, "TRUNCATE trigger must fire with TG_OP='TRUNCATE'");
    }

    // =========================================================================
    // C. INSTEAD OF trigger on view
    // =========================================================================

    @Test
    void instead_of_trigger_rewrites_view_insert() throws SQLException {
        exec("CREATE TABLE r15_t_iot_real (id int, v text)");
        exec("CREATE VIEW r15_t_iot_view AS SELECT id, v FROM r15_t_iot_real");
        exec("CREATE OR REPLACE FUNCTION r15_fn_iot() RETURNS trigger AS $$"
                + " BEGIN INSERT INTO r15_t_iot_real VALUES (NEW.id, NEW.v); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_iot INSTEAD OF INSERT ON r15_t_iot_view "
                + "FOR EACH ROW EXECUTE FUNCTION r15_fn_iot()");

        // Inserts into view must be rewritten by INSTEAD OF trigger
        exec("INSERT INTO r15_t_iot_view VALUES (1, 'hello')");

        int n = scalarInt("SELECT count(*)::int FROM r15_t_iot_real WHERE id=1 AND v='hello'");
        assertEquals(1, n,
                "INSTEAD OF INSERT on view must rewrite to trigger-driven INSERT");
    }

    // =========================================================================
    // D. TG_* variables in PL/pgSQL scope
    // =========================================================================

    @Test
    void tg_name_and_level_available() throws SQLException {
        exec("CREATE TABLE r15_tg_ctx (id int)");
        exec("CREATE TABLE r15_tg_ctx_log (trigger_name text, lvl text, tbl_schema text, op text)");
        exec("CREATE OR REPLACE FUNCTION r15_fn_ctx() RETURNS trigger AS $$"
                + " BEGIN INSERT INTO r15_tg_ctx_log VALUES (TG_NAME, TG_LEVEL, TG_TABLE_SCHEMA, TG_OP); "
                + " RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_ctx AFTER INSERT ON r15_tg_ctx "
                + "FOR EACH ROW EXECUTE FUNCTION r15_fn_ctx()");

        exec("INSERT INTO r15_tg_ctx VALUES (1)");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT trigger_name, lvl, tbl_schema, op FROM r15_tg_ctx_log")) {
            assertTrue(rs.next());
            assertEquals("tr_ctx", rs.getString("trigger_name"));
            assertEquals("ROW", rs.getString("lvl"));
            assertNotNull(rs.getString("tbl_schema"));
            assertEquals("INSERT", rs.getString("op"));
        }
    }

    @Test
    void tg_relid_is_oid() throws SQLException {
        exec("CREATE TABLE r15_tg_relid (id int)");
        exec("CREATE TABLE r15_tg_relid_log (reloid oid)");
        exec("CREATE OR REPLACE FUNCTION r15_fn_relid() RETURNS trigger AS $$"
                + " BEGIN INSERT INTO r15_tg_relid_log VALUES (TG_RELID); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_relid AFTER INSERT ON r15_tg_relid "
                + "FOR EACH ROW EXECUTE FUNCTION r15_fn_relid()");

        exec("INSERT INTO r15_tg_relid VALUES (1)");

        int rowCount = scalarInt(
                "SELECT count(*)::int FROM r15_tg_relid_log WHERE reloid = 'r15_tg_relid'::regclass::oid");
        assertEquals(1, rowCount,
                "TG_RELID must equal relid of triggered table");
    }

    // =========================================================================
    // E. Transition tables
    // =========================================================================

    @Test
    void transition_tables_expose_old_new_set() throws SQLException {
        exec("CREATE TABLE r15_tt (id int, v int)");
        exec("CREATE TABLE r15_tt_log (sum_new int, cnt_new int)");
        exec("CREATE OR REPLACE FUNCTION r15_fn_tt() RETURNS trigger AS $$"
                + " BEGIN "
                + "   INSERT INTO r15_tt_log "
                + "   SELECT COALESCE(SUM(v),0)::int, COUNT(*)::int FROM new_rows; "
                + "   RETURN NULL; "
                + " END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_tt AFTER INSERT ON r15_tt "
                + "REFERENCING NEW TABLE AS new_rows "
                + "FOR EACH STATEMENT EXECUTE FUNCTION r15_fn_tt()");

        exec("INSERT INTO r15_tt VALUES (1,10),(2,20),(3,30)");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT sum_new, cnt_new FROM r15_tt_log")) {
            assertTrue(rs.next());
            assertEquals(60, rs.getInt("sum_new"));
            assertEquals(3, rs.getInt("cnt_new"));
        }
    }

    // =========================================================================
    // F. CONSTRAINT TRIGGER (deferrable)
    // =========================================================================

    @Test
    void constraint_trigger_fires_at_commit_by_default() throws SQLException {
        exec("CREATE TABLE r15_ct (id int)");
        exec("CREATE TABLE r15_ct_log (note text)");
        exec("CREATE OR REPLACE FUNCTION r15_fn_ct() RETURNS trigger AS $$"
                + " BEGIN INSERT INTO r15_ct_log VALUES ('fired'); RETURN NULL; END; $$ LANGUAGE plpgsql");
        exec("CREATE CONSTRAINT TRIGGER tr_ct AFTER INSERT ON r15_ct "
                + "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION r15_fn_ct()");

        conn.setAutoCommit(false);
        exec("INSERT INTO r15_ct VALUES (1)");
        // Before commit, deferred trigger hasn't fired yet
        int n = scalarInt("SELECT count(*)::int FROM r15_ct_log");
        assertEquals(0, n,
                "DEFERRED constraint trigger should not fire until commit");
        conn.commit();
        conn.setAutoCommit(true);
        int after = scalarInt("SELECT count(*)::int FROM r15_ct_log");
        assertEquals(1, after, "Constraint trigger should fire at commit");
    }

    // =========================================================================
    // G. UPDATE OF column list
    // =========================================================================

    @Test
    void update_of_column_filters_trigger_fires() throws SQLException {
        exec("CREATE TABLE r15_uof (id int, a int, b int)");
        exec("CREATE TABLE r15_uof_log (id int)");
        exec("CREATE OR REPLACE FUNCTION r15_fn_uof() RETURNS trigger AS $$"
                + " BEGIN INSERT INTO r15_uof_log VALUES (NEW.id); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_uof AFTER UPDATE OF a ON r15_uof "
                + "FOR EACH ROW EXECUTE FUNCTION r15_fn_uof()");

        exec("INSERT INTO r15_uof VALUES (1,10,20),(2,30,40)");
        exec("UPDATE r15_uof SET b=999 WHERE id=1");  // no column 'a' updated
        int n1 = scalarInt("SELECT count(*)::int FROM r15_uof_log");
        assertEquals(0, n1, "Trigger UPDATE OF a should not fire when only b changed");

        exec("UPDATE r15_uof SET a=50 WHERE id=2");    // column 'a' updated
        int n2 = scalarInt("SELECT count(*)::int FROM r15_uof_log");
        assertEquals(1, n2, "Trigger UPDATE OF a should fire when a changed");
    }

    // =========================================================================
    // H. DISABLE / ENABLE TRIGGER
    // =========================================================================

    @Test
    void disable_trigger_stops_firing() throws SQLException {
        exec("CREATE TABLE r15_dis (id int)");
        exec("CREATE TABLE r15_dis_log (id int)");
        exec("CREATE OR REPLACE FUNCTION r15_fn_dis() RETURNS trigger AS $$"
                + " BEGIN INSERT INTO r15_dis_log VALUES (NEW.id); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_dis AFTER INSERT ON r15_dis "
                + "FOR EACH ROW EXECUTE FUNCTION r15_fn_dis()");

        exec("INSERT INTO r15_dis VALUES (1)");
        exec("ALTER TABLE r15_dis DISABLE TRIGGER tr_dis");
        exec("INSERT INTO r15_dis VALUES (2)");

        int n = scalarInt("SELECT count(*)::int FROM r15_dis_log");
        assertEquals(1, n, "DISABLE TRIGGER must stop further firings");

        exec("ALTER TABLE r15_dis ENABLE TRIGGER tr_dis");
        exec("INSERT INTO r15_dis VALUES (3)");

        int n2 = scalarInt("SELECT count(*)::int FROM r15_dis_log");
        assertEquals(2, n2, "ENABLE TRIGGER must restart firings");
    }

    @Test
    void pg_trigger_tgenabled_reflects_state() throws SQLException {
        exec("CREATE TABLE r15_tge (id int)");
        exec("CREATE OR REPLACE FUNCTION r15_fn_tge() RETURNS trigger AS $$"
                + " BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_tge AFTER INSERT ON r15_tge "
                + "FOR EACH ROW EXECUTE FUNCTION r15_fn_tge()");

        String st = scalarString(
                "SELECT tgenabled::text FROM pg_trigger WHERE tgname='tr_tge'");
        // PG: 'O' = enabled (origin), 'D' = disabled, 'R' = replica, 'A' = always
        assertTrue(st.equals("O") || st.equals("A"),
                "pg_trigger.tgenabled should be 'O' for enabled trigger; got " + st);

        exec("ALTER TABLE r15_tge DISABLE TRIGGER tr_tge");
        String st2 = scalarString(
                "SELECT tgenabled::text FROM pg_trigger WHERE tgname='tr_tge'");
        assertEquals("D", st2, "DISABLE TRIGGER must set tgenabled='D'");
    }

    // =========================================================================
    // I. information_schema.triggers
    // =========================================================================

    @Test
    void information_schema_triggers_view() throws SQLException {
        exec("CREATE TABLE r15_is_tg (id int)");
        exec("CREATE OR REPLACE FUNCTION r15_fn_is_tg() RETURNS trigger AS $$"
                + " BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_is_tg AFTER INSERT ON r15_is_tg "
                + "FOR EACH ROW EXECUTE FUNCTION r15_fn_is_tg()");

        int n = scalarInt(
                "SELECT count(*)::int FROM information_schema.triggers WHERE trigger_name='tr_is_tg'");
        assertTrue(n >= 1,
                "information_schema.triggers should have a row for the created trigger");
    }
}
