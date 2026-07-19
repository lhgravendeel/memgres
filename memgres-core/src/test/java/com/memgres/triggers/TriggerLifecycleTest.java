package com.memgres.triggers;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for trigger lifecycle issues: C2, H2, H6, M8.
 */
class TriggerLifecycleTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void stop() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private List<String> query(String sql) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append(",");
                    sb.append(rs.getString(i));
                }
                rows.add(sb.toString());
            }
        }
        return rows;
    }

    // ---- C2: Triggers survive DROP TABLE ----

    @Test
    void drop_table_removes_triggers() throws SQLException {
        exec("CREATE TABLE t_c2_drop (id int PRIMARY KEY, val text)");
        exec("CREATE OR REPLACE FUNCTION tr_c2_fn() RETURNS trigger AS $$ BEGIN RAISE NOTICE 'fired'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_c2 BEFORE INSERT ON t_c2_drop FOR EACH ROW EXECUTE FUNCTION tr_c2_fn()");
        exec("DROP TABLE t_c2_drop");

        // pg_trigger should not show the trigger
        List<String> rows = query("SELECT count(*) FROM pg_trigger WHERE tgname = 'tr_c2'");
        assertEquals("0", rows.get(0));

        exec("DROP FUNCTION tr_c2_fn()");
    }

    @Test
    void recreated_table_does_not_inherit_old_triggers() throws SQLException {
        exec("CREATE TABLE t_c2_re (id int PRIMARY KEY, val text)");
        exec("CREATE OR REPLACE FUNCTION tr_c2_re_fn() RETURNS trigger AS $$ BEGIN NEW.val := 'triggered'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_c2_re BEFORE INSERT ON t_c2_re FOR EACH ROW EXECUTE FUNCTION tr_c2_re_fn()");
        exec("DROP TABLE t_c2_re");

        // Recreate table with same name — old trigger must NOT fire
        exec("CREATE TABLE t_c2_re (id int PRIMARY KEY, val text)");
        exec("INSERT INTO t_c2_re VALUES (1, 'original')");
        List<String> rows = query("SELECT val FROM t_c2_re");
        assertEquals("original", rows.get(0), "Old trigger should not fire on recreated table");

        exec("DROP TABLE t_c2_re");
        exec("DROP FUNCTION tr_c2_re_fn()");
    }

    // ---- H2: Statement-level triggers ----

    @Test
    void before_statement_trigger_fires() throws SQLException {
        exec("CREATE TABLE t_h2_bs (id int, val text)");
        exec("CREATE TABLE t_h2_bs_log (msg text)");
        exec("CREATE OR REPLACE FUNCTION bs_fn() RETURNS trigger AS $$ BEGIN INSERT INTO t_h2_bs_log VALUES ('BS:' || TG_OP); RETURN NULL; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_bs BEFORE INSERT ON t_h2_bs FOR EACH STATEMENT EXECUTE FUNCTION bs_fn()");
        exec("INSERT INTO t_h2_bs VALUES (1, 'a'), (2, 'b')");

        List<String> log = query("SELECT msg FROM t_h2_bs_log");
        assertEquals(1, log.size());
        assertEquals("BS:INSERT", log.get(0));

        exec("DROP TABLE t_h2_bs, t_h2_bs_log");
        exec("DROP FUNCTION bs_fn()");
    }

    @Test
    void after_row_triggers_queue_not_interleave() throws SQLException {
        exec("CREATE TABLE t_h2_q (id int, val text)");
        exec("CREATE TABLE t_h2_q_log (msg text, seq serial)");
        exec("CREATE OR REPLACE FUNCTION q_fn() RETURNS trigger AS $$ BEGIN INSERT INTO t_h2_q_log(msg) VALUES (TG_WHEN || ':' || TG_LEVEL || ':' || NEW.id); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_br BEFORE INSERT ON t_h2_q FOR EACH ROW EXECUTE FUNCTION q_fn()");
        exec("CREATE TRIGGER tr_ar AFTER INSERT ON t_h2_q FOR EACH ROW EXECUTE FUNCTION q_fn()");
        exec("INSERT INTO t_h2_q VALUES (1, 'a'), (2, 'b')");

        List<String> log = query("SELECT msg FROM t_h2_q_log ORDER BY seq");
        // PG order: BEFORE for each row, then AFTER for each row
        // BEFORE:ROW:1, BEFORE:ROW:2, AFTER:ROW:1, AFTER:ROW:2
        assertTrue(log.size() >= 4, "Expected at least 4 trigger firings, got " + log.size());
        assertEquals("BEFORE:ROW:1", log.get(0));
        assertEquals("BEFORE:ROW:2", log.get(1));
        assertEquals("AFTER:ROW:1", log.get(2));
        assertEquals("AFTER:ROW:2", log.get(3));

        exec("DROP TABLE t_h2_q, t_h2_q_log");
        exec("DROP FUNCTION q_fn()");
    }

    @Test
    void update_transition_tables_populated() throws SQLException {
        exec("CREATE TABLE t_h2_tr (id int, val text)");
        exec("INSERT INTO t_h2_tr VALUES (1, 'a'), (2, 'b')");
        exec("CREATE TABLE t_h2_tr_log (msg text)");
        exec("CREATE OR REPLACE FUNCTION upd_stmt_fn() RETURNS trigger AS $$ DECLARE r RECORD; BEGIN FOR r IN SELECT * FROM newtab LOOP INSERT INTO t_h2_tr_log VALUES ('new:' || r.id || ':' || r.val); END LOOP; FOR r IN SELECT * FROM oldtab LOOP INSERT INTO t_h2_tr_log VALUES ('old:' || r.id || ':' || r.val); END LOOP; RETURN NULL; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_upd AFTER UPDATE ON t_h2_tr REFERENCING OLD TABLE AS oldtab NEW TABLE AS newtab FOR EACH STATEMENT EXECUTE FUNCTION upd_stmt_fn()");
        exec("UPDATE t_h2_tr SET val = 'updated' WHERE id = 1");

        List<String> log = query("SELECT msg FROM t_h2_tr_log ORDER BY msg");
        assertTrue(log.contains("new:1:updated"), "Expected new transition row");
        assertTrue(log.contains("old:1:a"), "Expected old transition row");

        exec("DROP TABLE t_h2_tr, t_h2_tr_log");
        exec("DROP FUNCTION upd_stmt_fn()");
    }

    @Test
    void delete_transition_tables_populated() throws SQLException {
        exec("CREATE TABLE t_h2_dtr (id int, val text)");
        exec("INSERT INTO t_h2_dtr VALUES (1, 'a'), (2, 'b')");
        exec("CREATE TABLE t_h2_dtr_log (msg text)");
        exec("CREATE OR REPLACE FUNCTION del_stmt_fn() RETURNS trigger AS $$ DECLARE r RECORD; BEGIN FOR r IN SELECT * FROM oldtab LOOP INSERT INTO t_h2_dtr_log VALUES ('old:' || r.id || ':' || r.val); END LOOP; RETURN NULL; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_del AFTER DELETE ON t_h2_dtr REFERENCING OLD TABLE AS oldtab FOR EACH STATEMENT EXECUTE FUNCTION del_stmt_fn()");
        exec("DELETE FROM t_h2_dtr WHERE id = 1");

        List<String> log = query("SELECT msg FROM t_h2_dtr_log");
        assertEquals(1, log.size());
        assertEquals("old:1:a", log.get(0));

        exec("DROP TABLE t_h2_dtr, t_h2_dtr_log");
        exec("DROP FUNCTION del_stmt_fn()");
    }

    // ---- H6: OLD field access in COALESCE ----

    @Test
    void old_field_in_insert_trigger_returns_null() throws SQLException {
        exec("CREATE TABLE t_h6_coal (id int PRIMARY KEY, val text)");
        exec("CREATE OR REPLACE FUNCTION coal_fn() RETURNS trigger AS $$ BEGIN NEW.val := COALESCE(OLD.val, 'from_old_null'); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_h6 BEFORE INSERT ON t_h6_coal FOR EACH ROW EXECUTE FUNCTION coal_fn()");
        exec("INSERT INTO t_h6_coal VALUES (1, 'test')");

        List<String> rows = query("SELECT val FROM t_h6_coal");
        // OLD is null during INSERT, so OLD.val is null, COALESCE picks 'from_old_null'
        assertEquals("from_old_null", rows.get(0));

        exec("DROP TABLE t_h6_coal");
        exec("DROP FUNCTION coal_fn()");
    }

    // ---- M8: TG_ARGV/TG_NARGS ----

    @Test
    void tg_argv_and_tg_nargs_populated() throws SQLException {
        exec("CREATE TABLE t_m8_argv (id int)");
        exec("CREATE TABLE t_m8_argv_log (msg text)");
        exec("CREATE OR REPLACE FUNCTION argv_fn() RETURNS trigger AS $$ BEGIN INSERT INTO t_m8_argv_log VALUES ('nargs=' || TG_NARGS || ',arg0=' || TG_ARGV[0] || ',arg1=' || TG_ARGV[1]); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_argv BEFORE INSERT ON t_m8_argv FOR EACH ROW EXECUTE FUNCTION argv_fn('hello', 'world')");
        exec("INSERT INTO t_m8_argv VALUES (1)");

        List<String> log = query("SELECT msg FROM t_m8_argv_log");
        assertEquals(1, log.size());
        assertEquals("nargs=2,arg0=hello,arg1=world", log.get(0));

        exec("DROP TABLE t_m8_argv, t_m8_argv_log");
        exec("DROP FUNCTION argv_fn()");
    }

    @Test
    void triggers_fire_in_name_order() throws SQLException {
        exec("CREATE TABLE t_m8_ord (id int)");
        exec("CREATE TABLE t_m8_ord_log (msg text, seq serial)");
        exec("CREATE OR REPLACE FUNCTION ord_fn() RETURNS trigger AS $$ BEGIN INSERT INTO t_m8_ord_log(msg) VALUES (TG_NAME); RETURN NEW; END; $$ LANGUAGE plpgsql");
        // Create z first, then a — PG fires in alphabetical order
        exec("CREATE TRIGGER z_trigger BEFORE INSERT ON t_m8_ord FOR EACH ROW EXECUTE FUNCTION ord_fn()");
        exec("CREATE TRIGGER a_trigger BEFORE INSERT ON t_m8_ord FOR EACH ROW EXECUTE FUNCTION ord_fn()");
        exec("INSERT INTO t_m8_ord VALUES (1)");

        List<String> log = query("SELECT msg FROM t_m8_ord_log ORDER BY seq");
        assertEquals(2, log.size());
        assertEquals("a_trigger", log.get(0), "Triggers should fire in alphabetical order");
        assertEquals("z_trigger", log.get(1));

        exec("DROP TABLE t_m8_ord, t_m8_ord_log");
        exec("DROP FUNCTION ord_fn()");
    }

    @Test
    void pg_trigger_tgtype_correct() throws SQLException {
        exec("CREATE TABLE t_m8_type (id int)");
        exec("CREATE OR REPLACE FUNCTION noop_fn() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_after_upd AFTER UPDATE ON t_m8_type FOR EACH ROW EXECUTE FUNCTION noop_fn()");

        // PG tgtype for AFTER UPDATE ROW: ROW(1) + UPDATE(16) = 17
        List<String> rows = query("SELECT tgtype FROM pg_trigger WHERE tgname = 'tr_after_upd'");
        assertEquals(1, rows.size());
        assertEquals("17", rows.get(0), "AFTER UPDATE ROW tgtype should be 17");

        exec("DROP TABLE t_m8_type");
        exec("DROP FUNCTION noop_fn()");
    }

    @Test
    void tg_table_name_reports_leaf_partition() throws SQLException {
        exec("CREATE TABLE t_m8_part (id int, val text) PARTITION BY RANGE (id)");
        exec("CREATE TABLE t_m8_part_1 PARTITION OF t_m8_part FOR VALUES FROM (1) TO (100)");
        exec("CREATE TABLE t_m8_part_log (msg text)");
        exec("CREATE OR REPLACE FUNCTION part_fn() RETURNS trigger AS $$ BEGIN INSERT INTO t_m8_part_log VALUES (TG_TABLE_NAME); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER tr_part BEFORE INSERT ON t_m8_part FOR EACH ROW EXECUTE FUNCTION part_fn()");
        exec("INSERT INTO t_m8_part VALUES (1, 'test')");

        List<String> log = query("SELECT msg FROM t_m8_part_log");
        assertEquals(1, log.size());
        // PG: TG_TABLE_NAME is the leaf partition table, not the parent
        assertEquals("t_m8_part_1", log.get(0), "TG_TABLE_NAME should be the leaf partition");

        exec("DROP TABLE t_m8_part CASCADE");
        exec("DROP TABLE t_m8_part_log");
        exec("DROP FUNCTION part_fn()");
    }
}
