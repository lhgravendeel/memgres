package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for trigger transition tables (REFERENCING NEW TABLE AS / OLD TABLE AS),
 * INSTEAD OF triggers on views, UPDATE OF column triggers,
 * and dynamic SQL in PL/pgSQL (EXECUTE with USING, format()).
 *
 * Key PG behaviors:
 * - Transition tables provide access to all affected rows in statement-level triggers
 * - INSTEAD OF triggers on views intercept DML
 * - WHEN (OLD.* IS DISTINCT FROM NEW.*) fires only on actual changes
 * - EXECUTE format(...) USING ... in PL/pgSQL for dynamic SQL
 */
class TriggerTransitionTableTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = 0; while (rs.next()) n++; return n;
        }
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    // ========================================================================
    // Transition tables (REFERENCING NEW TABLE AS)
    // ========================================================================

    @Test
    void statement_trigger_with_transition_table() throws SQLException {
        exec("CREATE TABLE tt_t(id int PRIMARY KEY, a int, b text)");
        exec("CREATE TABLE tt_log(seq serial, tag text, id int, a int, b text)");
        exec("""
            CREATE FUNCTION tt_trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              INSERT INTO tt_log(tag, id, a, b)
              SELECT 'stmt_trans', id, a, b FROM newtab;
              RETURN NULL;
            END;
            $$
            """);
        exec("""
            CREATE TRIGGER tt_trg
            AFTER INSERT ON tt_t
            REFERENCING NEW TABLE AS newtab
            FOR EACH STATEMENT
            EXECUTE FUNCTION tt_trg_fn()
            """);
        try {
            exec("INSERT INTO tt_t VALUES (1, 10, 'abc')");
            exec("INSERT INTO tt_t VALUES (2, 20, NULL)");

            int logCount = countRows("SELECT * FROM tt_log");
            // Each INSERT statement fires the trigger once, with the new rows visible in newtab
            assertTrue(logCount >= 2, "Transition table trigger should log at least 2 entries, got " + logCount);

            // Verify the transition table rows were accessible
            String firstTag = scalar("SELECT tag FROM tt_log ORDER BY seq LIMIT 1");
            assertEquals("stmt_trans", firstTag);
        } finally {
            exec("DROP TABLE IF EXISTS tt_t CASCADE");
            exec("DROP TABLE IF EXISTS tt_log CASCADE");
            exec("DROP FUNCTION IF EXISTS tt_trg_fn() CASCADE");
        }
    }

    // ========================================================================
    // INSTEAD OF trigger on view
    // ========================================================================

    @Test
    void instead_of_trigger_intercepts_view_insert() throws SQLException {
        exec("CREATE TABLE io_t(id int PRIMARY KEY, a int, b text)");
        exec("INSERT INTO io_t VALUES (1, 10, 'a'), (2, 20, 'b')");
        exec("CREATE VIEW io_v AS SELECT id, a, b FROM io_t");
        exec("""
            CREATE FUNCTION io_ins_fn() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              INSERT INTO io_t(id, a, b) VALUES (NEW.id, NEW.a, upper(NEW.b));
              RETURN NEW;
            END;
            $$
            """);
        exec("""
            CREATE TRIGGER io_ins_trg
            INSTEAD OF INSERT ON io_v
            FOR EACH ROW
            EXECUTE FUNCTION io_ins_fn()
            """);
        try {
            exec("INSERT INTO io_v VALUES (3, 30, 'from_view')");
            String b = scalar("SELECT b FROM io_t WHERE id = 3");
            // The trigger uppercases the b value
            assertEquals("FROM_VIEW", b,
                    "INSTEAD OF trigger should have uppercased the value");
        } finally {
            exec("DROP VIEW IF EXISTS io_v CASCADE");
            exec("DROP TABLE IF EXISTS io_t CASCADE");
            exec("DROP FUNCTION IF EXISTS io_ins_fn() CASCADE");
        }
    }

    @Test
    void instead_of_trigger_cannot_be_after_on_view() throws SQLException {
        exec("CREATE TABLE bad_t(id int PRIMARY KEY)");
        exec("CREATE VIEW bad_v AS SELECT * FROM bad_t");
        exec("""
            CREATE FUNCTION bad_fn() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RETURN NEW; END;
            $$
            """);
        try {
            // PG: AFTER INSERT ON view requires INSTEAD OF, not AFTER
            assertThrows(SQLException.class,
                    () -> exec("""
                        CREATE TRIGGER bad_trg
                        AFTER INSERT ON bad_v
                        FOR EACH ROW
                        EXECUTE FUNCTION bad_fn()
                        """),
                    "AFTER trigger on view should fail; views need INSTEAD OF");
        } finally {
            exec("DROP VIEW IF EXISTS bad_v CASCADE");
            exec("DROP TABLE IF EXISTS bad_t CASCADE");
            exec("DROP FUNCTION IF EXISTS bad_fn() CASCADE");
        }
    }

    // ========================================================================
    // UPDATE OF trigger with WHEN (OLD.* IS DISTINCT FROM NEW.*)
    // ========================================================================

    @Test
    void update_of_trigger_fires_only_on_listed_columns() throws SQLException {
        exec("CREATE TABLE uof_t(id int PRIMARY KEY, a int, b text, c int)");
        exec("CREATE TABLE uof_log(cnt int)");
        exec("INSERT INTO uof_log VALUES (0)");
        exec("INSERT INTO uof_t VALUES (1, 10, 'x', 100)");
        exec("""
            CREATE FUNCTION uof_fn() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              UPDATE uof_log SET cnt = cnt + 1;
              RETURN NEW;
            END;
            $$
            """);
        exec("""
            CREATE TRIGGER uof_trg
            AFTER UPDATE OF a, b ON uof_t
            FOR EACH ROW
            WHEN (OLD.* IS DISTINCT FROM NEW.*)
            EXECUTE FUNCTION uof_fn()
            """);
        try {
            // Update column 'c' which is NOT in the OF list, so the trigger should NOT fire
            exec("UPDATE uof_t SET c = 200 WHERE id = 1");
            assertEquals("0", scalar("SELECT cnt FROM uof_log"),
                    "Trigger should not fire when updating column not in OF list");

            // Update column 'a' which IS in the OF list, so the trigger should fire
            exec("UPDATE uof_t SET a = 99 WHERE id = 1");
            assertEquals("1", scalar("SELECT cnt FROM uof_log"),
                    "Trigger should fire when updating listed column");
        } finally {
            exec("DROP TABLE IF EXISTS uof_t CASCADE");
            exec("DROP TABLE IF EXISTS uof_log CASCADE");
            exec("DROP FUNCTION IF EXISTS uof_fn() CASCADE");
        }
    }

    // ========================================================================
    // Dynamic SQL in PL/pgSQL: EXECUTE ... USING
    // ========================================================================

    @Test
    void execute_format_with_using() throws SQLException {
        exec("CREATE TABLE dyn_t(id int PRIMARY KEY, note text)");
        try {
            exec("""
                DO $$
                DECLARE
                  tbl text := 'dyn_t';
                BEGIN
                  EXECUTE format('INSERT INTO %I VALUES ($1, $2)', tbl) USING 1, 'hello';
                END
                $$
                """);
            assertEquals("hello", scalar("SELECT note FROM dyn_t WHERE id = 1"),
                    "EXECUTE ... USING should bind $1/$2 parameters");
        } finally {
            exec("DROP TABLE IF EXISTS dyn_t");
        }
    }

    @Test
    void execute_into_variable() throws SQLException {
        exec("CREATE TABLE dyn_t2(id int PRIMARY KEY, note text)");
        exec("INSERT INTO dyn_t2 VALUES (1, 'a'), (2, 'b')");
        try {
            exec("""
                DO $$
                DECLARE
                  cnt int;
                BEGIN
                  EXECUTE 'SELECT count(*) FROM dyn_t2' INTO cnt;
                  IF cnt <> 2 THEN
                    RAISE EXCEPTION 'Expected 2, got %', cnt;
                  END IF;
                END
                $$
                """);
            // If we get here without exception, the EXECUTE INTO worked
        } finally {
            exec("DROP TABLE IF EXISTS dyn_t2");
        }
    }

    @Test
    void execute_create_table_dynamically() throws SQLException {
        exec("""
            DO $$
            BEGIN
              IF to_regclass('dyn_created') IS NULL THEN
                EXECUTE 'CREATE TABLE dyn_created(id int PRIMARY KEY, note text)';
              END IF;
            END
            $$
            """);
        try {
            // The table should exist now
            exec("INSERT INTO dyn_created VALUES (1, 'dynamic')");
            assertEquals("dynamic", scalar("SELECT note FROM dyn_created WHERE id = 1"));
        } finally {
            exec("DROP TABLE IF EXISTS dyn_created");
        }
    }
}
