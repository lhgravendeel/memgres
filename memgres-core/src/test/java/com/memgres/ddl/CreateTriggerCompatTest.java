package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CREATE TRIGGER syntax variants found in real-world schemas.
 */
class CreateTriggerCompatTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE trigger_log (id serial PRIMARY KEY, action text, table_name text, ts timestamp DEFAULT now())");
            s.execute("CREATE TABLE tracked_items (id serial PRIMARY KEY, name text, updated_at timestamp DEFAULT now())");
            s.execute("""
                CREATE FUNCTION log_changes()
                RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    INSERT INTO trigger_log (action, table_name) VALUES (TG_OP, TG_TABLE_NAME);
                    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
                    RETURN NEW;
                END;
                $$
            """);
            s.execute("""
                CREATE FUNCTION update_timestamp()
                RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    NEW.updated_at := now();
                    RETURN NEW;
                END;
                $$
            """);
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
    // REFERENCING OLD TABLE AS / NEW TABLE AS (transition tables)
    // =========================================================================

    @Test
    void testReferencingOldTable() throws SQLException {
        exec("CREATE TABLE ref_old_test (id serial PRIMARY KEY, data text)");
        exec("""
            CREATE FUNCTION process_old_rows()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO trigger_log (action, table_name) VALUES ('BATCH_DELETE', 'ref_old_test');
                RETURN NULL;
            END;
            $$
        """);
        exec("""
            CREATE TRIGGER ref_old_trigger
            AFTER DELETE ON ref_old_test
            REFERENCING OLD TABLE AS old_table
            FOR EACH STATEMENT
            EXECUTE FUNCTION process_old_rows()
        """);
    }

    @Test
    void testReferencingNewTable() throws SQLException {
        exec("CREATE TABLE ref_new_test (id serial PRIMARY KEY, data text)");
        exec("""
            CREATE FUNCTION process_new_rows()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RETURN NULL;
            END;
            $$
        """);
        exec("""
            CREATE TRIGGER ref_new_trigger
            AFTER INSERT ON ref_new_test
            REFERENCING NEW TABLE AS new_table
            FOR EACH STATEMENT
            EXECUTE FUNCTION process_new_rows()
        """);
    }

    @Test
    void testReferencingOldAndNewTable() throws SQLException {
        exec("CREATE TABLE ref_both_test (id serial PRIMARY KEY, val int)");
        exec("""
            CREATE FUNCTION process_changes()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RETURN NULL;
            END;
            $$
        """);
        exec("""
            CREATE TRIGGER ref_both_trigger
            AFTER UPDATE ON ref_both_test
            REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
            FOR EACH STATEMENT
            EXECUTE FUNCTION process_changes()
        """);
    }

    // =========================================================================
    // FOR EACH STATEMENT (vs FOR EACH ROW)
    // =========================================================================

    @Test
    void testForEachStatement() throws SQLException {
        exec("CREATE TABLE stmt_trigger_test (id serial PRIMARY KEY, val text)");
        exec("""
            CREATE TRIGGER stmt_trigger
            AFTER INSERT ON stmt_trigger_test
            FOR EACH STATEMENT
            EXECUTE FUNCTION log_changes()
        """);
        exec("INSERT INTO stmt_trigger_test (val) VALUES ('a'), ('b'), ('c')");
        // Statement trigger fires once per statement, not per row
    }

    // =========================================================================
    // WHEN clause with IS NOT DISTINCT FROM
    // =========================================================================

    @Test
    void testTriggerWhenIsNotDistinctFrom() throws SQLException {
        exec("""
            CREATE TRIGGER conditional_update
            BEFORE UPDATE ON tracked_items
            FOR EACH ROW
            WHEN (OLD.updated_at IS NOT DISTINCT FROM NEW.updated_at)
            EXECUTE FUNCTION update_timestamp()
        """);
        exec("INSERT INTO tracked_items (name) VALUES ('item1')");
        exec("UPDATE tracked_items SET name = 'item1_updated' WHERE name = 'item1'");
    }

    @Test
    void testTriggerWhenOldIsDistinctFromNew() throws SQLException {
        exec("CREATE TABLE when_test (id serial PRIMARY KEY, status text, updated_at timestamp DEFAULT now())");
        exec("""
            CREATE TRIGGER track_status_change
            AFTER UPDATE ON when_test
            FOR EACH ROW
            WHEN (OLD.status IS DISTINCT FROM NEW.status)
            EXECUTE FUNCTION log_changes()
        """);
    }

    @Test
    void testTriggerWhenCondition() throws SQLException {
        exec("CREATE TABLE when_cond_test (id serial PRIMARY KEY, category text, val int)");
        exec("""
            CREATE TRIGGER category_specific
            BEFORE INSERT ON when_cond_test
            FOR EACH ROW
            WHEN (NEW.category = 'tracked')
            EXECUTE FUNCTION log_changes()
        """);
    }

    // =========================================================================
    // EXECUTE FUNCTION vs EXECUTE PROCEDURE
    // =========================================================================

    @Test
    void testExecuteFunction() throws SQLException {
        exec("CREATE TABLE exec_fn_test (id serial PRIMARY KEY, name text)");
        exec("""
            CREATE TRIGGER exec_fn_trigger
            BEFORE INSERT ON exec_fn_test
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp()
        """);
    }

    @Test
    void testExecuteProcedure() throws SQLException {
        // Old syntax (pre-PG11) uses EXECUTE PROCEDURE, still accepted
        exec("CREATE TABLE exec_proc_test (id serial PRIMARY KEY, name text, updated_at timestamp DEFAULT now())");
        exec("""
            CREATE TRIGGER exec_proc_trigger
            BEFORE UPDATE ON exec_proc_test
            FOR EACH ROW
            EXECUTE PROCEDURE update_timestamp()
        """);
    }

    // =========================================================================
    // BEFORE / AFTER / INSTEAD OF
    // =========================================================================

    @Test
    void testInsteadOfTrigger() throws SQLException {
        exec("CREATE TABLE io_source (id serial PRIMARY KEY, name text)");
        exec("CREATE VIEW io_view AS SELECT * FROM io_source");
        exec("""
            CREATE FUNCTION io_insert()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO io_source (name) VALUES (NEW.name);
                RETURN NEW;
            END;
            $$
        """);
        exec("""
            CREATE TRIGGER io_trigger
            INSTEAD OF INSERT ON io_view
            FOR EACH ROW
            EXECUTE FUNCTION io_insert()
        """);
    }

    // =========================================================================
    // Trigger on specific columns
    // =========================================================================

    @Test
    void testTriggerOnSpecificColumns() throws SQLException {
        exec("CREATE TABLE col_trigger_test (id serial PRIMARY KEY, name text, email text, updated_at timestamp DEFAULT now())");
        exec("""
            CREATE TRIGGER name_or_email_change
            BEFORE UPDATE OF name, email ON col_trigger_test
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp()
        """);
    }

    // =========================================================================
    // CREATE OR REPLACE TRIGGER (PG 14+)
    // =========================================================================

    @Test
    void testCreateOrReplaceTrigger() throws SQLException {
        exec("CREATE TABLE repl_trigger_test (id serial PRIMARY KEY, val text)");
        exec("""
            CREATE TRIGGER repl_trigger
            BEFORE INSERT ON repl_trigger_test
            FOR EACH ROW
            EXECUTE FUNCTION log_changes()
        """);
        // Replace with different timing
        exec("""
            CREATE OR REPLACE TRIGGER repl_trigger
            AFTER INSERT ON repl_trigger_test
            FOR EACH ROW
            EXECUTE FUNCTION log_changes()
        """);
    }

    // =========================================================================
    // Schema-qualified trigger function
    // =========================================================================

    @Test
    void testTriggerWithSchemaQualifiedFunction() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS triggers");
        exec("""
            CREATE FUNCTION triggers.stamp_updated_at()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                NEW.updated_at := now();
                RETURN NEW;
            END;
            $$
        """);
        exec("CREATE TABLE sqt_test (id serial PRIMARY KEY, updated_at timestamp DEFAULT now())");
        exec("""
            CREATE TRIGGER sqt_trigger
            BEFORE UPDATE ON sqt_test
            FOR EACH ROW
            EXECUTE FUNCTION triggers.stamp_updated_at()
        """);
    }

    // =========================================================================
    // TRUNCATE trigger
    // =========================================================================

    @Test
    void testTruncateTrigger() throws SQLException {
        exec("CREATE TABLE trunc_trigger_test (id serial PRIMARY KEY, data text)");
        exec("""
            CREATE TRIGGER on_truncate
            AFTER TRUNCATE ON trunc_trigger_test
            FOR EACH STATEMENT
            EXECUTE FUNCTION log_changes()
        """);
    }

    // =========================================================================
    // Multiple events (INSERT OR UPDATE OR DELETE)
    // =========================================================================

    @Test
    void testMultiEventTrigger() throws SQLException {
        exec("CREATE TABLE multi_event_test (id serial PRIMARY KEY, val text)");
        exec("""
            CREATE TRIGGER multi_event
            AFTER INSERT OR UPDATE OR DELETE ON multi_event_test
            FOR EACH ROW
            EXECUTE FUNCTION log_changes()
        """);
    }

    // =========================================================================
    // CONSTRAINT trigger (deferrable)
    // =========================================================================

    @Test
    void testConstraintTrigger() throws SQLException {
        exec("CREATE TABLE constraint_trig_test (id serial PRIMARY KEY, val int)");
        exec("""
            CREATE CONSTRAINT TRIGGER deferred_check
            AFTER INSERT ON constraint_trig_test
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW
            EXECUTE FUNCTION log_changes()
        """);
    }
}
