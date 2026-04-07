package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for advanced CREATE TRIGGER patterns found in real-world schemas.
 *
 * v1 compat tests covered basic REFERENCING, WHEN, and FOR EACH STATEMENT.
 * These tests cover the edge cases that still fail:
 * - REFERENCING with EXECUTE FUNCTION that has arguments with string literals
 * - Triggers on schema-qualified tables (auth.users, myschema.tbl)
 * - EXECUTE FUNCTION with arguments (string literal params)
 * - Trigger with UPDATE OF column-list on schema-qualified table
 * - Trigger function call with space before parenthesis
 */
class TriggerAdvancedCompatTest {

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
            s.execute("CREATE TABLE base_tbl (id serial PRIMARY KEY, name text, lock_val int, updated_at timestamp DEFAULT now())");
            s.execute("CREATE FUNCTION noop_trigger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$");
            s.execute("CREATE FUNCTION notify_change(channel text) RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_notify(channel, 'changed'); RETURN NEW; END; $$");
            s.execute("CREATE FUNCTION stmt_noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END; $$");
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

    // =========================================================================
    // EXECUTE FUNCTION with string literal arguments
    // =========================================================================

    @Test
    void testExecuteFunctionWithStringArg() throws SQLException {
        exec("CREATE TABLE fn_arg_tbl (id serial PRIMARY KEY, val text)");
        exec("""
            CREATE TRIGGER notify_trig
            AFTER INSERT ON fn_arg_tbl
            FOR EACH ROW
            EXECUTE FUNCTION notify_change('my_channel')
        """);
    }

    @Test
    void testExecuteFunctionWithMultipleArgs() throws SQLException {
        exec("CREATE FUNCTION multi_arg_trigger(ch text, tbl text) RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$");
        exec("CREATE TABLE multi_arg_tbl (id serial PRIMARY KEY)");
        exec("""
            CREATE TRIGGER multi_arg_trig
            AFTER INSERT ON multi_arg_tbl
            FOR EACH ROW
            EXECUTE FUNCTION multi_arg_trigger('channel_name', 'table_name')
        """);
    }

    @Test
    void testExecuteProcedureWithStringArg() throws SQLException {
        // Old syntax (EXECUTE PROCEDURE) with args, still valid
        exec("CREATE TABLE proc_arg_tbl (id serial PRIMARY KEY)");
        exec("""
            CREATE TRIGGER proc_arg_trig
            AFTER UPDATE ON proc_arg_tbl
            FOR EACH ROW
            EXECUTE PROCEDURE notify_change('update_channel')
        """);
    }

    // =========================================================================
    // Triggers on schema-qualified tables
    // =========================================================================

    @Test
    void testTriggerOnSchemaQualifiedTable() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS myauth");
        exec("CREATE TABLE myauth.accounts (id serial PRIMARY KEY, name text, updated_at timestamp DEFAULT now())");
        exec("CREATE FUNCTION myauth.stamp_updated() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.updated_at := now(); RETURN NEW; END; $$");
        exec("""
            CREATE TRIGGER set_accounts_updated_at
            BEFORE UPDATE ON myauth.accounts
            FOR EACH ROW
            EXECUTE FUNCTION myauth.stamp_updated()
        """);
    }

    @Test
    void testTriggerOnSchemaQualifiedTableWithSpaceBeforeParen() throws SQLException {
        // Some codebases put a space before the parenthesis: EXECUTE FUNCTION fn ()
        exec("CREATE SCHEMA IF NOT EXISTS hooks");
        exec("CREATE TABLE hooks.entries (id serial PRIMARY KEY, updated_at timestamp DEFAULT now())");
        exec("CREATE FUNCTION hooks.auto_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.updated_at := now(); RETURN NEW; END; $$");
        exec("""
            CREATE TRIGGER auto_update_trigger
            BEFORE UPDATE ON hooks.entries
            FOR EACH ROW
            EXECUTE FUNCTION hooks.auto_update ()
        """);
    }

    // =========================================================================
    // REFERENCING with EXECUTE FUNCTION that has string literal arguments
    // =========================================================================

    @Test
    void testReferencingOldTableWithFunctionArg() throws SQLException {
        exec("CREATE TABLE ref_fn_arg (id serial PRIMARY KEY, data text)");
        exec("CREATE FUNCTION log_delete_with_name(tbl_name text) RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END; $$");
        exec("""
            CREATE TRIGGER ref_fn_trig
            AFTER DELETE ON ref_fn_arg
            REFERENCING OLD TABLE AS old_table
            FOR EACH STATEMENT
            EXECUTE FUNCTION log_delete_with_name('ref_fn_arg')
        """);
    }

    @Test
    void testReferencingOldTableWithTwoStringArgs() throws SQLException {
        exec("CREATE TABLE ref_two_args (id serial PRIMARY KEY, data text)");
        exec("CREATE FUNCTION log_with_context(tbl text, ctx text) RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END; $$");
        exec("""
            CREATE TRIGGER ref_two_args_trig
            AFTER DELETE ON ref_two_args
            REFERENCING OLD TABLE AS old_table
            FOR EACH STATEMENT
            EXECUTE FUNCTION log_with_context('ref_two_args', 'deletion')
        """);
    }

    // =========================================================================
    // UPDATE OF column-list triggers
    // =========================================================================

    @Test
    void testAfterUpdateOfColumn() throws SQLException {
        exec("CREATE TABLE update_of_tbl (id serial PRIMARY KEY, lock_val int, name text)");
        exec("""
            CREATE TRIGGER on_lock_change
            AFTER UPDATE OF lock_val ON update_of_tbl
            FOR EACH ROW
            EXECUTE FUNCTION notify_change('lock_changed')
        """);
    }

    @Test
    void testAfterUpdateOfMultipleColumns() throws SQLException {
        exec("CREATE TABLE update_of_multi (id serial PRIMARY KEY, email text, phone text, name text)");
        exec("""
            CREATE TRIGGER on_contact_change
            AFTER UPDATE OF email, phone ON update_of_multi
            FOR EACH ROW
            EXECUTE FUNCTION noop_trigger()
        """);
    }

    @Test
    void testAfterInsertOnVersionedTable() throws SQLException {
        // Pattern: trigger fires on INSERT for version tracking
        exec("CREATE TABLE versioned_items (id serial PRIMARY KEY, parent_id int, version int)");
        exec("""
            CREATE TRIGGER on_new_version
            AFTER INSERT ON versioned_items
            FOR EACH ROW
            EXECUTE FUNCTION notify_change('new_version')
        """);
    }

    // =========================================================================
    // Trigger with REFERENCING + EXECUTE FUNCTION with schema-qualified name + args
    // =========================================================================

    @Test
    void testReferencingWithSchemaQualifiedFuncAndArg() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS audit");
        exec("CREATE FUNCTION audit.record_deletes(tbl text) RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END; $$");
        exec("CREATE TABLE tracked_records (id serial PRIMARY KEY, data text)");
        exec("""
            CREATE TRIGGER audit_deletes
            AFTER DELETE ON tracked_records
            REFERENCING OLD TABLE AS old_table
            FOR EACH STATEMENT
            EXECUTE FUNCTION audit.record_deletes('tracked_records')
        """);
    }

    // =========================================================================
    // INSTEAD OF trigger on a view (with function args)
    // =========================================================================

    @Test
    void testInsteadOfWithFunctionArgs() throws SQLException {
        exec("CREATE TABLE io_base (id serial PRIMARY KEY, val text)");
        exec("CREATE VIEW io_view AS SELECT * FROM io_base");
        exec("CREATE FUNCTION handle_view_dml(op text) RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF op = 'INSERT' THEN INSERT INTO io_base (val) VALUES (NEW.val); END IF; RETURN NEW; END; $$");
        exec("""
            CREATE TRIGGER io_view_insert
            INSTEAD OF INSERT ON io_view
            FOR EACH ROW
            EXECUTE FUNCTION handle_view_dml('INSERT')
        """);
    }

    // =========================================================================
    // FOR EACH STATEMENT trigger with REFERENCING both OLD and NEW
    // =========================================================================

    @Test
    void testReferencingBothTablesOnUpdate() throws SQLException {
        exec("CREATE TABLE ref_both_tbl (id serial PRIMARY KEY, val int)");
        exec("""
            CREATE TRIGGER ref_both_trig
            AFTER UPDATE ON ref_both_tbl
            REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
            FOR EACH STATEMENT
            EXECUTE FUNCTION stmt_noop()
        """);
    }

    // =========================================================================
    // Trigger with WHEN clause referencing TG_OP or pg_trigger_depth()
    // =========================================================================

    @Test
    void testTriggerWhenPgTriggerDepth() throws SQLException {
        exec("CREATE TABLE depth_tbl (id serial PRIMARY KEY, val text)");
        exec("""
            CREATE TRIGGER no_recursion
            BEFORE INSERT ON depth_tbl
            FOR EACH ROW
            WHEN (pg_trigger_depth() = 0)
            EXECUTE FUNCTION noop_trigger()
        """);
    }
}
