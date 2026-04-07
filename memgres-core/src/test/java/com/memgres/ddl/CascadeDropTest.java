package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that DROP SCHEMA CASCADE properly removes ALL objects within the schema:
 * tables, composite types, enums, sequences, domains, views, functions, triggers,
 * indexes, and rules.
 *
 * The core bug: CASCADE drop only removes tables, functions, views, and triggers,
 * but leaves composite types, enums, sequences, and domains alive. This causes
 * "already exists" errors when the same schema is recreated in subsequent SQL files.
 *
 * Also: CASCADE currently drops ALL functions/views/triggers globally, not just
 * those in the target schema.
 */
class CascadeDropTest {

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

    // ========================================================================
    // Composite types must be dropped with schema
    // ========================================================================

    @Test
    void cascade_drops_composite_type() throws SQLException {
        exec("CREATE SCHEMA cd_test");
        exec("SET search_path = cd_test");
        exec("CREATE TYPE pair AS (x int, y text)");
        exec("DROP SCHEMA cd_test CASCADE");
        exec("SET search_path = public");

        // Re-create should succeed; type should be gone
        exec("CREATE SCHEMA cd_test");
        exec("SET search_path = cd_test");
        try {
            exec("CREATE TYPE pair AS (x int, y text)");
            // If we get here, the type was properly dropped
        } finally {
            exec("DROP SCHEMA cd_test CASCADE");
            exec("SET search_path = public");
        }
    }

    @Test
    void cascade_drops_composite_type_used_in_table() throws SQLException {
        exec("CREATE SCHEMA cd2");
        exec("SET search_path = cd2");
        exec("CREATE TYPE addr AS (street text, zip int)");
        exec("CREATE TABLE people(id int, home addr)");
        exec("DROP SCHEMA cd2 CASCADE");
        exec("SET search_path = public");

        // Both table and type should be gone
        exec("CREATE SCHEMA cd2");
        exec("SET search_path = cd2");
        try {
            exec("CREATE TYPE addr AS (street text, zip int)");
            exec("CREATE TABLE people(id int, home addr)");
        } finally {
            exec("DROP SCHEMA cd2 CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // Enum types must be dropped with schema
    // ========================================================================

    @Test
    void cascade_drops_enum_type() throws SQLException {
        exec("CREATE SCHEMA cd3");
        exec("SET search_path = cd3");
        exec("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
        exec("DROP SCHEMA cd3 CASCADE");
        exec("SET search_path = public");

        exec("CREATE SCHEMA cd3");
        exec("SET search_path = cd3");
        try {
            exec("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
        } finally {
            exec("DROP SCHEMA cd3 CASCADE");
            exec("SET search_path = public");
        }
    }

    @Test
    void cascade_drops_enum_used_in_table() throws SQLException {
        exec("CREATE SCHEMA cd4");
        exec("SET search_path = cd4");
        exec("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
        exec("CREATE TABLE t(id int, m mood)");
        exec("INSERT INTO t VALUES (1, 'ok')");
        exec("DROP SCHEMA cd4 CASCADE");
        exec("SET search_path = public");

        exec("CREATE SCHEMA cd4");
        exec("SET search_path = cd4");
        try {
            exec("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
            exec("CREATE TABLE t(id int, m mood)");
            exec("INSERT INTO t VALUES (1, 'happy')");
            assertEquals("happy", scalar("SELECT m FROM t WHERE id = 1"));
        } finally {
            exec("DROP SCHEMA cd4 CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // Sequences must be dropped with schema
    // ========================================================================

    @Test
    void cascade_drops_explicit_sequence() throws SQLException {
        exec("CREATE SCHEMA cd5");
        exec("SET search_path = cd5");
        exec("CREATE SEQUENCE my_seq START 1");
        exec("DROP SCHEMA cd5 CASCADE");
        exec("SET search_path = public");

        exec("CREATE SCHEMA cd5");
        exec("SET search_path = cd5");
        try {
            exec("CREATE SEQUENCE my_seq START 1");
        } finally {
            exec("DROP SCHEMA cd5 CASCADE");
            exec("SET search_path = public");
        }
    }

    @Test
    void cascade_drops_serial_sequence() throws SQLException {
        exec("CREATE SCHEMA cd6");
        exec("SET search_path = cd6");
        exec("CREATE TABLE t(id serial PRIMARY KEY, note text)");
        exec("DROP SCHEMA cd6 CASCADE");
        exec("SET search_path = public");

        exec("CREATE SCHEMA cd6");
        exec("SET search_path = cd6");
        try {
            exec("CREATE TABLE t(id serial PRIMARY KEY, note text)");
            exec("INSERT INTO t(note) VALUES ('ok')");
            assertEquals("1", scalar("SELECT id FROM t"));
        } finally {
            exec("DROP SCHEMA cd6 CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // Domains must be dropped with schema
    // ========================================================================

    @Test
    void cascade_drops_domain() throws SQLException {
        exec("CREATE SCHEMA cd7");
        exec("SET search_path = cd7");
        exec("CREATE DOMAIN posint AS int CHECK (VALUE > 0)");
        exec("DROP SCHEMA cd7 CASCADE");
        exec("SET search_path = public");

        exec("CREATE SCHEMA cd7");
        exec("SET search_path = cd7");
        try {
            exec("CREATE DOMAIN posint AS int CHECK (VALUE > 0)");
        } finally {
            exec("DROP SCHEMA cd7 CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // Indexes must be dropped with schema
    // ========================================================================

    @Test
    void cascade_drops_indexes() throws SQLException {
        exec("CREATE SCHEMA cd8");
        exec("SET search_path = cd8");
        exec("CREATE TABLE t(id int PRIMARY KEY, a int)");
        exec("CREATE INDEX idx_a ON t(a)");
        exec("DROP SCHEMA cd8 CASCADE");
        exec("SET search_path = public");

        exec("CREATE SCHEMA cd8");
        exec("SET search_path = cd8");
        try {
            exec("CREATE TABLE t(id int PRIMARY KEY, a int)");
            exec("CREATE INDEX idx_a ON t(a)");
        } finally {
            exec("DROP SCHEMA cd8 CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // CASCADE must NOT drop objects in OTHER schemas
    // ========================================================================

    @Test
    void cascade_does_not_drop_objects_in_other_schemas() throws SQLException {
        exec("CREATE SCHEMA keep_me");
        exec("SET search_path = keep_me");
        exec("CREATE TABLE survivor(id int PRIMARY KEY, note text)");
        exec("INSERT INTO survivor VALUES (1, 'alive')");
        exec("CREATE FUNCTION keep_fn() RETURNS int LANGUAGE SQL AS $$ SELECT 42 $$");
        exec("CREATE VIEW keep_v AS SELECT * FROM survivor");

        exec("CREATE SCHEMA drop_me");
        exec("SET search_path = drop_me");
        exec("CREATE TABLE doomed(id int)");
        exec("DROP SCHEMA drop_me CASCADE");

        // Objects in keep_me should still exist
        exec("SET search_path = keep_me");
        try {
            assertEquals("alive", scalar("SELECT note FROM survivor WHERE id = 1"),
                    "Table in other schema should survive CASCADE drop");
            assertEquals("42", scalar("SELECT keep_fn()"),
                    "Function in other schema should survive CASCADE drop");
            assertEquals("alive", scalar("SELECT note FROM keep_v WHERE id = 1"),
                    "View in other schema should survive CASCADE drop");
        } finally {
            exec("DROP SCHEMA keep_me CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // Functions scoped to schema must be dropped
    // ========================================================================

    @Test
    void cascade_drops_functions_in_schema() throws SQLException {
        exec("CREATE SCHEMA cd9");
        exec("SET search_path = cd9");
        exec("CREATE FUNCTION add_nums(a int, b int) RETURNS int LANGUAGE SQL AS $$ SELECT a + b $$");
        exec("DROP SCHEMA cd9 CASCADE");
        exec("SET search_path = public");

        exec("CREATE SCHEMA cd9");
        exec("SET search_path = cd9");
        try {
            exec("CREATE FUNCTION add_nums(a int, b int) RETURNS int LANGUAGE SQL AS $$ SELECT a + b $$");
            assertEquals("5", scalar("SELECT add_nums(2, 3)"));
        } finally {
            exec("DROP SCHEMA cd9 CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // Views in schema must be dropped
    // ========================================================================

    @Test
    void cascade_drops_views_in_schema() throws SQLException {
        exec("CREATE SCHEMA cd10");
        exec("SET search_path = cd10");
        exec("CREATE TABLE t(id int PRIMARY KEY)");
        exec("CREATE VIEW v AS SELECT * FROM t");
        exec("DROP SCHEMA cd10 CASCADE");
        exec("SET search_path = public");

        exec("CREATE SCHEMA cd10");
        exec("SET search_path = cd10");
        try {
            exec("CREATE TABLE t(id int PRIMARY KEY)");
            exec("CREATE VIEW v AS SELECT * FROM t");
        } finally {
            exec("DROP SCHEMA cd10 CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // Full lifecycle: create schema, populate, drop, recreate
    // ========================================================================

    @Test
    void full_schema_lifecycle_twice() throws SQLException {
        for (int round = 0; round < 2; round++) {
            exec("DROP SCHEMA IF EXISTS lifecycle CASCADE");
            exec("CREATE SCHEMA lifecycle");
            exec("SET search_path = lifecycle");

            exec("CREATE TYPE pair AS (x int, y text)");
            exec("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
            exec("CREATE DOMAIN posint AS int CHECK (VALUE > 0)");
            exec("CREATE SEQUENCE my_seq START 1");
            exec("CREATE TABLE t(id int PRIMARY KEY, p pair, m mood, val posint)");
            exec("CREATE VIEW v AS SELECT * FROM t");
            exec("CREATE FUNCTION f() RETURNS int LANGUAGE SQL AS $$ SELECT 1 $$");
            exec("CREATE INDEX idx ON t(val)");

            exec("INSERT INTO t VALUES (1, ROW(1,'a'), 'ok', 5)");
            assertEquals("1", scalar("SELECT id FROM t"));
            assertEquals("1", scalar("SELECT f()"));

            exec("DROP SCHEMA lifecycle CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // Rules and triggers in schema must be dropped
    // ========================================================================

    @Test
    void cascade_drops_triggers_in_schema() throws SQLException {
        exec("CREATE SCHEMA cd11");
        exec("SET search_path = cd11");
        exec("CREATE TABLE t(id int PRIMARY KEY, a int)");
        exec("CREATE TABLE log(msg text)");
        exec("""
            CREATE FUNCTION trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN INSERT INTO log VALUES ('fired'); RETURN NEW; END;
            $$
            """);
        exec("CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION trg_fn()");
        exec("DROP SCHEMA cd11 CASCADE");
        exec("SET search_path = public");

        exec("CREATE SCHEMA cd11");
        exec("SET search_path = cd11");
        try {
            exec("CREATE TABLE t(id int PRIMARY KEY, a int)");
            exec("CREATE TABLE log(msg text)");
            exec("""
                CREATE FUNCTION trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN INSERT INTO log VALUES ('fired'); RETURN NEW; END;
                $$
                """);
            exec("CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION trg_fn()");
            exec("INSERT INTO t VALUES (1, 10)");
            assertEquals("fired", scalar("SELECT msg FROM log LIMIT 1"));
        } finally {
            exec("DROP SCHEMA cd11 CASCADE");
            exec("SET search_path = public");
        }
    }
}
