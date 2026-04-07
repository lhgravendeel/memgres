package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ALTER SEQUENCE, ALTER TYPE, and other DDL object variants
 * found in real-world schemas.
 */
class SequenceAndTypeCompatTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
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
    // ALTER SEQUENCE ... OWNED BY
    // =========================================================================

    @Test
    void testAlterSequenceOwnedBy() throws SQLException {
        exec("CREATE TABLE seq_owner (id bigint NOT NULL, name text)");
        exec("CREATE SEQUENCE seq_owner_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1");
        exec("ALTER SEQUENCE seq_owner_id_seq OWNED BY seq_owner.id");
    }

    @Test
    void testAlterSequenceOwnedByNone() throws SQLException {
        exec("CREATE TABLE seq_detach (id bigint NOT NULL)");
        exec("CREATE SEQUENCE seq_detach_id_seq");
        exec("ALTER SEQUENCE seq_detach_id_seq OWNED BY seq_detach.id");
        exec("ALTER SEQUENCE seq_detach_id_seq OWNED BY NONE");
    }

    // =========================================================================
    // ALTER SEQUENCE options
    // =========================================================================

    @Test
    void testAlterSequenceRestart() throws SQLException {
        exec("CREATE SEQUENCE restart_seq START WITH 1");
        exec("SELECT nextval('restart_seq')");
        exec("ALTER SEQUENCE restart_seq RESTART WITH 100");
        assertEquals("100", query1("SELECT nextval('restart_seq')"));
    }

    @Test
    void testAlterSequenceIncrementBy() throws SQLException {
        exec("CREATE SEQUENCE incr_seq START WITH 1 INCREMENT BY 1");
        exec("ALTER SEQUENCE incr_seq INCREMENT BY 5");
        assertEquals("1", query1("SELECT nextval('incr_seq')"));
        assertEquals("6", query1("SELECT nextval('incr_seq')"));
    }

    @Test
    void testAlterSequenceMinMaxValue() throws SQLException {
        exec("CREATE SEQUENCE bounded_seq");
        exec("ALTER SEQUENCE bounded_seq MINVALUE 1 MAXVALUE 1000");
    }

    @Test
    void testAlterSequenceCycle() throws SQLException {
        exec("CREATE SEQUENCE cycle_seq START WITH 1 MAXVALUE 3");
        exec("ALTER SEQUENCE cycle_seq CYCLE");
        assertEquals("1", query1("SELECT nextval('cycle_seq')"));
        assertEquals("2", query1("SELECT nextval('cycle_seq')"));
        assertEquals("3", query1("SELECT nextval('cycle_seq')"));
        // Should cycle back to MINVALUE
        assertEquals("1", query1("SELECT nextval('cycle_seq')"));
    }

    @Test
    void testAlterSequenceNoCycle() throws SQLException {
        exec("CREATE SEQUENCE nocycle_seq START WITH 1 MAXVALUE 2 CYCLE");
        exec("ALTER SEQUENCE nocycle_seq NO CYCLE");
    }

    // =========================================================================
    // Schema-qualified sequences
    // =========================================================================

    @Test
    void testSchemaQualifiedSequence() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS seq_schema");
        exec("CREATE SEQUENCE seq_schema.my_seq START WITH 100");
        assertEquals("100", query1("SELECT nextval('seq_schema.my_seq')"));
    }

    // =========================================================================
    // pg_dump style: CREATE SEQUENCE with all options
    // =========================================================================

    @Test
    void testPgDumpStyleSequence() throws SQLException {
        exec("""
            CREATE SEQUENCE items_id_seq
                START WITH 1
                INCREMENT BY 1
                NO MINVALUE
                NO MAXVALUE
                CACHE 1
        """);
        assertEquals("1", query1("SELECT nextval('items_id_seq')"));
    }

    @Test
    void testPgDumpStyleSequenceAsSmallint() throws SQLException {
        exec("CREATE SEQUENCE small_seq AS smallint START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 32767");
    }

    @Test
    void testPgDumpStyleSequenceAsBigint() throws SQLException {
        exec("CREATE SEQUENCE big_seq AS bigint START WITH 1 INCREMENT BY 1");
    }

    // =========================================================================
    // ALTER TYPE ... ADD VALUE
    // =========================================================================

    @Test
    void testAlterTypeAddValue() throws SQLException {
        exec("CREATE TYPE color AS ENUM ('red', 'green', 'blue')");
        exec("ALTER TYPE color ADD VALUE 'yellow'");
        exec("CREATE TABLE at_test (id serial PRIMARY KEY, c color)");
        exec("INSERT INTO at_test (c) VALUES ('yellow')");
        assertEquals("yellow", query1("SELECT c FROM at_test"));
    }

    @Test
    void testAlterTypeAddValueIfNotExists() throws SQLException {
        exec("CREATE TYPE size AS ENUM ('small', 'medium', 'large')");
        exec("ALTER TYPE size ADD VALUE IF NOT EXISTS 'xlarge'");
        exec("ALTER TYPE size ADD VALUE IF NOT EXISTS 'xlarge'"); // Should not error
    }

    @Test
    void testAlterTypeAddValueBefore() throws SQLException {
        exec("CREATE TYPE priority AS ENUM ('low', 'high')");
        exec("ALTER TYPE priority ADD VALUE 'medium' BEFORE 'high'");
        exec("CREATE TABLE prio_test (id serial PRIMARY KEY, p priority)");
        exec("INSERT INTO prio_test (p) VALUES ('medium')");
    }

    @Test
    void testAlterTypeAddValueAfter() throws SQLException {
        exec("CREATE TYPE severity AS ENUM ('info', 'error')");
        exec("ALTER TYPE severity ADD VALUE 'warning' AFTER 'info'");
        exec("CREATE TABLE sev_test (id serial PRIMARY KEY, s severity)");
        exec("INSERT INTO sev_test (s) VALUES ('warning')");
    }

    // =========================================================================
    // ALTER TYPE ... RENAME VALUE
    // =========================================================================

    @Test
    void testAlterTypeRenameValue() throws SQLException {
        exec("CREATE TYPE fruit AS ENUM ('apple', 'bannana', 'cherry')");
        exec("ALTER TYPE fruit RENAME VALUE 'bannana' TO 'banana'");
        exec("CREATE TABLE fruit_test (id serial PRIMARY KEY, f fruit)");
        exec("INSERT INTO fruit_test (f) VALUES ('banana')");
        assertEquals("banana", query1("SELECT f FROM fruit_test"));
    }

    // =========================================================================
    // ALTER TYPE ... RENAME TO
    // =========================================================================

    @Test
    void testAlterTypeRenameTo() throws SQLException {
        exec("CREATE TYPE old_status AS ENUM ('on', 'off')");
        exec("ALTER TYPE old_status RENAME TO new_status");
        exec("CREATE TABLE ns_test (id serial PRIMARY KEY, s new_status)");
        exec("INSERT INTO ns_test (s) VALUES ('on')");
    }

    // =========================================================================
    // ALTER TYPE ... SET SCHEMA
    // =========================================================================

    @Test
    void testAlterTypeSetSchema() throws SQLException {
        exec("CREATE TYPE move_me AS ENUM ('a', 'b')");
        exec("CREATE SCHEMA IF NOT EXISTS type_dest");
        exec("ALTER TYPE move_me SET SCHEMA type_dest");
        exec("CREATE TABLE mm_test (id serial PRIMARY KEY, val type_dest.move_me)");
    }

    // =========================================================================
    // CREATE DOMAIN
    // =========================================================================

    @Test
    void testCreateDomain() throws SQLException {
        exec("CREATE DOMAIN positive_int AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE dom_test (id serial PRIMARY KEY, quantity positive_int)");
        exec("INSERT INTO dom_test (quantity) VALUES (5)");
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO dom_test (quantity) VALUES (-1)"));
    }

    @Test
    void testCreateDomainWithDefault() throws SQLException {
        exec("CREATE DOMAIN email_addr AS text CHECK (VALUE ~ '@') DEFAULT 'unknown@example.com'");
        exec("CREATE TABLE email_test (id serial PRIMARY KEY, email email_addr)");
        exec("INSERT INTO email_test DEFAULT VALUES");
        assertEquals("unknown@example.com", query1("SELECT email FROM email_test"));
    }

    @Test
    void testCreateDomainNotNull() throws SQLException {
        exec("CREATE DOMAIN nonempty_text AS text NOT NULL CHECK (VALUE <> '')");
        exec("CREATE TABLE net_test (id serial PRIMARY KEY, name nonempty_text)");
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO net_test (name) VALUES ('')"));
    }

    // =========================================================================
    // CREATE AGGREGATE (used by some projects)
    // =========================================================================

    @Test
    void testCreateAggregate() throws SQLException {
        exec("""
            CREATE FUNCTION array_append_agg(anyarray, anyelement)
            RETURNS anyarray LANGUAGE sql IMMUTABLE AS $$
                SELECT array_append($1, $2);
            $$
        """);
        exec("""
            CREATE AGGREGATE custom_array_agg(anyelement) (
                SFUNC = array_append_agg,
                STYPE = anyarray,
                INITCOND = '{}'
            )
        """);
    }
}
