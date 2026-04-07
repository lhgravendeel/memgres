package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for advanced FOREIGN KEY syntax variants.
 *
 * Covers:
 * - ON DELETE SET NULL (column_list): PG 15+ partial SET NULL
 * - ON UPDATE SET DEFAULT (column_list): PG 15+ partial SET DEFAULT
 * - Multi-column FK with partial SET NULL
 * - INTERVAL type in column definitions
 * - Composite FK referencing schema-qualified tables
 */
class ForeignKeyAdvancedTest {

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
    // ON DELETE SET NULL (column_list), PG 15+
    // =========================================================================

    @Test
    void testOnDeleteSetNullSingleColumn() throws SQLException {
        exec("CREATE TABLE fk_parent (id serial PRIMARY KEY, name text)");
        exec("""
            CREATE TABLE fk_child (
                id serial PRIMARY KEY,
                parent_id int,
                parent_name text,
                FOREIGN KEY (parent_id, parent_name)
                    REFERENCES fk_parent(id, name)
                    ON DELETE SET NULL (parent_name)
            )
        """);
    }

    @Test
    void testOnDeleteSetNullMultipleColumns() throws SQLException {
        exec("CREATE TABLE fk_p2 (a int, b int, PRIMARY KEY (a, b))");
        exec("""
            CREATE TABLE fk_c2 (
                id serial PRIMARY KEY,
                ref_a int,
                ref_b int,
                FOREIGN KEY (ref_a, ref_b)
                    REFERENCES fk_p2(a, b)
                    ON DELETE SET NULL (ref_a, ref_b)
            )
        """);
    }

    @Test
    void testOnUpdateSetDefaultPartialColumn() throws SQLException {
        exec("CREATE TABLE fk_p3 (id int PRIMARY KEY, code text UNIQUE)");
        exec("""
            CREATE TABLE fk_c3 (
                id serial PRIMARY KEY,
                parent_id int DEFAULT 0,
                parent_code text,
                FOREIGN KEY (parent_id) REFERENCES fk_p3(id)
                    ON UPDATE SET DEFAULT (parent_id)
            )
        """);
    }

    // =========================================================================
    // Composite FK to schema-qualified table
    // =========================================================================

    @Test
    void testCompositeFkToSchemaQualifiedTable() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS fk_ns");
        exec("CREATE TABLE fk_ns.agents (instance_id text NOT NULL, fingerprint text NOT NULL, PRIMARY KEY (instance_id, fingerprint))");
        exec("""
            CREATE TABLE fk_ns.agent_sessions (
                instance_id text NOT NULL,
                session_id text NOT NULL,
                agent_fingerprint text,
                PRIMARY KEY (instance_id, session_id),
                FOREIGN KEY (instance_id, agent_fingerprint)
                    REFERENCES fk_ns.agents(instance_id, fingerprint)
                    ON DELETE SET NULL (agent_fingerprint)
            )
        """);
    }

    // =========================================================================
    // INTERVAL type in CREATE TABLE
    // =========================================================================

    @Test
    void testIntervalColumn() throws SQLException {
        exec("""
            CREATE TABLE session_config (
                id serial PRIMARY KEY,
                lifetime INTERVAL,
                expiration TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
            )
        """);
        exec("INSERT INTO session_config (lifetime) VALUES ('2 hours')");
        assertNotNull(query1("SELECT lifetime FROM session_config"));
    }

    @Test
    void testIntervalWithComputedExpiration() throws SQLException {
        exec("""
            CREATE TABLE timed_entries (
                id serial PRIMARY KEY,
                lifetime INTERVAL NOT NULL DEFAULT '1 hour',
                created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
                expires_at TIMESTAMPTZ
            )
        """);
        exec("INSERT INTO timed_entries (lifetime) VALUES ('30 minutes')");
        exec("UPDATE timed_entries SET expires_at = created_at + lifetime WHERE id = 1");
        assertNotNull(query1("SELECT expires_at FROM timed_entries WHERE id = 1"));
    }

    @Test
    void testIntervalInTrigger() throws SQLException {
        exec("""
            CREATE TABLE expiring_records (
                id serial PRIMARY KEY,
                lifetime INTERVAL,
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                expiration TIMESTAMPTZ
            )
        """);
        exec("""
            CREATE FUNCTION compute_expiration()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.expiration := NEW.updated_at + NEW.lifetime;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        """);
        exec("""
            CREATE TRIGGER set_expiration
            BEFORE INSERT OR UPDATE OF lifetime ON expiring_records
            FOR EACH ROW
            EXECUTE FUNCTION compute_expiration()
        """);
        exec("INSERT INTO expiring_records (lifetime) VALUES ('3 hours')");
        assertNotNull(query1("SELECT expiration FROM expiring_records WHERE id = 1"));
    }

    // =========================================================================
    // FK with all action variants
    // =========================================================================

    @Test
    void testFkOnDeleteCascadeOnUpdateCascade() throws SQLException {
        exec("CREATE TABLE fk_cascade_p (id serial PRIMARY KEY)");
        exec("CREATE TABLE fk_cascade_c (id serial PRIMARY KEY, pid int REFERENCES fk_cascade_p(id) ON DELETE CASCADE ON UPDATE CASCADE)");
    }

    @Test
    void testFkOnDeleteSetNullOnUpdateRestrict() throws SQLException {
        exec("CREATE TABLE fk_mixed_p (id serial PRIMARY KEY)");
        exec("CREATE TABLE fk_mixed_c (id serial PRIMARY KEY, pid int REFERENCES fk_mixed_p(id) ON DELETE SET NULL ON UPDATE RESTRICT)");
    }

    @Test
    void testFkOnDeleteSetDefaultOnUpdateNoAction() throws SQLException {
        exec("CREATE TABLE fk_default_p (id serial PRIMARY KEY)");
        exec("INSERT INTO fk_default_p VALUES (0)");
        exec("CREATE TABLE fk_default_c (id serial PRIMARY KEY, pid int DEFAULT 0 REFERENCES fk_default_p(id) ON DELETE SET DEFAULT ON UPDATE NO ACTION)");
    }
}
