package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PostgreSQL-specific features: custom ENUMs, INET/CIDR types,
 * PL/pgSQL triggers, uuid_generate_v4(), and ::jsonb casts.
 */
class PgFeaturesTest {

    private Memgres memgres;

    @BeforeEach
    void setUp() {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterEach
    void tearDown() {
        if (memgres != null) {
            memgres.close();
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl(),
                memgres.getUser(),
                memgres.getPassword()
        );
    }

    // ---- Custom ENUM tests ----

    @Test
    void shouldCreateAndUseCustomEnum() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
            stmt.execute("CREATE TABLE person (name text, current_mood mood)");
            stmt.execute("INSERT INTO person (name, current_mood) VALUES ('Alice', 'happy')");
            stmt.execute("INSERT INTO person (name, current_mood) VALUES ('Bob', 'sad')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM person WHERE current_mood = 'happy'")) {
                assertTrue(rs.next());
                assertEquals("Alice", rs.getString("name"));
                assertEquals("happy", rs.getString("current_mood"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldRejectInvalidEnumValue() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TYPE color AS ENUM ('red', 'green', 'blue')");
            stmt.execute("CREATE TABLE items (name text, color color)");

            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO items (name, color) VALUES ('thing', 'purple')"));
        }
    }

    @Test
    void shouldAllowMultipleEnumTypes() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TYPE status AS ENUM ('active', 'inactive', 'pending')");
            stmt.execute("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')");
            stmt.execute("CREATE TABLE tasks (title text, status status, priority priority)");
            stmt.execute("INSERT INTO tasks (title, status, priority) VALUES ('Fix bug', 'active', 'high')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tasks")) {
                assertTrue(rs.next());
                assertEquals("Fix bug", rs.getString("title"));
                assertEquals("active", rs.getString("status"));
                assertEquals("high", rs.getString("priority"));
            }
        }
    }

    // ---- INET/CIDR type tests ----

    @Test
    void shouldStoreAndRetrieveInetValues() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE servers (name text, ip inet)");
            stmt.execute("INSERT INTO servers (name, ip) VALUES ('web1', '192.168.1.1')");
            stmt.execute("INSERT INTO servers (name, ip) VALUES ('web2', '10.0.0.5')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM servers WHERE name = 'web1'")) {
                assertTrue(rs.next());
                assertEquals("192.168.1.1", rs.getString("ip"));
            }
        }
    }

    @Test
    void shouldStoreAndRetrieveCidrValues() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE networks (name text, network cidr)");
            stmt.execute("INSERT INTO networks (name, network) VALUES ('office', '192.168.1.0/24')");
            stmt.execute("INSERT INTO networks (name, network) VALUES ('datacenter', '10.0.0.0/8')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM networks")) {
                assertTrue(rs.next());
                assertEquals("192.168.1.0/24", rs.getString("network"));
                assertTrue(rs.next());
                assertEquals("10.0.0.0/8", rs.getString("network"));
            }
        }
    }

    @Test
    void shouldStoreInetAndCidrTogether() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE hosts (hostname text, address inet, subnet cidr)");
            stmt.execute("INSERT INTO hosts (hostname, address, subnet) VALUES ('srv1', '192.168.1.100', '192.168.1.0/24')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM hosts")) {
                assertTrue(rs.next());
                assertEquals("srv1", rs.getString("hostname"));
                assertEquals("192.168.1.100", rs.getString("address"));
                assertEquals("192.168.1.0/24", rs.getString("subnet"));
            }
        }
    }

    // ---- PL/pgSQL trigger tests ----

    @Test
    void shouldFireBeforeInsertTrigger() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE audit_log (id serial PRIMARY KEY, message text, created_at text)");

            stmt.execute("CREATE FUNCTION set_created_at() RETURNS trigger AS $$ " +
                    "BEGIN " +
                    "NEW.created_at = NOW(); " +
                    "RETURN NEW; " +
                    "END; " +
                    "$$ LANGUAGE plpgsql");

            stmt.execute("CREATE TRIGGER trg_set_created BEFORE INSERT ON audit_log " +
                    "FOR EACH ROW EXECUTE FUNCTION set_created_at()");

            stmt.execute("INSERT INTO audit_log (message) VALUES ('test entry')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM audit_log")) {
                assertTrue(rs.next());
                assertEquals("test entry", rs.getString("message"));
                // created_at should have been set by the trigger
                assertNotNull(rs.getString("created_at"));
                assertFalse(rs.getString("created_at").isEmpty());
            }
        }
    }

    @Test
    void shouldFireBeforeUpdateTrigger() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE documents (id integer, title text, updated_at text)");

            stmt.execute("CREATE FUNCTION update_timestamp() RETURNS trigger AS $$ " +
                    "BEGIN " +
                    "NEW.updated_at = NOW(); " +
                    "RETURN NEW; " +
                    "END; " +
                    "$$ LANGUAGE plpgsql");

            stmt.execute("CREATE TRIGGER trg_update_ts BEFORE UPDATE ON documents " +
                    "FOR EACH ROW EXECUTE FUNCTION update_timestamp()");

            stmt.execute("INSERT INTO documents (id, title) VALUES (1, 'Draft')");

            // Verify no updated_at initially
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM documents WHERE id = 1")) {
                assertTrue(rs.next());
                assertNull(rs.getString("updated_at"));
            }

            stmt.execute("UPDATE documents SET title = 'Final' WHERE id = 1");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM documents WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("Final", rs.getString("title"));
                assertNotNull(rs.getString("updated_at"));
            }
        }
    }

    // ---- uuid_generate_v4() tests ----

    @Test
    void shouldGenerateUuidInSelectExpression() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
            try (ResultSet rs = stmt.executeQuery("SELECT uuid_generate_v4() AS id")) {
                assertTrue(rs.next());
                String uuid = rs.getString("id");
                assertNotNull(uuid);
                // Verify it's a valid UUID format
                assertDoesNotThrow(() -> java.util.UUID.fromString(uuid));
            }
        }
    }

    @Test
    void shouldGenerateUniqueUuids() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
            String uuid1, uuid2;
            try (ResultSet rs = stmt.executeQuery("SELECT uuid_generate_v4() AS id")) {
                rs.next();
                uuid1 = rs.getString("id");
            }
            try (ResultSet rs = stmt.executeQuery("SELECT uuid_generate_v4() AS id")) {
                rs.next();
                uuid2 = rs.getString("id");
            }

            assertNotEquals(uuid1, uuid2, "UUIDs should be unique");
        }
    }

    @Test
    void shouldUseUuidAsDefaultColumnValue() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE entities (id uuid DEFAULT uuid_generate_v4(), name text)");
            stmt.execute("INSERT INTO entities (name) VALUES ('Widget')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM entities")) {
                assertTrue(rs.next());
                String uuid = rs.getString("id");
                assertNotNull(uuid, "UUID default should have been generated");
                assertDoesNotThrow(() -> java.util.UUID.fromString(uuid));
                assertEquals("Widget", rs.getString("name"));
            }
        }
    }

    @Test
    void shouldSupportGenRandomUuid() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery("SELECT gen_random_uuid() AS id")) {
                assertTrue(rs.next());
                assertDoesNotThrow(() -> java.util.UUID.fromString(rs.getString("id")));
            }
        }
    }

    @Test
    void shouldAcceptCreateExtensionUuidOssp() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            // Should not throw; just a no-op
            stmt.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
            // Now uuid_generate_v4 should still work
            try (ResultSet rs = stmt.executeQuery("SELECT uuid_generate_v4() AS id")) {
                assertTrue(rs.next());
                assertDoesNotThrow(() -> java.util.UUID.fromString(rs.getString("id")));
            }
        }
    }

    // ---- ::jsonb cast tests ----

    @Test
    void shouldCastStringToJsonb() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery("SELECT '{\"key\": \"value\"}'::jsonb AS data")) {
                assertTrue(rs.next());
                assertEquals("{\"key\": \"value\"}", rs.getString("data"));
            }
        }
    }

    @Test
    void shouldInsertWithJsonbCast() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE configs (id integer, data jsonb)");
            stmt.execute("INSERT INTO configs (id, data) VALUES (1, '{\"setting\": true}'::jsonb)");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM configs WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("{\"setting\": true}", rs.getString("data"));
            }
        }
    }

    @Test
    void shouldCastToOtherTypes() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            // Cast to text
            try (ResultSet rs = stmt.executeQuery("SELECT '42'::text AS val")) {
                assertTrue(rs.next());
                assertEquals("42", rs.getString("val"));
            }
        }
    }

    @Test
    void shouldInsertJsonbArrays() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE events (id integer, tags jsonb)");
            stmt.execute("INSERT INTO events (id, tags) VALUES (1, '[\"a\", \"b\", \"c\"]'::jsonb)");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM events WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("[\"a\", \"b\", \"c\"]", rs.getString("tags"));
            }
        }
    }

    // ---- Combined feature test ----

    @Test
    void shouldCombineMultiplePgFeatures() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            // Use enums, uuid, inet, jsonb, and triggers together
            stmt.execute("CREATE TYPE role AS ENUM ('admin', 'user', 'guest')");

            stmt.execute("CREATE TABLE accounts (" +
                    "id uuid DEFAULT gen_random_uuid(), " +
                    "username text NOT NULL, " +
                    "role role, " +
                    "login_ip inet, " +
                    "metadata jsonb, " +
                    "created_at text" +
                    ")");

            stmt.execute("CREATE FUNCTION set_account_created() RETURNS trigger AS $$ " +
                    "BEGIN " +
                    "NEW.created_at = NOW(); " +
                    "RETURN NEW; " +
                    "END; " +
                    "$$ LANGUAGE plpgsql");

            stmt.execute("CREATE TRIGGER trg_account_created BEFORE INSERT ON accounts " +
                    "FOR EACH ROW EXECUTE FUNCTION set_account_created()");

            stmt.execute("INSERT INTO accounts (username, role, login_ip, metadata) " +
                    "VALUES ('alice', 'admin', '10.0.0.1', '{\"theme\": \"dark\"}'::jsonb)");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM accounts WHERE username = 'alice'")) {
                assertTrue(rs.next());

                // UUID was auto-generated
                String id = rs.getString("id");
                assertNotNull(id);
                assertDoesNotThrow(() -> java.util.UUID.fromString(id));

                assertEquals("alice", rs.getString("username"));
                assertEquals("admin", rs.getString("role"));
                assertEquals("10.0.0.1", rs.getString("login_ip"));
                assertEquals("{\"theme\": \"dark\"}", rs.getString("metadata"));

                // Trigger set the timestamp
                assertNotNull(rs.getString("created_at"));
            }
        }
    }
}
