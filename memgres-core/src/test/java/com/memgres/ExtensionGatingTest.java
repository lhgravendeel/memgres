package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that extension functions are gated behind CREATE EXTENSION,
 * matching PG 18 behavior where contrib modules must be explicitly installed.
 */
class ExtensionGatingTest {
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

    // ---- The catalogue says what is there, and nothing more ----

    /**
     * A name an extension brings is nowhere at all until the extension is installed: pg_proc is
     * where a reader asks what routines there are, and a row for a call the server itself refuses
     * is an answer nothing can act on.
     */
    @Test void extension_functions_are_not_in_pg_proc_until_installed() throws SQLException {
        Memgres fresh = Memgres.builder().port(0).build().start();
        try (Connection c = DriverManager.getConnection(fresh.getJdbcUrl() + "?preferQueryMode=simple",
                fresh.getUser(), fresh.getPassword());
             Statement s = c.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT count(*)::int FROM pg_proc"
                    + " WHERE proname IN ('gen_salt','crypt','digest','hmac','gen_random_bytes',"
                    + "'levenshtein','soundex','unaccent','similarity','word_similarity')")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            try (ResultSet rs = s.executeQuery(
                    "SELECT to_regprocedure('gen_salt(text)') IS NULL")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
            s.execute("CREATE EXTENSION pgcrypto");
            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*)::int FROM pg_proc WHERE proname = 'gen_salt'")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) > 0);
            }
        } finally {
            fresh.close();
        }
    }

    /** A word SQL spells without parentheses is no routine of the catalogue. */
    @Test void the_value_words_are_not_routines() throws SQLException {
        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT count(*)::int FROM pg_proc"
                    + " WHERE proname IN ('user','current_role','current_catalog')")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            try (ResultSet rs = s.executeQuery("SELECT to_regproc('user') IS NULL")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
            // They are still read where a value belongs.
            try (ResultSet rs = s.executeQuery("SELECT user")) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
        }
    }

    // ---- uuid-ossp: gated ----

    @Test void uuid_generate_v4_requires_extension() {
        assertFunctionRequiresExtension("SELECT uuid_generate_v4()");
    }

    @Test void uuid_generate_v1_requires_extension() {
        assertFunctionRequiresExtension("SELECT uuid_generate_v1()");
    }

    @Test void uuid_nil_requires_extension() {
        assertFunctionRequiresExtension("SELECT uuid_nil()");
    }

    @Test void uuid_ns_dns_requires_extension() {
        assertFunctionRequiresExtension("SELECT uuid_ns_dns()");
    }

    @Test void uuid_generate_v4_works_after_create_extension() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
            ResultSet rs = s.executeQuery("SELECT uuid_generate_v4()::text");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            rs.close();
        }
    }

    // ---- gen_random_uuid: built-in, no extension needed ----

    @Test void gen_random_uuid_works_without_extension() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT gen_random_uuid()::text");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            rs.close();
        }
    }

    // ---- pgcrypto: gated ----

    @Test void digest_requires_extension() {
        assertFunctionRequiresExtension("SELECT digest('hello', 'sha256')");
    }

    @Test void hmac_requires_extension() {
        assertFunctionRequiresExtension("SELECT hmac('msg', 'key', 'sha256')");
    }

    @Test void gen_salt_requires_extension() {
        assertFunctionRequiresExtension("SELECT gen_salt('bf')");
    }

    @Test void digest_works_after_create_extension() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
            ResultSet rs = s.executeQuery("SELECT encode(digest('hello'::text, 'sha256'), 'hex')");
            assertTrue(rs.next());
            assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", rs.getString(1));
            rs.close();
        }
    }

    // ---- pg_trgm: gated ----

    @Test void show_trgm_requires_extension() {
        assertFunctionRequiresExtension("SELECT show_trgm('hello')");
    }

    @Test void similarity_requires_extension() {
        assertFunctionRequiresExtension("SELECT similarity('hello', 'hallo')");
    }

    @Test void similarity_works_after_create_extension() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm");
            ResultSet rs = s.executeQuery("SELECT similarity('hello', 'hallo') > 0 AS ok");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
            rs.close();
        }
    }

    // ---- fuzzystrmatch: gated ----

    @Test void levenshtein_requires_extension() {
        assertFunctionRequiresExtension("SELECT levenshtein('kitten', 'sitting')");
    }

    @Test void soundex_requires_extension() {
        assertFunctionRequiresExtension("SELECT soundex('Robert')");
    }

    @Test void levenshtein_works_after_create_extension() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch");
            ResultSet rs = s.executeQuery("SELECT levenshtein('kitten', 'sitting')");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            rs.close();
        }
    }

    // ---- unaccent: gated ----

    @Test void unaccent_requires_extension() {
        assertFunctionRequiresExtension("SELECT unaccent('café')");
    }

    @Test void unaccent_works_after_create_extension() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS unaccent");
            ResultSet rs = s.executeQuery("SELECT unaccent('café')");
            assertTrue(rs.next());
            assertEquals("cafe", rs.getString(1));
            rs.close();
        }
    }

    // ---- Helper ----

    private void assertFunctionRequiresExtension(String sql) {
        try (Statement s = conn.createStatement()) {
            // Use a fresh Memgres instance without extensions to test gating
            // Since our shared instance may already have extensions, we test via a new DB
            Memgres fresh = Memgres.builder().port(0).build().start();
            try (Connection c = DriverManager.getConnection(
                    fresh.getJdbcUrl() + "?preferQueryMode=simple",
                    fresh.getUser(), fresh.getPassword())) {
                c.setAutoCommit(true);
                try (Statement st = c.createStatement()) {
                    st.executeQuery(sql);
                    fail("Expected function to require extension: " + sql);
                }
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("does not exist"),
                        "Expected 'does not exist' error but got: " + e.getMessage());
            } finally {
                fresh.close();
            }
        } catch (Exception e) {
            if (e instanceof SQLException && e.getMessage().contains("does not exist")) return;
            fail("Unexpected error: " + e.getMessage());
        }
    }
}
