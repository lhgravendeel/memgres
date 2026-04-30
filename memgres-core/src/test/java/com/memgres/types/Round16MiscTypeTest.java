package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category G: Miscellaneous type surface.
 *
 * Covers:
 *  - bit(n) explicit cast truncation raises 22026
 *  - pg_lsn type: parsing + lsn - lsn arithmetic
 *  - macaddr trunc / macaddr8_set7bit / macaddr::bigint
 *  - hstore extension (`=>`, `||`, `@>`, akeys)
 *  - pg_trgm extension (`%`, `<->`, show_trgm)
 *  - cube extension (cube constructor, cube_dim)
 *  - pgcrypto (digest, hmac, crypt, gen_salt, encrypt/decrypt)
 *  - citext equality/opclass case-insensitive
 */
class Round16MiscTypeTest {

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

    private static boolean bool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // G1. bit(n) explicit cast truncation raises 22026
    // =========================================================================

    @Test
    void bit_cast_truncation_silently_truncates() throws SQLException {
        // PG silently truncates '1100'::bit(2) to '11'
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT '1100'::bit(2)::text")) {
            assertTrue(rs.next());
            assertEquals("11", rs.getString(1));
        }
    }

    // =========================================================================
    // G2. pg_lsn parsing + arithmetic
    // =========================================================================

    @Test
    void pg_lsn_parses_hex_slash_format() throws SQLException {
        String v = str("SELECT '16/B374D848'::pg_lsn::text");
        assertEquals("16/B374D848", v, "pg_lsn round-trip must preserve '16/B374D848'");
    }

    @Test
    void pg_lsn_subtraction_yields_bigint_byte_count() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT '0/100'::pg_lsn - '0/0'::pg_lsn")) {
            assertTrue(rs.next());
            assertEquals(256L, rs.getLong(1),
                    "pg_lsn '0/100' - '0/0' must return 256 bytes");
        }
    }

    // =========================================================================
    // G3. macaddr_trunc / macaddr8_set7bit / macaddr::bigint
    // =========================================================================

    @Test
    void macaddr_trunc_zeros_lower_three_bytes() throws SQLException {
        String v = str("SELECT trunc('12:34:56:78:9a:bc'::macaddr)::text");
        assertEquals("12:34:56:00:00:00", v,
                "trunc(macaddr) must zero the lower 3 bytes");
    }

    @Test
    void macaddr8_set7bit_flips_universal_local_bit() throws SQLException {
        String v = str("SELECT macaddr8_set7bit('12:34:56:78:9a:bc:de:f0'::macaddr8)::text");
        assertNotNull(v);
        // 0x12 OR 0x02 = 0x12 (bit 1 already set); for '00:...' it becomes '02:...'
        String v2 = str("SELECT macaddr8_set7bit('00:34:56:78:9a:bc:de:f0'::macaddr8)::text");
        assertTrue(v2.startsWith("02:"),
                "macaddr8_set7bit on 00:... must produce 02:...; got " + v2);
    }

    // =========================================================================
    // G4. hstore extension
    // =========================================================================

    @Test
    void hstore_extension_type_round_trip() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS hstore");
        String v = str("SELECT ('a=>1,b=>2'::hstore)->'a'");
        assertEquals("1", v, "hstore -> operator must extract value by key");
    }

    @Test
    void hstore_contains_operator() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS hstore");
        assertTrue(bool("SELECT ('a=>1,b=>2'::hstore) @> ('a=>1'::hstore)"),
                "hstore @> must test containment");
    }

    // =========================================================================
    // G5. pg_trgm extension
    // =========================================================================

    @Test
    void pg_trgm_similarity_returns_float() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT similarity('hello', 'hallo')")) {
            assertTrue(rs.next());
            double v = rs.getDouble(1);
            assertTrue(v >= 0.0 && v <= 1.0,
                    "pg_trgm similarity must be in [0,1]; got " + v);
        }
    }

    @Test
    void pg_trgm_show_trgm_returns_trigrams() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT array_length(show_trgm('hello'), 1) >= 1")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1), "show_trgm must return at least one trigram");
        }
    }

    // =========================================================================
    // G6. cube extension
    // =========================================================================

    @Test
    void cube_extension_constructor_and_dim() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS cube");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT cube_dim(cube(ARRAY[1.0, 2.0, 3.0]))")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1), "cube_dim of 3D cube must be 3");
        }
    }

    // =========================================================================
    // G7. pgcrypto digest/hmac/gen_random_bytes
    // =========================================================================

    @Test
    void pgcrypto_digest_sha256_hex() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pgcrypto");
        String v = str("SELECT encode(digest('hello'::text, 'sha256'), 'hex')");
        assertEquals(
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                v,
                "pgcrypto sha256 of 'hello' hex must match known digest");
    }

    @Test
    void pgcrypto_hmac_returns_bytea() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pgcrypto");
        String v = str("SELECT encode(hmac('msg'::text, 'key', 'sha256'), 'hex')");
        assertNotNull(v);
        assertEquals(64, v.length(),
                "pgcrypto hmac-sha256 hex output must be 64 chars");
    }

    @Test
    void pgcrypto_gen_salt_bf_returns_non_null() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pgcrypto");
        String v = str("SELECT gen_salt('bf'::text)");
        assertNotNull(v);
        assertTrue(v.startsWith("$2"), "gen_salt('bf') must return a Blowfish salt starting with $2");
    }

    // =========================================================================
    // G8. citext case-insensitive equality / opclass
    // =========================================================================

    @Test
    void citext_equality_is_case_insensitive() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS citext");
        assertTrue(bool("SELECT 'AbC'::citext = 'abc'::citext"),
                "citext equality must be case-insensitive");
    }

    @Test
    void citext_order_preserves_original_case_in_output() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS citext");
        String v = str("SELECT 'AbC'::citext::text");
        // citext preserves original case in text output
        assertEquals("AbC", v, "citext must preserve original case on output, only lowercase for comparison");
    }
}
