package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: Locale & collation semantics.
 *
 * - ICU provider not implemented
 * - Non-deterministic collations not semantically enforced
 * - lower/upper use JVM String.toLowerCase (Turkish İ, German ß divergent)
 * - collversion column missing
 * - normalize(), convert/convert_from/convert_to missing
 * - client_encoding not tracked server-side
 */
class Round14LocaleCollationTest {

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

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. pg_collation catalog
    // =========================================================================

    @Test
    void pg_collation_has_collversion() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM information_schema.columns "
                        + "WHERE table_name = 'pg_collation' AND column_name = 'collversion'"));
    }

    @Test
    void pg_collation_has_collprovider() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM information_schema.columns "
                        + "WHERE table_name = 'pg_collation' AND column_name = 'collprovider'"));
    }

    @Test
    void pg_collation_has_colliculocale() throws SQLException {
        // PG 15+: colliculocale is present on ICU entries
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM information_schema.columns "
                        + "WHERE table_name = 'pg_collation' AND column_name = 'colliculocale'"));
    }

    @Test
    void pg_collation_has_deterministic() throws SQLException {
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM information_schema.columns "
                        + "WHERE table_name = 'pg_collation' AND column_name = 'collisdeterministic'"));
    }

    // =========================================================================
    // B. CREATE COLLATION variants
    // =========================================================================

    @Test
    void create_collation_from_locale() throws SQLException {
        exec("CREATE COLLATION r14_coll_loc (locale = 'en_US.UTF-8')");
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_collation WHERE collname = 'r14_coll_loc'"));
    }

    @Test
    void create_collation_icu_provider() throws SQLException {
        try {
            exec("CREATE COLLATION r14_coll_icu (provider = icu, locale = 'und-u-ks-level2')");
            assertEquals(1, scalarInt(
                    "SELECT count(*)::int FROM pg_collation WHERE collname = 'r14_coll_icu'"));
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"),
                    "provider=icu must parse, got: " + msg);
        }
    }

    @Test
    void create_collation_non_deterministic() throws SQLException {
        try {
            exec("CREATE COLLATION r14_coll_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)");
            // Non-deterministic case-insensitive collation
            String v = scalarString("SELECT ('ABC' = 'abc' COLLATE r14_coll_ci)::text");
            // PG boolean::text returns "true"/"false"
            assertEquals("true", v, "case-insensitive equality via non-deterministic collation");
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"),
                    "deterministic=false must parse, got: " + msg);
        }
    }

    @Test
    void create_collation_from_existing() throws SQLException {
        exec("CREATE COLLATION r14_coll_from FROM \"C\"");
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_collation WHERE collname = 'r14_coll_from'"));
    }

    // =========================================================================
    // C. Turkish / German casing
    // =========================================================================

    @Test
    void turkish_upper_dotted_i_via_icu() throws SQLException {
        // PG with icu_tr_TR: upper('i') = 'İ' (U+0130)
        try {
            exec("CREATE COLLATION IF NOT EXISTS r14_tr (provider = icu, locale = 'tr-TR')");
            assertEquals("İ",
                    scalarString("SELECT upper('i' COLLATE r14_tr)"));
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"),
                    "Turkish ICU collation must parse, got: " + msg);
        }
    }

    @Test
    void german_lower_sharp_s() throws SQLException {
        // PG: lower('ẞ') = 'ß' (when locale supports it)
        String v = scalarString("SELECT lower('\u1E9E')");
        assertEquals("ß", v, "PG lowercase of U+1E9E (capital sharp S) should be ß");
    }

    @Test
    void initcap_in_default_locale() throws SQLException {
        // PG's initcap is letter-boundary aware
        assertEquals("Hello World",
                scalarString("SELECT initcap('hello world')"));
    }

    // =========================================================================
    // D. normalize()
    // =========================================================================

    @Test
    void normalize_nfc_default() throws SQLException {
        // U+00E9 (é) NFC form is unchanged
        assertEquals("é", scalarString("SELECT normalize('é')"));
    }

    @Test
    void normalize_nfd() throws SQLException {
        // 'é' (U+00E9) in NFD becomes 'e' + '\u0301' (combining acute)
        String v = scalarString("SELECT normalize('é', NFD)");
        assertEquals("e\u0301", v, "normalize(NFD) of é must produce e + combining acute");
    }

    @Test
    void normalize_nfkc() throws SQLException {
        // ﬁ (U+FB01) normalizes in NFKC to 'fi'
        assertEquals("fi", scalarString("SELECT normalize('\uFB01', NFKC)"));
    }

    @Test
    void is_normalized_predicate() throws SQLException {
        // PG 13+: IS NFC NORMALIZED
        assertEquals("true", scalarString("SELECT ('abc' IS NFC NORMALIZED)::text"));
    }

    // =========================================================================
    // E. convert / convert_from / convert_to
    // =========================================================================

    @Test
    void convert_to_utf8() throws SQLException {
        // convert_to('abc', 'UTF8') -> \x616263
        assertEquals("\\x616263",
                scalarString("SELECT convert_to('abc', 'UTF8')::text"));
    }

    @Test
    void convert_from_utf8() throws SQLException {
        assertEquals("abc",
                scalarString("SELECT convert_from('\\x616263'::bytea, 'UTF8')"));
    }

    @Test
    void convert_function() throws SQLException {
        // convert(bytes, src_enc, dest_enc)
        String v = scalarString(
                "SELECT convert('\\x616263'::bytea, 'UTF8', 'UTF8')::text");
        assertNotNull(v);
    }

    // =========================================================================
    // F. client_encoding
    // =========================================================================

    @Test
    void pg_client_encoding_is_utf8() throws SQLException {
        // Standard value
        assertEquals("UTF8", scalarString("SELECT pg_client_encoding()"));
    }

    @Test
    void set_client_encoding_reflected_in_show() throws SQLException {
        exec("SET client_encoding = 'UTF8'");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SHOW client_encoding")) {
            assertTrue(rs.next());
            assertEquals("UTF8", rs.getString(1));
        }
    }

    // =========================================================================
    // G. unicode_assigned (PG 17+)
    // =========================================================================

    @Test
    void unicode_assigned_assigned_codepoint() throws SQLException {
        // 'A' is assigned
        assertEquals("true", scalarString("SELECT unicode_assigned('A')::text"));
    }

    @Test
    void unicode_assigned_unassigned_codepoint() throws SQLException {
        // U+E0000 is unassigned
        assertEquals("false", scalarString("SELECT unicode_assigned(U&'\\+0E0000')::text"));
    }

    // =========================================================================
    // H. unicode_version (PG 13+)
    // =========================================================================

    @Test
    void unicode_version_is_reported() throws SQLException {
        String v = scalarString("SELECT unicode_version()");
        assertNotNull(v);
        assertTrue(v.matches("\\d+\\.\\d+(\\.\\d+)?"),
                "unicode_version must match N.N[.N] form; got " + v);
    }

    @Test
    void icu_unicode_version_is_reported() throws SQLException {
        String v = scalarString("SELECT icu_unicode_version()");
        assertNotNull(v);
    }
}
