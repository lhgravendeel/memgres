package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 24: PG-vs-Memgres gap closure tests. Each test corresponds to a
 * concrete, reproducible difference between real PG 18 and Memgres as
 * observed via FeatureComparisonReport. Tests are written TDD-style:
 * they must fail before the matching core fix is applied, then pass
 * after. Every test here targets one isolated gap; keep them atomic.
 */
class Round24PgGapTest {

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

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // Missing scalar functions
    // ========================================================================

    @Test
    void trim_scale_removes_trailing_zeros_from_numeric() throws SQLException {
        // trim_scale(NUMERIC '123.4500') -> 123.45 in PG 18
        assertEquals("123.45", q("SELECT trim_scale(NUMERIC '123.4500')"));
        assertEquals("100", q("SELECT trim_scale(NUMERIC '100.000')"));
        assertEquals("0", q("SELECT trim_scale(NUMERIC '0.0')"));
    }

    @Test
    void scale_returns_numeric_scale() throws SQLException {
        assertEquals("2", q("SELECT scale(NUMERIC '10.25')"));
        assertEquals("0", q("SELECT scale(NUMERIC '100')"));
        assertEquals("4", q("SELECT scale(NUMERIC '1.2345')"));
    }

    @Test
    void bit_count_returns_set_bit_count_of_bytea() throws SQLException {
        // bit_count('\xff'::bytea) -> 8
        assertEquals("8", q("SELECT bit_count('\\xff'::bytea)::int"));
        assertEquals("0", q("SELECT bit_count('\\x00'::bytea)::int"));
        assertEquals("4", q("SELECT bit_count('\\x0f'::bytea)::int"));
    }

    // ========================================================================
    // Missing JSONB path / set functions
    // ========================================================================

    @Test
    void jsonb_path_query_array_returns_aggregated_array() throws SQLException {
        // jsonb_path_query_array('[1,2,3]'::jsonb, '$[*]') -> [1, 2, 3]
        String result = q("SELECT jsonb_path_query_array('[1,2,3]'::jsonb, '$[*]')::text");
        assertNotNull(result);
        // Allow any spacing in the PG-style output
        String normalized = result.replaceAll("\\s+", "");
        assertEquals("[1,2,3]", normalized);
    }

    @Test
    void jsonb_set_lax_with_return_target_on_null() throws SQLException {
        // jsonb_set_lax('{"a":1}'::jsonb, '{b}', NULL, true, 'return_target')
        //   -> original unchanged when new_value is null and mode='return_target'
        String result = q("SELECT jsonb_set_lax('{\"a\":1}'::jsonb, '{b}', NULL, true, 'return_target')::text");
        assertNotNull(result);
        assertTrue(result.contains("\"a\"") && result.contains("1"),
                "return_target mode should return original when value is null, got: " + result);
    }

    // ========================================================================
    // JSONB semantic operators
    // ========================================================================

    @Test
    void jsonb_minus_text_array_removes_multiple_keys() throws SQLException {
        // '{"a":1,"b":2,"c":3}'::jsonb - ARRAY['a','b'] -> {"c": 3}
        String result = q("SELECT ('{\"a\":1,\"b\":2,\"c\":3}'::jsonb - ARRAY['a','b'])::text");
        assertNotNull(result);
        assertFalse(result.contains("\"a\""), "key 'a' should be removed: " + result);
        assertFalse(result.contains("\"b\""), "key 'b' should be removed: " + result);
        assertTrue(result.contains("\"c\""), "key 'c' should remain: " + result);
    }

    // ========================================================================
    // reg* system-OID types
    // ========================================================================

    @Test
    void regconfig_cast_produces_config_name() throws SQLException {
        // 'english'::regconfig -> text rendering 'english'
        assertEquals("english", q("SELECT 'english'::regconfig::text"));
    }

    @Test
    void regdictionary_cast_produces_dictionary_name() throws SQLException {
        // 'simple'::regdictionary -> 'simple'
        assertEquals("simple", q("SELECT 'simple'::regdictionary::text"));
    }

    @Test
    void pg_lsn_cast_preserves_value() throws SQLException {
        // '16/B374D848'::pg_lsn::text -> '16/B374D848'
        assertEquals("16/B374D848", q("SELECT '16/B374D848'::pg_lsn::text"));
    }

    // ========================================================================
    // pg_constraint catalog columns
    // ========================================================================

    @Test
    void pg_constraint_has_confmatchtype_and_conpfeqop_columns() throws SQLException {
        exec("DROP TABLE IF EXISTS r24_fk_child, r24_fk_parent CASCADE");
        exec("CREATE TABLE r24_fk_parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE r24_fk_child (pid INT REFERENCES r24_fk_parent(id))");
        try {
            // confmatchtype should be a char; default FK match type in PG is 's' (SIMPLE)
            String mt = q("SELECT confmatchtype FROM pg_constraint " +
                          "WHERE conrelid = 'r24_fk_child'::regclass AND contype = 'f'");
            assertNotNull(mt, "confmatchtype column must exist");
            assertFalse(mt.isEmpty(), "confmatchtype should be a single char");

            // conpfeqop is an oid[] of the equality operators; must be non-null for FK
            String nonempty = q("SELECT (array_length(conpfeqop, 1) > 0)::text " +
                               "FROM pg_constraint " +
                               "WHERE conrelid = 'r24_fk_child'::regclass AND contype = 'f'");
            assertEquals("true", nonempty, "conpfeqop should be a non-empty oid array for FK");
        } finally {
            exec("DROP TABLE IF EXISTS r24_fk_child, r24_fk_parent CASCADE");
        }
    }

    // ========================================================================
    // pg_collation seeded defaults (Phase 1 — should already pass)
    // ========================================================================

    @Test
    void pg_collation_has_en_us_utf8_row() throws SQLException {
        // memgres should ship 'en_US.utf8' so `COLLATE "en_US.utf8"` works
        assertEquals("1", q("SELECT count(*)::text FROM pg_collation WHERE collname = 'en_US.utf8'"));
    }

    @Test
    void pg_collation_has_icu_provider_rows() throws SQLException {
        // und-x-icu with collprovider='i'
        assertEquals("i", q("SELECT collprovider FROM pg_collation WHERE collname = 'und-x-icu'"));
    }

    // ========================================================================
    // Parser: CTE MATERIALIZED / NOT MATERIALIZED hint (PG 12+)
    // ========================================================================

    @Test
    void cte_materialized_hint_parses_and_runs() throws SQLException {
        assertEquals("1", q("WITH w AS MATERIALIZED (SELECT 1 AS v) SELECT v FROM w"));
    }

    @Test
    void cte_not_materialized_hint_parses_and_runs() throws SQLException {
        assertEquals("2", q("WITH w AS NOT MATERIALIZED (SELECT 2 AS v) SELECT v FROM w"));
    }

    // ========================================================================
    // Auth & privilege system
    // ========================================================================

    @Test
    void has_parameter_privilege_function_exists() throws SQLException {
        // PG 15+: has_parameter_privilege(user, parameter, privilege) -> boolean
        assertEquals("true", q("SELECT has_parameter_privilege('postgres','work_mem','ALTER SYSTEM')::text"));
    }

    @Test
    void pg_roles_connection_limit_populated_from_ddl() throws SQLException {
        exec("DROP ROLE IF EXISTS r24_auth_cl");
        exec("CREATE ROLE r24_auth_cl WITH LOGIN CONNECTION LIMIT 5");
        try {
            assertEquals("5",
                    q("SELECT rolconnlimit::text FROM pg_roles WHERE rolname = 'r24_auth_cl'"));
        } finally {
            exec("DROP ROLE IF EXISTS r24_auth_cl");
        }
    }

    @Test
    void starts_with_operator_caret_at_on_text() throws SQLException {
        // PG 11+: text ^@ text starts-with predicate
        assertEquals("true",  q("SELECT ('hello world' ^@ 'hello')::text"));
        assertEquals("false", q("SELECT ('hello world' ^@ 'world')::text"));
    }

    @Test
    void hostmask_of_cidr_returns_inverse_of_netmask() throws SQLException {
        // hostmask('192.168.1.0/24'::cidr) -> 0.0.0.255
        assertEquals("0.0.0.255/32", q("SELECT hostmask('192.168.1.0/24'::cidr)::text"));
        assertEquals("0.0.0.0/32",   q("SELECT hostmask('10.0.0.0/32'::cidr)::text"));
    }

    @Test
    void regnamespace_cast_renders_schema_name_not_oid() throws SQLException {
        // 'public'::regnamespace::text -> 'public'  (not its numeric OID)
        assertEquals("public", q("SELECT 'public'::regnamespace::text"));
    }

    @Test
    void bytea_set_bit_uses_msb_first_numbering_within_byte() throws SQLException {
        // PG bit numbering: bit 0 is MSB of first byte, so set_bit('\x00',0,1) = '\x80'
        assertEquals("\\x80", q("SELECT set_bit('\\x00'::bytea, 0, 1)::text"));
        assertEquals("\\x01", q("SELECT set_bit('\\x00'::bytea, 7, 1)::text"));
        assertEquals("\\x1234563890", q("SELECT set_bit('\\x1234567890'::bytea, 25, 0)::text"));
    }

    @Test
    void pg_sequence_last_value_returns_current_sequence_value() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS r24_slv_seq");
        exec("CREATE SEQUENCE r24_slv_seq");
        exec("SELECT nextval('r24_slv_seq')"); // sets to 1
        try {
            assertEquals("1", q("SELECT pg_sequence_last_value('r24_slv_seq'::regclass)::text"));
        } finally {
            exec("DROP SEQUENCE IF EXISTS r24_slv_seq");
        }
    }

    @Test
    void pg_class_relforcerowsecurity_reflects_force_rls() throws SQLException {
        exec("DROP TABLE IF EXISTS r24_rls_f CASCADE");
        exec("CREATE TABLE r24_rls_f (id int)");
        exec("ALTER TABLE r24_rls_f ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE r24_rls_f FORCE ROW LEVEL SECURITY");
        try {
            assertEquals("true",
                    q("SELECT relforcerowsecurity::text FROM pg_class WHERE relname = 'r24_rls_f'"));
            assertEquals("true",
                    q("SELECT relrowsecurity::text FROM pg_class WHERE relname = 'r24_rls_f'"));
        } finally {
            exec("DROP TABLE IF EXISTS r24_rls_f CASCADE");
        }
    }

    @Test
    void intersect_all_uses_multiset_semantics() throws SQLException {
        // Left: {1,1,1,2}, Right: {1,1,2,3}. INTERSECT ALL -> {1,1,2} (3 rows)
        String cnt = q("SELECT count(*)::text FROM ("
                + "SELECT x FROM (VALUES (1),(1),(1),(2)) v(x) "
                + "INTERSECT ALL "
                + "SELECT x FROM (VALUES (1),(1),(2),(3)) w(x) "
                + ") sub");
        assertEquals("3", cnt);
    }

    @Test
    void plpgsql_raise_too_many_params_errors() throws SQLException {
        // PG validates RAISE format string vs argument count at CREATE FUNCTION time
        var ex = assertThrows(SQLException.class, () ->
                exec("CREATE OR REPLACE FUNCTION r24_raise_extra() RETURNS void AS $$ "
                        + "BEGIN RAISE NOTICE 'no-placeholder', 'leftover-arg'; END; $$ LANGUAGE plpgsql"));
        assertTrue(ex.getMessage().contains("too many parameters"),
                "Expected 'too many parameters' error, got: " + ex.getMessage());
    }

    @Test
    void pg_auth_members_admin_option_tracks_with_admin_option() throws SQLException {
        exec("DROP ROLE IF EXISTS r24_auth_a2, r24_auth_a1");
        exec("CREATE ROLE r24_auth_a1");
        exec("CREATE ROLE r24_auth_a2");
        exec("GRANT r24_auth_a1 TO r24_auth_a2 WITH ADMIN OPTION");
        try {
            String ao = q("SELECT admin_option::text FROM pg_auth_members m "
                    + "JOIN pg_roles r ON m.roleid = r.oid "
                    + "JOIN pg_roles m2 ON m.member = m2.oid "
                    + "WHERE r.rolname = 'r24_auth_a1' AND m2.rolname = 'r24_auth_a2'");
            assertEquals("true", ao);
        } finally {
            exec("DROP ROLE IF EXISTS r24_auth_a2, r24_auth_a1");
        }
    }
}
