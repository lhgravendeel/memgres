package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category D: Index variants whose PG-specific options must be
 * preserved in pg_index / pg_indexes.
 *
 * Covers:
 *  - INCLUDE covering columns
 *  - NULLS NOT DISTINCT (PG 15+)
 *  - DESC NULLS FIRST / ASC NULLS LAST
 *  - opclass preservation
 *  - CONCURRENTLY
 *  - ALTER INDEX ATTACH PARTITION
 *  - pg_indexes.indexdef preserves all options
 *  - Expression indexes
 */
class Round15IndexVariantsTest {

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

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // A. INCLUDE covering columns
    // =========================================================================

    @Test
    void index_include_preserved_in_indexdef() throws SQLException {
        exec("CREATE TABLE r15_ix_inc (a int, b int, c text)");
        exec("CREATE INDEX r15_ix_inc_idx ON r15_ix_inc (a) INCLUDE (b, c)");
        String def = scalarString(
                "SELECT indexdef FROM pg_indexes WHERE indexname='r15_ix_inc_idx'");
        assertNotNull(def);
        assertTrue(def.toUpperCase().contains("INCLUDE"),
                "indexdef must contain INCLUDE clause; got " + def);
        assertTrue(def.contains("b") && def.contains("c"),
                "indexdef must list included columns; got " + def);
    }

    @Test
    void index_include_pg_index_indkey_and_indnatts() throws SQLException {
        exec("CREATE TABLE r15_ix_i2 (a int, b int, c int)");
        exec("CREATE INDEX r15_ix_i2_idx ON r15_ix_i2 (a) INCLUDE (b, c)");
        // indnatts = total (3), indnkeyatts = key columns (1)
        int nat = scalarInt(
                "SELECT indnatts::int FROM pg_index i "
                        + "JOIN pg_class c ON i.indexrelid = c.oid "
                        + "WHERE c.relname='r15_ix_i2_idx'");
        int nka = scalarInt(
                "SELECT indnkeyatts::int FROM pg_index i "
                        + "JOIN pg_class c ON i.indexrelid = c.oid "
                        + "WHERE c.relname='r15_ix_i2_idx'");
        assertEquals(3, nat, "indnatts must include covering columns");
        assertEquals(1, nka, "indnkeyatts must count only key columns");
    }

    // =========================================================================
    // B. NULLS NOT DISTINCT (PG 15+)
    // =========================================================================

    @Test
    void unique_index_nulls_not_distinct_rejects_duplicate_nulls() throws SQLException {
        exec("CREATE TABLE r15_nnd (v int)");
        exec("CREATE UNIQUE INDEX r15_nnd_idx ON r15_nnd (v) NULLS NOT DISTINCT");
        exec("INSERT INTO r15_nnd VALUES (NULL)");
        try {
            exec("INSERT INTO r15_nnd VALUES (NULL)");
            fail("NULLS NOT DISTINCT should treat NULLs as equal — duplicate rejected");
        } catch (SQLException e) {
            // expected unique violation (23505)
            assertNotNull(e.getMessage());
        }
    }

    @Test
    void unique_index_nulls_not_distinct_in_indexdef() throws SQLException {
        exec("CREATE TABLE r15_nnd2 (v int)");
        exec("CREATE UNIQUE INDEX r15_nnd2_idx ON r15_nnd2 (v) NULLS NOT DISTINCT");
        String def = scalarString(
                "SELECT indexdef FROM pg_indexes WHERE indexname='r15_nnd2_idx'");
        assertTrue(def != null && def.toUpperCase().contains("NULLS NOT DISTINCT"),
                "indexdef should round-trip NULLS NOT DISTINCT; got " + def);
    }

    // =========================================================================
    // C. DESC / NULLS FIRST / NULLS LAST
    // =========================================================================

    @Test
    void index_desc_nulls_first_preserved() throws SQLException {
        exec("CREATE TABLE r15_ix_o (v int)");
        exec("CREATE INDEX r15_ix_o_idx ON r15_ix_o (v DESC NULLS FIRST)");
        String def = scalarString(
                "SELECT indexdef FROM pg_indexes WHERE indexname='r15_ix_o_idx'");
        assertTrue(def != null && def.toUpperCase().contains("DESC"),
                "indexdef must preserve DESC; got " + def);
        assertTrue(def.toUpperCase().contains("NULLS FIRST"),
                "indexdef must preserve NULLS FIRST; got " + def);
    }

    @Test
    void index_asc_nulls_last_preserved() throws SQLException {
        exec("CREATE TABLE r15_ix_o2 (v int)");
        exec("CREATE INDEX r15_ix_o2_idx ON r15_ix_o2 (v ASC NULLS LAST)");
        String def = scalarString(
                "SELECT indexdef FROM pg_indexes WHERE indexname='r15_ix_o2_idx'");
        assertNotNull(def);
        // ASC is the default so might be omitted; NULLS LAST is also default for ASC so may be omitted.
        // Just confirm the def is well-formed
        assertTrue(def.contains("r15_ix_o2"),
                "indexdef must reference the table; got " + def);
    }

    // =========================================================================
    // D. Opclass preservation
    // =========================================================================

    @Test
    void index_opclass_preserved() throws SQLException {
        exec("CREATE TABLE r15_ix_oc (v text)");
        exec("CREATE INDEX r15_ix_oc_idx ON r15_ix_oc (v text_pattern_ops)");
        String def = scalarString(
                "SELECT indexdef FROM pg_indexes WHERE indexname='r15_ix_oc_idx'");
        assertTrue(def != null && def.contains("text_pattern_ops"),
                "indexdef must preserve opclass; got " + def);
    }

    // =========================================================================
    // E. CONCURRENTLY
    // =========================================================================

    @Test
    void create_index_concurrently_accepted() throws SQLException {
        exec("CREATE TABLE r15_ix_c (id int)");
        exec("CREATE INDEX CONCURRENTLY r15_ix_c_idx ON r15_ix_c (id)");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_indexes WHERE indexname='r15_ix_c_idx'");
        assertEquals(1, n);
    }

    @Test
    void drop_index_concurrently_accepted() throws SQLException {
        exec("CREATE TABLE r15_ix_dc (id int)");
        exec("CREATE INDEX r15_ix_dc_idx ON r15_ix_dc (id)");
        exec("DROP INDEX CONCURRENTLY r15_ix_dc_idx");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_indexes WHERE indexname='r15_ix_dc_idx'");
        assertEquals(0, n);
    }

    // =========================================================================
    // F. ALTER INDEX ATTACH PARTITION
    // =========================================================================

    @Test
    void alter_index_attach_partition() throws SQLException {
        exec("CREATE TABLE r15_pp (id int, region int) PARTITION BY LIST (region)");
        exec("CREATE TABLE r15_pp_1 PARTITION OF r15_pp FOR VALUES IN (1)");
        exec("CREATE INDEX r15_pp_parent_idx ON r15_pp (id)");
        // Child should already have a matching index; force-attach is the variant we want
        exec("CREATE INDEX r15_pp_1_idx ON r15_pp_1 (id)");
        exec("ALTER INDEX r15_pp_parent_idx ATTACH PARTITION r15_pp_1_idx");

        int n = scalarInt(
                "SELECT count(*)::int FROM pg_index i "
                        + "JOIN pg_inherits h ON h.inhrelid = i.indexrelid "
                        + "JOIN pg_class pc ON h.inhparent = pc.oid "
                        + "WHERE pc.relname='r15_pp_parent_idx'");
        assertTrue(n >= 1,
                "ALTER INDEX ATTACH PARTITION should wire child→parent in pg_inherits");
    }

    // =========================================================================
    // G. Expression index
    // =========================================================================

    @Test
    void expression_index_indexdef_contains_expression() throws SQLException {
        exec("CREATE TABLE r15_ix_e (v text)");
        exec("CREATE INDEX r15_ix_e_idx ON r15_ix_e (lower(v))");
        String def = scalarString(
                "SELECT indexdef FROM pg_indexes WHERE indexname='r15_ix_e_idx'");
        assertTrue(def != null && def.toLowerCase().contains("lower"),
                "indexdef must contain expression 'lower(v)'; got " + def);
    }

    @Test
    void partial_index_indexdef_contains_where() throws SQLException {
        exec("CREATE TABLE r15_ix_p (id int, active bool)");
        exec("CREATE INDEX r15_ix_p_idx ON r15_ix_p (id) WHERE active");
        String def = scalarString(
                "SELECT indexdef FROM pg_indexes WHERE indexname='r15_ix_p_idx'");
        assertTrue(def != null && def.toUpperCase().contains("WHERE"),
                "indexdef must contain WHERE clause; got " + def);
    }

    // =========================================================================
    // H. B-tree / GIST / GIN / HASH method in indexdef
    // =========================================================================

    @Test
    void index_method_hash_in_indexdef() throws SQLException {
        exec("CREATE TABLE r15_ix_m (id int)");
        exec("CREATE INDEX r15_ix_m_idx ON r15_ix_m USING hash (id)");
        String def = scalarString(
                "SELECT indexdef FROM pg_indexes WHERE indexname='r15_ix_m_idx'");
        assertTrue(def != null && def.toLowerCase().contains("hash"),
                "indexdef must show USING hash; got " + def);
    }
}
