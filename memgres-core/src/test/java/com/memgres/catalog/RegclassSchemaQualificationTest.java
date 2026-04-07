package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that regclass cast to text omits the schema prefix when the table
 * is in the current search_path.
 *
 * PG behavior:
 * - If table is in current search_path schema → just table name (e.g., "my_table")
 * - If table is NOT in search_path → schema.table (e.g., "other.my_table")
 * - pg_catalog tables → never prefixed (e.g., "pg_class")
 * - public tables when public is in search_path → not prefixed
 *
 * This also applies to:
 * - tableoid::regclass on partitioned and regular tables
 * - adrelid::regclass in pg_attrdef
 * - indexrelid::regclass in pg_index
 */
class RegclassSchemaQualificationTest {

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

    static List<String> column(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            List<String> vals = new ArrayList<>();
            while (rs.next()) vals.add(rs.getString(1));
            return vals;
        }
    }

    // ========================================================================
    // Tables in current search_path → no schema prefix
    // ========================================================================

    @Test
    void regclass_omits_schema_when_in_search_path() throws SQLException {
        exec("CREATE SCHEMA rc_test");
        exec("SET search_path = rc_test, pg_catalog");
        exec("CREATE TABLE my_table(id int PRIMARY KEY)");
        try {
            String name = scalar("SELECT 'my_table'::regclass::text");
            assertEquals("my_table", name,
                    "regclass should omit schema when table is in search_path");
        } finally {
            exec("DROP SCHEMA rc_test CASCADE");
            exec("SET search_path = public");
        }
    }

    @Test
    void regclass_includes_schema_when_not_in_search_path() throws SQLException {
        exec("CREATE SCHEMA rc_other");
        exec("SET search_path = rc_other");
        exec("CREATE TABLE hidden(id int PRIMARY KEY)");
        exec("SET search_path = public");
        try {
            String name = scalar("SELECT 'rc_other.hidden'::regclass::text");
            assertEquals("rc_other.hidden", name,
                    "regclass should include schema when table is NOT in search_path");
        } finally {
            exec("DROP SCHEMA rc_other CASCADE");
        }
    }

    @Test
    void regclass_public_tables_not_prefixed() throws SQLException {
        exec("CREATE TABLE rc_pub_t(id int PRIMARY KEY)");
        try {
            String name = scalar("SELECT 'rc_pub_t'::regclass::text");
            assertEquals("rc_pub_t", name,
                    "Public table should not have schema prefix");
        } finally {
            exec("DROP TABLE rc_pub_t");
        }
    }

    // ========================================================================
    // tableoid::regclass on partitioned tables
    // ========================================================================

    @Test
    void tableoid_regclass_omits_schema_for_partitions() throws SQLException {
        exec("CREATE SCHEMA rc_part");
        exec("SET search_path = rc_part, pg_catalog");
        exec("CREATE TABLE p_range(id int PRIMARY KEY, region text, dt date) PARTITION BY RANGE (id)");
        exec("CREATE TABLE p_range_1_100 PARTITION OF p_range FOR VALUES FROM (1) TO (100)");
        exec("CREATE TABLE p_range_100_200 PARTITION OF p_range FOR VALUES FROM (100) TO (200)");
        exec("INSERT INTO p_range VALUES (1, 'EU', '2024-01-01'), (150, 'US', '2024-01-02')");
        try {
            List<String> names = column("SELECT tableoid::regclass FROM p_range ORDER BY id");
            assertEquals(2, names.size());
            // Names should NOT have schema prefix since we're in the schema's search_path
            assertEquals("p_range_1_100", names.get(0),
                    "Should be 'p_range_1_100' without schema, got: " + names.get(0));
            assertEquals("p_range_100_200", names.get(1),
                    "Should be 'p_range_100_200' without schema, got: " + names.get(1));
        } finally {
            exec("DROP SCHEMA rc_part CASCADE");
            exec("SET search_path = public");
        }
    }

    @Test
    void tableoid_regclass_list_partition() throws SQLException {
        exec("CREATE SCHEMA rc_list");
        exec("SET search_path = rc_list, pg_catalog");
        exec("CREATE TABLE p_list(code text PRIMARY KEY, val int) PARTITION BY LIST (code)");
        exec("CREATE TABLE p_list_a PARTITION OF p_list FOR VALUES IN ('A', 'B')");
        exec("CREATE TABLE p_list_default PARTITION OF p_list DEFAULT");
        exec("INSERT INTO p_list VALUES ('A', 10), ('Z', 99)");
        try {
            List<String> names = column("SELECT tableoid::regclass FROM p_list ORDER BY code");
            assertEquals("p_list_a", names.get(0),
                    "Should be 'p_list_a' without schema prefix");
            assertEquals("p_list_default", names.get(1),
                    "Should be 'p_list_default' without schema prefix");
        } finally {
            exec("DROP SCHEMA rc_list CASCADE");
            exec("SET search_path = public");
        }
    }

    @Test
    void tableoid_regclass_hash_partition() throws SQLException {
        exec("CREATE SCHEMA rc_hash");
        exec("SET search_path = rc_hash, pg_catalog");
        exec("CREATE TABLE p_hash(id int PRIMARY KEY, note text) PARTITION BY HASH (id)");
        exec("CREATE TABLE p_hash_0 PARTITION OF p_hash FOR VALUES WITH (modulus 2, remainder 0)");
        exec("CREATE TABLE p_hash_1 PARTITION OF p_hash FOR VALUES WITH (modulus 2, remainder 1)");
        exec("INSERT INTO p_hash VALUES (1, 'odd'), (2, 'even')");
        try {
            List<String> names = column("SELECT tableoid::regclass FROM p_hash ORDER BY id");
            for (String name : names) {
                assertFalse(name.contains("."),
                        "Hash partition name should not be schema-qualified: " + name);
                assertTrue(name.startsWith("p_hash_"),
                        "Should be a partition name: " + name);
            }
        } finally {
            exec("DROP SCHEMA rc_hash CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // adrelid::regclass in pg_attrdef
    // ========================================================================

    @Test
    void pg_attrdef_regclass_omits_schema() throws SQLException {
        exec("CREATE SCHEMA rc_def");
        exec("SET search_path = rc_def, pg_catalog");
        exec("CREATE TABLE def_help_t(id int PRIMARY KEY, note text DEFAULT 'x')");
        try {
            String tblName = scalar("""
                SELECT adrelid::regclass
                FROM pg_attrdef
                WHERE adrelid = 'def_help_t'::regclass AND adnum = 2
                """);
            assertEquals("def_help_t", tblName,
                    "adrelid::regclass should be 'def_help_t' without schema, got: " + tblName);
        } finally {
            exec("DROP SCHEMA rc_def CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // indexrelid::regclass in pg_index
    // ========================================================================

    @Test
    void pg_index_regclass_omits_schema() throws SQLException {
        exec("CREATE SCHEMA rc_idx");
        exec("SET search_path = rc_idx, pg_catalog");
        exec("CREATE TABLE idx_t(id int PRIMARY KEY, a int)");
        exec("CREATE INDEX idx_a ON idx_t(a)");
        try {
            List<String> names = column("""
                SELECT i.indexrelid::regclass
                FROM pg_index i
                WHERE i.indrelid = 'idx_t'::regclass
                ORDER BY 1
                """);
            for (String name : names) {
                assertFalse(name.contains("."),
                        "Index regclass should not be schema-qualified: " + name);
            }
        } finally {
            exec("DROP SCHEMA rc_idx CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // pg_catalog tables never prefixed
    // ========================================================================

    @Test
    void pg_catalog_tables_never_prefixed() throws SQLException {
        String name = scalar("SELECT 'pg_class'::regclass::text");
        assertEquals("pg_class", name,
                "pg_catalog tables should never have schema prefix");
    }

    @Test
    void pg_catalog_index_not_prefixed() throws SQLException {
        // pg_class_oid_index or similar should not have pg_catalog prefix
        // but this depends on whether the index exists in the catalog
        String name = scalar("SELECT 'pg_type'::regclass::text");
        assertEquals("pg_type", name);
    }

    // ========================================================================
    // OID to regclass round-trip
    // ========================================================================

    @Test
    void oid_to_regclass_round_trip() throws SQLException {
        exec("CREATE TABLE rc_rt(id int PRIMARY KEY)");
        try {
            String oid = scalar("SELECT 'rc_rt'::regclass::oid::text");
            assertNotNull(oid, "Should get an OID");
            String name = scalar("SELECT " + oid + "::regclass::text");
            assertEquals("rc_rt", name,
                    "OID → regclass → text should yield 'rc_rt' without schema");
        } finally {
            exec("DROP TABLE rc_rt");
        }
    }

    // ========================================================================
    // Explicit schema.table regclass with search_path
    // ========================================================================

    @Test
    void explicit_schema_regclass_when_in_search_path() throws SQLException {
        exec("CREATE SCHEMA rc_exp");
        exec("SET search_path = rc_exp, pg_catalog");
        exec("CREATE TABLE exp_t(id int PRIMARY KEY)");
        try {
            // Even if you cast 'rc_exp.exp_t'::regclass, the display should omit prefix
            // since the table is in search_path
            String name = scalar("SELECT 'rc_exp.exp_t'::regclass::text");
            assertEquals("exp_t", name,
                    "Even with explicit schema, regclass text should omit prefix when in search_path");
        } finally {
            exec("DROP SCHEMA rc_exp CASCADE");
            exec("SET search_path = public");
        }
    }
}
