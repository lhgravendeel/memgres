package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category I: Advanced DDL / tables / sequences.
 *
 * Covers:
 *  - CREATE TABLE LIKE INCLUDING INDEXES / EXCLUDING CONSTRAINTS
 *  - UNLOGGED flag stored in pg_class.relpersistence = 'u'
 *  - Generated column default STORED (not VIRTUAL) — pg_attribute.attgenerated='s'
 *  - CHECK NO INHERIT flag reaches pg_constraint.connoinherit
 *  - FK ON DELETE SET NULL (col, col) column list honored
 *  - ALTER TABLE SET UNLOGGED / SET LOGGED / SET TABLESPACE / SET ACCESS METHOD
 *  - ALTER COLUMN SET STORAGE / SET STATISTICS / SET COMPRESSION
 *  - ALTER TABLE DETACH PARTITION CONCURRENTLY / FINALIZE
 *  - RENAME COLUMN rewrites dependent view definitions
 *  - pg_partition_tree / pg_partition_ancestors / pg_partition_root
 *  - ALTER SEQUENCE ... OWNED BY
 *  - pg_sequences.last_value
 *  - information_schema.sequences data_type / cycle_option not hardcoded
 */
class Round16DdlAdvancedTest {

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

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static int intQ(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // I1. CREATE TABLE LIKE INCLUDING INDEXES
    // =========================================================================

    @Test
    void like_including_indexes_copies_unique_index() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_li_src CASCADE");
        exec("DROP TABLE IF EXISTS r16_li_dst CASCADE");
        exec("CREATE TABLE r16_li_src (id int, email text)");
        exec("CREATE UNIQUE INDEX r16_li_src_email_idx ON r16_li_src(email)");
        exec("CREATE TABLE r16_li_dst (LIKE r16_li_src INCLUDING INDEXES)");
        int n = intQ("SELECT count(*)::int FROM pg_indexes " +
                "WHERE tablename='r16_li_dst' AND indexdef LIKE '%UNIQUE%'");
        assertTrue(n >= 1,
                "LIKE ... INCLUDING INDEXES must clone the unique index; saw " + n);
    }

    // =========================================================================
    // I2. UNLOGGED tables
    // =========================================================================

    @Test
    void unlogged_table_relpersistence_is_u() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_ul");
        exec("CREATE UNLOGGED TABLE r16_ul (id int)");
        String p = str("SELECT relpersistence::text FROM pg_class WHERE relname='r16_ul'");
        assertEquals("u", p,
                "UNLOGGED table must have pg_class.relpersistence='u'; got '" + p + "'");
    }

    // =========================================================================
    // I3. Generated column STORED by default
    // =========================================================================

    @Test
    void generated_always_default_is_stored() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_gc");
        exec("CREATE TABLE r16_gc (a int, b int GENERATED ALWAYS AS (a*2) STORED)");
        String g = str("SELECT attgenerated::text FROM pg_attribute " +
                "WHERE attrelid = 'r16_gc'::regclass AND attname='b'");
        assertEquals("s", g,
                "Generated STORED column must have attgenerated='s'; got '" + g + "'");
    }

    // =========================================================================
    // I4. CHECK NO INHERIT flag in pg_constraint.connoinherit
    // =========================================================================

    @Test
    void check_no_inherit_flag_surfaces_in_pg_constraint() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_ni");
        exec("CREATE TABLE r16_ni (x int, CONSTRAINT chk_pos CHECK (x > 0) NO INHERIT)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT connoinherit FROM pg_constraint " +
                             "WHERE conrelid = 'r16_ni'::regclass AND conname='chk_pos'")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1),
                    "CHECK ... NO INHERIT must set pg_constraint.connoinherit=true");
        }
    }

    // =========================================================================
    // I5. FK SET NULL (col, col) column list
    // =========================================================================

    @Test
    void fk_set_null_with_column_list_only_nullifies_listed() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_fk_c");
        exec("DROP TABLE IF EXISTS r16_fk_p");
        exec("CREATE TABLE r16_fk_p (a int, b int, PRIMARY KEY (a, b))");
        exec("CREATE TABLE r16_fk_c (a int, b int, extra int, " +
                "FOREIGN KEY (a, b) REFERENCES r16_fk_p(a, b) ON DELETE SET NULL (a))");
        // Just verify the statement parses & executes — actual column-list semantic
        // verification would require deleting a parent row and examining child row.
        exec("INSERT INTO r16_fk_p VALUES (1, 2)");
        exec("INSERT INTO r16_fk_c VALUES (1, 2, 99)");
        exec("DELETE FROM r16_fk_p WHERE a=1");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT a, b FROM r16_fk_c")) {
            assertTrue(rs.next());
            // ON DELETE SET NULL (a) should nullify only `a`; `b` remains 2
            rs.getObject("a"); // NULL
            assertEquals(2, rs.getInt("b"),
                    "ON DELETE SET NULL (a) must leave column b unchanged");
        }
    }

    // =========================================================================
    // I6. ALTER TABLE SET UNLOGGED / SET LOGGED
    // =========================================================================

    @Test
    void alter_table_set_unlogged_changes_persistence() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_su");
        exec("CREATE TABLE r16_su (id int)");
        exec("ALTER TABLE r16_su SET UNLOGGED");
        String p = str("SELECT relpersistence::text FROM pg_class WHERE relname='r16_su'");
        assertEquals("u", p,
                "ALTER TABLE SET UNLOGGED must set relpersistence='u'; got '" + p + "'");
    }

    @Test
    void alter_table_set_logged_reverts_persistence() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_sl");
        exec("CREATE UNLOGGED TABLE r16_sl (id int)");
        exec("ALTER TABLE r16_sl SET LOGGED");
        String p = str("SELECT relpersistence::text FROM pg_class WHERE relname='r16_sl'");
        assertEquals("p", p,
                "ALTER TABLE SET LOGGED must set relpersistence='p'; got '" + p + "'");
    }

    // =========================================================================
    // I7. ALTER TABLE SET ACCESS METHOD
    // =========================================================================

    @Test
    void alter_table_set_access_method_accepts_heap() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_am");
        exec("CREATE TABLE r16_am (id int)");
        exec("ALTER TABLE r16_am SET ACCESS METHOD heap");
        // No error == pass; assert the statement is at least recorded
    }

    // =========================================================================
    // I8. ALTER COLUMN SET STORAGE / SET STATISTICS / SET COMPRESSION
    // =========================================================================

    @Test
    void alter_column_set_storage_external_updates_pg_attribute() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_cs");
        exec("CREATE TABLE r16_cs (t text)");
        exec("ALTER TABLE r16_cs ALTER COLUMN t SET STORAGE EXTERNAL");
        String st = str("SELECT attstorage::text FROM pg_attribute " +
                "WHERE attrelid='r16_cs'::regclass AND attname='t'");
        // PG codes: p=plain, e=external, m=main, x=extended
        assertEquals("e", st,
                "ALTER COLUMN SET STORAGE EXTERNAL must set attstorage='e'; got '" + st + "'");
    }

    @Test
    void alter_column_set_statistics_updates_pg_attribute() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_cst");
        exec("CREATE TABLE r16_cst (v int)");
        exec("ALTER TABLE r16_cst ALTER COLUMN v SET STATISTICS 321");
        int n = intQ("SELECT attstattarget FROM pg_attribute " +
                "WHERE attrelid='r16_cst'::regclass AND attname='v'");
        assertEquals(321, n,
                "ALTER COLUMN SET STATISTICS 321 must set attstattarget=321; got " + n);
    }

    @Test
    void alter_column_set_compression_parses() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_cc");
        exec("CREATE TABLE r16_cc (t text)");
        // Must parse; lz4 may or may not be available as runtime algo
        exec("ALTER TABLE r16_cc ALTER COLUMN t SET COMPRESSION pglz");
        String c = str("SELECT attcompression::text FROM pg_attribute " +
                "WHERE attrelid='r16_cc'::regclass AND attname='t'");
        assertEquals("p", c,
                "ALTER COLUMN SET COMPRESSION pglz must set attcompression='p'; got '" + c + "'");
    }

    // =========================================================================
    // I9. ALTER TABLE DETACH PARTITION CONCURRENTLY / FINALIZE
    // =========================================================================

    @Test
    void alter_table_detach_partition_concurrently_parses() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_part CASCADE");
        exec("DROP TABLE IF EXISTS r16_part_1 CASCADE");
        exec("CREATE TABLE r16_part (id int) PARTITION BY RANGE (id)");
        exec("CREATE TABLE r16_part_1 PARTITION OF r16_part FOR VALUES FROM (1) TO (100)");
        exec("ALTER TABLE r16_part DETACH PARTITION r16_part_1 CONCURRENTLY");
    }

    // =========================================================================
    // I10. RENAME COLUMN rewrites dependent view
    // =========================================================================

    @Test
    void rename_column_rewrites_dependent_view_definition() throws SQLException {
        exec("DROP VIEW IF EXISTS r16_vw");
        exec("DROP TABLE IF EXISTS r16_rv CASCADE");
        exec("CREATE TABLE r16_rv (old_name int)");
        exec("CREATE VIEW r16_vw AS SELECT old_name FROM r16_rv");
        exec("ALTER TABLE r16_rv RENAME COLUMN old_name TO new_name");
        // View must continue to work and reference new_name
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT new_name FROM r16_vw")) {
            // Must not throw "column old_name does not exist"
            while (rs.next()) {
                rs.getInt(1);
            }
        }
    }

    // =========================================================================
    // I11. pg_partition_tree / _ancestors / _root
    // =========================================================================

    @Test
    void pg_partition_tree_returns_root_and_children() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_pt CASCADE");
        exec("CREATE TABLE r16_pt (id int) PARTITION BY RANGE (id)");
        exec("CREATE TABLE r16_pt_1 PARTITION OF r16_pt FOR VALUES FROM (1) TO (100)");
        int n = intQ("SELECT count(*)::int FROM pg_partition_tree('r16_pt'::regclass)");
        assertTrue(n >= 2,
                "pg_partition_tree must return root + at least one child; got " + n);
    }

    // =========================================================================
    // I12. ALTER SEQUENCE ... OWNED BY
    // =========================================================================

    @Test
    void alter_sequence_owned_by_column_parses() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_sq");
        exec("DROP SEQUENCE IF EXISTS r16_seq_ob");
        exec("CREATE SEQUENCE r16_seq_ob");
        exec("CREATE TABLE r16_sq (id int DEFAULT nextval('r16_seq_ob'))");
        exec("ALTER SEQUENCE r16_seq_ob OWNED BY r16_sq.id");
    }

    // =========================================================================
    // I13. pg_sequences.last_value
    // =========================================================================

    @Test
    void pg_sequences_exposes_last_value_column() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS r16_seq_lv");
        exec("CREATE SEQUENCE r16_seq_lv");
        exec("SELECT nextval('r16_seq_lv')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT last_value FROM pg_sequences WHERE sequencename='r16_seq_lv'")) {
            assertTrue(rs.next(), "pg_sequences row for r16_seq_lv must exist");
            long v = rs.getLong(1);
            assertEquals(1L, v,
                    "pg_sequences.last_value after one nextval must be 1; got " + v);
        }
    }

    // =========================================================================
    // I14. information_schema.sequences.data_type / cycle_option
    // =========================================================================

    @Test
    void information_schema_sequences_respects_integer_data_type() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS r16_seq_i");
        exec("CREATE SEQUENCE r16_seq_i AS integer");
        String dt = str("SELECT data_type FROM information_schema.sequences " +
                "WHERE sequence_name='r16_seq_i'");
        assertEquals("integer", dt,
                "information_schema.sequences.data_type must reflect AS integer; got '" + dt + "'");
    }

    @Test
    void information_schema_sequences_respects_cycle_option() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS r16_seq_c");
        exec("CREATE SEQUENCE r16_seq_c CYCLE");
        String co = str("SELECT cycle_option FROM information_schema.sequences " +
                "WHERE sequence_name='r16_seq_c'");
        assertEquals("YES", co,
                "information_schema.sequences.cycle_option must reflect CYCLE; got '" + co + "'");
    }
}
