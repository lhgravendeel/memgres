package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ALTER TABLE constraint-integrity fixes:
 * - RENAME COLUMN keeps PK/UNIQUE/CHECK/FK enforcement attached (and preserves all column attributes)
 * - ADD PRIMARY KEY / ADD UNIQUE validate existing rows (23505 duplicates, 23502 NULLs in PK)
 *   and ADD PRIMARY KEY marks columns NOT NULL
 * - ADD COLUMN with a volatile default evaluates per existing row; serial/identity backfills
 * - ALTER COLUMN TYPE without USING only allows assignment-castable conversions (42804 otherwise)
 * - DROP COLUMN drops dependent constraints instead of leaving them stale in pg_constraint
 */
class AlterTableConstraintIntegrityTest {

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
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private SQLException expectError(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), "expected error from: " + sql);
        return e;
    }

    // =========================================================================
    // 1. RENAME COLUMN keeps constraints attached
    // =========================================================================

    @Test
    void rename_column_keeps_primary_key_enforcement() throws SQLException {
        exec("CREATE TABLE rn_pk (id int PRIMARY KEY, v text)");
        exec("INSERT INTO rn_pk VALUES (1, 'a')");
        exec("ALTER TABLE rn_pk RENAME COLUMN id TO ident");
        // duplicate PK must still be rejected under the new name
        SQLException e = expectError("INSERT INTO rn_pk VALUES (1, 'b')");
        assertEquals("23505", e.getSQLState());
        // non-duplicate still works
        exec("INSERT INTO rn_pk VALUES (2, 'b')");
        // old column name is gone
        SQLException e2 = expectError("INSERT INTO rn_pk (id, v) VALUES (3, 'c')");
        assertEquals("42703", e2.getSQLState());
        exec("DROP TABLE rn_pk");
    }

    @Test
    void rename_column_keeps_unique_enforcement() throws SQLException {
        exec("CREATE TABLE rn_uq (id int, email text UNIQUE)");
        exec("INSERT INTO rn_uq VALUES (1, 'a@x.com')");
        exec("ALTER TABLE rn_uq RENAME COLUMN email TO mail");
        SQLException e = expectError("INSERT INTO rn_uq VALUES (2, 'a@x.com')");
        assertEquals("23505", e.getSQLState());
        exec("INSERT INTO rn_uq VALUES (2, 'b@x.com')");
        exec("DROP TABLE rn_uq");
    }

    @Test
    void rename_column_keeps_check_enforcement() throws SQLException {
        exec("CREATE TABLE rn_ck (id int, price int CHECK (price > 0))");
        exec("INSERT INTO rn_ck VALUES (1, 10)");
        exec("ALTER TABLE rn_ck RENAME COLUMN price TO amount");
        // valid row must not fail with 42703 (stale old column name in CHECK)
        exec("INSERT INTO rn_ck VALUES (2, 20)");
        SQLException e = expectError("INSERT INTO rn_ck VALUES (3, -5)");
        assertEquals("23514", e.getSQLState());
        // UPDATE path too
        SQLException e2 = expectError("UPDATE rn_ck SET amount = -1 WHERE id = 1");
        assertEquals("23514", e2.getSQLState());
        exec("DROP TABLE rn_ck");
    }

    @Test
    void rename_referenced_column_keeps_fk_enforcement() throws SQLException {
        exec("CREATE TABLE rn_parent (id int PRIMARY KEY)");
        exec("CREATE TABLE rn_child (cid int, pid int REFERENCES rn_parent(id))");
        exec("INSERT INTO rn_parent VALUES (1)");
        exec("INSERT INTO rn_child VALUES (1, 1)");
        exec("ALTER TABLE rn_parent RENAME COLUMN id TO pkid");
        // FK still enforced against the renamed referenced column
        SQLException e = expectError("INSERT INTO rn_child VALUES (2, 99)");
        assertEquals("23503", e.getSQLState());
        exec("INSERT INTO rn_child VALUES (3, 1)");
        exec("DROP TABLE rn_child");
        exec("DROP TABLE rn_parent");
    }

    @Test
    void rename_fk_local_column_keeps_fk_enforcement() throws SQLException {
        exec("CREATE TABLE rn_parent2 (id int PRIMARY KEY)");
        exec("CREATE TABLE rn_child2 (cid int, pid int REFERENCES rn_parent2(id))");
        exec("INSERT INTO rn_parent2 VALUES (1)");
        exec("ALTER TABLE rn_child2 RENAME COLUMN pid TO parent_ref");
        SQLException e = expectError("INSERT INTO rn_child2 VALUES (1, 42)");
        assertEquals("23503", e.getSQLState());
        exec("INSERT INTO rn_child2 VALUES (1, 1)");
        exec("DROP TABLE rn_child2");
        exec("DROP TABLE rn_parent2");
    }

    @Test
    void rename_column_updates_constraint_catalog_columns() throws SQLException {
        exec("CREATE TABLE rn_cat (id int, CONSTRAINT rn_cat_uq UNIQUE (id))");
        exec("ALTER TABLE rn_cat RENAME COLUMN id TO ident");
        String cols = query1(
                "SELECT string_agg(column_name, ',') FROM information_schema.constraint_column_usage "
                + "WHERE constraint_name = 'rn_cat_uq'");
        assertEquals("ident", cols);
        exec("DROP TABLE rn_cat");
    }

    // =========================================================================
    // 2. RENAME COLUMN preserves column attributes (enum/domain/array)
    // =========================================================================

    @Test
    void rename_column_preserves_enum_attributes() throws SQLException {
        exec("CREATE TYPE rn_mood AS ENUM ('happy', 'sad')");
        exec("CREATE TABLE rn_enum (id int, m rn_mood)");
        exec("INSERT INTO rn_enum VALUES (1, 'happy')");
        exec("ALTER TABLE rn_enum RENAME COLUMN m TO feeling");
        // enum validation still enforced -> enumTypeName preserved
        SQLException e = expectError("INSERT INTO rn_enum VALUES (2, 'angry')");
        assertEquals("22P02", e.getSQLState());
        exec("INSERT INTO rn_enum VALUES (2, 'sad')");
        assertEquals("happy", query1("SELECT feeling FROM rn_enum WHERE id = 1"));
        // catalog still reports the enum type
        assertEquals("rn_mood", query1(
                "SELECT udt_name FROM information_schema.columns "
                + "WHERE table_name = 'rn_enum' AND column_name = 'feeling'"));
        exec("DROP TABLE rn_enum");
        exec("DROP TYPE rn_mood");
    }

    @Test
    void rename_column_preserves_domain_attributes() throws SQLException {
        exec("CREATE DOMAIN rn_posint AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE rn_dom (id int, qty rn_posint)");
        exec("INSERT INTO rn_dom VALUES (1, 5)");
        exec("ALTER TABLE rn_dom RENAME COLUMN qty TO quantity");
        // domain CHECK still enforced -> domainTypeName preserved
        SQLException e = expectError("INSERT INTO rn_dom VALUES (2, -3)");
        assertEquals("23514", e.getSQLState());
        exec("INSERT INTO rn_dom VALUES (2, 7)");
        exec("DROP TABLE rn_dom");
        exec("DROP DOMAIN rn_posint");
    }

    @Test
    void rename_column_preserves_enum_array_attributes() throws SQLException {
        exec("CREATE TYPE rn_color AS ENUM ('red', 'green')");
        exec("CREATE TABLE rn_arr (id int, cs rn_color[])");
        exec("INSERT INTO rn_arr VALUES (1, ARRAY['red','green']::rn_color[])");
        exec("ALTER TABLE rn_arr RENAME COLUMN cs TO colors");
        // still readable as an array under the new name
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT colors FROM rn_arr WHERE id = 1")) {
            assertTrue(rs.next());
            Array arr = rs.getArray(1);
            assertNotNull(arr, "renamed enum-array column should still round-trip as an array");
            Object[] vals = (Object[]) arr.getArray();
            assertEquals(2, vals.length);
        }
        exec("DROP TABLE rn_arr");
        exec("DROP TYPE rn_color");
    }

    @Test
    void rename_column_preserves_default() throws SQLException {
        exec("CREATE TABLE rn_def (id int, status text DEFAULT 'active')");
        exec("ALTER TABLE rn_def RENAME COLUMN status TO state");
        exec("INSERT INTO rn_def (id) VALUES (1)");
        assertEquals("active", query1("SELECT state FROM rn_def WHERE id = 1"));
        exec("DROP TABLE rn_def");
    }

    // =========================================================================
    // 3. ADD PRIMARY KEY / ADD UNIQUE validate existing rows
    // =========================================================================

    @Test
    void add_primary_key_with_duplicates_errors_23505() throws SQLException {
        exec("CREATE TABLE apk_dup (id int, v text)");
        exec("INSERT INTO apk_dup VALUES (1, 'a'), (1, 'b')");
        SQLException e = expectError("ALTER TABLE apk_dup ADD PRIMARY KEY (id)");
        assertEquals("23505", e.getSQLState());
        assertTrue(e.getMessage().contains("could not create unique index"),
                "unexpected message: " + e.getMessage());
        // constraint must NOT have been added
        assertEquals("0", query1("SELECT count(*) FROM pg_constraint WHERE conrelid = 'apk_dup'::regclass AND contype = 'p'"));
        // and further duplicates are still accepted (no half-installed constraint)
        exec("INSERT INTO apk_dup VALUES (1, 'c')");
        exec("DROP TABLE apk_dup");
    }

    @Test
    void add_primary_key_with_nulls_errors_23502() throws SQLException {
        exec("CREATE TABLE apk_null (id int, v text)");
        exec("INSERT INTO apk_null VALUES (1, 'a'), (NULL, 'b')");
        SQLException e = expectError("ALTER TABLE apk_null ADD PRIMARY KEY (id)");
        assertEquals("23502", e.getSQLState());
        assertTrue(e.getMessage().contains("contains null values"),
                "unexpected message: " + e.getMessage());
        exec("DROP TABLE apk_null");
    }

    @Test
    void add_primary_key_marks_columns_not_null() throws SQLException {
        exec("CREATE TABLE apk_nn (id int, v text)");
        exec("INSERT INTO apk_nn VALUES (1, 'a')");
        exec("ALTER TABLE apk_nn ADD PRIMARY KEY (id)");
        assertEquals("NO", query1(
                "SELECT is_nullable FROM information_schema.columns "
                + "WHERE table_name = 'apk_nn' AND column_name = 'id'"));
        assertEquals("true", query1(
                "SELECT attnotnull::text FROM pg_attribute "
                + "WHERE attrelid = 'apk_nn'::regclass AND attname = 'id'"));
        // NOT NULL is enforced, not just displayed
        SQLException e = expectError("INSERT INTO apk_nn VALUES (NULL, 'b')");
        assertEquals("23502", e.getSQLState());
        exec("DROP TABLE apk_nn");
    }

    @Test
    void add_unique_with_duplicates_errors_23505() throws SQLException {
        exec("CREATE TABLE auq_dup (id int, email text)");
        exec("INSERT INTO auq_dup VALUES (1, 'x@y.com'), (2, 'x@y.com')");
        SQLException e = expectError("ALTER TABLE auq_dup ADD CONSTRAINT auq_dup_email_key UNIQUE (email)");
        assertEquals("23505", e.getSQLState());
        exec("DROP TABLE auq_dup");
    }

    @Test
    void add_unique_allows_multiple_nulls() throws SQLException {
        exec("CREATE TABLE auq_null (id int, email text)");
        exec("INSERT INTO auq_null VALUES (1, NULL), (2, NULL)");
        // NULLs are distinct by default: no error
        exec("ALTER TABLE auq_null ADD CONSTRAINT auq_null_email_key UNIQUE (email)");
        exec("DROP TABLE auq_null");
    }

    @Test
    void add_multicolumn_unique_with_duplicates_errors_23505() throws SQLException {
        exec("CREATE TABLE auq_multi (a int, b int)");
        exec("INSERT INTO auq_multi VALUES (1, 1), (1, 2), (1, 1)");
        SQLException e = expectError("ALTER TABLE auq_multi ADD CONSTRAINT auq_multi_key UNIQUE (a, b)");
        assertEquals("23505", e.getSQLState());
        exec("DROP TABLE auq_multi");
    }

    @Test
    void add_unique_succeeds_on_distinct_data_and_enforces() throws SQLException {
        exec("CREATE TABLE auq_ok (id int, email text)");
        exec("INSERT INTO auq_ok VALUES (1, 'a@x'), (2, 'b@x')");
        exec("ALTER TABLE auq_ok ADD CONSTRAINT auq_ok_email_key UNIQUE (email)");
        SQLException e = expectError("INSERT INTO auq_ok VALUES (3, 'a@x')");
        assertEquals("23505", e.getSQLState());
        exec("DROP TABLE auq_ok");
    }

    // =========================================================================
    // 4. ADD COLUMN with volatile default / serial backfill
    // =========================================================================

    @Test
    void add_column_volatile_uuid_default_is_per_row() throws SQLException {
        exec("CREATE TABLE ac_uuid (id int)");
        exec("INSERT INTO ac_uuid VALUES (1), (2), (3), (4), (5)");
        exec("ALTER TABLE ac_uuid ADD COLUMN u uuid DEFAULT gen_random_uuid()");
        assertEquals("0", query1("SELECT count(*) FROM ac_uuid WHERE u IS NULL"));
        assertEquals("5", query1("SELECT count(DISTINCT u) FROM ac_uuid"));
        exec("DROP TABLE ac_uuid");
    }

    @Test
    void add_column_volatile_random_default_is_per_row() throws SQLException {
        exec("CREATE TABLE ac_rand (id int)");
        exec("INSERT INTO ac_rand SELECT g FROM generate_series(1, 20) g");
        exec("ALTER TABLE ac_rand ADD COLUMN r double precision DEFAULT random()");
        assertEquals("0", query1("SELECT count(*) FROM ac_rand WHERE r IS NULL"));
        // 20 random() draws colliding is astronomically unlikely
        assertEquals("20", query1("SELECT count(DISTINCT r) FROM ac_rand"));
        exec("DROP TABLE ac_rand");
    }

    @Test
    void add_column_stable_now_default_is_single_value() throws SQLException {
        exec("CREATE TABLE ac_now (id int)");
        exec("INSERT INTO ac_now VALUES (1), (2), (3)");
        // now() is STABLE: one value per statement is correct
        exec("ALTER TABLE ac_now ADD COLUMN ts timestamptz DEFAULT now()");
        assertEquals("0", query1("SELECT count(*) FROM ac_now WHERE ts IS NULL"));
        assertEquals("1", query1("SELECT count(DISTINCT ts) FROM ac_now"));
        exec("DROP TABLE ac_now");
    }

    @Test
    void add_column_serial_backfills_and_is_not_null() throws SQLException {
        exec("CREATE TABLE ac_serial (v text)");
        exec("INSERT INTO ac_serial VALUES ('a'), ('b'), ('c')");
        exec("ALTER TABLE ac_serial ADD COLUMN id serial");
        assertEquals("0", query1("SELECT count(*) FROM ac_serial WHERE id IS NULL"));
        assertEquals("3", query1("SELECT count(DISTINCT id) FROM ac_serial"));
        assertEquals("1", query1("SELECT min(id) FROM ac_serial"));
        assertEquals("3", query1("SELECT max(id) FROM ac_serial"));
        assertEquals("NO", query1(
                "SELECT is_nullable FROM information_schema.columns "
                + "WHERE table_name = 'ac_serial' AND column_name = 'id'"));
        // next insert continues the sequence
        exec("INSERT INTO ac_serial (v) VALUES ('d')");
        assertEquals("4", query1("SELECT id FROM ac_serial WHERE v = 'd'"));
        exec("DROP TABLE ac_serial");
    }

    @Test
    void add_column_identity_backfills_and_is_not_null() throws SQLException {
        exec("CREATE TABLE ac_ident (v text)");
        exec("INSERT INTO ac_ident VALUES ('a'), ('b')");
        exec("ALTER TABLE ac_ident ADD COLUMN id int GENERATED ALWAYS AS IDENTITY");
        assertEquals("0", query1("SELECT count(*) FROM ac_ident WHERE id IS NULL"));
        assertEquals("2", query1("SELECT count(DISTINCT id) FROM ac_ident"));
        assertEquals("NO", query1(
                "SELECT is_nullable FROM information_schema.columns "
                + "WHERE table_name = 'ac_ident' AND column_name = 'id'"));
        exec("DROP TABLE ac_ident");
    }

    @Test
    void add_column_nextval_default_backfills_per_row() throws SQLException {
        exec("CREATE SEQUENCE ac_seq_backfill");
        exec("CREATE TABLE ac_nextval (v text)");
        exec("INSERT INTO ac_nextval VALUES ('a'), ('b'), ('c')");
        exec("ALTER TABLE ac_nextval ADD COLUMN n bigint DEFAULT nextval('ac_seq_backfill')");
        assertEquals("0", query1("SELECT count(*) FROM ac_nextval WHERE n IS NULL"));
        assertEquals("3", query1("SELECT count(DISTINCT n) FROM ac_nextval"));
        exec("DROP TABLE ac_nextval");
        exec("DROP SEQUENCE ac_seq_backfill");
    }

    // =========================================================================
    // 5. ALTER COLUMN TYPE without USING
    // =========================================================================

    @Test
    void alter_type_text_to_int_without_using_errors_42804() throws SQLException {
        exec("CREATE TABLE at_t2i (v text)");
        exec("INSERT INTO at_t2i VALUES ('123')");
        SQLException e = expectError("ALTER TABLE at_t2i ALTER COLUMN v TYPE integer");
        assertEquals("42804", e.getSQLState());
        assertTrue(e.getMessage().contains("cannot be cast automatically"),
                "unexpected message: " + e.getMessage());
        exec("DROP TABLE at_t2i");
    }

    @Test
    void alter_type_text_to_date_without_using_errors_42804() throws SQLException {
        exec("CREATE TABLE at_t2d (v text)");
        SQLException e = expectError("ALTER TABLE at_t2d ALTER COLUMN v TYPE date");
        assertEquals("42804", e.getSQLState());
        exec("DROP TABLE at_t2d");
    }

    @Test
    void alter_type_text_to_int_with_using_works() throws SQLException {
        exec("CREATE TABLE at_using (v text)");
        exec("INSERT INTO at_using VALUES ('123')");
        exec("ALTER TABLE at_using ALTER COLUMN v TYPE integer USING v::integer");
        assertEquals("124", query1("SELECT v + 1 FROM at_using"));
        exec("DROP TABLE at_using");
    }

    @Test
    void alter_type_varchar_to_text_without_using_works() throws SQLException {
        exec("CREATE TABLE at_v2t (v varchar(10))");
        exec("INSERT INTO at_v2t VALUES ('hello')");
        exec("ALTER TABLE at_v2t ALTER COLUMN v TYPE text");
        assertEquals("hello", query1("SELECT v FROM at_v2t"));
        exec("DROP TABLE at_v2t");
    }

    @Test
    void alter_type_int_to_text_without_using_works() throws SQLException {
        // PG allows assignment casts TO string-category types (I/O coercion)
        exec("CREATE TABLE at_i2t (v int)");
        exec("INSERT INTO at_i2t VALUES (42)");
        exec("ALTER TABLE at_i2t ALTER COLUMN v TYPE text");
        assertEquals("42", query1("SELECT v FROM at_i2t"));
        exec("DROP TABLE at_i2t");
    }

    @Test
    void alter_type_int_to_bigint_without_using_works() throws SQLException {
        exec("CREATE TABLE at_i2b (v int)");
        exec("INSERT INTO at_i2b VALUES (7)");
        exec("ALTER TABLE at_i2b ALTER COLUMN v TYPE bigint");
        assertEquals("7", query1("SELECT v FROM at_i2b"));
        exec("DROP TABLE at_i2b");
    }

    @Test
    void alter_type_int_to_boolean_without_using_errors_42804() throws SQLException {
        exec("CREATE TABLE at_i2bool (v int)");
        SQLException e = expectError("ALTER TABLE at_i2bool ALTER COLUMN v TYPE boolean");
        assertEquals("42804", e.getSQLState());
        exec("DROP TABLE at_i2bool");
    }

    // =========================================================================
    // 6. DROP COLUMN drops dependent constraints
    // =========================================================================

    @Test
    void drop_column_removes_its_constraints_from_pg_constraint() throws SQLException {
        exec("CREATE TABLE dc_cons (a int, b int, c int, "
                + "CONSTRAINT dc_cons_b_key UNIQUE (b), "
                + "CONSTRAINT dc_cons_c_check CHECK (c > 0))");
        exec("ALTER TABLE dc_cons DROP COLUMN b");
        assertEquals("0", query1("SELECT count(*) FROM pg_constraint WHERE conname = 'dc_cons_b_key'"));
        exec("ALTER TABLE dc_cons DROP COLUMN c");
        assertEquals("0", query1("SELECT count(*) FROM pg_constraint WHERE conname = 'dc_cons_c_check'"));
        exec("DROP TABLE dc_cons");
    }

    @Test
    void drop_column_removes_multicolumn_unique_containing_it() throws SQLException {
        exec("CREATE TABLE dc_multi (a int, b int, CONSTRAINT dc_multi_key UNIQUE (a, b))");
        exec("INSERT INTO dc_multi VALUES (1, 1), (1, 2)");
        exec("ALTER TABLE dc_multi DROP COLUMN b");
        assertEquals("0", query1("SELECT count(*) FROM pg_constraint WHERE conname = 'dc_multi_key'"));
        // uniqueness on the surviving column is NOT enforced (whole constraint dropped)
        exec("INSERT INTO dc_multi VALUES (1)");
        exec("DROP TABLE dc_multi");
    }

    @Test
    void drop_pk_column_removes_pk_and_stops_enforcement() throws SQLException {
        exec("CREATE TABLE dc_pk (id int PRIMARY KEY, v text)");
        exec("INSERT INTO dc_pk VALUES (1, 'a')");
        exec("ALTER TABLE dc_pk DROP COLUMN id");
        assertEquals("0", query1(
                "SELECT count(*) FROM pg_constraint WHERE conrelid = 'dc_pk'::regclass AND contype = 'p'"));
        exec("DROP TABLE dc_pk");
    }

    @Test
    void drop_column_referenced_by_fk_requires_cascade() throws SQLException {
        exec("CREATE TABLE dc_parent (id int PRIMARY KEY, v text)");
        exec("CREATE TABLE dc_child (pid int REFERENCES dc_parent(id))");
        SQLException e = expectError("ALTER TABLE dc_parent DROP COLUMN id");
        assertEquals("2BP01", e.getSQLState());
        // with CASCADE the dependent FK is dropped too
        exec("ALTER TABLE dc_parent DROP COLUMN id CASCADE");
        assertEquals("0", query1(
                "SELECT count(*) FROM pg_constraint WHERE conrelid = 'dc_child'::regclass AND contype = 'f'"));
        exec("INSERT INTO dc_child VALUES (12345)"); // FK no longer enforced
        exec("DROP TABLE dc_child");
        exec("DROP TABLE dc_parent");
    }
}
