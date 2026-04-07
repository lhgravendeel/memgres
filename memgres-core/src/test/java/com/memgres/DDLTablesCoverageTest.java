package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive coverage tests for checklist items 60-62 (DDL Tables).
 *
 * 60. CREATE TABLE
 * 61. ALTER TABLE
 * 62. DROP TABLE
 */
class DDLTablesCoverageTest {

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

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private boolean queryBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private boolean tableExists(String name) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '" + name + "'")) {
            rs.next();
            return rs.getInt(1) > 0;
        }
    }

    // =========================================================================
    // 60. CREATE TABLE
    // =========================================================================

    // --- Basic table creation with column types ---

    @Test
    void ct_basic_int_text() throws SQLException {
        exec("CREATE TABLE ct_basic_it (id INTEGER, name TEXT)");
        exec("INSERT INTO ct_basic_it VALUES (1, 'hello')");
        assertEquals(1, queryInt("SELECT id FROM ct_basic_it"));
        assertEquals("hello", query1("SELECT name FROM ct_basic_it"));
        exec("DROP TABLE IF EXISTS ct_basic_it");
    }

    @Test
    void ct_basic_boolean() throws SQLException {
        exec("CREATE TABLE ct_basic_bool (flag BOOLEAN)");
        exec("INSERT INTO ct_basic_bool VALUES (true)");
        assertTrue(queryBool("SELECT flag FROM ct_basic_bool"));
        exec("DROP TABLE IF EXISTS ct_basic_bool");
    }

    @Test
    void ct_basic_numeric() throws SQLException {
        exec("CREATE TABLE ct_basic_num (val NUMERIC(10,2))");
        exec("INSERT INTO ct_basic_num VALUES (123.45)");
        assertEquals("123.45", query1("SELECT val FROM ct_basic_num"));
        exec("DROP TABLE IF EXISTS ct_basic_num");
    }

    @Test
    void ct_basic_varchar() throws SQLException {
        exec("CREATE TABLE ct_basic_vc (name VARCHAR(50))");
        exec("INSERT INTO ct_basic_vc VALUES ('test')");
        assertEquals("test", query1("SELECT name FROM ct_basic_vc"));
        exec("DROP TABLE IF EXISTS ct_basic_vc");
    }

    @Test
    void ct_basic_timestamp() throws SQLException {
        exec("CREATE TABLE ct_basic_ts (created TIMESTAMP)");
        exec("INSERT INTO ct_basic_ts VALUES ('2024-01-15 10:30:00')");
        String result = query1("SELECT created FROM ct_basic_ts");
        assertTrue(result.contains("2024-01-15"));
        exec("DROP TABLE IF EXISTS ct_basic_ts");
    }

    @Test
    void ct_basic_date() throws SQLException {
        exec("CREATE TABLE ct_basic_dt (d DATE)");
        exec("INSERT INTO ct_basic_dt VALUES ('2024-06-15')");
        assertEquals("2024-06-15", query1("SELECT d FROM ct_basic_dt"));
        exec("DROP TABLE IF EXISTS ct_basic_dt");
    }

    @Test
    void ct_basic_time() throws SQLException {
        exec("CREATE TABLE ct_basic_tm (t TIME)");
        exec("INSERT INTO ct_basic_tm VALUES ('14:30:00')");
        String result = query1("SELECT t FROM ct_basic_tm");
        assertTrue(result.contains("14:30"));
        exec("DROP TABLE IF EXISTS ct_basic_tm");
    }

    @Test
    void ct_basic_bigint() throws SQLException {
        exec("CREATE TABLE ct_basic_bi (big BIGINT)");
        exec("INSERT INTO ct_basic_bi VALUES (9999999999)");
        assertEquals("9999999999", query1("SELECT big FROM ct_basic_bi"));
        exec("DROP TABLE IF EXISTS ct_basic_bi");
    }

    @Test
    void ct_basic_smallint() throws SQLException {
        exec("CREATE TABLE ct_basic_si (small SMALLINT)");
        exec("INSERT INTO ct_basic_si VALUES (42)");
        assertEquals(42, queryInt("SELECT small FROM ct_basic_si"));
        exec("DROP TABLE IF EXISTS ct_basic_si");
    }

    @Test
    void ct_basic_real() throws SQLException {
        exec("CREATE TABLE ct_basic_rl (r REAL)");
        exec("INSERT INTO ct_basic_rl VALUES (3.14)");
        String val = query1("SELECT r FROM ct_basic_rl");
        assertTrue(val.startsWith("3.14"));
        exec("DROP TABLE IF EXISTS ct_basic_rl");
    }

    @Test
    void ct_basic_double_precision() throws SQLException {
        exec("CREATE TABLE ct_basic_dp (d DOUBLE PRECISION)");
        exec("INSERT INTO ct_basic_dp VALUES (2.718281828)");
        String val = query1("SELECT d FROM ct_basic_dp");
        assertTrue(val.startsWith("2.71828"));
        exec("DROP TABLE IF EXISTS ct_basic_dp");
    }

    @Test
    void ct_basic_char() throws SQLException {
        exec("CREATE TABLE ct_basic_ch (code CHAR(5))");
        exec("INSERT INTO ct_basic_ch VALUES ('AB')");
        String val = query1("SELECT code FROM ct_basic_ch");
        // CHAR(5) pads with spaces
        assertTrue(val.length() <= 5);
        exec("DROP TABLE IF EXISTS ct_basic_ch");
    }

    @Test
    void ct_basic_timestamptz() throws SQLException {
        exec("CREATE TABLE ct_basic_tstz (created TIMESTAMPTZ)");
        exec("INSERT INTO ct_basic_tstz VALUES ('2024-01-15 10:30:00+00')");
        assertNotNull(query1("SELECT created FROM ct_basic_tstz"));
        exec("DROP TABLE IF EXISTS ct_basic_tstz");
    }

    @Test
    void ct_basic_uuid() throws SQLException {
        exec("CREATE TABLE ct_basic_uuid (id UUID)");
        exec("INSERT INTO ct_basic_uuid VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        assertEquals("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", query1("SELECT id FROM ct_basic_uuid"));
        exec("DROP TABLE IF EXISTS ct_basic_uuid");
    }

    @Test
    void ct_basic_bytea() throws SQLException {
        exec("CREATE TABLE ct_basic_bytea (data BYTEA)");
        exec("INSERT INTO ct_basic_bytea VALUES ('\\x48656c6c6f')");
        assertNotNull(query1("SELECT data FROM ct_basic_bytea"));
        exec("DROP TABLE IF EXISTS ct_basic_bytea");
    }

    @Test
    void ct_basic_json() throws SQLException {
        exec("CREATE TABLE ct_basic_json (data JSON)");
        exec("INSERT INTO ct_basic_json VALUES ('{\"key\": \"val\"}')");
        assertNotNull(query1("SELECT data FROM ct_basic_json"));
        exec("DROP TABLE IF EXISTS ct_basic_json");
    }

    @Test
    void ct_basic_jsonb() throws SQLException {
        exec("CREATE TABLE ct_basic_jsonb (data JSONB)");
        exec("INSERT INTO ct_basic_jsonb VALUES ('{\"a\": 1}')");
        assertNotNull(query1("SELECT data FROM ct_basic_jsonb"));
        exec("DROP TABLE IF EXISTS ct_basic_jsonb");
    }

    @Test
    void ct_basic_interval() throws SQLException {
        exec("CREATE TABLE ct_basic_interval (dur INTERVAL)");
        exec("INSERT INTO ct_basic_interval VALUES ('1 day')");
        assertNotNull(query1("SELECT dur FROM ct_basic_interval"));
        exec("DROP TABLE IF EXISTS ct_basic_interval");
    }

    @Test
    void ct_multiple_types_in_one_table() throws SQLException {
        exec("CREATE TABLE ct_multi_types (id INTEGER, name TEXT, active BOOLEAN, " +
                "score NUMERIC(5,2), created TIMESTAMP, code VARCHAR(10))");
        exec("INSERT INTO ct_multi_types VALUES (1, 'alice', true, 95.50, '2024-01-01 00:00:00', 'ABC')");
        assertEquals(1, queryInt("SELECT id FROM ct_multi_types"));
        assertEquals("alice", query1("SELECT name FROM ct_multi_types"));
        assertTrue(queryBool("SELECT active FROM ct_multi_types"));
        exec("DROP TABLE IF EXISTS ct_multi_types");
    }

    // --- Column constraints ---

    @Test
    void ct_not_null_constraint() throws SQLException {
        exec("CREATE TABLE ct_nn (id INTEGER NOT NULL)");
        exec("INSERT INTO ct_nn VALUES (1)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_nn VALUES (NULL)"));
        exec("DROP TABLE IF EXISTS ct_nn");
    }

    @Test
    void ct_null_allowed_explicit() throws SQLException {
        exec("CREATE TABLE ct_nullable (id INTEGER NULL)");
        exec("INSERT INTO ct_nullable VALUES (NULL)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM ct_nullable"));
        exec("DROP TABLE IF EXISTS ct_nullable");
    }

    @Test
    void ct_default_literal() throws SQLException {
        exec("CREATE TABLE ct_def_lit (id INTEGER, name TEXT DEFAULT 'unknown')");
        exec("INSERT INTO ct_def_lit (id) VALUES (1)");
        assertEquals("unknown", query1("SELECT name FROM ct_def_lit"));
        exec("DROP TABLE IF EXISTS ct_def_lit");
    }

    @Test
    void ct_default_numeric() throws SQLException {
        exec("CREATE TABLE ct_def_num (id INTEGER, val INTEGER DEFAULT 42)");
        exec("INSERT INTO ct_def_num (id) VALUES (1)");
        assertEquals(42, queryInt("SELECT val FROM ct_def_num"));
        exec("DROP TABLE IF EXISTS ct_def_num");
    }

    @Test
    void ct_default_boolean() throws SQLException {
        exec("CREATE TABLE ct_def_bool (id INTEGER, active BOOLEAN DEFAULT true)");
        exec("INSERT INTO ct_def_bool (id) VALUES (1)");
        assertTrue(queryBool("SELECT active FROM ct_def_bool"));
        exec("DROP TABLE IF EXISTS ct_def_bool");
    }

    @Test
    void ct_default_now_function() throws SQLException {
        exec("CREATE TABLE ct_def_now (id INTEGER, created TIMESTAMP DEFAULT NOW())");
        exec("INSERT INTO ct_def_now (id) VALUES (1)");
        assertNotNull(query1("SELECT created FROM ct_def_now"));
        exec("DROP TABLE IF EXISTS ct_def_now");
    }

    @Test
    void ct_column_primary_key() throws SQLException {
        exec("CREATE TABLE ct_pk (id INTEGER PRIMARY KEY, name TEXT)");
        exec("INSERT INTO ct_pk VALUES (1, 'a')");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_pk VALUES (1, 'b')"));
        exec("DROP TABLE IF EXISTS ct_pk");
    }

    @Test
    void ct_column_unique() throws SQLException {
        exec("CREATE TABLE ct_uniq (email TEXT UNIQUE)");
        exec("INSERT INTO ct_uniq VALUES ('a@b.com')");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_uniq VALUES ('a@b.com')"));
        exec("DROP TABLE IF EXISTS ct_uniq");
    }

    @Test
    void ct_check_positive() throws SQLException {
        exec("CREATE TABLE ct_chk_pos (val INTEGER, CONSTRAINT chk_pos CHECK (val > 0))");
        exec("INSERT INTO ct_chk_pos VALUES (1)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_chk_pos VALUES (-1)"));
        exec("DROP TABLE IF EXISTS ct_chk_pos");
    }

    @Test
    void ct_check_in_list() throws SQLException {
        exec("CREATE TABLE ct_chk_in (status TEXT, CONSTRAINT chk_in CHECK (status IN ('active', 'inactive')))");
        exec("INSERT INTO ct_chk_in VALUES ('active')");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_chk_in VALUES ('deleted')"));
        exec("DROP TABLE IF EXISTS ct_chk_in");
    }

    @Test
    void ct_inline_references() throws SQLException {
        exec("CREATE TABLE ct_ref_parent (id INTEGER PRIMARY KEY)");
        exec("CREATE TABLE ct_ref_child (pid INTEGER REFERENCES ct_ref_parent(id))");
        exec("INSERT INTO ct_ref_parent VALUES (1)");
        exec("INSERT INTO ct_ref_child VALUES (1)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_ref_child VALUES (99)"));
        exec("DROP TABLE IF EXISTS ct_ref_child");
        exec("DROP TABLE IF EXISTS ct_ref_parent");
    }

    @Test
    void ct_not_null_with_default() throws SQLException {
        exec("CREATE TABLE ct_nn_def (id INTEGER, status TEXT NOT NULL DEFAULT 'pending')");
        exec("INSERT INTO ct_nn_def (id) VALUES (1)");
        assertEquals("pending", query1("SELECT status FROM ct_nn_def"));
        exec("DROP TABLE IF EXISTS ct_nn_def");
    }

    @Test
    void ct_pk_implies_not_null() throws SQLException {
        exec("CREATE TABLE ct_pk_nn (id INTEGER PRIMARY KEY)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_pk_nn VALUES (NULL)"));
        exec("DROP TABLE IF EXISTS ct_pk_nn");
    }

    // --- Table-level constraints ---

    @Test
    void ct_table_composite_pk() throws SQLException {
        exec("CREATE TABLE ct_cpk (a INTEGER, b INTEGER, PRIMARY KEY (a, b))");
        exec("INSERT INTO ct_cpk VALUES (1, 1)");
        exec("INSERT INTO ct_cpk VALUES (1, 2)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_cpk VALUES (1, 1)"));
        exec("DROP TABLE IF EXISTS ct_cpk");
    }

    @Test
    void ct_table_composite_unique() throws SQLException {
        exec("CREATE TABLE ct_cuniq (a INTEGER, b INTEGER, UNIQUE (a, b))");
        exec("INSERT INTO ct_cuniq VALUES (1, 1)");
        exec("INSERT INTO ct_cuniq VALUES (1, 2)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_cuniq VALUES (1, 1)"));
        exec("DROP TABLE IF EXISTS ct_cuniq");
    }

    @Test
    void ct_table_check() throws SQLException {
        exec("CREATE TABLE ct_tchk (a INTEGER, b INTEGER, CHECK (a < b))");
        exec("INSERT INTO ct_tchk VALUES (1, 2)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_tchk VALUES (5, 3)"));
        exec("DROP TABLE IF EXISTS ct_tchk");
    }

    @Test
    void ct_fk_on_delete_cascade() throws SQLException {
        exec("CREATE TABLE ct_fk_cas_p (id INTEGER PRIMARY KEY)");
        exec("CREATE TABLE ct_fk_cas_c (pid INTEGER, FOREIGN KEY (pid) REFERENCES ct_fk_cas_p(id) ON DELETE CASCADE)");
        exec("INSERT INTO ct_fk_cas_p VALUES (1)");
        exec("INSERT INTO ct_fk_cas_c VALUES (1)");
        exec("DELETE FROM ct_fk_cas_p WHERE id = 1");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM ct_fk_cas_c"));
        exec("DROP TABLE IF EXISTS ct_fk_cas_c");
        exec("DROP TABLE IF EXISTS ct_fk_cas_p");
    }

    @Test
    void ct_fk_on_delete_set_null() throws SQLException {
        exec("CREATE TABLE ct_fk_sn_p (id INTEGER PRIMARY KEY)");
        exec("CREATE TABLE ct_fk_sn_c (pid INTEGER, FOREIGN KEY (pid) REFERENCES ct_fk_sn_p(id) ON DELETE SET NULL)");
        exec("INSERT INTO ct_fk_sn_p VALUES (1)");
        exec("INSERT INTO ct_fk_sn_c VALUES (1)");
        exec("DELETE FROM ct_fk_sn_p WHERE id = 1");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM ct_fk_sn_c"));
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pid FROM ct_fk_sn_c")) {
            assertTrue(rs.next());
            rs.getInt(1);
            assertTrue(rs.wasNull());
        }
        exec("DROP TABLE IF EXISTS ct_fk_sn_c");
        exec("DROP TABLE IF EXISTS ct_fk_sn_p");
    }

    @Test
    void ct_fk_on_delete_restrict() throws SQLException {
        exec("CREATE TABLE ct_fk_res_p (id INTEGER PRIMARY KEY)");
        exec("CREATE TABLE ct_fk_res_c (pid INTEGER, FOREIGN KEY (pid) REFERENCES ct_fk_res_p(id) ON DELETE RESTRICT)");
        exec("INSERT INTO ct_fk_res_p VALUES (1)");
        exec("INSERT INTO ct_fk_res_c VALUES (1)");
        assertThrows(SQLException.class, () -> exec("DELETE FROM ct_fk_res_p WHERE id = 1"));
        exec("DROP TABLE IF EXISTS ct_fk_res_c");
        exec("DROP TABLE IF EXISTS ct_fk_res_p");
    }

    @Test
    void ct_fk_on_delete_no_action() throws SQLException {
        exec("CREATE TABLE ct_fk_na_p (id INTEGER PRIMARY KEY)");
        exec("CREATE TABLE ct_fk_na_c (pid INTEGER, FOREIGN KEY (pid) REFERENCES ct_fk_na_p(id) ON DELETE NO ACTION)");
        exec("INSERT INTO ct_fk_na_p VALUES (1)");
        exec("INSERT INTO ct_fk_na_c VALUES (1)");
        assertThrows(SQLException.class, () -> exec("DELETE FROM ct_fk_na_p WHERE id = 1"));
        exec("DROP TABLE IF EXISTS ct_fk_na_c");
        exec("DROP TABLE IF EXISTS ct_fk_na_p");
    }

    @Test
    void ct_named_pk() throws SQLException {
        exec("CREATE TABLE ct_npk (id INTEGER, CONSTRAINT pk_ct_npk PRIMARY KEY (id))");
        exec("INSERT INTO ct_npk VALUES (1)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_npk VALUES (1)"));
        exec("DROP TABLE IF EXISTS ct_npk");
    }

    @Test
    void ct_named_unique() throws SQLException {
        exec("CREATE TABLE ct_nuq (val TEXT, CONSTRAINT uq_ct_nuq UNIQUE (val))");
        exec("INSERT INTO ct_nuq VALUES ('x')");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_nuq VALUES ('x')"));
        exec("DROP TABLE IF EXISTS ct_nuq");
    }

    @Test
    void ct_named_check() throws SQLException {
        exec("CREATE TABLE ct_nchk (val INTEGER, CONSTRAINT chk_ct_nchk CHECK (val > 0))");
        exec("INSERT INTO ct_nchk VALUES (5)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_nchk VALUES (-1)"));
        exec("DROP TABLE IF EXISTS ct_nchk");
    }

    @Test
    void ct_named_fk() throws SQLException {
        exec("CREATE TABLE ct_nfk_p (id INTEGER PRIMARY KEY)");
        exec("CREATE TABLE ct_nfk_c (pid INTEGER, CONSTRAINT fk_ct_nfk FOREIGN KEY (pid) REFERENCES ct_nfk_p(id))");
        exec("INSERT INTO ct_nfk_p VALUES (1)");
        exec("INSERT INTO ct_nfk_c VALUES (1)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_nfk_c VALUES (99)"));
        exec("DROP TABLE IF EXISTS ct_nfk_c");
        exec("DROP TABLE IF EXISTS ct_nfk_p");
    }

    @Test
    void ct_composite_pk_both_needed() throws SQLException {
        exec("CREATE TABLE ct_cpk2 (a INTEGER, b INTEGER, PRIMARY KEY (a, b))");
        exec("INSERT INTO ct_cpk2 VALUES (1, 1)");
        exec("INSERT INTO ct_cpk2 VALUES (1, 2)");
        exec("INSERT INTO ct_cpk2 VALUES (2, 1)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_cpk2 VALUES (1, 1)"));
        assertEquals(3, queryInt("SELECT COUNT(*) FROM ct_cpk2"));
        exec("DROP TABLE IF EXISTS ct_cpk2");
    }

    // --- IF NOT EXISTS ---

    @Test
    void ct_if_not_exists_first_creates() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS ct_ine1 (id INTEGER)");
        exec("INSERT INTO ct_ine1 VALUES (1)");
        assertEquals(1, queryInt("SELECT id FROM ct_ine1"));
        exec("DROP TABLE IF EXISTS ct_ine1");
    }

    @Test
    void ct_if_not_exists_second_is_noop() throws SQLException {
        exec("CREATE TABLE ct_ine2 (id INTEGER)");
        exec("INSERT INTO ct_ine2 VALUES (1)");
        // Second create should not error or wipe data
        exec("CREATE TABLE IF NOT EXISTS ct_ine2 (id INTEGER, name TEXT)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM ct_ine2"));
        exec("DROP TABLE IF EXISTS ct_ine2");
    }

    // --- TEMPORARY / TEMP ---

    @Test
    void ct_temporary_table() throws SQLException {
        exec("CREATE TEMPORARY TABLE ct_tmp1 (id INTEGER)");
        exec("INSERT INTO ct_tmp1 VALUES (42)");
        assertEquals(42, queryInt("SELECT id FROM ct_tmp1"));
        exec("DROP TABLE IF EXISTS ct_tmp1");
    }

    @Test
    void ct_temp_keyword() throws SQLException {
        exec("CREATE TEMP TABLE ct_tmp2 (name TEXT)");
        exec("INSERT INTO ct_tmp2 VALUES ('temp')");
        assertEquals("temp", query1("SELECT name FROM ct_tmp2"));
        exec("DROP TABLE IF EXISTS ct_tmp2");
    }

    @Test
    void ct_temp_insert_select() throws SQLException {
        exec("CREATE TEMP TABLE ct_tmp3 (id INTEGER, label TEXT)");
        exec("INSERT INTO ct_tmp3 VALUES (1, 'a'), (2, 'b')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM ct_tmp3"));
        assertEquals("b", query1("SELECT label FROM ct_tmp3 WHERE id = 2"));
        exec("DROP TABLE IF EXISTS ct_tmp3");
    }

    // --- UNLOGGED ---

    @Test
    void ct_unlogged_table_accepted() throws SQLException {
        exec("CREATE UNLOGGED TABLE ct_unlog (id INTEGER, name TEXT)");
        exec("INSERT INTO ct_unlog VALUES (1, 'test')");
        assertEquals(1, queryInt("SELECT id FROM ct_unlog"));
        exec("DROP TABLE IF EXISTS ct_unlog");
    }

    @Test
    void ct_unlogged_with_constraints() throws SQLException {
        exec("CREATE UNLOGGED TABLE ct_unlog2 (id INTEGER PRIMARY KEY, val TEXT NOT NULL)");
        exec("INSERT INTO ct_unlog2 VALUES (1, 'x')");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_unlog2 VALUES (1, 'y')"));
        exec("DROP TABLE IF EXISTS ct_unlog2");
    }

    // --- SERIAL types ---

    @Test
    void ct_serial() throws SQLException {
        exec("CREATE TABLE ct_ser (id SERIAL, name TEXT)");
        exec("INSERT INTO ct_ser (name) VALUES ('a')");
        exec("INSERT INTO ct_ser (name) VALUES ('b')");
        assertEquals(1, queryInt("SELECT id FROM ct_ser WHERE name = 'a'"));
        assertEquals(2, queryInt("SELECT id FROM ct_ser WHERE name = 'b'"));
        exec("DROP TABLE IF EXISTS ct_ser");
    }

    @Test
    void ct_bigserial() throws SQLException {
        exec("CREATE TABLE ct_bser (id BIGSERIAL, name TEXT)");
        exec("INSERT INTO ct_bser (name) VALUES ('x')");
        assertEquals(1, queryInt("SELECT id FROM ct_bser"));
        exec("DROP TABLE IF EXISTS ct_bser");
    }

    @Test
    void ct_smallserial() throws SQLException {
        exec("CREATE TABLE ct_sser (id SMALLSERIAL, name TEXT)");
        exec("INSERT INTO ct_sser (name) VALUES ('y')");
        assertEquals(1, queryInt("SELECT id FROM ct_sser"));
        exec("DROP TABLE IF EXISTS ct_sser");
    }

    @Test
    void ct_serial_auto_increment_starts_at_1() throws SQLException {
        exec("CREATE TABLE ct_ser_start (id SERIAL, txt TEXT)");
        exec("INSERT INTO ct_ser_start (txt) VALUES ('first')");
        assertEquals(1, queryInt("SELECT id FROM ct_ser_start"));
        exec("DROP TABLE IF EXISTS ct_ser_start");
    }

    @Test
    void ct_serial_auto_increment_sequential() throws SQLException {
        exec("CREATE TABLE ct_ser_seq (id SERIAL, txt TEXT)");
        exec("INSERT INTO ct_ser_seq (txt) VALUES ('a')");
        exec("INSERT INTO ct_ser_seq (txt) VALUES ('b')");
        exec("INSERT INTO ct_ser_seq (txt) VALUES ('c')");
        assertEquals(1, queryInt("SELECT id FROM ct_ser_seq WHERE txt = 'a'"));
        assertEquals(2, queryInt("SELECT id FROM ct_ser_seq WHERE txt = 'b'"));
        assertEquals(3, queryInt("SELECT id FROM ct_ser_seq WHERE txt = 'c'"));
        exec("DROP TABLE IF EXISTS ct_ser_seq");
    }

    @Test
    void ct_pk_and_serial_combined() throws SQLException {
        exec("CREATE TABLE ct_ser_pk (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO ct_ser_pk (name) VALUES ('a')");
        exec("INSERT INTO ct_ser_pk (name) VALUES ('b')");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_ser_pk (id, name) VALUES (1, 'c')"));
        exec("DROP TABLE IF EXISTS ct_ser_pk");
    }

    // --- GENERATED STORED columns ---

    @Test
    void ct_generated_stored() throws SQLException {
        exec("CREATE TABLE ct_gen_stored (a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a + b) STORED)");
        exec("INSERT INTO ct_gen_stored (a, b) VALUES (3, 4)");
        assertEquals(7, queryInt("SELECT c FROM ct_gen_stored"));
        exec("DROP TABLE IF EXISTS ct_gen_stored");
    }

    @Test
    void ct_generated_stored_expression() throws SQLException {
        exec("CREATE TABLE ct_gen_expr (price NUMERIC, qty INTEGER, total NUMERIC GENERATED ALWAYS AS (price * qty) STORED)");
        exec("INSERT INTO ct_gen_expr (price, qty) VALUES (9.99, 3)");
        assertEquals("29.97", query1("SELECT total FROM ct_gen_expr"));
        exec("DROP TABLE IF EXISTS ct_gen_expr");
    }

    @Test
    void ct_generated_stored_concat() throws SQLException {
        exec("CREATE TABLE ct_gen_conc (first_name TEXT, last_name TEXT, full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)");
        exec("INSERT INTO ct_gen_conc (first_name, last_name) VALUES ('John', 'Doe')");
        assertEquals("John Doe", query1("SELECT full_name FROM ct_gen_conc"));
        exec("DROP TABLE IF EXISTS ct_gen_conc");
    }

    @Test
    void ct_generated_stored_multiply() throws SQLException {
        exec("CREATE TABLE ct_gen_mul (w INTEGER, h INTEGER, area INTEGER GENERATED ALWAYS AS (w * h) STORED)");
        exec("INSERT INTO ct_gen_mul (w, h) VALUES (5, 10)");
        assertEquals(50, queryInt("SELECT area FROM ct_gen_mul"));
        exec("DROP TABLE IF EXISTS ct_gen_mul");
    }

    // --- GENERATED AS IDENTITY ---

    @Test
    void ct_generated_always_as_identity() throws SQLException {
        exec("CREATE TABLE ct_gen_always (id INTEGER GENERATED ALWAYS AS IDENTITY, name TEXT)");
        exec("INSERT INTO ct_gen_always (name) VALUES ('alice')");
        exec("INSERT INTO ct_gen_always (name) VALUES ('bob')");
        assertEquals(1, queryInt("SELECT id FROM ct_gen_always WHERE name = 'alice'"));
        assertEquals(2, queryInt("SELECT id FROM ct_gen_always WHERE name = 'bob'"));
        exec("DROP TABLE IF EXISTS ct_gen_always");
    }

    @Test
    void ct_generated_by_default_as_identity() throws SQLException {
        exec("CREATE TABLE ct_gen_bydef (id INTEGER GENERATED BY DEFAULT AS IDENTITY, name TEXT)");
        exec("INSERT INTO ct_gen_bydef (name) VALUES ('x')");
        exec("INSERT INTO ct_gen_bydef (name) VALUES ('y')");
        assertEquals(1, queryInt("SELECT id FROM ct_gen_bydef WHERE name = 'x'"));
        assertEquals(2, queryInt("SELECT id FROM ct_gen_bydef WHERE name = 'y'"));
        exec("DROP TABLE IF EXISTS ct_gen_bydef");
    }

    @Test
    void ct_identity_bigint() throws SQLException {
        exec("CREATE TABLE ct_id_bi (id BIGINT GENERATED ALWAYS AS IDENTITY, val TEXT)");
        exec("INSERT INTO ct_id_bi (val) VALUES ('a')");
        exec("INSERT INTO ct_id_bi (val) VALUES ('b')");
        assertEquals(1, queryInt("SELECT id FROM ct_id_bi WHERE val = 'a'"));
        exec("DROP TABLE IF EXISTS ct_id_bi");
    }

    // --- INHERITS ---

    @Test
    void ct_inherits_child_has_parent_columns() throws SQLException {
        exec("CREATE TABLE ct_inh_parent (id INTEGER, name TEXT)");
        exec("CREATE TABLE ct_inh_child (extra TEXT) INHERITS (ct_inh_parent)");
        exec("INSERT INTO ct_inh_child VALUES (1, 'hello', 'bonus')");
        assertEquals(1, queryInt("SELECT id FROM ct_inh_child"));
        assertEquals("hello", query1("SELECT name FROM ct_inh_child"));
        assertEquals("bonus", query1("SELECT extra FROM ct_inh_child"));
        exec("DROP TABLE IF EXISTS ct_inh_child");
        exec("DROP TABLE IF EXISTS ct_inh_parent");
    }

    @Test
    void ct_inherits_insert_child_visible_in_parent() throws SQLException {
        exec("CREATE TABLE ct_inh2_p (id INTEGER, name TEXT)");
        exec("CREATE TABLE ct_inh2_c (extra TEXT) INHERITS (ct_inh2_p)");
        exec("INSERT INTO ct_inh2_c VALUES (1, 'child_row', 'extra')");
        // In PG, parent query includes child rows
        assertEquals(1, queryInt("SELECT COUNT(*) FROM ct_inh2_p"));
        exec("DROP TABLE IF EXISTS ct_inh2_c");
        exec("DROP TABLE IF EXISTS ct_inh2_p");
    }

    @Test
    void ct_inherits_own_column_plus_parent() throws SQLException {
        exec("CREATE TABLE ct_inh3_p (id INTEGER, name TEXT)");
        exec("CREATE TABLE ct_inh3_c (age INTEGER) INHERITS (ct_inh3_p)");
        exec("INSERT INTO ct_inh3_c VALUES (1, 'Bob', 30)");
        assertEquals(30, queryInt("SELECT age FROM ct_inh3_c WHERE name = 'Bob'"));
        exec("DROP TABLE IF EXISTS ct_inh3_c");
        exec("DROP TABLE IF EXISTS ct_inh3_p");
    }

    // --- PARTITION BY ---

    @Test
    void ct_partition_by_range() throws SQLException {
        exec("CREATE TABLE ct_part_r (id INTEGER, created DATE) PARTITION BY RANGE (created)");
        exec("CREATE TABLE ct_part_r_2024 PARTITION OF ct_part_r FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')");
        exec("INSERT INTO ct_part_r_2024 VALUES (1, '2024-06-15')");
        assertEquals(1, queryInt("SELECT id FROM ct_part_r_2024"));
        exec("DROP TABLE IF EXISTS ct_part_r_2024");
        exec("DROP TABLE IF EXISTS ct_part_r");
    }

    @Test
    void ct_partition_by_range_boundaries() throws SQLException {
        exec("CREATE TABLE ct_part_rb (id INTEGER, val INTEGER) PARTITION BY RANGE (val)");
        exec("CREATE TABLE ct_part_rb_lo PARTITION OF ct_part_rb FOR VALUES FROM (0) TO (100)");
        exec("CREATE TABLE ct_part_rb_hi PARTITION OF ct_part_rb FOR VALUES FROM (100) TO (200)");
        exec("INSERT INTO ct_part_rb_lo VALUES (1, 50)");
        exec("INSERT INTO ct_part_rb_hi VALUES (2, 150)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM ct_part_rb_lo"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM ct_part_rb_hi"));
        exec("DROP TABLE IF EXISTS ct_part_rb_lo");
        exec("DROP TABLE IF EXISTS ct_part_rb_hi");
        exec("DROP TABLE IF EXISTS ct_part_rb");
    }

    @Test
    void ct_partition_by_list() throws SQLException {
        exec("CREATE TABLE ct_part_l (id INTEGER, region TEXT) PARTITION BY LIST (region)");
        exec("CREATE TABLE ct_part_l_us PARTITION OF ct_part_l FOR VALUES IN ('us')");
        exec("CREATE TABLE ct_part_l_eu PARTITION OF ct_part_l FOR VALUES IN ('eu')");
        exec("INSERT INTO ct_part_l_us VALUES (1, 'us')");
        exec("INSERT INTO ct_part_l_eu VALUES (2, 'eu')");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM ct_part_l_us"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM ct_part_l_eu"));
        exec("DROP TABLE IF EXISTS ct_part_l_us");
        exec("DROP TABLE IF EXISTS ct_part_l_eu");
        exec("DROP TABLE IF EXISTS ct_part_l");
    }

    @Test
    void ct_partition_list_multiple_values() throws SQLException {
        exec("CREATE TABLE ct_part_lm (id INTEGER, status TEXT) PARTITION BY LIST (status)");
        exec("CREATE TABLE ct_part_lm_active PARTITION OF ct_part_lm FOR VALUES IN ('active', 'pending')");
        exec("INSERT INTO ct_part_lm_active VALUES (1, 'active')");
        exec("INSERT INTO ct_part_lm_active VALUES (2, 'pending')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM ct_part_lm_active"));
        exec("DROP TABLE IF EXISTS ct_part_lm_active");
        exec("DROP TABLE IF EXISTS ct_part_lm");
    }

    @Test
    void ct_partition_by_hash() throws SQLException {
        exec("CREATE TABLE ct_part_h (id INTEGER, val TEXT) PARTITION BY HASH (id)");
        exec("CREATE TABLE ct_part_h_0 PARTITION OF ct_part_h FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
        exec("CREATE TABLE ct_part_h_1 PARTITION OF ct_part_h FOR VALUES WITH (MODULUS 2, REMAINDER 1)");
        exec("INSERT INTO ct_part_h_0 VALUES (1, 'a')");
        exec("INSERT INTO ct_part_h_1 VALUES (2, 'b')");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM ct_part_h_0"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM ct_part_h_1"));
        exec("DROP TABLE IF EXISTS ct_part_h_0");
        exec("DROP TABLE IF EXISTS ct_part_h_1");
        exec("DROP TABLE IF EXISTS ct_part_h");
    }

    // --- LIKE source_table ---

    @Test
    void ct_like_copies_columns() throws SQLException {
        exec("CREATE TABLE ct_like_src (id INTEGER, name TEXT, active BOOLEAN)");
        exec("CREATE TABLE ct_like_dst (LIKE ct_like_src)");
        exec("INSERT INTO ct_like_dst VALUES (1, 'copied', true)");
        assertEquals(1, queryInt("SELECT id FROM ct_like_dst"));
        assertEquals("copied", query1("SELECT name FROM ct_like_dst"));
        assertTrue(queryBool("SELECT active FROM ct_like_dst"));
        exec("DROP TABLE IF EXISTS ct_like_dst");
        exec("DROP TABLE IF EXISTS ct_like_src");
    }

    @Test
    void ct_like_including_all() throws SQLException {
        exec("CREATE TABLE ct_like_inc_src (id INTEGER, val TEXT)");
        exec("CREATE TABLE ct_like_inc_dst (LIKE ct_like_inc_src INCLUDING ALL)");
        exec("INSERT INTO ct_like_inc_dst VALUES (1, 'test')");
        assertEquals(1, queryInt("SELECT id FROM ct_like_inc_dst"));
        exec("DROP TABLE IF EXISTS ct_like_inc_dst");
        exec("DROP TABLE IF EXISTS ct_like_inc_src");
    }

    @Test
    void ct_like_excluding_all() throws SQLException {
        exec("CREATE TABLE ct_like_exc_src (id INTEGER NOT NULL, val TEXT)");
        exec("CREATE TABLE ct_like_exc_dst (LIKE ct_like_exc_src EXCLUDING ALL)");
        exec("INSERT INTO ct_like_exc_dst VALUES (1, 'test')");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM ct_like_exc_dst"));
        exec("DROP TABLE IF EXISTS ct_like_exc_dst");
        exec("DROP TABLE IF EXISTS ct_like_exc_src");
    }

    @Test
    void ct_like_with_extra_columns() throws SQLException {
        exec("CREATE TABLE ct_like_extra_src (id INTEGER, name TEXT)");
        exec("CREATE TABLE ct_like_extra_dst (LIKE ct_like_extra_src, bonus INTEGER)");
        exec("INSERT INTO ct_like_extra_dst VALUES (1, 'a', 99)");
        assertEquals(99, queryInt("SELECT bonus FROM ct_like_extra_dst"));
        exec("DROP TABLE IF EXISTS ct_like_extra_dst");
        exec("DROP TABLE IF EXISTS ct_like_extra_src");
    }

    // --- CREATE TABLE AS SELECT ---

    @Test
    void ct_as_select_basic() throws SQLException {
        exec("CREATE TABLE ct_ctas_src (id INTEGER, name TEXT)");
        exec("INSERT INTO ct_ctas_src VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        exec("CREATE TABLE ct_ctas_dst AS SELECT * FROM ct_ctas_src");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM ct_ctas_dst"));
        assertEquals("b", query1("SELECT name FROM ct_ctas_dst WHERE id = 2"));
        exec("DROP TABLE IF EXISTS ct_ctas_dst");
        exec("DROP TABLE IF EXISTS ct_ctas_src");
    }

    @Test
    void ct_as_select_filtered() throws SQLException {
        exec("CREATE TABLE ct_ctas_filt_src (id INTEGER, val INTEGER)");
        exec("INSERT INTO ct_ctas_filt_src VALUES (1, 10), (2, 20), (3, 30)");
        exec("CREATE TABLE ct_ctas_filt_dst AS SELECT * FROM ct_ctas_filt_src WHERE val > 15");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM ct_ctas_filt_dst"));
        exec("DROP TABLE IF EXISTS ct_ctas_filt_dst");
        exec("DROP TABLE IF EXISTS ct_ctas_filt_src");
    }

    @Test
    void ct_as_select_with_expression() throws SQLException {
        exec("CREATE TABLE ct_ctas_expr_src (a INTEGER, b INTEGER)");
        exec("INSERT INTO ct_ctas_expr_src VALUES (3, 4)");
        exec("CREATE TABLE ct_ctas_expr_dst AS SELECT a, b, a + b AS total FROM ct_ctas_expr_src");
        assertEquals(7, queryInt("SELECT total FROM ct_ctas_expr_dst"));
        exec("DROP TABLE IF EXISTS ct_ctas_expr_dst");
        exec("DROP TABLE IF EXISTS ct_ctas_expr_src");
    }

    // --- Multiple constraints on one table ---

    @Test
    void ct_multiple_constraints_on_table() throws SQLException {
        exec("CREATE TABLE ct_multi_con (" +
                "id SERIAL PRIMARY KEY, " +
                "email TEXT NOT NULL UNIQUE, " +
                "age INTEGER, " +
                "status TEXT DEFAULT 'active', " +
                "CONSTRAINT chk_age CHECK (age >= 0)" +
                ")");
        exec("INSERT INTO ct_multi_con (email, age) VALUES ('a@b.com', 25)");
        assertEquals(1, queryInt("SELECT id FROM ct_multi_con"));
        assertEquals("active", query1("SELECT status FROM ct_multi_con"));
        // PK prevents duplicates
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_multi_con (id, email, age) VALUES (1, 'c@d.com', 30)"));
        // UNIQUE prevents duplicate email
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_multi_con (email, age) VALUES ('a@b.com', 30)"));
        // CHECK prevents negative age
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_multi_con (email, age) VALUES ('e@f.com', -1)"));
        exec("DROP TABLE IF EXISTS ct_multi_con");
    }

    @Test
    void ct_default_with_literal() throws SQLException {
        exec("CREATE TABLE ct_def_lit2 (id INTEGER, val TEXT DEFAULT 'hello world')");
        exec("INSERT INTO ct_def_lit2 (id) VALUES (1)");
        assertEquals("hello world", query1("SELECT val FROM ct_def_lit2"));
        exec("DROP TABLE IF EXISTS ct_def_lit2");
    }

    @Test
    void ct_default_uuid_gen() throws SQLException {
        exec("CREATE TABLE ct_def_uuid (id UUID DEFAULT gen_random_uuid(), name TEXT)");
        exec("INSERT INTO ct_def_uuid (name) VALUES ('test')");
        String uuid = query1("SELECT id FROM ct_def_uuid");
        assertNotNull(uuid);
        assertTrue(uuid.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));
        exec("DROP TABLE IF EXISTS ct_def_uuid");
    }

    @Test
    void ct_check_range() throws SQLException {
        exec("CREATE TABLE ct_chk_rng (val INTEGER, CONSTRAINT chk_rng CHECK (val BETWEEN 1 AND 100))");
        exec("INSERT INTO ct_chk_rng VALUES (50)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_chk_rng VALUES (0)"));
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_chk_rng VALUES (101)"));
        exec("DROP TABLE IF EXISTS ct_chk_rng");
    }

    @Test
    void ct_unique_allows_multiple_nulls() throws SQLException {
        exec("CREATE TABLE ct_uniq_null (val TEXT UNIQUE)");
        exec("INSERT INTO ct_uniq_null VALUES (NULL)");
        exec("INSERT INTO ct_uniq_null VALUES (NULL)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM ct_uniq_null"));
        exec("DROP TABLE IF EXISTS ct_uniq_null");
    }

    @Test
    void ct_fk_references_pk() throws SQLException {
        exec("CREATE TABLE ct_fkref_p (id INTEGER PRIMARY KEY)");
        exec("CREATE TABLE ct_fkref_c (ref_id INTEGER REFERENCES ct_fkref_p(id))");
        exec("INSERT INTO ct_fkref_p VALUES (10)");
        exec("INSERT INTO ct_fkref_c VALUES (10)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO ct_fkref_c VALUES (99)"));
        exec("DROP TABLE IF EXISTS ct_fkref_c");
        exec("DROP TABLE IF EXISTS ct_fkref_p");
    }

    @Test
    void ct_fk_null_allowed() throws SQLException {
        exec("CREATE TABLE ct_fknull_p (id INTEGER PRIMARY KEY)");
        exec("CREATE TABLE ct_fknull_c (ref_id INTEGER REFERENCES ct_fknull_p(id))");
        exec("INSERT INTO ct_fknull_c VALUES (NULL)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM ct_fknull_c"));
        exec("DROP TABLE IF EXISTS ct_fknull_c");
        exec("DROP TABLE IF EXISTS ct_fknull_p");
    }

    // =========================================================================
    // 61. ALTER TABLE
    // =========================================================================

    // --- ADD COLUMN ---

    @Test
    void at_add_column_basic() throws SQLException {
        exec("CREATE TABLE at_add1 (id INTEGER)");
        exec("ALTER TABLE at_add1 ADD COLUMN name TEXT");
        exec("INSERT INTO at_add1 VALUES (1, 'hello')");
        assertEquals("hello", query1("SELECT name FROM at_add1"));
        exec("DROP TABLE IF EXISTS at_add1");
    }

    @Test
    void at_add_column_without_column_keyword() throws SQLException {
        exec("CREATE TABLE at_add_nk (id INTEGER)");
        exec("ALTER TABLE at_add_nk ADD name TEXT");
        exec("INSERT INTO at_add_nk VALUES (1, 'test')");
        assertEquals("test", query1("SELECT name FROM at_add_nk"));
        exec("DROP TABLE IF EXISTS at_add_nk");
    }

    @Test
    void at_add_column_with_not_null_default() throws SQLException {
        exec("CREATE TABLE at_add2 (id INTEGER)");
        exec("ALTER TABLE at_add2 ADD COLUMN status TEXT NOT NULL DEFAULT 'active'");
        exec("INSERT INTO at_add2 (id) VALUES (1)");
        assertEquals("active", query1("SELECT status FROM at_add2 WHERE id = 1"));
        exec("DROP TABLE IF EXISTS at_add2");
    }

    @Test
    void at_add_column_integer() throws SQLException {
        exec("CREATE TABLE at_add3 (name TEXT)");
        exec("ALTER TABLE at_add3 ADD COLUMN age INTEGER");
        exec("INSERT INTO at_add3 VALUES ('bob', 30)");
        assertEquals(30, queryInt("SELECT age FROM at_add3"));
        exec("DROP TABLE IF EXISTS at_add3");
    }

    @Test
    void at_add_column_boolean_default() throws SQLException {
        exec("CREATE TABLE at_add4 (id INTEGER)");
        exec("ALTER TABLE at_add4 ADD COLUMN active BOOLEAN DEFAULT true");
        exec("INSERT INTO at_add4 (id) VALUES (1)");
        assertTrue(queryBool("SELECT active FROM at_add4"));
        exec("DROP TABLE IF EXISTS at_add4");
    }

    @Test
    void at_add_column_with_default_literal() throws SQLException {
        exec("CREATE TABLE at_add_dl (id INTEGER)");
        exec("ALTER TABLE at_add_dl ADD COLUMN label TEXT DEFAULT 'none'");
        exec("INSERT INTO at_add_dl (id) VALUES (1)");
        assertEquals("none", query1("SELECT label FROM at_add_dl"));
        exec("DROP TABLE IF EXISTS at_add_dl");
    }

    @Test
    void at_add_column_timestamp_default_now() throws SQLException {
        exec("CREATE TABLE at_add_ts (id INTEGER)");
        exec("ALTER TABLE at_add_ts ADD COLUMN created TIMESTAMP DEFAULT NOW()");
        exec("INSERT INTO at_add_ts (id) VALUES (1)");
        assertNotNull(query1("SELECT created FROM at_add_ts"));
        exec("DROP TABLE IF EXISTS at_add_ts");
    }

    // --- DROP COLUMN ---

    @Test
    void at_drop_column() throws SQLException {
        exec("CREATE TABLE at_drop1 (id INTEGER, name TEXT, extra TEXT)");
        exec("ALTER TABLE at_drop1 DROP COLUMN extra");
        exec("INSERT INTO at_drop1 VALUES (1, 'test')");
        assertEquals("test", query1("SELECT name FROM at_drop1"));
        exec("DROP TABLE IF EXISTS at_drop1");
    }

    @Test
    void at_drop_column_if_exists_present() throws SQLException {
        exec("CREATE TABLE at_drop2 (id INTEGER, name TEXT)");
        exec("ALTER TABLE at_drop2 DROP COLUMN IF EXISTS name");
        exec("INSERT INTO at_drop2 VALUES (1)");
        assertEquals(1, queryInt("SELECT id FROM at_drop2"));
        exec("DROP TABLE IF EXISTS at_drop2");
    }

    @Test
    void at_drop_column_if_exists_absent() throws SQLException {
        exec("CREATE TABLE at_drop3 (id INTEGER)");
        // Should not error since IF EXISTS is used
        exec("ALTER TABLE at_drop3 DROP COLUMN IF EXISTS nonexistent");
        exec("INSERT INTO at_drop3 VALUES (1)");
        assertEquals(1, queryInt("SELECT id FROM at_drop3"));
        exec("DROP TABLE IF EXISTS at_drop3");
    }

    @Test
    void at_drop_column_cascade() throws SQLException {
        exec("CREATE TABLE at_drop_cas (id INTEGER, name TEXT, extra TEXT)");
        exec("ALTER TABLE at_drop_cas DROP COLUMN extra CASCADE");
        exec("INSERT INTO at_drop_cas VALUES (1, 'ok')");
        assertEquals("ok", query1("SELECT name FROM at_drop_cas"));
        exec("DROP TABLE IF EXISTS at_drop_cas");
    }

    @Test
    void at_drop_then_re_add_column() throws SQLException {
        exec("CREATE TABLE at_dra (id INTEGER, val TEXT)");
        exec("ALTER TABLE at_dra DROP COLUMN val");
        exec("ALTER TABLE at_dra ADD COLUMN val INTEGER");
        exec("INSERT INTO at_dra VALUES (1, 42)");
        assertEquals(42, queryInt("SELECT val FROM at_dra"));
        exec("DROP TABLE IF EXISTS at_dra");
    }

    // --- ALTER COLUMN ---

    @Test
    void at_alter_column_set_type_varchar_to_text() throws SQLException {
        exec("CREATE TABLE at_stype1 (id INTEGER, name VARCHAR(50))");
        exec("ALTER TABLE at_stype1 ALTER COLUMN name TYPE TEXT");
        exec("INSERT INTO at_stype1 VALUES (1, 'a long name that would exceed varchar(50) easily if it matters')");
        assertNotNull(query1("SELECT name FROM at_stype1"));
        exec("DROP TABLE IF EXISTS at_stype1");
    }

    @Test
    void at_alter_column_set_type_int_to_bigint() throws SQLException {
        exec("CREATE TABLE at_stype2 (id INTEGER, val INTEGER)");
        exec("ALTER TABLE at_stype2 ALTER COLUMN val TYPE BIGINT");
        exec("INSERT INTO at_stype2 VALUES (1, 9999999999)");
        assertEquals("9999999999", query1("SELECT val FROM at_stype2"));
        exec("DROP TABLE IF EXISTS at_stype2");
    }

    @Test
    void at_alter_column_type_shorthand() throws SQLException {
        exec("CREATE TABLE at_stype3 (id INTEGER, val TEXT)");
        exec("ALTER TABLE at_stype3 ALTER COLUMN val TYPE VARCHAR(100)");
        exec("INSERT INTO at_stype3 VALUES (1, 'short')");
        assertEquals("short", query1("SELECT val FROM at_stype3"));
        exec("DROP TABLE IF EXISTS at_stype3");
    }

    @Test
    void at_alter_column_set_type_with_using() throws SQLException {
        exec("CREATE TABLE at_stype_using (id INTEGER, val TEXT)");
        exec("INSERT INTO at_stype_using VALUES (1, '42')");
        exec("ALTER TABLE at_stype_using ALTER COLUMN val TYPE INTEGER USING val::integer");
        // After type change, column should accept integer operations
        exec("INSERT INTO at_stype_using VALUES (2, 100)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM at_stype_using"));
        exec("DROP TABLE IF EXISTS at_stype_using");
    }

    @Test
    void at_alter_type_text_to_varchar() throws SQLException {
        exec("CREATE TABLE at_t2v (id INTEGER, val TEXT)");
        exec("ALTER TABLE at_t2v ALTER COLUMN val TYPE VARCHAR(200)");
        exec("INSERT INTO at_t2v VALUES (1, 'converted')");
        assertEquals("converted", query1("SELECT val FROM at_t2v"));
        exec("DROP TABLE IF EXISTS at_t2v");
    }

    @Test
    void at_alter_column_set_default() throws SQLException {
        exec("CREATE TABLE at_sd (id INTEGER, val TEXT)");
        exec("ALTER TABLE at_sd ALTER COLUMN val SET DEFAULT 'hello'");
        exec("INSERT INTO at_sd (id) VALUES (1)");
        assertEquals("hello", query1("SELECT val FROM at_sd"));
        exec("DROP TABLE IF EXISTS at_sd");
    }

    @Test
    void at_alter_column_drop_default() throws SQLException {
        exec("CREATE TABLE at_dd (id INTEGER, val TEXT DEFAULT 'x')");
        exec("INSERT INTO at_dd (id) VALUES (1)");
        assertEquals("x", query1("SELECT val FROM at_dd WHERE id = 1"));
        exec("ALTER TABLE at_dd ALTER COLUMN val DROP DEFAULT");
        exec("INSERT INTO at_dd (id) VALUES (2)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM at_dd WHERE id = 2")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1));
        }
        exec("DROP TABLE IF EXISTS at_dd");
    }

    @Test
    void at_set_default_numeric() throws SQLException {
        exec("CREATE TABLE at_sd_num (id INTEGER, priority INTEGER)");
        exec("ALTER TABLE at_sd_num ALTER COLUMN priority SET DEFAULT 5");
        exec("INSERT INTO at_sd_num (id) VALUES (1)");
        assertEquals(5, queryInt("SELECT priority FROM at_sd_num"));
        exec("DROP TABLE IF EXISTS at_sd_num");
    }

    @Test
    void at_alter_column_set_not_null() throws SQLException {
        exec("CREATE TABLE at_snn (id INTEGER, val TEXT)");
        exec("ALTER TABLE at_snn ALTER COLUMN val SET NOT NULL");
        assertThrows(SQLException.class, () -> exec("INSERT INTO at_snn VALUES (1, NULL)"));
        exec("INSERT INTO at_snn VALUES (1, 'ok')");
        assertEquals("ok", query1("SELECT val FROM at_snn"));
        exec("DROP TABLE IF EXISTS at_snn");
    }

    @Test
    void at_alter_column_drop_not_null() throws SQLException {
        exec("CREATE TABLE at_dnn (id INTEGER, val TEXT NOT NULL)");
        exec("ALTER TABLE at_dnn ALTER COLUMN val DROP NOT NULL");
        exec("INSERT INTO at_dnn VALUES (1, NULL)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM at_dnn"));
        exec("DROP TABLE IF EXISTS at_dnn");
    }

    @Test
    void at_set_not_null_then_drop_not_null() throws SQLException {
        exec("CREATE TABLE at_sndnn (id INTEGER, val TEXT)");
        exec("ALTER TABLE at_sndnn ALTER COLUMN val SET NOT NULL");
        assertThrows(SQLException.class, () -> exec("INSERT INTO at_sndnn VALUES (1, NULL)"));
        exec("ALTER TABLE at_sndnn ALTER COLUMN val DROP NOT NULL");
        exec("INSERT INTO at_sndnn VALUES (1, NULL)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM at_sndnn"));
        exec("DROP TABLE IF EXISTS at_sndnn");
    }

    @Test
    void at_multiple_set_defaults() throws SQLException {
        exec("CREATE TABLE at_multi_def (id INTEGER, a TEXT, b TEXT)");
        exec("ALTER TABLE at_multi_def ALTER COLUMN a SET DEFAULT 'aa', ALTER COLUMN b SET DEFAULT 'bb'");
        exec("INSERT INTO at_multi_def (id) VALUES (1)");
        assertEquals("aa", query1("SELECT a FROM at_multi_def"));
        assertEquals("bb", query1("SELECT b FROM at_multi_def"));
        exec("DROP TABLE IF EXISTS at_multi_def");
    }

    // --- ADD CONSTRAINT ---

    @Test
    void at_add_constraint_pk() throws SQLException {
        exec("CREATE TABLE at_cpk (id INTEGER, name TEXT)");
        exec("ALTER TABLE at_cpk ADD CONSTRAINT pk_at_cpk PRIMARY KEY (id)");
        exec("INSERT INTO at_cpk VALUES (1, 'a')");
        assertThrows(SQLException.class, () -> exec("INSERT INTO at_cpk VALUES (1, 'b')"));
        exec("DROP TABLE IF EXISTS at_cpk");
    }

    @Test
    void at_add_constraint_unique() throws SQLException {
        exec("CREATE TABLE at_cuq (id INTEGER, email TEXT)");
        exec("ALTER TABLE at_cuq ADD CONSTRAINT uq_email UNIQUE (email)");
        exec("INSERT INTO at_cuq VALUES (1, 'a@b.com')");
        assertThrows(SQLException.class, () -> exec("INSERT INTO at_cuq VALUES (2, 'a@b.com')"));
        exec("DROP TABLE IF EXISTS at_cuq");
    }

    @Test
    void at_add_constraint_check() throws SQLException {
        exec("CREATE TABLE at_cchk (id INTEGER, val INTEGER)");
        exec("ALTER TABLE at_cchk ADD CONSTRAINT chk_val CHECK (val > 0)");
        exec("INSERT INTO at_cchk VALUES (1, 5)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO at_cchk VALUES (2, -1)"));
        exec("DROP TABLE IF EXISTS at_cchk");
    }

    @Test
    void at_add_constraint_fk() throws SQLException {
        exec("CREATE TABLE at_cfk_p (id INTEGER PRIMARY KEY)");
        exec("CREATE TABLE at_cfk_c (pid INTEGER)");
        exec("ALTER TABLE at_cfk_c ADD CONSTRAINT fk_pid FOREIGN KEY (pid) REFERENCES at_cfk_p(id)");
        exec("INSERT INTO at_cfk_p VALUES (1)");
        exec("INSERT INTO at_cfk_c VALUES (1)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO at_cfk_c VALUES (99)"));
        exec("DROP TABLE IF EXISTS at_cfk_c");
        exec("DROP TABLE IF EXISTS at_cfk_p");
    }

    @Test
    void at_add_check_constraint_named() throws SQLException {
        exec("CREATE TABLE at_chk_named (id INTEGER, score INTEGER)");
        exec("ALTER TABLE at_chk_named ADD CONSTRAINT chk_score CHECK (score >= 0 AND score <= 100)");
        exec("INSERT INTO at_chk_named VALUES (1, 50)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO at_chk_named VALUES (2, 150)"));
        exec("DROP TABLE IF EXISTS at_chk_named");
    }

    @Test
    void at_add_unique_constraint_prevents_dups() throws SQLException {
        exec("CREATE TABLE at_uq_dup (id INTEGER, code TEXT)");
        exec("INSERT INTO at_uq_dup VALUES (1, 'ABC')");
        exec("ALTER TABLE at_uq_dup ADD CONSTRAINT uq_code UNIQUE (code)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO at_uq_dup VALUES (2, 'ABC')"));
        exec("DROP TABLE IF EXISTS at_uq_dup");
    }

    @Test
    void at_add_pk_enforces_uniqueness() throws SQLException {
        exec("CREATE TABLE at_pk_enf (id INTEGER, val TEXT)");
        exec("ALTER TABLE at_pk_enf ADD CONSTRAINT pk_enf PRIMARY KEY (id)");
        exec("INSERT INTO at_pk_enf VALUES (1, 'a')");
        assertThrows(SQLException.class, () -> exec("INSERT INTO at_pk_enf VALUES (1, 'b')"));
        exec("DROP TABLE IF EXISTS at_pk_enf");
    }

    @Test
    void at_add_fk_cascade() throws SQLException {
        exec("CREATE TABLE at_fk_cas_p (id INTEGER PRIMARY KEY)");
        exec("CREATE TABLE at_fk_cas_c (pid INTEGER)");
        exec("ALTER TABLE at_fk_cas_c ADD CONSTRAINT fk_cas FOREIGN KEY (pid) REFERENCES at_fk_cas_p(id) ON DELETE CASCADE");
        exec("INSERT INTO at_fk_cas_p VALUES (1)");
        exec("INSERT INTO at_fk_cas_c VALUES (1)");
        exec("DELETE FROM at_fk_cas_p WHERE id = 1");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM at_fk_cas_c"));
        exec("DROP TABLE IF EXISTS at_fk_cas_c");
        exec("DROP TABLE IF EXISTS at_fk_cas_p");
    }

    // --- DROP CONSTRAINT ---

    @Test
    void at_drop_constraint() throws SQLException {
        exec("CREATE TABLE at_dc (id INTEGER, val TEXT, CONSTRAINT uq_dc UNIQUE (val))");
        exec("INSERT INTO at_dc VALUES (1, 'a')");
        assertThrows(SQLException.class, () -> exec("INSERT INTO at_dc VALUES (2, 'a')"));
        exec("ALTER TABLE at_dc DROP CONSTRAINT uq_dc");
        exec("INSERT INTO at_dc VALUES (2, 'a')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM at_dc"));
        exec("DROP TABLE IF EXISTS at_dc");
    }

    @Test
    void at_drop_constraint_if_exists_present() throws SQLException {
        exec("CREATE TABLE at_dc_ie (id INTEGER, CONSTRAINT chk_dc CHECK (id > 0))");
        exec("ALTER TABLE at_dc_ie DROP CONSTRAINT IF EXISTS chk_dc");
        exec("INSERT INTO at_dc_ie VALUES (-1)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM at_dc_ie"));
        exec("DROP TABLE IF EXISTS at_dc_ie");
    }

    @Test
    void at_drop_constraint_if_exists_absent() throws SQLException {
        exec("CREATE TABLE at_dc_ie2 (id INTEGER)");
        // Should not error since constraint does not exist but IF EXISTS is used
        exec("ALTER TABLE at_dc_ie2 DROP CONSTRAINT IF EXISTS nonexistent");
        exec("INSERT INTO at_dc_ie2 VALUES (1)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM at_dc_ie2"));
        exec("DROP TABLE IF EXISTS at_dc_ie2");
    }

    @Test
    void at_drop_constraint_cascade() throws SQLException {
        exec("CREATE TABLE at_dc_cas (id INTEGER, val TEXT, CONSTRAINT uq_dc_cas UNIQUE (val))");
        exec("ALTER TABLE at_dc_cas DROP CONSTRAINT uq_dc_cas CASCADE");
        exec("INSERT INTO at_dc_cas VALUES (1, 'dup')");
        exec("INSERT INTO at_dc_cas VALUES (2, 'dup')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM at_dc_cas"));
        exec("DROP TABLE IF EXISTS at_dc_cas");
    }

    @Test
    void at_drop_pk_constraint() throws SQLException {
        exec("CREATE TABLE at_dpk (id INTEGER, CONSTRAINT pk_dpk PRIMARY KEY (id))");
        exec("ALTER TABLE at_dpk DROP CONSTRAINT pk_dpk");
        exec("INSERT INTO at_dpk VALUES (1)");
        exec("INSERT INTO at_dpk VALUES (1)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM at_dpk"));
        exec("DROP TABLE IF EXISTS at_dpk");
    }

    // --- RENAME COLUMN ---

    @Test
    void at_rename_column() throws SQLException {
        exec("CREATE TABLE at_rnc (id INTEGER, old_name TEXT)");
        exec("ALTER TABLE at_rnc RENAME COLUMN old_name TO new_name");
        exec("INSERT INTO at_rnc VALUES (1, 'test')");
        assertEquals("test", query1("SELECT new_name FROM at_rnc"));
        exec("DROP TABLE IF EXISTS at_rnc");
    }

    @Test
    void at_rename_column_and_query() throws SQLException {
        exec("CREATE TABLE at_rnc2 (id INTEGER, val TEXT)");
        exec("INSERT INTO at_rnc2 VALUES (1, 'before')");
        exec("ALTER TABLE at_rnc2 RENAME COLUMN val TO value");
        assertEquals("before", query1("SELECT value FROM at_rnc2"));
        exec("DROP TABLE IF EXISTS at_rnc2");
    }

    @Test
    void at_rename_column_preserves_data() throws SQLException {
        exec("CREATE TABLE at_rnc3 (id INTEGER, x TEXT)");
        exec("INSERT INTO at_rnc3 VALUES (1, 'data')");
        exec("ALTER TABLE at_rnc3 RENAME COLUMN x TO y");
        assertEquals("data", query1("SELECT y FROM at_rnc3 WHERE id = 1"));
        exec("DROP TABLE IF EXISTS at_rnc3");
    }

    // --- RENAME TABLE ---

    @Test
    void at_rename_table() throws SQLException {
        exec("CREATE TABLE at_rnt_old (id INTEGER)");
        exec("INSERT INTO at_rnt_old VALUES (1)");
        exec("ALTER TABLE at_rnt_old RENAME TO at_rnt_new");
        assertEquals(1, queryInt("SELECT id FROM at_rnt_new"));
        exec("DROP TABLE IF EXISTS at_rnt_new");
        exec("DROP TABLE IF EXISTS at_rnt_old");
    }

    @Test
    void at_rename_table_data_preserved() throws SQLException {
        exec("CREATE TABLE at_rnt2_old (id INTEGER, name TEXT)");
        exec("INSERT INTO at_rnt2_old VALUES (1, 'keep')");
        exec("INSERT INTO at_rnt2_old VALUES (2, 'this')");
        exec("ALTER TABLE at_rnt2_old RENAME TO at_rnt2_new");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM at_rnt2_new"));
        assertEquals("keep", query1("SELECT name FROM at_rnt2_new WHERE id = 1"));
        exec("DROP TABLE IF EXISTS at_rnt2_new");
        exec("DROP TABLE IF EXISTS at_rnt2_old");
    }

    // --- RENAME CONSTRAINT ---

    @Test
    void at_rename_constraint() throws SQLException {
        exec("CREATE TABLE at_rn_con (id INTEGER, CONSTRAINT old_pk PRIMARY KEY (id))");
        exec("ALTER TABLE at_rn_con RENAME CONSTRAINT old_pk TO new_pk");
        // After rename, constraint still works
        exec("INSERT INTO at_rn_con VALUES (1)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO at_rn_con VALUES (1)"));
        exec("DROP TABLE IF EXISTS at_rn_con");
    }

    @Test
    void at_rename_constraint_unique() throws SQLException {
        exec("CREATE TABLE at_rn_uq (val TEXT, CONSTRAINT uq_old UNIQUE (val))");
        exec("ALTER TABLE at_rn_uq RENAME CONSTRAINT uq_old TO uq_new");
        exec("INSERT INTO at_rn_uq VALUES ('a')");
        assertThrows(SQLException.class, () -> exec("INSERT INTO at_rn_uq VALUES ('a')"));
        exec("DROP TABLE IF EXISTS at_rn_uq");
    }

    // --- SET SCHEMA ---

    @Test
    void at_set_schema() throws SQLException {
        exec("CREATE SCHEMA at_target_schema");
        exec("CREATE TABLE at_schema_tbl (id INTEGER, val TEXT)");
        exec("INSERT INTO at_schema_tbl VALUES (1, 'moved')");
        exec("ALTER TABLE at_schema_tbl SET SCHEMA at_target_schema");
        assertEquals("moved", query1("SELECT val FROM at_target_schema.at_schema_tbl WHERE id = 1"));
        exec("DROP TABLE IF EXISTS at_target_schema.at_schema_tbl");
    }

    // --- ATTACH / DETACH PARTITION ---

    @Test
    void at_attach_partition_list() throws SQLException {
        exec("CREATE TABLE at_att_parent (id INTEGER, region TEXT) PARTITION BY LIST (region)");
        exec("CREATE TABLE at_att_part (id INTEGER, region TEXT)");
        exec("ALTER TABLE at_att_parent ATTACH PARTITION at_att_part FOR VALUES IN ('us')");
        exec("INSERT INTO at_att_part VALUES (1, 'us')");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM at_att_part"));
        exec("DROP TABLE IF EXISTS at_att_part");
        exec("DROP TABLE IF EXISTS at_att_parent");
    }

    @Test
    void at_attach_partition_range() throws SQLException {
        exec("CREATE TABLE at_att_rng_p (id INTEGER, val INTEGER) PARTITION BY RANGE (val)");
        exec("CREATE TABLE at_att_rng_c (id INTEGER, val INTEGER)");
        exec("ALTER TABLE at_att_rng_p ATTACH PARTITION at_att_rng_c FOR VALUES FROM (1) TO (100)");
        exec("INSERT INTO at_att_rng_c VALUES (1, 50)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM at_att_rng_c"));
        exec("DROP TABLE IF EXISTS at_att_rng_c");
        exec("DROP TABLE IF EXISTS at_att_rng_p");
    }

    @Test
    void at_attach_partition_default() throws SQLException {
        exec("CREATE TABLE at_att_def_p (id INTEGER, val TEXT) PARTITION BY LIST (val)");
        exec("CREATE TABLE at_att_def_c (id INTEGER, val TEXT)");
        exec("ALTER TABLE at_att_def_p ATTACH PARTITION at_att_def_c DEFAULT");
        exec("INSERT INTO at_att_def_c VALUES (1, 'anything')");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM at_att_def_c"));
        exec("DROP TABLE IF EXISTS at_att_def_c");
        exec("DROP TABLE IF EXISTS at_att_def_p");
    }

    @Test
    void at_detach_partition() throws SQLException {
        exec("CREATE TABLE at_det_p (id INTEGER, region TEXT) PARTITION BY LIST (region)");
        exec("CREATE TABLE at_det_c PARTITION OF at_det_p FOR VALUES IN ('eu')");
        exec("INSERT INTO at_det_c VALUES (1, 'eu')");
        exec("ALTER TABLE at_det_p DETACH PARTITION at_det_c");
        // After detach, partition is standalone table
        assertEquals(1, queryInt("SELECT COUNT(*) FROM at_det_c"));
        exec("DROP TABLE IF EXISTS at_det_c");
        exec("DROP TABLE IF EXISTS at_det_p");
    }

    // --- ENABLE/DISABLE ROW LEVEL SECURITY ---

    @Test
    void at_enable_rls() throws SQLException {
        exec("CREATE TABLE at_rls (id INTEGER, val TEXT)");
        exec("ALTER TABLE at_rls ENABLE ROW LEVEL SECURITY");
        exec("INSERT INTO at_rls VALUES (1, 'secured')");
        assertEquals("secured", query1("SELECT val FROM at_rls"));
        exec("DROP TABLE IF EXISTS at_rls");
    }

    @Test
    void at_disable_rls() throws SQLException {
        exec("CREATE TABLE at_rls2 (id INTEGER, val TEXT)");
        exec("ALTER TABLE at_rls2 ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE at_rls2 DISABLE ROW LEVEL SECURITY");
        exec("INSERT INTO at_rls2 VALUES (1, 'open')");
        assertEquals("open", query1("SELECT val FROM at_rls2"));
        exec("DROP TABLE IF EXISTS at_rls2");
    }

    // --- OWNER TO ---

    @Test
    void at_owner_to_noop() throws SQLException {
        exec("CREATE TABLE at_owner (id INTEGER, val TEXT)");
        exec("ALTER TABLE at_owner OWNER TO postgres");
        exec("INSERT INTO at_owner VALUES (1, 'still works')");
        assertEquals("still works", query1("SELECT val FROM at_owner"));
        exec("DROP TABLE IF EXISTS at_owner");
    }

    // --- Multiple actions in single ALTER TABLE ---

    @Test
    void at_multiple_actions_add_columns() throws SQLException {
        exec("CREATE TABLE at_mul1 (id INTEGER)");
        exec("ALTER TABLE at_mul1 ADD COLUMN name TEXT, ADD COLUMN age INTEGER");
        exec("INSERT INTO at_mul1 VALUES (1, 'alice', 30)");
        assertEquals("alice", query1("SELECT name FROM at_mul1"));
        assertEquals(30, queryInt("SELECT age FROM at_mul1"));
        exec("DROP TABLE IF EXISTS at_mul1");
    }

    @Test
    void at_multiple_actions_add_and_drop() throws SQLException {
        exec("CREATE TABLE at_mul2 (id INTEGER, old_col TEXT)");
        exec("ALTER TABLE at_mul2 DROP COLUMN old_col, ADD COLUMN new_col INTEGER");
        exec("INSERT INTO at_mul2 VALUES (1, 42)");
        assertEquals(42, queryInt("SELECT new_col FROM at_mul2"));
        exec("DROP TABLE IF EXISTS at_mul2");
    }

    @Test
    void at_multiple_alter_columns() throws SQLException {
        exec("CREATE TABLE at_mul3 (id INTEGER, a TEXT, b TEXT)");
        exec("ALTER TABLE at_mul3 ALTER COLUMN a SET NOT NULL, ALTER COLUMN b SET DEFAULT 'x'");
        exec("INSERT INTO at_mul3 (id, a) VALUES (1, 'val')");
        assertEquals("x", query1("SELECT b FROM at_mul3"));
        assertThrows(SQLException.class, () -> exec("INSERT INTO at_mul3 (id, a) VALUES (2, NULL)"));
        exec("DROP TABLE IF EXISTS at_mul3");
    }

    // =========================================================================
    // 62. DROP TABLE
    // =========================================================================

    @Test
    void dt_basic_drop() throws SQLException {
        exec("CREATE TABLE dt_basic (id INTEGER)");
        assertTrue(tableExists("dt_basic"));
        exec("DROP TABLE dt_basic");
        assertFalse(tableExists("dt_basic"));
    }

    @Test
    void dt_basic_drop_with_data() throws SQLException {
        exec("CREATE TABLE dt_data (id INTEGER, name TEXT)");
        exec("INSERT INTO dt_data VALUES (1, 'a'), (2, 'b')");
        exec("DROP TABLE dt_data");
        assertFalse(tableExists("dt_data"));
    }

    @Test
    void dt_if_exists_table_present() throws SQLException {
        exec("CREATE TABLE dt_ie_present (id INTEGER)");
        exec("DROP TABLE IF EXISTS dt_ie_present");
        assertFalse(tableExists("dt_ie_present"));
    }

    @Test
    void dt_if_exists_table_absent() throws SQLException {
        // Should not error on non-existing table
        exec("DROP TABLE IF EXISTS dt_ie_absent_nonexistent");
    }

    @Test
    void dt_if_exists_already_dropped() throws SQLException {
        exec("CREATE TABLE dt_ie_double (id INTEGER)");
        exec("DROP TABLE dt_ie_double");
        // Second drop with IF EXISTS should not error
        exec("DROP TABLE IF EXISTS dt_ie_double");
    }

    @Test
    void dt_without_if_exists_errors_on_missing() throws SQLException {
        assertThrows(SQLException.class, () -> exec("DROP TABLE dt_no_ie_nonexistent_xyz"));
    }

    @Test
    void dt_cascade_basic() throws SQLException {
        exec("CREATE TABLE dt_cas_basic (id INTEGER)");
        exec("INSERT INTO dt_cas_basic VALUES (1)");
        exec("DROP TABLE dt_cas_basic CASCADE");
        assertFalse(tableExists("dt_cas_basic"));
    }

    @Test
    void dt_cascade_with_dependent_view() throws SQLException {
        exec("CREATE TABLE dt_cas_view_src (id INTEGER, name TEXT)");
        exec("INSERT INTO dt_cas_view_src VALUES (1, 'x')");
        exec("CREATE VIEW dt_cas_view AS SELECT * FROM dt_cas_view_src");
        exec("DROP TABLE dt_cas_view_src CASCADE");
        assertFalse(tableExists("dt_cas_view_src"));
    }

    @Test
    void dt_restrict_no_deps() throws SQLException {
        exec("CREATE TABLE dt_restrict (id INTEGER)");
        exec("INSERT INTO dt_restrict VALUES (1)");
        exec("DROP TABLE dt_restrict RESTRICT");
        assertFalse(tableExists("dt_restrict"));
    }

    @Test
    void dt_verify_gone_cannot_select() throws SQLException {
        exec("CREATE TABLE dt_gone_sel (id INTEGER)");
        exec("DROP TABLE dt_gone_sel");
        assertThrows(SQLException.class, () -> exec("SELECT * FROM dt_gone_sel"));
    }

    @Test
    void dt_verify_gone_cannot_insert() throws SQLException {
        exec("CREATE TABLE dt_gone_ins (id INTEGER)");
        exec("DROP TABLE dt_gone_ins");
        assertThrows(SQLException.class, () -> exec("INSERT INTO dt_gone_ins VALUES (1)"));
    }

    @Test
    void dt_verify_gone_info_schema() throws SQLException {
        exec("CREATE TABLE dt_gone_is (id INTEGER)");
        exec("DROP TABLE dt_gone_is");
        assertFalse(tableExists("dt_gone_is"));
    }

    @Test
    void dt_recreate_after_drop() throws SQLException {
        exec("CREATE TABLE dt_recreate (id INTEGER)");
        exec("INSERT INTO dt_recreate VALUES (1)");
        exec("DROP TABLE dt_recreate");
        exec("CREATE TABLE dt_recreate (id INTEGER, name TEXT)");
        exec("INSERT INTO dt_recreate VALUES (2, 'new')");
        assertEquals(2, queryInt("SELECT id FROM dt_recreate"));
        assertEquals("new", query1("SELECT name FROM dt_recreate"));
        exec("DROP TABLE IF EXISTS dt_recreate");
    }

    @Test
    void dt_drop_empty_table() throws SQLException {
        exec("CREATE TABLE dt_empty (id INTEGER, name TEXT)");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM dt_empty"));
        exec("DROP TABLE dt_empty");
        assertFalse(tableExists("dt_empty"));
    }

    @Test
    void dt_drop_table_clears_constraints() throws SQLException {
        exec("CREATE TABLE dt_con_tbl (id INTEGER PRIMARY KEY, val TEXT UNIQUE)");
        exec("INSERT INTO dt_con_tbl VALUES (1, 'a')");
        exec("DROP TABLE dt_con_tbl");
        // Recreate with same name, constraints should be fresh
        exec("CREATE TABLE dt_con_tbl (id INTEGER, val TEXT)");
        exec("INSERT INTO dt_con_tbl VALUES (1, 'a')");
        exec("INSERT INTO dt_con_tbl VALUES (1, 'a')"); // no PK or UNIQUE now
        assertEquals(2, queryInt("SELECT COUNT(*) FROM dt_con_tbl"));
        exec("DROP TABLE IF EXISTS dt_con_tbl");
    }

    @Test
    void dt_drop_table_serial_resets() throws SQLException {
        exec("CREATE TABLE dt_serial_reset (id SERIAL, name TEXT)");
        exec("INSERT INTO dt_serial_reset (name) VALUES ('a')");
        exec("INSERT INTO dt_serial_reset (name) VALUES ('b')");
        exec("DROP TABLE dt_serial_reset");
        // Recreate - serial should start at 1 again
        exec("CREATE TABLE dt_serial_reset (id SERIAL, name TEXT)");
        exec("INSERT INTO dt_serial_reset (name) VALUES ('new')");
        assertEquals(1, queryInt("SELECT id FROM dt_serial_reset WHERE name = 'new'"));
        exec("DROP TABLE IF EXISTS dt_serial_reset");
    }

    @Test
    void dt_cascade_keyword_accepted() throws SQLException {
        exec("CREATE TABLE dt_cas_kw (id INTEGER)");
        exec("DROP TABLE dt_cas_kw CASCADE");
        assertFalse(tableExists("dt_cas_kw"));
    }

    @Test
    void dt_restrict_keyword_accepted() throws SQLException {
        exec("CREATE TABLE dt_res_kw (id INTEGER)");
        exec("DROP TABLE dt_res_kw RESTRICT");
        assertFalse(tableExists("dt_res_kw"));
    }

    @Test
    void dt_if_exists_cascade_combined() throws SQLException {
        exec("CREATE TABLE dt_ie_cas (id INTEGER)");
        exec("DROP TABLE IF EXISTS dt_ie_cas CASCADE");
        assertFalse(tableExists("dt_ie_cas"));
        // Second drop with IF EXISTS CASCADE should also not error
        exec("DROP TABLE IF EXISTS dt_ie_cas CASCADE");
    }

    // --- DROP TABLE with multiple tables ---

    @Test
    void dt_drop_multiple_tables() throws SQLException {
        exec("CREATE TABLE dt_multi_a (id INTEGER)");
        exec("CREATE TABLE dt_multi_b (id INTEGER)");
        exec("INSERT INTO dt_multi_a VALUES (1)");
        exec("INSERT INTO dt_multi_b VALUES (2)");
        exec("DROP TABLE dt_multi_a, dt_multi_b");
        assertFalse(tableExists("dt_multi_a"));
        assertFalse(tableExists("dt_multi_b"));
    }

    @Test
    void dt_drop_multiple_tables_if_exists() throws SQLException {
        exec("CREATE TABLE dt_multi_ie_a (id INTEGER)");
        exec("CREATE TABLE dt_multi_ie_b (id INTEGER)");
        exec("DROP TABLE IF EXISTS dt_multi_ie_a, dt_multi_ie_b");
        assertFalse(tableExists("dt_multi_ie_a"));
        assertFalse(tableExists("dt_multi_ie_b"));
    }

    @Test
    void dt_drop_multiple_tables_three() throws SQLException {
        exec("CREATE TABLE dt_m3_a (id INTEGER)");
        exec("CREATE TABLE dt_m3_b (id INTEGER)");
        exec("CREATE TABLE dt_m3_c (id INTEGER)");
        exec("DROP TABLE dt_m3_a, dt_m3_b, dt_m3_c");
        assertFalse(tableExists("dt_m3_a"));
        assertFalse(tableExists("dt_m3_b"));
        assertFalse(tableExists("dt_m3_c"));
    }
}
