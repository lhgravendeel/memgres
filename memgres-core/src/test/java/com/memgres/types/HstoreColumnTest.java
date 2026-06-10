package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for hstore as a column type, covering:
 *  - Column creation (CREATE TABLE, ALTER TABLE ADD COLUMN)
 *  - INSERT / UPDATE / DELETE with hstore data
 *  - SELECT with hstore operators (->  @>  ?  ||  etc.)
 *  - Casting to/from hstore (text, json, jsonb, record)
 *  - hstore functions (akeys, avals, each, hstore_to_json, etc.)
 *  - NULL handling, empty hstore, defaults
 *  - WHERE clause filtering on hstore columns
 *  - hstore with other column types in same table
 */
class HstoreColumnTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE EXTENSION IF NOT EXISTS hstore");
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
            String v = rs.getString(1);
            assertFalse(rs.next(), "expected single row");
            return v;
        }
    }

    private static boolean bool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    private static int count(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // A. Column creation
    // =========================================================================

    @Test
    void create_table_with_hstore_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_create");
        exec("CREATE TABLE hs_create (id serial PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_create (data) VALUES ('color=>blue,size=>large')");
        assertEquals("blue", str("SELECT data->'color' FROM hs_create WHERE id = 1"));
    }

    @Test
    void alter_table_add_hstore_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_alter");
        exec("CREATE TABLE hs_alter (id serial PRIMARY KEY, name text)");
        exec("ALTER TABLE hs_alter ADD COLUMN settings hstore");
        exec("INSERT INTO hs_alter (name, settings) VALUES ('acme', 'theme=>dark,lang=>en')");
        assertEquals("dark", str("SELECT settings->'theme' FROM hs_alter WHERE name = 'acme'"));
    }

    @Test
    void hstore_column_with_default() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_default");
        exec("CREATE TABLE hs_default (id int PRIMARY KEY, opts hstore DEFAULT 'debug=>false')");
        exec("INSERT INTO hs_default (id) VALUES (1)");
        assertEquals("false", str("SELECT opts->'debug' FROM hs_default WHERE id = 1"));
    }

    @Test
    void hstore_column_not_null_constraint() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_notnull");
        exec("CREATE TABLE hs_notnull (id int PRIMARY KEY, tags hstore NOT NULL)");
        exec("INSERT INTO hs_notnull VALUES (1, 'a=>1')");
        assertThrows(SQLException.class, () -> exec("INSERT INTO hs_notnull VALUES (2, NULL)"));
    }

    // =========================================================================
    // B. INSERT / UPDATE / DELETE
    // =========================================================================

    @Test
    void insert_hstore_literal() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_ins");
        exec("CREATE TABLE hs_ins (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_ins VALUES (1, 'a=>1, b=>2, c=>3')");
        assertEquals("1", str("SELECT data->'a' FROM hs_ins WHERE id = 1"));
        assertEquals("3", str("SELECT data->'c' FROM hs_ins WHERE id = 1"));
    }

    @Test
    void insert_hstore_null_value() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_null");
        exec("CREATE TABLE hs_null (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_null VALUES (1, 'a=>1, b=>NULL')");
        assertNull(str("SELECT data->'b' FROM hs_null WHERE id = 1"));
    }

    @Test
    void insert_hstore_null_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_cnull");
        exec("CREATE TABLE hs_cnull (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_cnull VALUES (1, NULL)");
        assertNull(str("SELECT data FROM hs_cnull WHERE id = 1"));
    }

    @Test
    void insert_empty_hstore() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_empty");
        exec("CREATE TABLE hs_empty (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_empty VALUES (1, '')");
        assertNull(str("SELECT data->'anything' FROM hs_empty WHERE id = 1"));
    }

    @Test
    void update_hstore_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_upd");
        exec("CREATE TABLE hs_upd (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_upd VALUES (1, 'x=>old')");
        exec("UPDATE hs_upd SET data = 'x=>new,y=>added' WHERE id = 1");
        assertEquals("new", str("SELECT data->'x' FROM hs_upd WHERE id = 1"));
        assertEquals("added", str("SELECT data->'y' FROM hs_upd WHERE id = 1"));
    }

    @Test
    void update_hstore_to_null() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_upd2");
        exec("CREATE TABLE hs_upd2 (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_upd2 VALUES (1, 'a=>1')");
        exec("UPDATE hs_upd2 SET data = NULL WHERE id = 1");
        assertNull(str("SELECT data FROM hs_upd2 WHERE id = 1"));
    }

    @Test
    void delete_with_hstore_condition() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_del");
        exec("CREATE TABLE hs_del (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_del VALUES (1, 'keep=>yes'), (2, 'keep=>no')");
        exec("DELETE FROM hs_del WHERE data->'keep' = 'no'");
        assertEquals(1, count("SELECT count(*) FROM hs_del"));
        assertEquals("yes", str("SELECT data->'keep' FROM hs_del"));
    }

    // =========================================================================
    // C. Operators on hstore columns
    // =========================================================================

    @Test
    void arrow_operator_on_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_op");
        exec("CREATE TABLE hs_op (id int PRIMARY KEY, meta hstore)");
        exec("INSERT INTO hs_op VALUES (1, 'name=>Alice, role=>admin')");
        assertEquals("Alice", str("SELECT meta->'name' FROM hs_op WHERE id = 1"));
    }

    @Test
    void contains_operator_on_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_cont");
        exec("CREATE TABLE hs_cont (id int PRIMARY KEY, tags hstore)");
        exec("INSERT INTO hs_cont VALUES (1, 'a=>1, b=>2'), (2, 'c=>3')");
        assertTrue(bool("SELECT tags @> 'a=>1' FROM hs_cont WHERE id = 1"));
        assertFalse(bool("SELECT tags @> 'a=>1' FROM hs_cont WHERE id = 2"));
    }

    @Test
    void contained_by_operator_on_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_contby");
        exec("CREATE TABLE hs_contby (id int PRIMARY KEY, tags hstore)");
        exec("INSERT INTO hs_contby VALUES (1, 'a=>1')");
        assertTrue(bool("SELECT tags <@ 'a=>1, b=>2' FROM hs_contby WHERE id = 1"));
    }

    @Test
    void hstore_concat_operator_on_columns() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_concat");
        exec("CREATE TABLE hs_concat (id int PRIMARY KEY, base hstore, extra hstore)");
        exec("INSERT INTO hs_concat VALUES (1, 'a=>1', 'b=>2')");
        String merged = str("SELECT (base || extra)->'b' FROM hs_concat WHERE id = 1");
        assertEquals("2", merged);
    }

    @Test
    void hstore_key_exists_via_exist_function() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_exists");
        exec("CREATE TABLE hs_exists (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_exists VALUES (1, 'x=>1, y=>2')");
        assertTrue(bool("SELECT exist(data, 'x') FROM hs_exists WHERE id = 1"));
        assertFalse(bool("SELECT exist(data, 'z') FROM hs_exists WHERE id = 1"));
    }

    @Test
    void hstore_delete_key_operator() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_delkey");
        exec("CREATE TABLE hs_delkey (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_delkey VALUES (1, 'a=>1, b=>2, c=>3')");
        // Must use ::text cast — untyped 'b' resolves as hstore in PG (same-type preference), which fails to parse
        assertFalse(bool("SELECT exist(data - 'b'::text, 'b') FROM hs_delkey WHERE id = 1"));
        assertTrue(bool("SELECT exist(data - 'b'::text, 'a') FROM hs_delkey WHERE id = 1"));
    }

    // =========================================================================
    // D. Casting
    // =========================================================================

    @Test
    void cast_text_to_hstore_in_insert() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_cast_ins");
        exec("CREATE TABLE hs_cast_ins (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_cast_ins VALUES (1, 'key=>val'::hstore)");
        assertEquals("val", str("SELECT data->'key' FROM hs_cast_ins WHERE id = 1"));
    }

    @Test
    void cast_hstore_column_to_text() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_to_text");
        exec("CREATE TABLE hs_to_text (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_to_text VALUES (1, 'a=>1')");
        String text = str("SELECT data::text FROM hs_to_text WHERE id = 1");
        assertNotNull(text);
        assertTrue(text.contains("a") && text.contains("1"), "text rep should contain key and value");
    }

    @Test
    void cast_hstore_to_json() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_to_json");
        exec("CREATE TABLE hs_to_json (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_to_json VALUES (1, 'x=>10, y=>20')");
        String json = str("SELECT hstore_to_json(data)::text FROM hs_to_json WHERE id = 1");
        assertNotNull(json);
        assertTrue(json.contains("\"x\"") && json.contains("\"10\""), "json should contain hstore keys/values");
    }

    @Test
    void cast_hstore_to_jsonb() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_to_jsonb");
        exec("CREATE TABLE hs_to_jsonb (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_to_jsonb VALUES (1, 'a=>1')");
        String jsonb = str("SELECT hstore_to_jsonb(data)::text FROM hs_to_jsonb WHERE id = 1");
        assertNotNull(jsonb);
        assertTrue(jsonb.contains("\"a\""), "jsonb should contain hstore key");
    }

    @Test
    void cast_arrow_result_to_int() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_arrow_int");
        exec("CREATE TABLE hs_arrow_int (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_arrow_int VALUES (1, 'count=>42')");
        int val = count("SELECT (data->'count')::int FROM hs_arrow_int WHERE id = 1");
        assertEquals(42, val);
    }

    @Test
    void cast_arrow_result_to_numeric() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_arrow_num");
        exec("CREATE TABLE hs_arrow_num (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_arrow_num VALUES (1, 'price=>19.99')");
        String val = str("SELECT (data->'price')::numeric FROM hs_arrow_num WHERE id = 1");
        assertEquals("19.99", val);
    }

    @Test
    void cast_arrow_result_to_boolean() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_arrow_bool");
        exec("CREATE TABLE hs_arrow_bool (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_arrow_bool VALUES (1, 'active=>true')");
        assertTrue(bool("SELECT (data->'active')::boolean FROM hs_arrow_bool WHERE id = 1"));
    }

    // =========================================================================
    // E. hstore functions
    // =========================================================================

    @Test
    void akeys_on_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_akeys");
        exec("CREATE TABLE hs_akeys (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_akeys VALUES (1, 'b=>2, a=>1')");
        String keys = str("SELECT array_to_string(akeys(data), ',') FROM hs_akeys WHERE id = 1");
        assertNotNull(keys);
        assertTrue(keys.contains("a") && keys.contains("b"));
    }

    @Test
    void avals_on_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_avals");
        exec("CREATE TABLE hs_avals (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_avals VALUES (1, 'x=>10, y=>20')");
        String vals = str("SELECT array_to_string(avals(data), ',') FROM hs_avals WHERE id = 1");
        assertNotNull(vals);
        assertTrue(vals.contains("10") && vals.contains("20"));
    }

    @Test
    void skeys_on_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_skeys");
        exec("CREATE TABLE hs_skeys (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_skeys VALUES (1, 'p=>1, q=>2')");
        int cnt = count("SELECT count(*) FROM hs_skeys, skeys(data)");
        assertEquals(2, cnt);
    }

    @Test
    void svals_on_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_svals");
        exec("CREATE TABLE hs_svals (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_svals VALUES (1, 'p=>1, q=>2')");
        int cnt = count("SELECT count(*) FROM hs_svals, svals(data)");
        assertEquals(2, cnt);
    }

    @Test
    void each_on_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_each");
        exec("CREATE TABLE hs_each (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_each VALUES (1, 'a=>1, b=>2')");
        int cnt = count("SELECT count(*) FROM hs_each, each(data)");
        assertEquals(2, cnt);
    }

    @Test
    void hstore_from_arrays() throws SQLException {
        String val = str("SELECT (hstore(ARRAY['a','b'], ARRAY['1','2']))->'b'");
        assertEquals("2", val);
    }

    @Test
    void hstore_from_key_value_pair() throws SQLException {
        String val = str("SELECT (hstore('name', 'alice'))->'name'");
        assertEquals("alice", val);
    }

    @Test
    void exist_function() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_exist");
        exec("CREATE TABLE hs_exist (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_exist VALUES (1, 'a=>1, b=>2')");
        assertTrue(bool("SELECT exist(data, 'a') FROM hs_exist WHERE id = 1"));
        assertFalse(bool("SELECT exist(data, 'z') FROM hs_exist WHERE id = 1"));
    }

    @Test
    void defined_function() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_defined");
        exec("CREATE TABLE hs_defined (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_defined VALUES (1, 'a=>1, b=>NULL')");
        assertTrue(bool("SELECT defined(data, 'a') FROM hs_defined WHERE id = 1"));
        assertFalse(bool("SELECT defined(data, 'b') FROM hs_defined WHERE id = 1"));
    }

    @Test
    void delete_function() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_delete_fn");
        exec("CREATE TABLE hs_delete_fn (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_delete_fn VALUES (1, 'a=>1, b=>2, c=>3')");
        assertFalse(bool("SELECT delete(data, 'b') ? 'b' FROM hs_delete_fn WHERE id = 1"));
    }

    @Test
    void slice_function() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_slice");
        exec("CREATE TABLE hs_slice (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_slice VALUES (1, 'a=>1, b=>2, c=>3')");
        String val = str("SELECT (slice(data, ARRAY['a','c']))->'a' FROM hs_slice WHERE id = 1");
        assertEquals("1", val);
    }

    // =========================================================================
    // F. WHERE clause filtering
    // =========================================================================

    @Test
    void where_arrow_equals() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_where");
        exec("CREATE TABLE hs_where (id int PRIMARY KEY, props hstore)");
        exec("INSERT INTO hs_where VALUES (1, 'status=>active'), (2, 'status=>inactive'), (3, 'status=>active')");
        assertEquals(2, count("SELECT count(*) FROM hs_where WHERE props->'status' = 'active'"));
    }

    @Test
    void where_contains() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_where2");
        exec("CREATE TABLE hs_where2 (id int PRIMARY KEY, tags hstore)");
        exec("INSERT INTO hs_where2 VALUES (1, 'env=>prod, region=>us'), (2, 'env=>staging, region=>eu')");
        assertEquals(1, count("SELECT count(*) FROM hs_where2 WHERE tags @> 'env=>prod'"));
    }

    // =========================================================================
    // G. hstore with other column types
    // =========================================================================

    @Test
    void hstore_alongside_json_and_text_columns() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_multi");
        exec("CREATE TABLE hs_multi (id int PRIMARY KEY, name text, meta hstore, extra jsonb)");
        exec("INSERT INTO hs_multi VALUES (1, 'test', 'k=>v', '{\"j\":1}')");
        assertEquals("v", str("SELECT meta->'k' FROM hs_multi WHERE id = 1"));
    }

    @Test
    void multiple_hstore_columns() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_multi2");
        exec("CREATE TABLE hs_multi2 (id int PRIMARY KEY, config hstore, overrides hstore)");
        exec("INSERT INTO hs_multi2 VALUES (1, 'a=>1', 'a=>2')");
        assertEquals("1", str("SELECT config->'a' FROM hs_multi2 WHERE id = 1"));
        assertEquals("2", str("SELECT overrides->'a' FROM hs_multi2 WHERE id = 1"));
    }

    // =========================================================================
    // H. Edge cases
    // =========================================================================

    @Test
    void hstore_with_special_characters_in_keys_and_values() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_special");
        exec("CREATE TABLE hs_special (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_special VALUES (1, '\"with space\"=>\"has => arrow\", \"quo\\\"te\"=>val')");
        assertEquals("has => arrow", str("SELECT data->'with space' FROM hs_special WHERE id = 1"));
    }

    @Test
    void hstore_large_number_of_keys() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_large");
        exec("CREATE TABLE hs_large (id int PRIMARY KEY, data hstore)");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            if (i > 0) sb.append(",");
            sb.append("k").append(i).append("=>v").append(i);
        }
        exec("INSERT INTO hs_large VALUES (1, '" + sb + "')");
        assertEquals("v50", str("SELECT data->'k50' FROM hs_large WHERE id = 1"));
    }

    @Test
    void hstore_in_subquery() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_sub");
        exec("CREATE TABLE hs_sub (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_sub VALUES (1, 'x=>10'), (2, 'x=>20')");
        assertEquals("20", str("SELECT data->'x' FROM hs_sub WHERE id = (SELECT id FROM hs_sub WHERE data->'x' = '20')"));
    }

    // =========================================================================
    // I. Equality and comparison semantics
    // =========================================================================

    @Test
    void hstore_equality_is_key_order_independent() throws SQLException {
        assertTrue(bool("SELECT 'a=>1, b=>2'::hstore = 'b=>2, a=>1'::hstore"));
    }

    @Test
    void hstore_inequality() throws SQLException {
        assertTrue(bool("SELECT 'a=>1'::hstore <> 'a=>2'::hstore"));
    }

    @Test
    void hstore_equality_ignores_whitespace() throws SQLException {
        assertTrue(bool("SELECT 'a => 1 , b => 2'::hstore = 'a=>1,b=>2'::hstore"));
    }

    // =========================================================================
    // J. Duplicate keys in input
    // =========================================================================

    @Test
    void duplicate_keys_first_value_wins() throws SQLException {
        // PG keeps the first occurrence, not the last
        assertEquals("1", str("SELECT ('a=>1, a=>2'::hstore)->'a'"));
    }

    @Test
    void duplicate_keys_deduplicated_count() throws SQLException {
        assertEquals(1, count("SELECT array_length(akeys('a=>1, a=>2'::hstore), 1)"));
    }

    // =========================================================================
    // K. hstore text output format
    // =========================================================================

    @Test
    void hstore_text_output_single_key() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_txtout");
        exec("CREATE TABLE hs_txtout (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_txtout VALUES (1, 'a=>1')");
        String txt = str("SELECT data::text FROM hs_txtout WHERE id = 1");
        // PG outputs: "a"=>"1"
        assertEquals("\"a\"=>\"1\"", txt);
    }

    // =========================================================================
    // L. Concat operator with untyped literal
    // =========================================================================

    @Test
    void concat_with_untyped_literal() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_cat");
        exec("CREATE TABLE hs_cat (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_cat VALUES (1, 'a=>1, b=>2')");
        // Without ::hstore cast — may trigger operator resolution issues like - operator
        assertEquals("99", str("SELECT (data || 'b=>99')->'b' FROM hs_cat WHERE id = 1"));
    }

    // =========================================================================
    // M. hstore_to_json/jsonb with NULL values
    // =========================================================================

    @Test
    void hstore_to_json_null_value_is_json_null() throws SQLException {
        assertNull(str("SELECT hstore_to_json('a=>1, b=>NULL'::hstore)->>'b'"));
    }

    @Test
    void hstore_to_jsonb_null_value_is_json_null() throws SQLException {
        assertNull(str("SELECT hstore_to_jsonb('a=>1, b=>NULL'::hstore)->>'b'"));
    }

    @Test
    void hstore_to_json_non_null_value_present() throws SQLException {
        assertEquals("1", str("SELECT hstore_to_json('a=>1, b=>NULL'::hstore)->>'a'"));
    }

    // =========================================================================
    // N. RETURNING clause with hstore
    // =========================================================================

    @Test
    void insert_returning_hstore_arrow() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_ret");
        exec("CREATE TABLE hs_ret (id int PRIMARY KEY, data hstore)");
        assertEquals("hello", str("INSERT INTO hs_ret VALUES (1, 'key=>hello') RETURNING data->'key'"));
    }

    @Test
    void update_returning_hstore_arrow() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_ret2");
        exec("CREATE TABLE hs_ret2 (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_ret2 VALUES (1, 'key=>hello')");
        assertEquals("world", str("UPDATE hs_ret2 SET data = 'key=>world' WHERE id = 1 RETURNING data->'key'"));
    }

    // =========================================================================
    // O. DISTINCT and GROUP BY on hstore
    // =========================================================================

    @Test
    void distinct_on_hstore_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_dist");
        exec("CREATE TABLE hs_dist (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_dist VALUES (1, 'a=>1'), (2, 'a=>1'), (3, 'b=>2')");
        assertEquals(2, count("SELECT count(*) FROM (SELECT DISTINCT data FROM hs_dist) sub"));
    }

    @Test
    void group_by_on_hstore_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_grp");
        exec("CREATE TABLE hs_grp (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_grp VALUES (1, 'a=>1'), (2, 'a=>1'), (3, 'b=>2')");
        assertEquals(2, count("SELECT count(*) FROM (SELECT data, count(*) FROM hs_grp GROUP BY data) sub"));
    }

    // =========================================================================
    // P. COALESCE and CASE with hstore
    // =========================================================================

    @Test
    void coalesce_hstore_null_fallback() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_coal");
        exec("CREATE TABLE hs_coal (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_coal VALUES (1, NULL)");
        assertEquals("fallback", str("SELECT COALESCE(data, 'default=>fallback'::hstore)->'default' FROM hs_coal WHERE id = 1"));
    }

    @Test
    void coalesce_hstore_non_null_passthrough() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_coal2");
        exec("CREATE TABLE hs_coal2 (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_coal2 VALUES (1, 'x=>1')");
        assertEquals("1", str("SELECT COALESCE(data, 'default=>fallback'::hstore)->'x' FROM hs_coal2 WHERE id = 1"));
    }

    @Test
    void case_expression_returning_hstore() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_case");
        exec("CREATE TABLE hs_case (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_case VALUES (1, 'a=>1')");
        assertEquals("yes", str("SELECT (CASE WHEN id = 1 THEN 'found=>yes'::hstore ELSE 'found=>no'::hstore END)->'found' FROM hs_case WHERE id = 1"));
    }

    // =========================================================================
    // Q. hstore constructor edge cases
    // =========================================================================

    @Test
    void hstore_constructor_with_null_value() throws SQLException {
        assertNull(str("SELECT (hstore('key', NULL))->'key'"));
    }

    @Test
    void hstore_constructor_null_value_exists_but_not_defined() throws SQLException {
        assertTrue(bool("SELECT exist(hstore('key', NULL), 'key')"));
        assertFalse(bool("SELECT defined(hstore('key', NULL), 'key')"));
    }

    // =========================================================================
    // R. delete function with array argument
    // =========================================================================

    @Test
    void delete_function_with_array() throws SQLException {
        assertFalse(bool("SELECT exist(delete('a=>1, b=>2, c=>3'::hstore, ARRAY['a','b']), 'a')"));
        assertFalse(bool("SELECT exist(delete('a=>1, b=>2, c=>3'::hstore, ARRAY['a','b']), 'b')"));
        assertTrue(bool("SELECT exist(delete('a=>1, b=>2, c=>3'::hstore, ARRAY['a','b']), 'c')"));
    }

    // =========================================================================
    // S. slice with non-existent keys
    // =========================================================================

    @Test
    void slice_all_missing_keys() throws SQLException {
        // slice with non-existent keys returns empty hstore; array_length of empty array is NULL in PG
        assertNull(str("SELECT array_length(akeys(slice('a=>1, b=>2'::hstore, ARRAY['x','y'])), 1)"));
    }

    @Test
    void slice_mix_existing_and_missing_keys() throws SQLException {
        assertEquals("1", str("SELECT (slice('a=>1, b=>2'::hstore, ARRAY['a','x']))->'a'"));
        assertNull(str("SELECT (slice('a=>1, b=>2'::hstore, ARRAY['a','x']))->'x'"));
    }

    // =========================================================================
    // T. pg_typeof on hstore
    // =========================================================================

    @Test
    void pg_typeof_hstore_column() throws SQLException {
        exec("DROP TABLE IF EXISTS hs_typeof");
        exec("CREATE TABLE hs_typeof (id int PRIMARY KEY, data hstore)");
        exec("INSERT INTO hs_typeof VALUES (1, 'a=>1')");
        assertEquals("hstore", str("SELECT pg_typeof(data)::text FROM hs_typeof WHERE id = 1"));
    }

    // =========================================================================
    // U. hstore - hstore (delete matching pairs)
    // =========================================================================

    @Test
    void subtract_hstore_matching_pair_removed() throws SQLException {
        assertFalse(bool("SELECT exist('a=>1, b=>2'::hstore - 'a=>1'::hstore, 'a')"));
        assertTrue(bool("SELECT exist('a=>1, b=>2'::hstore - 'a=>1'::hstore, 'b')"));
    }

    @Test
    void subtract_hstore_non_matching_value_kept() throws SQLException {
        // Key 'a' has value '1', subtracting 'a=>999' should NOT remove it (value doesn't match)
        assertTrue(bool("SELECT exist('a=>1, b=>2'::hstore - 'a=>999'::hstore, 'a')"));
    }

    // =========================================================================
    // V. Empty string keys and values
    // =========================================================================

    @Test
    void empty_string_as_value_unquoted_is_invalid() throws SQLException {
        // PG rejects 'a=>' as invalid hstore syntax — "syntax error in hstore: unexpected end of string"
        assertThrows(SQLException.class, () -> str("SELECT ('a=>'::hstore)->'a'"));
    }

    @Test
    void empty_string_as_value_quoted_is_valid() throws SQLException {
        // Valid PG syntax for empty string value: 'a=>""'
        assertEquals("", str("SELECT ('a=>\"\"'::hstore)->'a'"));
    }

    @Test
    void empty_string_as_key() throws SQLException {
        assertEquals("1", str("SELECT ('\"\"=>1'::hstore)->''"));
    }

    // =========================================================================
    // W. Concat merge order — right side wins
    // =========================================================================

    @Test
    void concat_right_side_wins_on_duplicate_key() throws SQLException {
        assertEquals("new", str("SELECT ('a=>old'::hstore || 'a=>new'::hstore)->'a'"));
    }

    @Test
    void concat_preserves_both_sides_unique_keys() throws SQLException {
        assertEquals("1", str("SELECT ('a=>1'::hstore || 'b=>2'::hstore)->'a'"));
        assertEquals("2", str("SELECT ('a=>1'::hstore || 'b=>2'::hstore)->'b'"));
    }

    // =========================================================================
    // X. Multi-key arrow extraction (hstore -> text[])
    // =========================================================================

    @Test
    void arrow_text_array_extraction() throws SQLException {
        String result = str("SELECT ('a=>1, b=>2, c=>3'::hstore -> ARRAY['a','c'])::text");
        assertNotNull(result);
        assertTrue(result.contains("1") && result.contains("3"));
    }

    @Test
    void arrow_text_array_missing_key_returns_null_element() throws SQLException {
        // Missing key 'z' should produce NULL in the array
        assertTrue(bool("SELECT ('a=>1'::hstore -> ARRAY['a','z'])[2] IS NULL"));
    }

    // =========================================================================
    // Y. hstore_to_array and hstore_to_matrix
    // =========================================================================

    @Test
    void hstore_to_array_returns_flat_array() throws SQLException {
        String result = str("SELECT hstore_to_array('a=>1, b=>2'::hstore)::text");
        assertNotNull(result);
        // Flat array: alternating keys and values
        assertTrue(result.contains("a") && result.contains("1") && result.contains("b") && result.contains("2"));
    }

    @Test
    void hstore_to_matrix_returns_2d_array() throws SQLException {
        String result = str("SELECT hstore_to_matrix('a=>1'::hstore)::text");
        assertNotNull(result);
        assertTrue(result.contains("a") && result.contains("1"));
    }

    // =========================================================================
    // Z. delete(hstore, hstore) function
    // =========================================================================

    @Test
    void delete_function_with_hstore_arg() throws SQLException {
        // delete(h, hstore) removes matching key+value pairs
        assertFalse(bool("SELECT exist(delete('a=>1, b=>2'::hstore, 'a=>1'::hstore), 'a')"));
        assertTrue(bool("SELECT exist(delete('a=>1, b=>2'::hstore, 'a=>1'::hstore), 'b')"));
    }

    @Test
    void delete_function_with_hstore_non_matching_value() throws SQLException {
        // Non-matching value — key NOT removed
        assertTrue(bool("SELECT exist(delete('a=>1, b=>2'::hstore, 'a=>999'::hstore), 'a')"));
    }

    // =========================================================================
    // AA. isexists / isdefined aliases
    // =========================================================================

    @Test
    void isexists_alias() throws SQLException {
        assertTrue(bool("SELECT isexists('a=>1, b=>2'::hstore, 'a')"));
        assertFalse(bool("SELECT isexists('a=>1, b=>2'::hstore, 'z')"));
    }

    @Test
    void isdefined_alias() throws SQLException {
        assertTrue(bool("SELECT isdefined('a=>1, b=>NULL'::hstore, 'a')"));
        assertFalse(bool("SELECT isdefined('a=>1, b=>NULL'::hstore, 'b')"));
    }

    // =========================================================================
    // AB. hstore_to_json_loose type inference
    // =========================================================================

    @Test
    void hstore_to_json_loose_unquotes_numbers() throws SQLException {
        // Use single-key to avoid ordering issues
        String json = str("SELECT hstore_to_json_loose('count=>42'::hstore)::text");
        assertNotNull(json);
        // PG unquotes numeric values in loose mode
        assertTrue(json.contains(": 42") || json.contains(":42"), "number should be unquoted");
    }

    @Test
    void hstore_to_json_loose_keeps_booleans_quoted() throws SQLException {
        // PG does NOT unquote booleans in hstore_to_json_loose — only numbers
        String json = str("SELECT hstore_to_json_loose('active=>true'::hstore)::text");
        assertNotNull(json);
        assertTrue(json.contains("\"true\""), "boolean should stay quoted in PG-compatible loose mode");
    }

    @Test
    void hstore_to_jsonb_loose_unquotes_numbers() throws SQLException {
        String json = str("SELECT hstore_to_jsonb_loose('x=>3.14'::hstore)::text");
        assertNotNull(json);
        assertTrue(json.contains(": 3.14") || json.contains(":3.14"), "number should be unquoted");
    }

    @Test
    void hstore_to_jsonb_loose_keeps_booleans_quoted() throws SQLException {
        // PG does NOT unquote booleans in hstore_to_jsonb_loose — only numbers
        String json = str("SELECT hstore_to_jsonb_loose('active=>true'::hstore)::text");
        assertNotNull(json);
        assertTrue(json.contains("\"true\""), "boolean should stay quoted in PG-compatible loose mode");
    }

    // =========================================================================
    // AC. hstore(text[]) single-array constructors
    // =========================================================================

    @Test
    void hstore_from_flat_array() throws SQLException {
        // hstore(ARRAY['a','1','b','2']) — alternating key/value flat array
        String val = str("SELECT (hstore(ARRAY['a','1','b','2']))->'b'");
        assertEquals("2", val);
    }

    @Test
    void hstore_from_2d_array() throws SQLException {
        // hstore(ARRAY[['c','3'],['d','4']]) — 2D key/value array
        String val = str("SELECT (hstore(ARRAY[['c','3'],['d','4']]))->'d'");
        assertEquals("4", val);
    }

    // =========================================================================
    // AD. Implicit cast hstore → json / jsonb
    // =========================================================================

    @Test
    void implicit_cast_hstore_to_json() throws SQLException {
        String val = str("SELECT ('a=>1'::hstore::json)->>'a'");
        assertEquals("1", val);
    }

    @Test
    void implicit_cast_hstore_to_jsonb() throws SQLException {
        String val = str("SELECT ('a=>1'::hstore::jsonb)->>'a'");
        assertEquals("1", val);
    }

    // =========================================================================
    // AE. Subscript access h['key']
    // =========================================================================

    @Test
    void subscript_fetch_existing_key() throws SQLException {
        exec("CREATE TABLE hs_sub_test (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_sub_test VALUES (1, 'x=>10, y=>20')");
            String val = str("SELECT data['x'] FROM hs_sub_test WHERE id = 1");
            assertEquals("10", val);
        } finally {
            exec("DROP TABLE IF EXISTS hs_sub_test");
        }
    }

    @Test
    void subscript_fetch_missing_key_returns_null() throws SQLException {
        exec("CREATE TABLE hs_sub_test2 (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_sub_test2 VALUES (1, 'x=>10')");
            assertTrue(bool("SELECT data['z'] IS NULL FROM hs_sub_test2 WHERE id = 1"));
        } finally {
            exec("DROP TABLE IF EXISTS hs_sub_test2");
        }
    }

    @Test
    void subscript_update_existing_key() throws SQLException {
        exec("CREATE TABLE hs_sub_test3 (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_sub_test3 VALUES (1, 'x=>10, y=>20')");
            exec("UPDATE hs_sub_test3 SET data['x'] = '99' WHERE id = 1");
            String val = str("SELECT data['x'] FROM hs_sub_test3 WHERE id = 1");
            assertEquals("99", val);
        } finally {
            exec("DROP TABLE IF EXISTS hs_sub_test3");
        }
    }

    @Test
    void subscript_update_new_key() throws SQLException {
        exec("CREATE TABLE hs_sub_test4 (id int PRIMARY KEY, data hstore)");
        try {
            exec("INSERT INTO hs_sub_test4 VALUES (1, 'x=>10')");
            exec("UPDATE hs_sub_test4 SET data['z'] = 'new' WHERE id = 1");
            String val = str("SELECT data['z'] FROM hs_sub_test4 WHERE id = 1");
            assertEquals("new", val);
        } finally {
            exec("DROP TABLE IF EXISTS hs_sub_test4");
        }
    }

    // =========================================================================
    // AF. hstore - text with explicit ::text cast
    // =========================================================================

    @Test
    void delete_key_with_explicit_text_cast() throws SQLException {
        // PG requires ::text cast for key deletion to disambiguate from hstore-hstore subtraction
        assertFalse(bool("SELECT exist('a=>1, b=>2'::hstore - 'b'::text, 'b')"));
        assertTrue(bool("SELECT exist('a=>1, b=>2'::hstore - 'b'::text, 'a')"));
    }
}
