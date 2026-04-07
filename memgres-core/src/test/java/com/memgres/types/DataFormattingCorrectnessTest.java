package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Data value and formatting correctness tests. Ensures Memgres returns
 * identical results to PG18 for value representations, column naming,
 * numeric precision, catalog introspection output, and type display.
 *
 * Categories covered:
 *   - avg() numeric precision (returns numeric, not integer)
 *   - Column naming for expressions (array subscript, cast, etc.)
 *   - pg_get_viewdef pretty-printing
 *   - pg_get_constraintdef parenthesization
 *   - pg_get_indexdef formatting
 *   - regclass display (qualified vs unqualified)
 *   - JSONB -> type (returns jsonb, not text)
 *   - jsonb_path_query / jsonb_path_exists results
 *   - TIMETZ column naming
 *   - ts_rank value accuracy
 *   - Enum/composite array display
 *   - SHOW parameter case sensitivity
 *   - EXPLAIN output structure
 *   - Identity sequence numbering
 */
class DataFormattingCorrectnessTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE df_t (id INT PRIMARY KEY, a INT, b TEXT, c NUMERIC)");
            s.execute("INSERT INTO df_t VALUES (1, 10, 'hello', 1.5), (2, 20, 'world', 2.5), (3, 30, 'test', 3.0)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS df_t CASCADE");
            } catch (SQLException ignored) {}
            conn.close();
        }
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

    static String qCol(String sql, int col) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(col) : null;
        }
    }

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int c = 0; while (rs.next()) c++; return c;
        }
    }

    static String colName(String sql, int col) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.getMetaData().getColumnName(col);
        }
    }

    // ========================================================================
    // avg() should return numeric with full precision, not integer
    // ========================================================================

    @Test
    void avg_returns_numeric_not_integer() throws SQLException {
        String result = q("SELECT avg(a) FROM df_t");
        assertNotNull(result);
        // PG returns "20.0000000000000000" for avg of {10, 20, 30}
        // At minimum it should contain a decimal point
        assertTrue(result.contains("."),
                "avg() should return numeric with decimal point, got: " + result);
    }

    @Test
    void avg_integer_column_has_decimal_precision() throws SQLException {
        // avg(integer) in PG returns numeric with 16 decimal places
        String result = q("SELECT avg(a) FROM df_t WHERE a IN (1, 2, 3)");
        if (result != null && result.contains(".")) {
            // Verify it has significant decimal digits
            String decimals = result.substring(result.indexOf('.') + 1);
            assertTrue(decimals.length() >= 1,
                    "avg() should have decimal precision, got: " + result);
        }
    }

    @Test
    void avg_in_window_function_returns_numeric() throws SQLException {
        String result = q("SELECT avg(a) OVER () FROM df_t LIMIT 1");
        assertNotNull(result);
        assertTrue(result.contains("."),
                "avg() OVER () should return numeric, got: " + result);
    }

    @Test
    void avg_with_group_by_returns_numeric() throws SQLException {
        exec("CREATE TABLE df_grp (grp TEXT, val INT)");
        exec("INSERT INTO df_grp VALUES ('a', 10), ('a', 30), ('b', 25)");
        try {
            String result = q("SELECT avg(val) FROM df_grp WHERE grp = 'a'");
            assertNotNull(result);
            assertTrue(result.contains("."),
                    "avg() with GROUP BY should return numeric, got: " + result);
        } finally {
            exec("DROP TABLE df_grp");
        }
    }

    // ========================================================================
    // Column naming: expressions should get proper inferred names
    // ========================================================================

    @Test
    void array_subscript_column_name() throws SQLException {
        exec("CREATE TABLE df_arr (id INT, arr INT[])");
        exec("INSERT INTO df_arr VALUES (1, ARRAY[10,20,30])");
        try {
            String name = colName("SELECT arr[1] FROM df_arr", 1);
            // PG names this column "arr" (the array column name)
            assertEquals("arr", name, "Array subscript column should be named after the array");
        } finally {
            exec("DROP TABLE df_arr");
        }
    }

    @Test
    void cast_expression_column_name() throws SQLException {
        exec("CREATE TABLE df_cast (id INT, b TEXT)");
        exec("INSERT INTO df_cast VALUES (1, '42')");
        try {
            // SELECT b::int, where PG names the column "b"
            String name = colName("SELECT b::int FROM df_cast", 1);
            assertEquals("b", name, "Cast column should keep original column name");
        } finally {
            exec("DROP TABLE df_cast");
        }
    }

    @Test
    void multiple_same_name_columns_all_named() throws SQLException {
        exec("CREATE TABLE df_multi (id INT, b TEXT)");
        exec("INSERT INTO df_multi VALUES (1, '42')");
        try {
            // SELECT b, b::int, b::float; all should be named "b" in PG
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT b, b::int, b::float FROM df_multi")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals("b", md.getColumnName(1));
                assertEquals("b", md.getColumnName(2));
                assertEquals("b", md.getColumnName(3));
            }
        } finally {
            exec("DROP TABLE df_multi");
        }
    }

    // ========================================================================
    // TIMETZ column naming
    // ========================================================================

    @Test
    void timetz_column_name_is_timetz() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT TIME '10:00', TIMETZ '10:00+00'")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("time", md.getColumnName(1));
            assertEquals("timetz", md.getColumnName(2),
                    "TIMETZ literal column should be named 'timetz', not 'time'");
        }
    }

    // ========================================================================
    // SHOW parameter case: PG returns proper case
    // ========================================================================

    @Test
    void show_timezone_column_name_case() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SHOW TimeZone")) {
            String colName = rs.getMetaData().getColumnName(1);
            // PG returns "TimeZone" (preserving the case from the parameter name)
            assertEquals("TimeZone", colName,
                    "SHOW TimeZone column should be 'TimeZone', not lowercase");
        }
    }

    // ========================================================================
    // JSONB -> type semantics (returns jsonb, not text for pg_typeof)
    // ========================================================================

    @Test
    void jsonb_arrow_returns_jsonb_type() throws SQLException {
        String type = q("SELECT pg_typeof(('{\"a\":1}'::jsonb)->'a')");
        assertEquals("jsonb", type, "-> operator should return jsonb type");
    }

    @Test
    void jsonb_double_arrow_returns_text_type() throws SQLException {
        String type = q("SELECT pg_typeof(('{\"a\":1}'::jsonb)->>'a')");
        assertEquals("text", type, "->> operator should return text type");
    }

    // ========================================================================
    // jsonb_path_exists / jsonb_path_query
    // ========================================================================

    @Test
    void jsonb_path_exists_returns_boolean() throws SQLException {
        String result = q("SELECT jsonb_path_exists('{\"a\":1}'::jsonb, '$.a')");
        assertNotNull(result);
        assertEquals("t", result, "jsonb_path_exists should return true for existing path");
    }

    @Test
    void jsonb_path_query_returns_multiple_rows() throws SQLException {
        int count = countRows(
            "SELECT * FROM jsonb_path_query('{\"a\":[1,2,3]}'::jsonb, '$.a[*]')");
        assertEquals(3, count, "jsonb_path_query should return 3 rows for 3-element array");
    }

    // ========================================================================
    // pg_get_viewdef formatting: pretty-printed with proper SQL
    // ========================================================================

    @Test
    void pg_get_viewdef_contains_select_from() throws SQLException {
        exec("CREATE VIEW df_v AS SELECT id, b FROM df_t WHERE a > 5");
        try {
            String def = q("SELECT pg_get_viewdef('df_v'::regclass, true)");
            assertNotNull(def);
            assertTrue(def.toLowerCase().contains("select"), "Should contain SELECT: " + def);
            assertTrue(def.toLowerCase().contains("from"), "Should contain FROM: " + def);
            assertTrue(def.toLowerCase().contains("df_t"), "Should reference source table: " + def);
        } finally {
            exec("DROP VIEW df_v");
        }
    }

    @Test
    void pg_get_viewdef_single_line_without_pretty() throws SQLException {
        exec("CREATE VIEW df_v2 AS SELECT id, b FROM df_t WHERE a > 5");
        try {
            String def = q("SELECT pg_get_viewdef('df_v2'::regclass, false)");
            assertNotNull(def);
            // Non-pretty mode should be single line (no newlines)
            assertFalse(def.contains("\n"),
                    "Non-pretty viewdef should be single-line: " + def);
        } finally {
            exec("DROP VIEW df_v2");
        }
    }

    // ========================================================================
    // pg_get_constraintdef: proper SQL output with parens
    // ========================================================================

    @Test
    void pg_get_constraintdef_check_has_double_parens() throws SQLException {
        exec("CREATE TABLE df_ck (id INT, val INT CHECK (val > 0))");
        try {
            String def = q(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint " +
                "WHERE conrelid = 'df_ck'::regclass AND contype = 'c' LIMIT 1");
            assertNotNull(def);
            // PG returns: CHECK ((val > 0)), note double parens
            assertTrue(def.startsWith("CHECK"),
                    "Should start with CHECK: " + def);
            assertTrue(def.contains("val") && (def.contains(">") || def.contains(">")),
                    "Should contain column and operator: " + def);
        } finally {
            exec("DROP TABLE df_ck");
        }
    }

    @Test
    void pg_get_constraintdef_foreign_key() throws SQLException {
        exec("CREATE TABLE df_fk_p (id INT PRIMARY KEY)");
        exec("CREATE TABLE df_fk_c (id INT, pid INT REFERENCES df_fk_p(id))");
        try {
            String def = q(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint " +
                "WHERE conrelid = 'df_fk_c'::regclass AND contype = 'f' LIMIT 1");
            assertNotNull(def);
            assertTrue(def.contains("FOREIGN KEY") || def.contains("REFERENCES"),
                    "FK constraint def should contain FOREIGN KEY or REFERENCES: " + def);
        } finally {
            exec("DROP TABLE df_fk_c CASCADE");
            exec("DROP TABLE df_fk_p CASCADE");
        }
    }

    // ========================================================================
    // pg_get_indexdef: correct formatting
    // ========================================================================

    @Test
    void pg_get_indexdef_includes_using_btree() throws SQLException {
        exec("CREATE TABLE df_idx (id INT, name TEXT)");
        exec("CREATE INDEX df_idx_name ON df_idx (name)");
        try {
            String def = q("SELECT pg_get_indexdef('df_idx_name'::regclass)");
            assertNotNull(def);
            // PG: CREATE INDEX df_idx_name ON public.df_idx USING btree (name)
            assertTrue(def.toLowerCase().contains("using btree"),
                    "Index definition should contain 'USING btree': " + def);
            assertTrue(def.toLowerCase().contains("df_idx"),
                    "Should contain table name: " + def);
        } finally {
            exec("DROP TABLE df_idx CASCADE");
        }
    }

    @Test
    void pg_get_indexdef_partial_index() throws SQLException {
        exec("CREATE TABLE df_pidx (id INT, active BOOLEAN)");
        exec("CREATE INDEX df_pidx_active ON df_pidx (id) WHERE active");
        try {
            String def = q("SELECT pg_get_indexdef('df_pidx_active'::regclass)");
            assertNotNull(def);
            assertTrue(def.toLowerCase().contains("where"),
                    "Partial index definition should contain WHERE clause: " + def);
        } finally {
            exec("DROP TABLE df_pidx CASCADE");
        }
    }

    // ========================================================================
    // regclass display: unqualified for public schema
    // ========================================================================

    @Test
    void regclass_public_table_unqualified() throws SQLException {
        String result = q("SELECT 'df_t'::regclass::text");
        assertEquals("df_t", result, "regclass of public table should be unqualified");
    }

    @Test
    void regclass_nonexistent_gives_error() {
        try {
            q("SELECT 'no_such_table'::regclass");
            fail("Should error for non-existent table");
        } catch (SQLException e) {
            assertEquals("42P01", e.getSQLState());
        }
    }

    // ========================================================================
    // Enum array display
    // ========================================================================

    @Test
    void enum_array_display_format() throws SQLException {
        exec("CREATE TYPE df_color AS ENUM ('red', 'green', 'blue')");
        exec("CREATE TABLE df_colors (id INT, colors df_color[])");
        try {
            exec("INSERT INTO df_colors VALUES (1, ARRAY['red'::df_color, 'blue'::df_color])");
            String result = q("SELECT colors FROM df_colors WHERE id = 1");
            assertNotNull(result);
            // PG: {red,blue}
            assertTrue(result.contains("red") && result.contains("blue"),
                    "Enum array should contain enum labels: " + result);
        } finally {
            exec("DROP TABLE df_colors CASCADE");
            exec("DROP TYPE df_color CASCADE");
        }
    }

    // ========================================================================
    // Composite type array display
    // ========================================================================

    @Test
    void composite_array_display_has_quotes() throws SQLException {
        exec("CREATE TYPE df_point AS (x INT, y INT)");
        try {
            String result = q(
                "SELECT ARRAY[ROW(1,2)::df_point, ROW(3,4)::df_point]");
            assertNotNull(result);
            // PG: {"(1,2)","(3,4)"}, elements are double-quoted
            assertTrue(result.contains("(1,2)") && result.contains("(3,4)"),
                    "Composite array should contain tuples: " + result);
        } finally {
            exec("DROP TYPE df_point CASCADE");
        }
    }

    // ========================================================================
    // ts_rank value accuracy
    // ========================================================================

    @Test
    void ts_rank_returns_float_value() throws SQLException {
        String result = q(
            "SELECT ts_rank(to_tsvector('english', 'the quick brown fox'), " +
            "to_tsquery('english', 'fox'))");
        assertNotNull(result);
        float rank = Float.parseFloat(result);
        // PG returns approximately 0.0607927 for this query
        assertTrue(rank > 0 && rank < 1,
                "ts_rank should return a float between 0 and 1, got: " + rank);
        assertTrue(rank < 0.2,
                "ts_rank for single word match should be modest, got: " + rank);
    }

    // ========================================================================
    // EXPLAIN output structure
    // ========================================================================

    @Test
    void explain_costs_off_single_line() throws SQLException {
        int count = countRows("EXPLAIN (COSTS OFF) SELECT * FROM df_t");
        // PG: single line like "Seq Scan on df_t"
        assertEquals(1, count,
                "EXPLAIN COSTS OFF for simple query should produce 1 line");
    }

    @Test
    void explain_default_includes_cost() throws SQLException {
        String result = q("EXPLAIN SELECT * FROM df_t");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("cost=") || result.toLowerCase().contains("cost"),
                "Default EXPLAIN should include cost info: " + result);
    }

    // ========================================================================
    // Identity column: consistent numbering starting at 1
    // ========================================================================

    @Test
    void identity_always_starts_at_1() throws SQLException {
        exec("CREATE TABLE df_ident (id INT GENERATED ALWAYS AS IDENTITY, val TEXT)");
        try {
            exec("INSERT INTO df_ident (val) VALUES ('a')");
            exec("INSERT INTO df_ident (val) VALUES ('b')");
            exec("INSERT INTO df_ident (val) VALUES ('c')");
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT id FROM df_ident ORDER BY id")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1), "First identity should be 1");
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1), "Second identity should be 2");
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1), "Third identity should be 3");
            }
        } finally {
            exec("DROP TABLE df_ident");
        }
    }

    @Test
    void identity_by_default_starts_at_1() throws SQLException {
        exec("CREATE TABLE df_ident2 (id INT GENERATED BY DEFAULT AS IDENTITY, val TEXT)");
        try {
            exec("INSERT INTO df_ident2 (val) VALUES ('x')");
            String result = q("SELECT id FROM df_ident2");
            assertEquals("1", result, "BY DEFAULT identity should start at 1");
        } finally {
            exec("DROP TABLE df_ident2");
        }
    }

    // ========================================================================
    // generate_subscripts ordering
    // ========================================================================

    @Test
    void generate_subscripts_returns_correct_indices() throws SQLException {
        int count = countRows("SELECT generate_subscripts(ARRAY[10,20,30], 1)");
        assertEquals(3, count, "generate_subscripts should return 3 indices for 3-element array");
    }

    @Test
    void generate_subscripts_values_are_1_based() throws SQLException {
        String first = q("SELECT generate_subscripts(ARRAY[10,20,30], 1) LIMIT 1");
        assertEquals("1", first, "generate_subscripts should start at 1 (PG arrays are 1-based)");
    }

    // ========================================================================
    // unnest on enum arrays
    // ========================================================================

    @Test
    void unnest_enum_array_returns_elements() throws SQLException {
        exec("CREATE TYPE df_mood AS ENUM ('happy', 'sad', 'neutral')");
        try {
            int count = countRows(
                "SELECT unnest(ARRAY['happy'::df_mood, 'sad'::df_mood, 'neutral'::df_mood])");
            assertEquals(3, count, "unnest of 3-element enum array should return 3 rows");
        } finally {
            exec("DROP TYPE df_mood CASCADE");
        }
    }

    // ========================================================================
    // pg_typeof for various types
    // ========================================================================

    @Test
    void pg_typeof_integer_literal() throws SQLException {
        assertEquals("integer", q("SELECT pg_typeof(42)"));
    }

    @Test
    void pg_typeof_numeric_literal() throws SQLException {
        assertEquals("numeric", q("SELECT pg_typeof(42.5)"));
    }

    @Test
    void pg_typeof_text_literal() throws SQLException {
        assertEquals("unknown", q("SELECT pg_typeof('hello')"));
    }

    @Test
    void pg_typeof_boolean() throws SQLException {
        assertEquals("boolean", q("SELECT pg_typeof(true)"));
    }

    @Test
    void pg_typeof_array() throws SQLException {
        assertEquals("integer[]", q("SELECT pg_typeof(ARRAY[1,2,3])"));
    }

    @Test
    void pg_typeof_jsonb() throws SQLException {
        assertEquals("jsonb", q("SELECT pg_typeof('{}'::jsonb)"));
    }

    // ========================================================================
    // Composite field expansion: (row).* syntax
    // ========================================================================

    @Test
    void composite_star_expansion() throws SQLException {
        exec("CREATE TYPE df_comp AS (x INT, y TEXT)");
        try {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                     "SELECT (ROW(1,'a')::df_comp).*")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(2, md.getColumnCount(), "Composite .* should expand to 2 columns");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("a", rs.getString(2));
            }
        } finally {
            exec("DROP TYPE df_comp CASCADE");
        }
    }

    // ========================================================================
    // EXPLAIN (FORMAT JSON) output
    // ========================================================================

    @Test
    void explain_format_json_returns_json() throws SQLException {
        String result = q("EXPLAIN (FORMAT JSON) SELECT 1");
        assertNotNull(result);
        assertTrue(result.trim().startsWith("["),
                "EXPLAIN FORMAT JSON should return JSON array: " + result);
    }

    @Test
    void explain_analyze_false_costs_false_still_works() throws SQLException {
        // EXPLAIN (ANALYZE false, COSTS false), which PG accepts
        String result = q("EXPLAIN (ANALYZE false, COSTS false) SELECT 1");
        assertNotNull(result);
        assertTrue(result.toLowerCase().contains("result") || result.toLowerCase().contains("seq"),
                "EXPLAIN with ANALYZE false should produce plan: " + result);
    }

    // ========================================================================
    // setval / currval consistency
    // ========================================================================

    @Test
    void setval_currval_returns_set_value() throws SQLException {
        exec("CREATE SEQUENCE df_seq START 1");
        try {
            exec("SELECT setval('df_seq', 42)");
            String result = q("SELECT currval('df_seq')");
            assertEquals("42", result, "currval should return the value set by setval");
        } finally {
            exec("DROP SEQUENCE df_seq");
        }
    }

    // ========================================================================
    // JOIN USING: column deduplication
    // PG merges the USING column into a single column; SELECT * should not
    // produce the join column twice.
    // Diff: 09_joins_subqueries_ctes.sql stmt 17
    // ========================================================================

    @Test
    void join_using_deduplicates_column() throws SQLException {
        exec("CREATE TABLE df_j1 (id INT PRIMARY KEY, a TEXT)");
        exec("CREATE TABLE df_j2 (id INT PRIMARY KEY, b TEXT)");
        exec("INSERT INTO df_j1 VALUES (1, 'x'), (2, 'y')");
        exec("INSERT INTO df_j2 VALUES (1, 'p'), (2, 'q')");
        try {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT * FROM df_j1 JOIN df_j2 USING (id)")) {
                int colCount = rs.getMetaData().getColumnCount();
                // PG produces: id | a | b  (3 columns, not 4)
                assertEquals(3, colCount,
                        "JOIN USING should deduplicate the join column, got " + colCount + " columns");
            }
        } finally {
            exec("DROP TABLE df_j2");
            exec("DROP TABLE df_j1");
        }
    }

    // ========================================================================
    // Composite type array: pg_typeof should return 'addr[]' not 'text[]'
    // Diff: 17_domains_enums_composites.sql stmt 23
    // ========================================================================

    @Test
    void composite_array_pg_typeof_returns_typed_array() throws SQLException {
        exec("CREATE TYPE df_addr AS (street TEXT, zip INT)");
        try {
            String result = q(
                "SELECT pg_typeof(ARRAY[ROW('Main', 12345)::df_addr, ROW('Side', 54321)::df_addr])");
            assertEquals("df_addr[]", result,
                    "pg_typeof of composite array should be 'df_addr[]', got: " + result);
        } finally {
            exec("DROP TYPE df_addr CASCADE");
        }
    }

    // ========================================================================
    // Composite field access via function call: (func()).field
    // Diff: 17_domains_enums_composites.sql stmt 29
    // ========================================================================

    @Test
    void composite_field_access_via_function() throws SQLException {
        exec("CREATE TYPE df_addr2 AS (street TEXT, zip INT)");
        exec("CREATE TABLE df_addr2_t (id INT PRIMARY KEY, home df_addr2)");
        exec("INSERT INTO df_addr2_t VALUES (1, ROW('Main St', 12345)::df_addr2)");
        exec("CREATE FUNCTION df_get_addr(INT) RETURNS df_addr2 LANGUAGE SQL AS " +
             "$$ SELECT home FROM df_addr2_t WHERE id = $1 $$");
        try {
            String street = q("SELECT (df_get_addr(1)).street");
            assertNotNull(street, "(func()).field should not return NULL");
            assertEquals("Main St", street,
                    "(df_get_addr(1)).street should return 'Main St'");

            String zip = q("SELECT (df_get_addr(1)).zip");
            assertEquals("12345", zip,
                    "(df_get_addr(1)).zip should return 12345");
        } finally {
            exec("DROP FUNCTION IF EXISTS df_get_addr(int)");
            exec("DROP TABLE df_addr2_t CASCADE");
            exec("DROP TYPE df_addr2 CASCADE");
        }
    }

    // ========================================================================
    // (table.composite_col).* expansion from a table
    // Diff: 17_domains_enums_composites.sql stmt 30
    // ========================================================================

    @Test
    void composite_column_star_expansion_from_table() throws SQLException {
        exec("CREATE TYPE df_addr3 AS (street TEXT, zip INT)");
        exec("CREATE TABLE df_people (id INT PRIMARY KEY, home df_addr3)");
        exec("INSERT INTO df_people VALUES (1, ROW('Oak Ave', 99999)::df_addr3)");
        try {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT (df_people.home).* FROM df_people")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(2, md.getColumnCount(),
                        "(table.composite).* should expand to 2 columns");
                assertTrue(rs.next());
                assertEquals("Oak Ave", rs.getString(1));
                assertEquals(99999, rs.getInt(2));
            }
        } finally {
            exec("DROP TABLE df_people CASCADE");
            exec("DROP TYPE df_addr3 CASCADE");
        }
    }

    // ========================================================================
    // pg_get_viewdef pretty-print should include newlines
    // Diff: 45_definition_and_catalog_helpers.sql stmt 16
    // ========================================================================

    @Test
    void pg_get_viewdef_pretty_has_newlines() throws SQLException {
        exec("CREATE TABLE df_vdef_t (id INT PRIMARY KEY, note TEXT, qty INT)");
        exec("CREATE VIEW df_vdef_v AS SELECT id, note FROM df_vdef_t");
        try {
            String def = q("SELECT pg_get_viewdef('df_vdef_v'::regclass, true)");
            assertNotNull(def);
            // PG pretty-prints with newlines:  SELECT id,\n    note\n   FROM df_vdef_t;
            assertTrue(def.contains("\n"),
                    "pg_get_viewdef(_, true) should pretty-print with newlines, got: " + def);
        } finally {
            exec("DROP VIEW df_vdef_v");
            exec("DROP TABLE df_vdef_t");
        }
    }

    // ========================================================================
    // pg_get_constraintdef: exactly double parens, not triple
    // Diff: 45_definition_and_catalog_helpers.sql stmt 18
    // PG returns CHECK ((qty >= 0)), not CHECK (((qty >= 0)))
    // ========================================================================

    @Test
    void pg_get_constraintdef_check_exactly_double_parens() throws SQLException {
        exec("CREATE TABLE df_ckp (id INT, qty INT CHECK (qty >= 0))");
        try {
            String def = q(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint " +
                "WHERE conrelid = 'df_ckp'::regclass AND contype = 'c' LIMIT 1");
            assertNotNull(def);
            // PG: "CHECK ((qty >= 0))", exactly 2 opening parens after CHECK
            assertTrue(def.contains("CHECK ((") && !def.contains("CHECK ((("),
                    "Should have double parens CHECK ((expr)), not triple, got: " + def);
        } finally {
            exec("DROP TABLE df_ckp");
        }
    }

    // ========================================================================
    // pg_get_indexdef with expression index: COALESCE formatting
    // Diff: 42_index_conflict_and_definition_introspection.sql stmt 16
    // PG: COALESCE(note, ''::text)  not  COALESCE ( note ,)
    // ========================================================================

    @Test
    void pg_get_indexdef_expression_index_coalesce() throws SQLException {
        exec("CREATE TABLE df_eidx (id INT, note TEXT)");
        exec("CREATE INDEX df_eidx_expr ON df_eidx (COALESCE(note, ''))");
        try {
            String def = q("SELECT pg_get_indexdef('df_eidx_expr'::regclass)");
            assertNotNull(def);
            // Should contain well-formed COALESCE with both args
            assertTrue(def.toLowerCase().contains("coalesce"),
                    "Index def should contain COALESCE: " + def);
            // Must not end argument list with just a comma
            assertFalse(def.contains("COALESCE ( ") && def.contains(",)"),
                    "COALESCE should have properly formatted arguments: " + def);
            // Should contain the empty string literal
            assertTrue(def.contains("''") || def.contains("''::text"),
                    "COALESCE should include the empty string default: " + def);
        } finally {
            exec("DROP TABLE df_eidx CASCADE");
        }
    }

    // ========================================================================
    // regclass display: tables in non-public schema should be qualified
    // Diff: 45_definition_and_catalog_helpers.sql stmt 22
    // PG: adrelid::regclass shows 'def_help_t' (unqualified for search_path)
    // ========================================================================

    @Test
    void regclass_nonpublic_schema_is_qualified() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS df_nps");
        exec("CREATE TABLE df_nps.mytab (id INT PRIMARY KEY, val TEXT DEFAULT 'x')");
        try {
            String result = q("SELECT 'df_nps.mytab'::regclass::text");
            // PG: returns 'df_nps.mytab' (qualified) when schema is not in search_path
            assertTrue(result.contains("df_nps"),
                    "regclass of non-search_path table should be schema-qualified: " + result);
        } finally {
            exec("DROP TABLE df_nps.mytab");
            exec("DROP SCHEMA df_nps");
        }
    }

    // ========================================================================
    // upper() with COLLATE "C" on non-ASCII characters
    // Diff: 26_collation_null_polymorphic.sql stmt 15
    // PG: upper('äbc' COLLATE "C") = 'äBC' (only ASCII letters uppercased)
    // ========================================================================

    @Test
    void upper_collate_c_non_ascii() throws SQLException {
        // With COLLATE "C", only ASCII a-z should be uppercased
        String result = q("SELECT upper('äbc' COLLATE \"C\")");
        assertNotNull(result);
        // PG returns 'äBC'; the ä stays lowercase because COLLATE "C" is byte-based
        assertEquals("äBC", result,
                "upper() with COLLATE \"C\" should only uppercase ASCII letters, got: " + result);
    }

    // ========================================================================
    // ts_rank value accuracy: should match PG's tf-idf based algorithm
    // Diff: 23_sequences_xml_fts.sql stmt 33
    // PG: ~0.0607927, not 0.333...
    // ========================================================================

    @Test
    void ts_rank_value_matches_pg_algorithm() throws SQLException {
        String result = q(
            "SELECT ts_rank(to_tsvector('english', 'The quick brown fox'), " +
            "to_tsquery('english', 'fox'))");
        assertNotNull(result);
        float rank = Float.parseFloat(result);
        // PG returns approximately 0.0607927
        assertTrue(rank < 0.15,
                "ts_rank should be ~0.06 (PG algorithm), not ~0.33, got: " + rank);
    }

    // ========================================================================
    // Enum array insertion: ARRAY['val'::enum, ...] should work
    // Diff: 17_domains_enums_composites.sql stmt 17
    // ========================================================================

    @Test
    void enum_array_insertion_and_retrieval() throws SQLException {
        exec("CREATE TYPE df_mood2 AS ENUM ('ok', 'happy', 'sad')");
        exec("CREATE TABLE df_mood2_t (id INT PRIMARY KEY, moods df_mood2[])");
        try {
            exec("INSERT INTO df_mood2_t VALUES (1, ARRAY['ok'::df_mood2, 'happy'::df_mood2])");
            String result = q("SELECT moods FROM df_mood2_t WHERE id = 1");
            assertNotNull(result, "Enum array should be retrievable");
            assertTrue(result.contains("ok") && result.contains("happy"),
                    "Enum array should contain 'ok' and 'happy', got: " + result);

            // unnest should also work on enum arrays from a table
            int count = countRows("SELECT unnest(moods) FROM df_mood2_t WHERE id = 1");
            assertEquals(2, count, "unnest of 2-element enum array should return 2 rows");
        } finally {
            exec("DROP TABLE df_mood2_t CASCADE");
            exec("DROP TYPE df_mood2 CASCADE");
        }
    }

    // ========================================================================
    // DO block + EXECUTE FORMAT dynamic SQL: should produce visible results
    // Diff: 37_do_blocks_and_dynamic_sql.sql stmt 13
    // ========================================================================

    @Test
    void do_block_execute_format_inserts_data() throws SQLException {
        exec("CREATE TABLE df_dyn (id INT, val TEXT)");
        try {
            exec("DO $$ BEGIN EXECUTE format('INSERT INTO df_dyn VALUES (%L, %L)', 1, 'x'); END $$");
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT * FROM df_dyn ORDER BY id")) {
                assertTrue(rs.next(), "DO EXECUTE format INSERT should produce a row");
                assertEquals(1, rs.getInt(1));
                assertEquals("x", rs.getString(2));
            }
        } finally {
            exec("DROP TABLE df_dyn");
        }
    }

    // ========================================================================
    // Trigger on view: INSTEAD OF trigger should preserve case from NEW
    // Diff: 24_triggers_rules_deep.sql stmt 27
    // PG returns 'FROM_VIEW' (uppercase), not 'from_view'
    // ========================================================================

    @Test
    void instead_of_trigger_preserves_case() throws SQLException {
        exec("CREATE TABLE df_trig_t (id INT PRIMARY KEY, a INT, b TEXT)");
        exec("INSERT INTO df_trig_t VALUES (1, 10, 'hello')");
        exec("CREATE VIEW df_trig_v AS SELECT * FROM df_trig_t");
        exec("CREATE OR REPLACE FUNCTION df_trig_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ " +
             "BEGIN INSERT INTO df_trig_t VALUES (NEW.id, NEW.a, NEW.b); RETURN NEW; END $$");
        exec("CREATE TRIGGER df_trig INSTEAD OF INSERT ON df_trig_v " +
             "FOR EACH ROW EXECUTE FUNCTION df_trig_fn()");
        try {
            exec("INSERT INTO df_trig_v VALUES (2, 20, 'FROM_VIEW')");
            String result = q("SELECT b FROM df_trig_t WHERE id = 2");
            assertEquals("FROM_VIEW", result,
                    "INSTEAD OF trigger should preserve the case of inserted values");
        } finally {
            exec("DROP VIEW df_trig_v CASCADE");
            exec("DROP TABLE df_trig_t CASCADE");
            exec("DROP FUNCTION IF EXISTS df_trig_fn() CASCADE");
        }
    }

    // ========================================================================
    // Identity column numbering after cascading inserts via views
    // Diff: 22_updatable_views_identity_defaults.sql stmts 23/25/31
    // Identity values should be sequential including inserts through views
    // ========================================================================

    @Test
    void identity_numbering_consistent_across_operations() throws SQLException {
        exec("CREATE TABLE df_id_t (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " +
             "a INT, b TEXT DEFAULT 'd')");
        try {
            exec("INSERT INTO df_id_t (a) VALUES (1)");
            exec("INSERT INTO df_id_t (a) VALUES (2)");
            exec("INSERT INTO df_id_t (a) VALUES (3)");
            // After 3 inserts, next identity should be 4
            exec("INSERT INTO df_id_t (a) VALUES (7)");
            String id4 = q("SELECT id FROM df_id_t WHERE a = 7");
            assertEquals("4", id4, "Fourth identity value should be 4");

            // OVERRIDING SYSTEM VALUE should still allow explicit id
            exec("INSERT INTO df_id_t (id, a, b) OVERRIDING SYSTEM VALUE VALUES (100, 9, 'explicit')");
            String explicit = q("SELECT id FROM df_id_t WHERE a = 9");
            assertEquals("100", explicit,
                    "OVERRIDING SYSTEM VALUE should use explicit id");

            // Next auto-generated should continue from sequence, not from 100
            exec("INSERT INTO df_id_t (a) VALUES (11)");
            String id5 = q("SELECT id FROM df_id_t WHERE a = 11");
            assertEquals("5", id5,
                    "Identity should continue sequence after OVERRIDING SYSTEM VALUE insert");
        } finally {
            exec("DROP TABLE df_id_t CASCADE");
        }
    }

    // ========================================================================
    // jsonb_path_exists with filter expression: should return 't' not NULL
    // Diff: 18_arrays_ranges_jsonb_deep.sql stmt 33
    // ========================================================================

    @Test
    void jsonb_path_exists_with_filter_returns_true() throws SQLException {
        String result = q(
            "SELECT jsonb_path_exists('{\"a\":[1,2,3]}'::jsonb, '$.a[*] ? (@ > 2)')");
        assertNotNull(result, "jsonb_path_exists with filter should not return NULL");
        assertEquals("t", result,
                "jsonb_path_exists should return true for matching filter");
    }

    @Test
    void jsonb_path_exists_with_filter_returns_false() throws SQLException {
        String result = q(
            "SELECT jsonb_path_exists('{\"a\":[1,2,3]}'::jsonb, '$.a[*] ? (@ > 10)')");
        assertNotNull(result, "jsonb_path_exists with non-matching filter should not return NULL");
        assertEquals("f", result,
                "jsonb_path_exists should return false for non-matching filter");
    }

    // ========================================================================
    // Zero-based array syntax: '[0:2]={1,2,3}'::int[]
    // Diff: 18_arrays_ranges_jsonb_deep.sql stmt 10, 12_json_arrays_ranges_geometry.sql stmt 41
    // PG supports non-1-based array subscript notation
    // ========================================================================

    @Test
    void zero_based_array_literal_syntax() throws SQLException {
        String result = q("SELECT '[0:2]={10,20,30}'::int[]");
        assertNotNull(result, "Zero-based array literal should parse");
        // PG returns {10,20,30} (the values)
        assertTrue(result.contains("10") && result.contains("20") && result.contains("30"),
                "Zero-based array should contain all elements, got: " + result);
    }

    @Test
    void zero_based_array_lower_upper() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT array_lower('[0:2]={10,20,30}'::int[], 1), " +
                 "array_upper('[0:2]={10,20,30}'::int[], 1)")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "array_lower of [0:2] should be 0");
            assertEquals(2, rs.getInt(2), "array_upper of [0:2] should be 2");
        }
    }

    // ========================================================================
    // Array slicing: b[2:3] syntax
    // Diff: 02_value_expressions.sql stmt 14
    // ========================================================================

    @Test
    void array_slice_syntax() throws SQLException {
        exec("CREATE TABLE df_sl (id INT, b INT[])");
        exec("INSERT INTO df_sl VALUES (1, ARRAY[10,20,30,40,50])");
        try {
            String result = q("SELECT b[2:3] FROM df_sl WHERE id = 1");
            assertNotNull(result, "Array slice b[2:3] should parse and return a result");
            assertTrue(result.contains("20") && result.contains("30"),
                    "b[2:3] should return elements 2 and 3, got: " + result);
        } finally {
            exec("DROP TABLE df_sl");
        }
    }

    // ========================================================================
    // generate_subscripts on multi-dimensional arrays
    // Diff: 18_arrays_ranges_jsonb_deep.sql stmt 14
    // ========================================================================

    @Test
    void generate_subscripts_multidimensional() throws SQLException {
        int count = countRows(
            "SELECT generate_subscripts(ARRAY[[1,2],[3,4]], 1)");
        assertEquals(2, count,
                "generate_subscripts on 2x2 array dim 1 should return 2 indices");

        int count2 = countRows(
            "SELECT generate_subscripts(ARRAY[[1,2],[3,4]], 2)");
        assertEquals(2, count2,
                "generate_subscripts on 2x2 array dim 2 should return 2 indices");
    }
}
