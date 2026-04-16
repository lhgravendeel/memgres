package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 13 gaps: SQL parser features that PG 18 supports but Memgres
 * does not (or only partially) parse.
 *
 * These tests fail today: they invoke syntax that Memgres' parser currently
 * rejects, misinterprets, or silently ignores.
 *
 * Coverage:
 *   A. XMLTABLE                       — query XML with XPath into a table
 *   B. IS JSON predicate              — IS JSON / IS JSON OBJECT / ARRAY / ...
 *   C. CREATE STATISTICS              — extended planner statistics
 *   D. CREATE COLLATION               — user-defined collations
 *   E. MATERIALIZED / NOT MATERIALIZED — WITH clause materialization hints
 *   F. BEGIN ATOMIC ... END           — SQL-standard function body
 *   G. CREATE RULE                    — PG rewrite rules
 *   H. CREATE OPERATOR advanced       — MERGES/HASHES/COMMUTATOR/NEGATOR
 *   I. CREATE FUNCTION options        — PARALLEL SAFE / LEAKPROOF / COST / ROWS / SUPPORT
 *   J. TRIGGER transition tables      — REFERENCING NEW TABLE / OLD TABLE
 *   K. CREATE FOREIGN TABLE / FDW     — foreign-data wrapper DDL family
 *   L. CREATE TYPE ... AS RANGE       — range type definition
 */
class Round13ParserGapsTest {

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
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getString(1);
        }
    }

    private static boolean scalarBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getBoolean(1);
        }
    }

    // =========================================================================
    // A. XMLTABLE
    // =========================================================================

    @Test
    void xmltable_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT * FROM XMLTABLE('/root/row' "
                             + "PASSING '<root><row><a>1</a></row><row><a>2</a></row></root>'::xml "
                             + "COLUMNS a int PATH 'a') ORDER BY a")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt("a"));
            assertTrue(rs.next()); assertEquals(2, rs.getInt("a"));
        }
    }

    // =========================================================================
    // B. IS JSON predicate family (SQL/JSON standard, PG 16+)
    // =========================================================================

    @Test
    void is_json_predicate_basic() throws SQLException {
        assertTrue(scalarBool("SELECT '{\"a\":1}' IS JSON"));
        assertFalse(scalarBool("SELECT 'not json' IS JSON"));
    }

    @Test
    void is_json_object() throws SQLException {
        assertTrue(scalarBool("SELECT '{\"a\":1}' IS JSON OBJECT"));
        assertFalse(scalarBool("SELECT '[1,2,3]' IS JSON OBJECT"));
    }

    @Test
    void is_json_array() throws SQLException {
        assertTrue(scalarBool("SELECT '[1,2,3]' IS JSON ARRAY"));
        assertFalse(scalarBool("SELECT '{\"a\":1}' IS JSON ARRAY"));
    }

    @Test
    void is_json_scalar() throws SQLException {
        assertTrue(scalarBool("SELECT '42' IS JSON SCALAR"));
        assertFalse(scalarBool("SELECT '[1,2,3]' IS JSON SCALAR"));
    }

    @Test
    void is_json_number() throws SQLException {
        // Only the literal (scalar number) matches
        assertTrue(scalarBool("SELECT '42' IS JSON NUMBER"));
        assertFalse(scalarBool("SELECT '\"hi\"' IS JSON NUMBER"));
    }

    @Test
    void is_json_string() throws SQLException {
        assertTrue(scalarBool("SELECT '\"abc\"' IS JSON STRING"));
        assertFalse(scalarBool("SELECT '42' IS JSON STRING"));
    }

    @Test
    void is_json_boolean() throws SQLException {
        assertTrue(scalarBool("SELECT 'true' IS JSON BOOLEAN"));
        assertFalse(scalarBool("SELECT '42' IS JSON BOOLEAN"));
    }

    @Test
    void is_json_null() throws SQLException {
        assertTrue(scalarBool("SELECT 'null' IS JSON NULL"));
        assertFalse(scalarBool("SELECT '42' IS JSON NULL"));
    }

    @Test
    void is_not_json_negation() throws SQLException {
        assertTrue(scalarBool("SELECT 'not json' IS NOT JSON"));
    }

    // =========================================================================
    // C. CREATE STATISTICS
    // =========================================================================

    @Test
    void create_statistics_basic() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_stats CASCADE");
        exec("CREATE TABLE r13_stats (a int, b int, c int)");
        // PG syntax: CREATE STATISTICS name (kinds) ON cols FROM table
        exec("CREATE STATISTICS r13_stats_ab (dependencies, ndistinct) ON a, b FROM r13_stats");
        // Verify it exists in pg_statistic_ext
        assertEquals("r13_stats_ab",
                scalarString("SELECT stxname FROM pg_statistic_ext WHERE stxname = 'r13_stats_ab'"));
        exec("DROP STATISTICS r13_stats_ab");
    }

    @Test
    void create_statistics_mcv_kind() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_stats2 CASCADE");
        exec("CREATE TABLE r13_stats2 (a int, b int)");
        exec("CREATE STATISTICS r13_stats_mcv (mcv) ON a, b FROM r13_stats2");
        exec("DROP STATISTICS r13_stats_mcv");
    }

    // =========================================================================
    // D. CREATE COLLATION
    // =========================================================================

    @Test
    void create_collation_fromLocale() throws SQLException {
        exec("DROP COLLATION IF EXISTS r13_coll");
        exec("CREATE COLLATION r13_coll (locale = 'en_US.UTF-8')");
        exec("DROP COLLATION r13_coll");
    }

    @Test
    void create_collation_deterministic_false() throws SQLException {
        exec("DROP COLLATION IF EXISTS r13_nd_coll");
        // Non-deterministic collation (PG 12+)
        exec("CREATE COLLATION r13_nd_coll (provider = icu, locale = 'und-u-ks-level2', deterministic = false)");
        exec("DROP COLLATION r13_nd_coll");
    }

    @Test
    void create_collation_fromExisting() throws SQLException {
        exec("DROP COLLATION IF EXISTS r13_cp_coll");
        exec("CREATE COLLATION r13_cp_coll FROM \"C\"");
        exec("DROP COLLATION r13_cp_coll");
    }

    // =========================================================================
    // E. Materialized CTEs
    // =========================================================================

    @Test
    void cte_materialized_keyword() throws SQLException {
        String v = scalarString(
                "WITH x AS MATERIALIZED (SELECT 1 AS a) SELECT a::text FROM x");
        assertEquals("1", v);
    }

    @Test
    void cte_not_materialized_keyword() throws SQLException {
        String v = scalarString(
                "WITH x AS NOT MATERIALIZED (SELECT 1 AS a) SELECT a::text FROM x");
        assertEquals("1", v);
    }

    // =========================================================================
    // F. BEGIN ATOMIC (SQL-standard function body, PG 14+)
    // =========================================================================

    @Test
    void begin_atomic_missingEnd_sqlstate42601() {
        // CREATE FUNCTION with BEGIN ATOMIC but no END should error with 42601
        SQLException ex = assertThrows(SQLException.class, () -> exec(
                "CREATE OR REPLACE FUNCTION r13_bad_atomic() RETURNS int "
                        + "LANGUAGE SQL BEGIN ATOMIC SELECT 1; "
                        + " /* no END */ "));
        assertTrue("42601".equals(ex.getSQLState())
                        || ex.getMessage().toLowerCase().contains("syntax"),
                "expected 42601 or syntax error; got " + ex.getSQLState() + " — " + ex.getMessage());
    }

    @Test
    void begin_atomic_single_statement() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_atomic_fn()");
        exec("CREATE FUNCTION r13_atomic_fn() RETURNS int "
                + "LANGUAGE SQL BEGIN ATOMIC SELECT 42; END");
        assertEquals("42", scalarString("SELECT r13_atomic_fn()::text"));
    }

    @Test
    void begin_atomic_multi_statement() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_atomic_mult(int)");
        exec("CREATE FUNCTION r13_atomic_mult(x int) RETURNS int "
                + "LANGUAGE SQL BEGIN ATOMIC "
                + "  SELECT x * 2; "
                + "END");
        assertEquals("10", scalarString("SELECT r13_atomic_mult(5)::text"));
    }

    // =========================================================================
    // G. CREATE RULE
    // =========================================================================

    @Test
    void create_rule_basic() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_rule_tbl CASCADE");
        exec("CREATE TABLE r13_rule_tbl (id int, v text)");
        exec("CREATE RULE r13_rule AS ON INSERT TO r13_rule_tbl "
                + "DO INSTEAD NOTHING");
        // Insert must succeed (rule says DO INSTEAD NOTHING, so no actual row)
        exec("INSERT INTO r13_rule_tbl VALUES (1, 'x')");
        assertEquals("0",
                scalarString("SELECT count(*)::text FROM r13_rule_tbl"));
    }

    // =========================================================================
    // H. Advanced CREATE OPERATOR
    // =========================================================================

    @Test
    void create_operator_withCommutator() throws SQLException {
        // int4eq already exists; define === as alias with a commutator hint.
        exec("DROP OPERATOR IF EXISTS === (int, int)");
        exec("CREATE OPERATOR === ("
                + "  LEFTARG = int, RIGHTARG = int,"
                + "  PROCEDURE = int4eq,"
                + "  COMMUTATOR = ===,"
                + "  HASHES, MERGES)");
        assertTrue(scalarBool("SELECT 1 === 1"));
        exec("DROP OPERATOR === (int, int)");
    }

    @Test
    void create_operator_withNegator() throws SQLException {
        exec("DROP OPERATOR IF EXISTS !== (int, int)");
        exec("DROP OPERATOR IF EXISTS ==~ (int, int)");
        exec("CREATE OPERATOR ==~ ("
                + "  LEFTARG = int, RIGHTARG = int, PROCEDURE = int4eq)");
        exec("CREATE OPERATOR !== ("
                + "  LEFTARG = int, RIGHTARG = int,"
                + "  PROCEDURE = int4ne,"
                + "  NEGATOR = ==~)");
        assertTrue(scalarBool("SELECT 1 !== 2"));
        exec("DROP OPERATOR !== (int, int)");
        exec("DROP OPERATOR ==~ (int, int)");
    }

    // =========================================================================
    // I. Advanced CREATE FUNCTION options
    // =========================================================================

    @Test
    void create_function_parallel_safe() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_parallel_fn()");
        exec("CREATE FUNCTION r13_parallel_fn() RETURNS int "
                + "LANGUAGE SQL PARALLEL SAFE AS 'SELECT 1'");
        assertEquals("1", scalarString("SELECT r13_parallel_fn()::text"));
        // pg_proc.proparallel should be 's' (safe)
        assertEquals("s",
                scalarString("SELECT proparallel FROM pg_proc WHERE proname='r13_parallel_fn'"));
    }

    @Test
    void create_function_leakproof() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_leakproof_fn()");
        exec("CREATE FUNCTION r13_leakproof_fn() RETURNS int "
                + "LANGUAGE SQL LEAKPROOF AS 'SELECT 1'");
        assertTrue(scalarBool(
                "SELECT proleakproof FROM pg_proc WHERE proname='r13_leakproof_fn'"));
    }

    @Test
    void create_function_cost_rows() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_cost_fn()");
        exec("CREATE FUNCTION r13_cost_fn() RETURNS SETOF int "
                + "LANGUAGE SQL COST 500 ROWS 100 AS 'SELECT generate_series(1,10)'");
        assertEquals("500",
                scalarString("SELECT procost::text FROM pg_proc WHERE proname='r13_cost_fn'"));
        assertEquals("100",
                scalarString("SELECT prorows::text FROM pg_proc WHERE proname='r13_cost_fn'"));
    }

    @Test
    void create_function_support_option() throws SQLException {
        // SUPPORT option adds a planner support function (PG 12+)
        exec("DROP FUNCTION IF EXISTS r13_supp_fn(int)");
        SQLException ex = assertThrows(SQLException.class, () -> exec(
                "CREATE FUNCTION r13_supp_fn(int) RETURNS int "
                        + "LANGUAGE SQL SUPPORT nonexistent_support AS 'SELECT $1'"));
        // Expected: the syntax parses, and it fails ONLY because the support fn doesn't exist.
        assertFalse(ex.getMessage().toLowerCase().contains("syntax"),
                "SUPPORT should parse; got syntax error: " + ex.getMessage());
    }

    // =========================================================================
    // J. Trigger transition tables
    // =========================================================================

    @Test
    void trigger_referencing_new_table() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_trig_tbl CASCADE");
        exec("DROP FUNCTION IF EXISTS r13_trig_fn() CASCADE");
        exec("CREATE TABLE r13_trig_tbl (id int)");
        exec("CREATE FUNCTION r13_trig_fn() RETURNS trigger LANGUAGE plpgsql AS $$ "
                + "BEGIN RETURN NULL; END $$");
        // REFERENCING NEW TABLE AS ... for statement-level trigger
        exec("CREATE TRIGGER r13_trig AFTER INSERT ON r13_trig_tbl "
                + "REFERENCING NEW TABLE AS new_rows "
                + "FOR EACH STATEMENT EXECUTE FUNCTION r13_trig_fn()");
        // Insert should not error
        exec("INSERT INTO r13_trig_tbl VALUES (1)");
    }

    @Test
    void trigger_referencing_old_and_new_tables() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_trig2_tbl CASCADE");
        exec("DROP FUNCTION IF EXISTS r13_trig2_fn() CASCADE");
        exec("CREATE TABLE r13_trig2_tbl (id int)");
        exec("CREATE FUNCTION r13_trig2_fn() RETURNS trigger LANGUAGE plpgsql AS $$ "
                + "BEGIN RETURN NULL; END $$");
        exec("CREATE TRIGGER r13_trig2 AFTER UPDATE ON r13_trig2_tbl "
                + "REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows "
                + "FOR EACH STATEMENT EXECUTE FUNCTION r13_trig2_fn()");
    }

    // =========================================================================
    // K. Foreign data wrappers
    // =========================================================================

    @Test
    void create_foreign_data_wrapper() throws SQLException {
        exec("DROP FOREIGN DATA WRAPPER IF EXISTS r13_fdw CASCADE");
        exec("CREATE FOREIGN DATA WRAPPER r13_fdw");
        exec("DROP FOREIGN DATA WRAPPER r13_fdw");
    }

    @Test
    void create_server_and_foreign_table() throws SQLException {
        exec("DROP SERVER IF EXISTS r13_srv CASCADE");
        exec("DROP FOREIGN DATA WRAPPER IF EXISTS r13_fdw2 CASCADE");
        exec("CREATE FOREIGN DATA WRAPPER r13_fdw2");
        exec("CREATE SERVER r13_srv FOREIGN DATA WRAPPER r13_fdw2");
        exec("CREATE FOREIGN TABLE r13_ftbl (id int, v text) SERVER r13_srv");
        exec("DROP FOREIGN TABLE r13_ftbl");
        exec("DROP SERVER r13_srv");
        exec("DROP FOREIGN DATA WRAPPER r13_fdw2");
    }

    @Test
    void create_user_mapping() throws SQLException {
        exec("DROP SERVER IF EXISTS r13_srv2 CASCADE");
        exec("DROP FOREIGN DATA WRAPPER IF EXISTS r13_fdw3 CASCADE");
        exec("CREATE FOREIGN DATA WRAPPER r13_fdw3");
        exec("CREATE SERVER r13_srv2 FOREIGN DATA WRAPPER r13_fdw3");
        exec("CREATE USER MAPPING FOR CURRENT_USER SERVER r13_srv2 "
                + "OPTIONS (user 'u', password 'p')");
        exec("DROP USER MAPPING FOR CURRENT_USER SERVER r13_srv2");
        exec("DROP SERVER r13_srv2");
        exec("DROP FOREIGN DATA WRAPPER r13_fdw3");
    }

    // =========================================================================
    // L. CREATE TYPE AS RANGE
    // =========================================================================

    @Test
    void create_type_as_range() throws SQLException {
        exec("DROP TYPE IF EXISTS r13_myrange CASCADE");
        exec("CREATE TYPE r13_myrange AS RANGE (subtype = int4)");
        // The constructor function should work just like int4range
        assertEquals("[1,10)",
                scalarString("SELECT r13_myrange(1, 10)::text"));
        exec("DROP TYPE r13_myrange");
    }

    @Test
    void create_type_as_range_with_subtype_diff() throws SQLException {
        exec("DROP TYPE IF EXISTS r13_numrange CASCADE");
        exec("CREATE TYPE r13_numrange AS RANGE ("
                + "  subtype = numeric, "
                + "  subtype_diff = numeric_sub)");
        exec("DROP TYPE r13_numrange");
    }
}
