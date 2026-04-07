package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PG 18 compatibility tests covering SQL-state accuracy and parser fixes found
 * in the verification suite diff report.
 *
 * Covers:
 * - V1-1:  Double factorial operator (!!), expects 42883
 * - V2-6:  Bare SELECT after comment, expects 42601
 * - V2-39: CREATE VIEW AS bare SELECT, expects 42601
 * - V1-2:  LENGTH with two string args, expects 22023
 * - V2-13: GROUP BY ROLLUP () on non-existent table, 42601 (PG) vs 42P01 (memgres)
 * - V2-14: SELECT open as type cast, expects 42704
 * - V2-18: Hash partition with remainder >= modulus, expects 42P16
 * - V2-38: AFTER trigger on view, expects 42809
 * - V2-41: ALTER COLUMN SET GENERATED on non-identity column, expects 55000
 * - V2-63/64: ON CONFLICT WHERE with no matching partial index, expects 42P10
 */
class SqlStateAndParserFixesTest {

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

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // V1-1: Double factorial operator (!!)
    // ========================================================================

    /**
     * PG 18: SELECT 1 !! 2. The double factorial operator does not exist as a
     * binary infix operator. PG gives 42883 (undefined_function / undefined operator).
     */
    @Test
    void double_factorial_operator_gives_42883() {
        // SELECT 1 !! 2: double factorial operator does not exist in PG
        SQLException ex = assertThrows(SQLException.class, () -> exec("SELECT 1 !! 2"));
        assertEquals("42883", ex.getSQLState(), "!! operator should give 42883, got " + ex.getSQLState());
    }

    // ========================================================================
    // V2-6: Bare SELECT after comment
    // ========================================================================

    /**
     * PG 18: a line comment followed by a bare SELECT (no column list) succeeds.
     * PG returns one row with zero columns. Memgres must accept this form too.
     */
    @Test
    void bare_select_after_comment_succeeds() {
        // -- comment\nSELECT (bare SELECT with no columns); PG returns 1 row
        assertDoesNotThrow(() -> exec("-- comment\nSELECT"),
                "Bare SELECT after comment should succeed (PG 18 behavior)");
    }

    // ========================================================================
    // V2-39: CREATE VIEW AS bare SELECT
    // ========================================================================

    /**
     * PG 18: CREATE VIEW bare_v AS SELECT (no column list) succeeds.
     * PG creates a view with zero columns. Memgres must accept this form too.
     */
    @Test
    void create_view_as_bare_select_succeeds() {
        try {
            assertDoesNotThrow(() -> exec("CREATE VIEW bare_v AS SELECT"),
                    "CREATE VIEW AS bare SELECT should succeed (PG 18 behavior)");
        } finally {
            try { exec("DROP VIEW IF EXISTS bare_v"); } catch (SQLException ignored) {}
        }
    }

    // ========================================================================
    // V1-2: LENGTH with two string arguments
    // ========================================================================

    /**
     * PG 18: SELECT LENGTH('a', 'b'). LENGTH takes exactly one argument.
     * Calling it with two string args gives 22023 (invalid_parameter_value)
     * because no matching overload exists that accepts two text arguments in
     * the expected way (the two-arg form expects (text, name) encoding).
     *
     * PG resolves the two-text-arg call to the encoding-aware overload, which
     * then rejects the encoding name. Either 22023 or 42883 is acceptable.
     */
    @Test
    void length_two_string_args_gives_error() {
        SQLException ex = assertThrows(SQLException.class, () -> exec("SELECT LENGTH('a', 'b')"));
        String state = ex.getSQLState();
        // PG 18 gives 22023; some versions give 42883. Both indicate the call is invalid
        assertTrue("22023".equals(state) || "42883".equals(state),
                "LENGTH('a','b') should give 22023 or 42883, got " + state);
    }

    /**
     * Stricter variant: assert exactly 22023, matching the PG 18 baseline.
     */
    @Test
    void length_two_string_args_gives_22023() {
        SQLException ex = assertThrows(SQLException.class, () -> exec("SELECT LENGTH('a', 'b')"));
        assertEquals("22023", ex.getSQLState(),
                "LENGTH('a','b') should give 22023, got " + ex.getSQLState());
    }

    // ========================================================================
    // V2-13: GROUP BY ROLLUP () with non-existent table
    // ========================================================================

    /**
     * PG 18 diff V2-13: querying a non-existent table with GROUP BY ROLLUP ()
     * gives 42601 (syntax_error) in PG, but 42P01 (undefined_table) in memgres.
     * PG appears to reject the empty ROLLUP grouping set at parse/analysis time
     * before resolving the table name.
     *
     * The test documents the expected PG state; memgres currently gives 42P01.
     */
    @Test
    void group_by_rollup_empty_nonexistent_table_gives_42601() {
        // PG gives 42601 for ROLLUP(); memgres currently gives 42P01
        SQLException ex = assertThrows(SQLException.class,
                () -> query("SELECT b, sum(c) FROM nonexistent_rbr_s GROUP BY ROLLUP ()"));
        assertEquals("42601", ex.getSQLState(),
                "ROLLUP() with non-existent table should give 42601, got " + ex.getSQLState());
    }

    /**
     * ROLLUP() with no arguments is a syntax error in PG 18, even on existing tables.
     */
    @Test
    void group_by_rollup_empty_on_existing_table_gives_42601() throws SQLException {
        exec("CREATE TABLE rup_s(b int, c int)");
        exec("INSERT INTO rup_s VALUES (1,10),(2,20)");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> query("SELECT sum(c) FROM rup_s GROUP BY ROLLUP ()"));
            assertEquals("42601", ex.getSQLState(),
                    "ROLLUP() with no arguments should give 42601, got " + ex.getSQLState());
        } finally {
            exec("DROP TABLE IF EXISTS rup_s");
        }
    }

    // ========================================================================
    // V2-14: SELECT open '...'::path, where 'open' is parsed as a type name
    // ========================================================================

    /**
     * PG 18: SELECT open '( (0,0), (1,1), (2,2) )'::path. Here 'open' is parsed
     * as a type cast target (type name), not as the open() function. Since no
     * type named 'open' exists in the search path, PG gives 42704
     * (undefined_object).
     */
    @Test
    void select_open_path_gives_42704() {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SELECT open '( (0,0), (1,1), (2,2) )'::path"));
        assertEquals("42704", ex.getSQLState(),
                "open as type name should give 42704, got " + ex.getSQLState());
    }

    // ========================================================================
    // V2-18: Hash partition with remainder >= modulus
    // ========================================================================

    /**
     * PG 18: FOR VALUES WITH (modulus 2, remainder 5). Remainder must be
     * strictly less than modulus. PG gives 42P16
     * (invalid_table_definition).
     */
    @Test
    void hash_partition_bad_remainder_gives_42P16() throws SQLException {
        exec("CREATE TABLE hp_parent(id int, label text) PARTITION BY HASH(id)");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("CREATE TABLE hp_bad PARTITION OF hp_parent FOR VALUES WITH (modulus 2, remainder 5)"));
            assertEquals("42P16", ex.getSQLState(),
                    "remainder >= modulus should give 42P16, got " + ex.getSQLState());
        } finally {
            try { exec("DROP TABLE IF EXISTS hp_bad"); } catch (SQLException ignored) {}
            exec("DROP TABLE IF EXISTS hp_parent");
        }
    }

    /**
     * Valid hash partition (remainder < modulus) must succeed.
     */
    @Test
    void hash_partition_valid_remainder_succeeds() throws SQLException {
        exec("CREATE TABLE hp2_parent(id int, label text) PARTITION BY HASH(id)");
        try {
            exec("CREATE TABLE hp2_part0 PARTITION OF hp2_parent FOR VALUES WITH (modulus 2, remainder 0)");
            exec("CREATE TABLE hp2_part1 PARTITION OF hp2_parent FOR VALUES WITH (modulus 2, remainder 1)");
            // Partitions exist; verify by inserting a row
            exec("INSERT INTO hp2_parent VALUES (1, 'x')");
            String cnt = scalar("SELECT count(*) FROM hp2_parent");
            assertEquals("1", cnt, "Inserted row should be visible through the partitioned table");
        } finally {
            try { exec("DROP TABLE IF EXISTS hp2_part0"); } catch (SQLException ignored) {}
            try { exec("DROP TABLE IF EXISTS hp2_part1"); } catch (SQLException ignored) {}
            exec("DROP TABLE IF EXISTS hp2_parent");
        }
    }

    // ========================================================================
    // V2-38: AFTER trigger on view
    // ========================================================================

    /**
     * PG 18: AFTER ... FOR EACH ROW triggers are not allowed on views.
     * Only INSTEAD OF triggers may be row-level on a view.
     * PG gives 42809 (wrong_object_type).
     */
    @Test
    void after_trigger_on_view_gives_42809() throws SQLException {
        exec("CREATE TABLE trg_base(id int)");
        exec("CREATE VIEW trg_v AS SELECT * FROM trg_base");
        exec("CREATE FUNCTION trg_fn_ret() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("CREATE TRIGGER bad_trg AFTER INSERT ON trg_v FOR EACH ROW EXECUTE FUNCTION trg_fn_ret()"));
            assertEquals("42809", ex.getSQLState(),
                    "AFTER trigger on view should give 42809, got " + ex.getSQLState());
        } finally {
            try { exec("DROP TRIGGER IF EXISTS bad_trg ON trg_v"); } catch (SQLException ignored) {}
            try { exec("DROP FUNCTION IF EXISTS trg_fn_ret() CASCADE"); } catch (SQLException ignored) {}
            try { exec("DROP VIEW IF EXISTS trg_v"); } catch (SQLException ignored) {}
            try { exec("DROP TABLE IF EXISTS trg_base"); } catch (SQLException ignored) {}
        }
    }

    // ========================================================================
    // V2-41: ALTER COLUMN SET GENERATED ALWAYS on non-identity column
    // ========================================================================

    /**
     * PG 18: ALTER TABLE t ALTER COLUMN a SET GENERATED ALWAYS. Column 'a'
     * must already be an identity column. Applying SET GENERATED to a plain
     * integer column gives 55000 (object_not_in_prerequisite_state).
     */
    @Test
    void alter_column_set_generated_on_non_identity_gives_55000() throws SQLException {
        exec("CREATE TABLE gen_t(a int)");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("ALTER TABLE gen_t ALTER COLUMN a SET GENERATED ALWAYS"));
            assertEquals("55000", ex.getSQLState(),
                    "SET GENERATED on non-identity column should give 55000, got " + ex.getSQLState());
        } finally {
            exec("DROP TABLE IF EXISTS gen_t");
        }
    }

    /**
     * SET GENERATED on an actual identity column must succeed.
     */
    @Test
    void alter_column_set_generated_on_identity_column_succeeds() throws SQLException {
        exec("CREATE TABLE gen2_t(id int PRIMARY KEY, a int GENERATED ALWAYS AS IDENTITY)");
        try {
            exec("ALTER TABLE gen2_t ALTER COLUMN a SET GENERATED BY DEFAULT");
            exec("ALTER TABLE gen2_t ALTER COLUMN a SET GENERATED ALWAYS");
        } catch (SQLException ex) {
            fail("SET GENERATED on identity column should succeed: " + ex.getMessage());
        } finally {
            exec("DROP TABLE IF EXISTS gen2_t");
        }
    }

    // ========================================================================
    // V2-63/64: ON CONFLICT WHERE with no matching partial index
    // ========================================================================

    /**
     * PG 18: INSERT ... ON CONFLICT (id) WHERE tenant_id = 10 DO UPDATE ...
     * There is no partial unique index on (id) WHERE tenant_id = 10, so PG
     * gives 42P10 (invalid_column_reference / no unique constraint matching).
     */
    @Test
    void on_conflict_where_no_matching_index_gives_42P10() throws SQLException {
        exec("CREATE TABLE ocp_t(id int PRIMARY KEY, tenant_id int, val text)");
        try {
            // ON CONFLICT (id) WHERE tenant_id = 10, but no partial unique index exists
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("INSERT INTO ocp_t VALUES (1, 10, 'x') ON CONFLICT (id) WHERE tenant_id = 10 DO UPDATE SET val = EXCLUDED.val"));
            assertEquals("42P10", ex.getSQLState(),
                    "ON CONFLICT WHERE with no matching partial index should give 42P10, got " + ex.getSQLState());
        } finally {
            exec("DROP TABLE IF EXISTS ocp_t");
        }
    }

    /**
     * ON CONFLICT WHERE with a matching partial unique index must succeed and
     * perform the upsert correctly.
     */
    @Test
    void on_conflict_where_with_matching_index_succeeds() throws SQLException {
        exec("CREATE TABLE ocp2_t(id int, tenant_id int, val text)");
        exec("CREATE UNIQUE INDEX ocp2_uq ON ocp2_t(id) WHERE tenant_id = 10");
        try {
            exec("INSERT INTO ocp2_t VALUES (1, 10, 'original')");
            exec("INSERT INTO ocp2_t VALUES (1, 10, 'updated') ON CONFLICT (id) WHERE tenant_id = 10 DO UPDATE SET val = EXCLUDED.val");
            String val = scalar("SELECT val FROM ocp2_t WHERE id = 1 AND tenant_id = 10");
            assertEquals("updated", val,
                    "ON CONFLICT DO UPDATE should overwrite existing row, got: " + val);
        } finally {
            exec("DROP TABLE IF EXISTS ocp2_t");
        }
    }
}
