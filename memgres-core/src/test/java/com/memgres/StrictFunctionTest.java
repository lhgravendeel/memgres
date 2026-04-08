package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for STRICT (RETURNS NULL ON NULL INPUT) function support.
 * Covers general function calls and aggregate SFUNC behavior.
 */
class StrictFunctionTest {

    private Memgres memgres;

    @BeforeEach
    void setUp() throws SQLException {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterEach
    void tearDown() {
        if (memgres != null) memgres.close();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    // ========== General STRICT function tests ==========

    @Test
    void strictFunctionReturnsNullOnNullArg() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION strict_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql STRICT");
            try (ResultSet rs = stmt.executeQuery("SELECT strict_add(1, NULL)")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
        }
    }

    @Test
    void strictFunctionWorksWithNonNullArgs() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION strict_add2(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql STRICT");
            try (ResultSet rs = stmt.executeQuery("SELECT strict_add2(3, 4)")) {
                assertTrue(rs.next());
                assertEquals(7, rs.getInt(1));
            }
        }
    }

    @Test
    void strictFunctionFirstArgNull() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION strict_concat(a text, b text) RETURNS text AS $$ "
                    + "BEGIN RETURN a || b; END; $$ LANGUAGE plpgsql STRICT");
            try (ResultSet rs = stmt.executeQuery("SELECT strict_concat(NULL, 'world')")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
        }
    }

    @Test
    void strictFunctionAllArgsNull() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION strict_mul(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * b; END; $$ LANGUAGE plpgsql STRICT");
            try (ResultSet rs = stmt.executeQuery("SELECT strict_mul(NULL, NULL)")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
        }
    }

    @Test
    void returnsNullOnNullInputSyntax() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION rnoni_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql RETURNS NULL ON NULL INPUT");
            try (ResultSet rs = stmt.executeQuery("SELECT rnoni_add(1, NULL)")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
        }
    }

    @Test
    void calledOnNullInputAllowsNulls() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // CALLED ON NULL INPUT is the default — function body executes even with NULL args
            stmt.execute("CREATE FUNCTION nullable_fn(a integer) RETURNS text AS $$ "
                    + "BEGIN IF a IS NULL THEN RETURN 'was null'; ELSE RETURN 'was ' || a::text; END IF; END; "
                    + "$$ LANGUAGE plpgsql CALLED ON NULL INPUT");
            try (ResultSet rs = stmt.executeQuery("SELECT nullable_fn(NULL)")) {
                assertTrue(rs.next());
                assertEquals("was null", rs.getString(1));
            }
        }
    }

    @Test
    void nonStrictFunctionDefaultAllowsNulls() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Default (no STRICT keyword) = CALLED ON NULL INPUT
            stmt.execute("CREATE FUNCTION default_fn(a integer) RETURNS text AS $$ "
                    + "BEGIN IF a IS NULL THEN RETURN 'got null'; ELSE RETURN a::text; END IF; END; "
                    + "$$ LANGUAGE plpgsql");
            try (ResultSet rs = stmt.executeQuery("SELECT default_fn(NULL)")) {
                assertTrue(rs.next());
                assertEquals("got null", rs.getString(1));
            }
        }
    }

    @Test
    void strictFunctionWithSqlLanguage() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION strict_sql_add(a integer, b integer) RETURNS integer AS $$ "
                    + "SELECT a + b; $$ LANGUAGE sql STRICT");
            try (ResultSet rs = stmt.executeQuery("SELECT strict_sql_add(5, NULL)")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
            try (ResultSet rs = stmt.executeQuery("SELECT strict_sql_add(5, 3)")) {
                assertTrue(rs.next());
                assertEquals(8, rs.getInt(1));
            }
        }
    }

    @Test
    void strictFunctionInWhereClause() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE t1 (id integer, val integer)");
            stmt.execute("INSERT INTO t1 VALUES (1, 10), (2, NULL), (3, 30)");
            stmt.execute("CREATE FUNCTION strict_double(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * 2; END; $$ LANGUAGE plpgsql STRICT");
            // strict_double(NULL) returns NULL, so WHERE condition is false for row 2
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT id FROM t1 WHERE strict_double(val) > 25 ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));  // 30*2=60 > 25
                assertFalse(rs.next());  // row 1: 20 not > 25, row 2: NULL not > 25
            }
        }
    }

    @Test
    void strictFunctionSingleArg() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION strict_inc(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql STRICT");
            try (ResultSet rs = stmt.executeQuery("SELECT strict_inc(NULL)")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
            try (ResultSet rs = stmt.executeQuery("SELECT strict_inc(41)")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
            }
        }
    }

    // ========== STRICT SFUNC in aggregate tests ==========

    @Test
    void strictSfuncSkipsNullInputRows() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE agg_nulls (val integer)");
            stmt.execute("INSERT INTO agg_nulls VALUES (10), (NULL), (20), (NULL), (30)");
            // STRICT sfunc: NULL inputs are automatically skipped
            stmt.execute("CREATE FUNCTION strict_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE AGGREGATE strict_sum(integer) ("
                    + "SFUNC = strict_sum_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT strict_sum(val) FROM agg_nulls")) {
                assertTrue(rs.next());
                assertEquals(60, rs.getInt(1));  // 10+20+30, NULLs skipped
            }
        }
    }

    @Test
    void strictSfuncNoInitcondUsesFirstNonNull() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE agg_first (val integer)");
            stmt.execute("INSERT INTO agg_first VALUES (NULL), (NULL), (10), (20), (30)");
            // STRICT sfunc, no INITCOND: first non-NULL becomes initial state
            stmt.execute("CREATE FUNCTION strict_add_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE AGGREGATE strict_add_agg(integer) ("
                    + "SFUNC = strict_add_sf, STYPE = integer)");
            try (ResultSet rs = stmt.executeQuery("SELECT strict_add_agg(val) FROM agg_first")) {
                assertTrue(rs.next());
                // NULLs skipped, first non-NULL (10) becomes state, then +20=30, +30=60
                assertEquals(60, rs.getInt(1));
            }
        }
    }

    @Test
    void strictSfuncAllNullsReturnsNull() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE all_null (val integer)");
            stmt.execute("INSERT INTO all_null VALUES (NULL), (NULL), (NULL)");
            stmt.execute("CREATE FUNCTION strict_sum2_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE AGGREGATE strict_sum2(integer) ("
                    + "SFUNC = strict_sum2_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT strict_sum2(val) FROM all_null")) {
                assertTrue(rs.next());
                // All inputs NULL, all skipped, state stays at INITCOND (0)
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    void strictSfuncAllNullsNoInitcondReturnsNull() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE all_null2 (val integer)");
            stmt.execute("INSERT INTO all_null2 VALUES (NULL), (NULL)");
            stmt.execute("CREATE FUNCTION strict_sum3_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE AGGREGATE strict_sum3(integer) ("
                    + "SFUNC = strict_sum3_sf, STYPE = integer)");
            try (ResultSet rs = stmt.executeQuery("SELECT strict_sum3(val) FROM all_null2")) {
                assertTrue(rs.next());
                // No INITCOND, all NULLs → never got a first non-NULL → NULL
                assertNull(rs.getObject(1));
            }
        }
    }

    @Test
    void strictSfuncWithInitcondAndNulls() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE mixed (val integer)");
            stmt.execute("INSERT INTO mixed VALUES (5), (NULL), (10), (NULL), (15)");
            stmt.execute("CREATE FUNCTION strict_acc(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE AGGREGATE strict_total(integer) ("
                    + "SFUNC = strict_acc, STYPE = integer, INITCOND = '100')");
            try (ResultSet rs = stmt.executeQuery("SELECT strict_total(val) FROM mixed")) {
                assertTrue(rs.next());
                assertEquals(130, rs.getInt(1));  // 100 + 5 + 10 + 15, NULLs skipped
            }
        }
    }

    @Test
    void nonStrictSfuncProcessesNulls() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Non-strict sfunc should process NULL rows (no auto-skip)
            stmt.execute("CREATE TABLE ns_vals (val integer)");
            stmt.execute("INSERT INTO ns_vals VALUES (10), (NULL), (20)");
            stmt.execute("CREATE FUNCTION ns_count_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + 1; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE ns_count(integer) ("
                    + "SFUNC = ns_count_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT ns_count(val) FROM ns_vals")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));  // counts all rows including NULL
            }
        }
    }

    @Test
    void strictSfuncWithGroupBy() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE grp_nulls (grp text, val integer)");
            stmt.execute("INSERT INTO grp_nulls VALUES ('a', 10), ('a', NULL), ('a', 20)");
            stmt.execute("INSERT INTO grp_nulls VALUES ('b', NULL), ('b', NULL)");
            stmt.execute("INSERT INTO grp_nulls VALUES ('c', 5), ('c', 15)");
            stmt.execute("CREATE FUNCTION sg_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE AGGREGATE sg_sum(integer) ("
                    + "SFUNC = sg_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT grp, sg_sum(val) FROM grp_nulls GROUP BY grp ORDER BY grp")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals(30, rs.getInt(2));   // 10+20, NULL skipped
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertEquals(0, rs.getInt(2));    // all NULLs skipped, stays at INITCOND
                assertTrue(rs.next());
                assertEquals("c", rs.getString(1));
                assertEquals(20, rs.getInt(2));   // 5+15
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void strictSfuncWithFinalfunc() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE sf_vals (val integer)");
            stmt.execute("INSERT INTO sf_vals VALUES (10), (NULL), (20), (NULL), (30)");
            stmt.execute("CREATE FUNCTION sf_sum(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE FUNCTION sf_double(state integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state * 2; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE sf_sum_doubled(integer) ("
                    + "SFUNC = sf_sum, STYPE = integer, INITCOND = '0', FINALFUNC = sf_double)");
            try (ResultSet rs = stmt.executeQuery("SELECT sf_sum_doubled(val) FROM sf_vals")) {
                assertTrue(rs.next());
                assertEquals(120, rs.getInt(1));  // (10+20+30)*2 = 120, NULLs skipped
            }
        }
    }

    @Test
    void strictSfuncEmptyTable() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE empty_strict (val integer)");
            stmt.execute("CREATE FUNCTION es_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE AGGREGATE es_sum(integer) ("
                    + "SFUNC = es_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT es_sum(val) FROM empty_strict")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));  // empty table, stays at INITCOND
            }
        }
    }

    @Test
    void strictSfuncNoInitcondNoNulls() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE no_nulls (val integer)");
            stmt.execute("INSERT INTO no_nulls VALUES (10), (20), (30)");
            stmt.execute("CREATE FUNCTION nn_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE AGGREGATE nn_sum(integer) ("
                    + "SFUNC = nn_sf, STYPE = integer)");
            try (ResultSet rs = stmt.executeQuery("SELECT nn_sum(val) FROM no_nulls")) {
                assertTrue(rs.next());
                // First non-NULL (10) becomes state, then +20=30, +30=60
                assertEquals(60, rs.getInt(1));
            }
        }
    }

    // ========== STRICT interaction corner cases ==========

    @Test
    void strictSfuncWithDistinct() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE dist_strict (val integer)");
            stmt.execute("INSERT INTO dist_strict VALUES (10), (10), (NULL), (20), (20), (NULL), (30)");
            stmt.execute("CREATE FUNCTION ds_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE AGGREGATE ds_sum(integer) ("
                    + "SFUNC = ds_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT ds_sum(DISTINCT val) FROM dist_strict")) {
                assertTrue(rs.next());
                // NULLs skipped by STRICT, duplicates removed by DISTINCT: 10+20+30
                assertEquals(60, rs.getInt(1));
            }
        }
    }

    @Test
    void strictSfuncWithFilter() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE flt_strict (val integer, active boolean)");
            stmt.execute("INSERT INTO flt_strict VALUES (10, true)");
            stmt.execute("INSERT INTO flt_strict VALUES (NULL, true)");
            stmt.execute("INSERT INTO flt_strict VALUES (20, false)");
            stmt.execute("INSERT INTO flt_strict VALUES (30, true)");
            stmt.execute("INSERT INTO flt_strict VALUES (NULL, true)");
            stmt.execute("CREATE FUNCTION fs_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE AGGREGATE fs_sum(integer) ("
                    + "SFUNC = fs_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT fs_sum(val) FILTER (WHERE active) FROM flt_strict")) {
                assertTrue(rs.next());
                // FILTER keeps active=true rows: 10, NULL, 30, NULL
                // STRICT skips NULLs: 10+30 = 40
                assertEquals(40, rs.getInt(1));
            }
        }
    }

    @Test
    void strictSfuncMultiArgNullInOneArg() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE multi_strict (a integer, b integer)");
            stmt.execute("INSERT INTO multi_strict VALUES (10, 2)");
            stmt.execute("INSERT INTO multi_strict VALUES (20, NULL)");  // NULL in second arg
            stmt.execute("INSERT INTO multi_strict VALUES (NULL, 3)");   // NULL in first arg
            stmt.execute("INSERT INTO multi_strict VALUES (30, 4)");
            stmt.execute("CREATE FUNCTION ms_sf(state integer, a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + a * b; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE AGGREGATE ms_wsum(integer, integer) ("
                    + "SFUNC = ms_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT ms_wsum(a, b) FROM multi_strict")) {
                assertTrue(rs.next());
                // Rows with any NULL arg skipped: 10*2 + 30*4 = 140
                assertEquals(140, rs.getInt(1));
            }
        }
    }

    @Test
    void strictSfuncStateBecomesNullBlocksSubsequent() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE state_null (val integer)");
            stmt.execute("INSERT INTO state_null VALUES (10), (20), (30), (40)");
            // This SFUNC returns NULL when accumulated sum exceeds 25
            // Once state is NULL, STRICT means all subsequent rows are skipped
            stmt.execute("CREATE FUNCTION sn_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN IF state + val > 25 THEN RETURN NULL; ELSE RETURN state + val; END IF; END; "
                    + "$$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE AGGREGATE sn_sum(integer) ("
                    + "SFUNC = sn_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT sn_sum(val) FROM state_null")) {
                assertTrue(rs.next());
                // 0+10=10, 10+20=30 > 25 → NULL, state=NULL → STRICT skips 30 and 40
                assertNull(rs.getObject(1));
            }
        }
    }

    @Test
    void strictFunctionCalledFromAnotherFunction() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION inner_strict(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * 10; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE FUNCTION outer_fn(x integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN inner_strict(x); END; $$ LANGUAGE plpgsql");
            // outer_fn is NOT strict, so it executes with NULL arg
            // inner_strict IS strict, so it returns NULL when called with NULL
            try (ResultSet rs = stmt.executeQuery("SELECT outer_fn(NULL)")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
            try (ResultSet rs = stmt.executeQuery("SELECT outer_fn(5)")) {
                assertTrue(rs.next());
                assertEquals(50, rs.getInt(1));
            }
        }
    }

    @Test
    void strictSfuncWithDistinctAndNullsNoInitcond() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE dsn (val integer)");
            stmt.execute("INSERT INTO dsn VALUES (NULL), (10), (10), (NULL), (20)");
            stmt.execute("CREATE FUNCTION dsn_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE AGGREGATE dsn_sum(integer) ("
                    + "SFUNC = dsn_sf, STYPE = integer)");
            try (ResultSet rs = stmt.executeQuery("SELECT dsn_sum(DISTINCT val) FROM dsn")) {
                assertTrue(rs.next());
                // NULLs skipped, first non-NULL (10) becomes state (no DISTINCT tracking),
                // second 10 is new to DISTINCT: 10+10=20, then +20=40
                assertEquals(40, rs.getInt(1));
            }
        }
    }
}
