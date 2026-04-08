package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CREATE AGGREGATE support (user-defined aggregates).
 * Covers basic SFUNC+STYPE, INITCOND, FINALFUNC, GROUP BY, FILTER,
 * DISTINCT, empty groups, NULL handling, DROP AGGREGATE, and text aggregation.
 */
class CreateAggregateTest {

    private Memgres memgres;

    @BeforeEach
    void setUp() throws SQLException {
        memgres = Memgres.builder().port(0).build().start();
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE nums (id integer, val integer, grp text)");
            stmt.execute("INSERT INTO nums VALUES (1, 10, 'a')");
            stmt.execute("INSERT INTO nums VALUES (2, 20, 'a')");
            stmt.execute("INSERT INTO nums VALUES (3, 30, 'b')");
            stmt.execute("INSERT INTO nums VALUES (4, 40, 'b')");
            stmt.execute("INSERT INTO nums VALUES (5, 50, 'b')");
        }
    }

    @AfterEach
    void tearDown() {
        if (memgres != null) memgres.close();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    // ---- Basic SFUNC + STYPE ----

    @Test
    void basicSumAggregate() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Create a simple sum function
            stmt.execute("CREATE FUNCTION my_sum_sfunc(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE my_sum(integer) ("
                    + "SFUNC = my_sum_sfunc, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT my_sum(val) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(150, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    // ---- INITCOND ----

    @Test
    void aggregateWithNonZeroInitcond() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION add_int(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE sum_plus_100(integer) ("
                    + "SFUNC = add_int, STYPE = integer, INITCOND = '100')");
            try (ResultSet rs = stmt.executeQuery("SELECT sum_plus_100(val) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(250, rs.getInt(1));  // 100 + 10+20+30+40+50
            }
        }
    }

    @Test
    void aggregateWithoutInitcondStartsNull() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // COALESCE handles the NULL initial state
            stmt.execute("CREATE FUNCTION coalesce_add(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN IF state IS NULL THEN RETURN val; ELSE RETURN state + val; END IF; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE my_sum_no_init(integer) ("
                    + "SFUNC = coalesce_add, STYPE = integer)");
            try (ResultSet rs = stmt.executeQuery("SELECT my_sum_no_init(val) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(150, rs.getInt(1));
            }
        }
    }

    // ---- FINALFUNC ----

    @Test
    void aggregateWithFinalfunc() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // SFUNC accumulates sum, FINALFUNC doubles it
            stmt.execute("CREATE FUNCTION acc_sum(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION double_it(state integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state * 2; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE sum_doubled(integer) ("
                    + "SFUNC = acc_sum, STYPE = integer, INITCOND = '0', FINALFUNC = double_it)");
            try (ResultSet rs = stmt.executeQuery("SELECT sum_doubled(val) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(300, rs.getInt(1));  // (10+20+30+40+50)*2
            }
        }
    }

    @Test
    void finalfuncChangesReturnType() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Count items then return as text
            stmt.execute("CREATE FUNCTION count_sfunc(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + 1; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION count_to_text(state integer) RETURNS text AS $$ "
                    + "BEGIN RETURN 'count=' || state::text; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE count_as_text(integer) ("
                    + "SFUNC = count_sfunc, STYPE = integer, INITCOND = '0', FINALFUNC = count_to_text)");
            try (ResultSet rs = stmt.executeQuery("SELECT count_as_text(val) FROM nums")) {
                assertTrue(rs.next());
                assertEquals("count=5", rs.getString(1));
            }
        }
    }

    // ---- GROUP BY ----

    @Test
    void aggregateWithGroupBy() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sum_sfunc(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE grp_sum(integer) ("
                    + "SFUNC = sum_sfunc, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT grp, grp_sum(val) FROM nums GROUP BY grp ORDER BY grp")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals(30, rs.getInt(2));   // 10+20
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertEquals(120, rs.getInt(2));  // 30+40+50
                assertFalse(rs.next());
            }
        }
    }

    // ---- Empty group / no rows ----

    @Test
    void aggregateOnEmptyTable() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE empty_t (val integer)");
            stmt.execute("CREATE FUNCTION sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE empty_sum(integer) ("
                    + "SFUNC = sum_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT empty_sum(val) FROM empty_t")) {
                assertTrue(rs.next());
                // With INITCOND, empty group returns the initial value
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    void aggregateOnEmptyTableNoInitcond() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE empty_t2 (val integer)");
            stmt.execute("CREATE FUNCTION coalesce_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN IF state IS NULL THEN RETURN val; ELSE RETURN state + val; END IF; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE empty_sum2(integer) ("
                    + "SFUNC = coalesce_sf, STYPE = integer)");
            try (ResultSet rs = stmt.executeQuery("SELECT empty_sum2(val) FROM empty_t2")) {
                assertTrue(rs.next());
                // No INITCOND, empty group -> NULL
                assertNull(rs.getObject(1));
            }
        }
    }

    // ---- NULL handling ----

    @Test
    void aggregateSkipsNullInputValues() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE with_nulls (val integer)");
            stmt.execute("INSERT INTO with_nulls VALUES (10)");
            stmt.execute("INSERT INTO with_nulls VALUES (NULL)");
            stmt.execute("INSERT INTO with_nulls VALUES (30)");
            stmt.execute("CREATE FUNCTION null_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN IF val IS NULL THEN RETURN state; ELSE RETURN state + val; END IF; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE null_sum(integer) ("
                    + "SFUNC = null_sum_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT null_sum(val) FROM with_nulls")) {
                assertTrue(rs.next());
                // The sfunc handles NULLs explicitly
                assertEquals(40, rs.getInt(1));
            }
        }
    }

    // ---- DROP AGGREGATE ----

    @Test
    void dropAggregateRemovesIt() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION dsum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE dsum(integer) ("
                    + "SFUNC = dsum_sf, STYPE = integer, INITCOND = '0')");
            // Use it first
            try (ResultSet rs = stmt.executeQuery("SELECT dsum(val) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(150, rs.getInt(1));
            }
            // Drop it
            stmt.execute("DROP AGGREGATE dsum(integer)");
            // Now it should fail
            assertThrows(SQLException.class, () -> {
                try (ResultSet rs = stmt.executeQuery("SELECT dsum(val) FROM nums")) {
                    rs.next();
                }
            });
        }
    }

    // ---- Text aggregation ----

    @Test
    void textConcatenationAggregate() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE words (word text)");
            stmt.execute("INSERT INTO words VALUES ('hello')");
            stmt.execute("INSERT INTO words VALUES ('world')");
            stmt.execute("INSERT INTO words VALUES ('foo')");
            stmt.execute("CREATE FUNCTION concat_sf(state text, val text) RETURNS text AS $$ "
                    + "BEGIN IF state = '' THEN RETURN val; ELSE RETURN state || ',' || val; END IF; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE my_concat(text) ("
                    + "SFUNC = concat_sf, STYPE = text, INITCOND = '')");
            try (ResultSet rs = stmt.executeQuery("SELECT my_concat(word) FROM words")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                // The concatenation should have all three words
                assertTrue(result.contains("hello"));
                assertTrue(result.contains("world"));
                assertTrue(result.contains("foo"));
            }
        }
    }

    // ---- Bigint / numeric state type ----

    @Test
    void aggregateWithBigintState() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION bigsum_sf(state bigint, val integer) RETURNS bigint AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE bigsum(integer) ("
                    + "SFUNC = bigsum_sf, STYPE = bigint, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT bigsum(val) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(150L, rs.getLong(1));
            }
        }
    }

    // ---- Multiple aggregates in same query ----

    @Test
    void multipleUserAggregatesInQuery() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION add_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION mul_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state * val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE my_add(integer) ("
                    + "SFUNC = add_sf, STYPE = integer, INITCOND = '0')");
            stmt.execute("CREATE AGGREGATE my_mul(integer) ("
                    + "SFUNC = mul_sf, STYPE = integer, INITCOND = '1')");
            try (ResultSet rs = stmt.executeQuery("SELECT my_add(val), my_mul(val) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(150, rs.getInt(1));       // 10+20+30+40+50
                assertEquals(12000000, rs.getInt(2));  // 10*20*30*40*50
            }
        }
    }

    // ---- User aggregate alongside built-in aggregate ----

    @Test
    void userAggregateWithBuiltinAggregate() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION custom_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE custom_sum(integer) ("
                    + "SFUNC = custom_sum_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT custom_sum(val), sum(val), count(*) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(150, rs.getInt(1));  // custom aggregate
                assertEquals(150, rs.getInt(2));  // built-in sum
                assertEquals(5, rs.getLong(3));    // built-in count
            }
        }
    }

    // ---- Aggregate with WHERE clause ----

    @Test
    void aggregateWithWhereClause() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION where_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE where_sum(integer) ("
                    + "SFUNC = where_sum_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT where_sum(val) FROM nums WHERE grp = 'b'")) {
                assertTrue(rs.next());
                assertEquals(120, rs.getInt(1));  // 30+40+50
            }
        }
    }

    // ---- Max aggregate (stateful comparison) ----

    @Test
    void userDefinedMaxAggregate() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_max_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN IF state IS NULL OR val > state THEN RETURN val; ELSE RETURN state; END IF; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE my_max(integer) ("
                    + "SFUNC = my_max_sf, STYPE = integer)");
            try (ResultSet rs = stmt.executeQuery("SELECT my_max(val) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(50, rs.getInt(1));
            }
        }
    }

    // ---- Aggregate with HAVING ----

    @Test
    void aggregateInHavingClause() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION hav_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE hav_sum(integer) ("
                    + "SFUNC = hav_sum_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT grp, hav_sum(val) FROM nums GROUP BY grp HAVING hav_sum(val) > 50 ORDER BY grp")) {
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertEquals(120, rs.getInt(2));
                assertFalse(rs.next());
            }
        }
    }

    // ---- SQL-language state function ----

    @Test
    void aggregateWithSqlLanguageFunction() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sql_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "SELECT state + val; $$ LANGUAGE sql");
            stmt.execute("CREATE AGGREGATE sql_sum(integer) ("
                    + "SFUNC = sql_sum_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT sql_sum(val) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(150, rs.getInt(1));
            }
        }
    }

    // ---- CREATE AGGREGATE parsing with many ignored attributes ----

    @Test
    void aggregateWithExtraAttributes() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION ext_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            // This uses extra attributes that should be parsed and ignored
            stmt.execute("CREATE AGGREGATE ext_sum(integer) ("
                    + "SFUNC = ext_sum_sf, STYPE = integer, INITCOND = '0', "
                    + "PARALLEL = safe)");
            try (ResultSet rs = stmt.executeQuery("SELECT ext_sum(val) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(150, rs.getInt(1));
            }
        }
    }

    // ---- Multi-argument aggregate (SFUNC with state + 2 values) ----

    @Test
    void aggregateWithMultipleArguments() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Weighted sum: state + val * weight
            stmt.execute("CREATE TABLE weighted (val integer, weight integer)");
            stmt.execute("INSERT INTO weighted VALUES (10, 2)");
            stmt.execute("INSERT INTO weighted VALUES (20, 3)");
            stmt.execute("INSERT INTO weighted VALUES (5, 4)");
            stmt.execute("CREATE FUNCTION wsum_sf(state integer, v integer, w integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + v * w; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE weighted_sum(integer, integer) ("
                    + "SFUNC = wsum_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT weighted_sum(val, weight) FROM weighted")) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt(1));  // 10*2 + 20*3 + 5*4
            }
        }
    }

    // ---- Aggregate used inside expression ----

    @Test
    void aggregateInExpression() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION expr_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE expr_sum(integer) ("
                    + "SFUNC = expr_sum_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT expr_sum(val) + 10 FROM nums")) {
                assertTrue(rs.next());
                assertEquals(160, rs.getInt(1));  // 150 + 10
            }
        }
    }

    // ---- Aggregate in subquery ----

    @Test
    void aggregateInSubquery() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sub_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE sub_sum(integer) ("
                    + "SFUNC = sub_sum_sf, STYPE = integer, INITCOND = '0')");
            // Use aggregate in subquery to filter rows with val above average-ish threshold
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT val FROM nums WHERE val > (SELECT sub_sum(val) / 5 FROM nums) ORDER BY val")) {
                assertTrue(rs.next());
                assertEquals(40, rs.getInt(1));  // 150/5=30, so 40 and 50 qualify
                assertTrue(rs.next());
                assertEquals(50, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    // ---- FINALFUNC on empty group (no rows processed) ----

    @Test
    void finalfuncCalledOnEmptyGroup() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE empty_ff (val integer)");
            stmt.execute("CREATE FUNCTION ff_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION ff_final(state integer) RETURNS text AS $$ "
                    + "BEGIN RETURN 'result=' || state::text; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE ff_sum(integer) ("
                    + "SFUNC = ff_sum_sf, STYPE = integer, INITCOND = '0', FINALFUNC = ff_final)");
            try (ResultSet rs = stmt.executeQuery("SELECT ff_sum(val) FROM empty_ff")) {
                assertTrue(rs.next());
                // Empty group with INITCOND=0, FINALFUNC should be called on 0
                assertEquals("result=0", rs.getString(1));
            }
        }
    }

    // ---- Aggregate with unquoted numeric INITCOND ----

    @Test
    void aggregateWithUnquotedNumericInitcond() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION uq_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE uq_sum(integer) ("
                    + "SFUNC = uq_sum_sf, STYPE = integer, INITCOND = 0)");
            try (ResultSet rs = stmt.executeQuery("SELECT uq_sum(val) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(150, rs.getInt(1));
            }
        }
    }

    // ---- DISTINCT ----

    @Test
    void aggregateWithDistinct() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE dups (val integer)");
            stmt.execute("INSERT INTO dups VALUES (10)");
            stmt.execute("INSERT INTO dups VALUES (10)");
            stmt.execute("INSERT INTO dups VALUES (20)");
            stmt.execute("INSERT INTO dups VALUES (20)");
            stmt.execute("INSERT INTO dups VALUES (30)");
            stmt.execute("CREATE FUNCTION dist_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE dist_sum(integer) ("
                    + "SFUNC = dist_sum_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT dist_sum(DISTINCT val) FROM dups")) {
                assertTrue(rs.next());
                assertEquals(60, rs.getInt(1));  // 10+20+30 (each once)
            }
        }
    }

    // ---- FILTER clause ----

    @Test
    void aggregateWithFilterClause() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION flt_sum_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE flt_sum(integer) ("
                    + "SFUNC = flt_sum_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT flt_sum(val) FILTER (WHERE grp = 'a') FROM nums")) {
                assertTrue(rs.next());
                assertEquals(30, rs.getInt(1));  // only group 'a': 10+20
            }
        }
    }

    // ---- Single row ----

    @Test
    void aggregateOverSingleRow() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE one_row (val integer)");
            stmt.execute("INSERT INTO one_row VALUES (42)");
            stmt.execute("CREATE FUNCTION one_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE one_sum(integer) ("
                    + "SFUNC = one_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT one_sum(val) FROM one_row")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
            }
        }
    }

    // ---- All NULL inputs ----

    @Test
    void aggregateWithAllNullInputs() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE all_nulls (val integer)");
            stmt.execute("INSERT INTO all_nulls VALUES (NULL)");
            stmt.execute("INSERT INTO all_nulls VALUES (NULL)");
            stmt.execute("INSERT INTO all_nulls VALUES (NULL)");
            stmt.execute("CREATE FUNCTION anull_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN IF val IS NULL THEN RETURN state; ELSE RETURN state + val; END IF; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE anull_sum(integer) ("
                    + "SFUNC = anull_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT anull_sum(val) FROM all_nulls")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));  // INITCOND unchanged
            }
        }
    }

    // ---- State becomes NULL mid-stream ----

    @Test
    void sfuncReturningNullMidStream() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Returns NULL if val > 25, otherwise accumulates
            stmt.execute("CREATE FUNCTION nullify_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN IF val > 25 THEN RETURN NULL; ELSE RETURN COALESCE(state, 0) + val; END IF; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE nullify_sum(integer) ("
                    + "SFUNC = nullify_sf, STYPE = integer, INITCOND = '0')");
            // vals are 10,20,30,40,50. After 30 (>25), state becomes NULL
            try (ResultSet rs = stmt.executeQuery("SELECT nullify_sum(val) FROM nums")) {
                assertTrue(rs.next());
                // 0+10=10, 10+20=30, 30>25 -> NULL, NULL->coalesce(NULL,0)+40=40, 50>25 -> NULL
                assertNull(rs.getObject(1));
            }
        }
    }

    // ---- Aggregate with CASE expression as argument ----

    @Test
    void aggregateWithCaseArgument() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION case_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE case_sum(integer) ("
                    + "SFUNC = case_sf, STYPE = integer, INITCOND = '0')");
            // Only sum values from group 'b' via CASE
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT case_sum(CASE WHEN grp = 'b' THEN val ELSE 0 END) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(120, rs.getInt(1));  // 0+0+30+40+50
            }
        }
    }

    // ---- Aggregate over JOIN ----

    @Test
    void aggregateOverJoin() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE multipliers (grp text, factor integer)");
            stmt.execute("INSERT INTO multipliers VALUES ('a', 2)");
            stmt.execute("INSERT INTO multipliers VALUES ('b', 3)");
            stmt.execute("CREATE FUNCTION join_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE join_sum(integer) ("
                    + "SFUNC = join_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT join_sum(n.val * m.factor) FROM nums n JOIN multipliers m ON n.grp = m.grp")) {
                assertTrue(rs.next());
                // a: 10*2+20*2=60, b: 30*3+40*3+50*3=360 -> total 420
                assertEquals(420, rs.getInt(1));
            }
        }
    }

    // ---- DROP AGGREGATE IF EXISTS on non-existent ----

    @Test
    void dropAggregateIfExistsNonExistent() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Should not throw
            stmt.execute("DROP AGGREGATE IF EXISTS nonexistent(integer)");
        }
    }

    // ---- Aggregate result used with COALESCE ----

    @Test
    void aggregateResultUsableInCoalesce() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION coal_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE coal_sum(integer) ("
                    + "SFUNC = coal_sf, STYPE = integer, INITCOND = '0')");
            // Non-empty table: COALESCE is a pass-through for non-null aggregate result
            try (ResultSet rs = stmt.executeQuery("SELECT COALESCE(coal_sum(val), -1) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(150, rs.getInt(1));
            }
        }
    }

    // ---- Same aggregate on different columns ----

    @Test
    void sameAggregateOnDifferentColumns() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION multi_col_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE multi_col_sum(integer) ("
                    + "SFUNC = multi_col_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT multi_col_sum(val), multi_col_sum(id) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(150, rs.getInt(1));  // sum of val: 10+20+30+40+50
                assertEquals(15, rs.getInt(2));   // sum of id: 1+2+3+4+5
            }
        }
    }

    // ---- Nested aggregate should error ----

    @Test
    void nestedAggregateThrowsError() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION nest_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE nest_sum(integer) ("
                    + "SFUNC = nest_sf, STYPE = integer, INITCOND = '0')");
            assertThrows(SQLException.class, () -> {
                stmt.executeQuery("SELECT nest_sum(nest_sum(val)) FROM nums");
            });
        }
    }

    // ---- Aggregate in CTE ----

    @Test
    void aggregateInCte() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION cte_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE cte_sum(integer) ("
                    + "SFUNC = cte_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery(
                    "WITH totals AS (SELECT grp, cte_sum(val) AS total FROM nums GROUP BY grp) "
                    + "SELECT grp, total FROM totals ORDER BY grp")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals(30, rs.getInt(2));
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertEquals(120, rs.getInt(2));
                assertFalse(rs.next());
            }
        }
    }

    // ---- Aggregate in INSERT...SELECT ----

    @Test
    void aggregateInInsertSelect() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE results (grp text, total integer)");
            stmt.execute("CREATE FUNCTION ins_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE ins_sum(integer) ("
                    + "SFUNC = ins_sf, STYPE = integer, INITCOND = '0')");
            stmt.execute("INSERT INTO results SELECT grp, ins_sum(val) FROM nums GROUP BY grp");
            try (ResultSet rs = stmt.executeQuery("SELECT grp, total FROM results ORDER BY grp")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals(30, rs.getInt(2));
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertEquals(120, rs.getInt(2));
                assertFalse(rs.next());
            }
        }
    }

    // ---- FINALFUNC NOT called when no INITCOND and empty group ----

    @Test
    void finalfuncNotCalledOnEmptyGroupWithoutInitcond() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE empty_noinit (val integer)");
            // FINALFUNC would turn any non-null into 'has_value', but empty+no INITCOND = NULL
            stmt.execute("CREATE FUNCTION noinit_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN IF state IS NULL THEN RETURN val; ELSE RETURN state + val; END IF; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION noinit_ff(state integer) RETURNS text AS $$ "
                    + "BEGIN IF state IS NULL THEN RETURN 'empty'; ELSE RETURN 'has_value'; END IF; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE noinit_agg(integer) ("
                    + "SFUNC = noinit_sf, STYPE = integer, FINALFUNC = noinit_ff)");
            try (ResultSet rs = stmt.executeQuery("SELECT noinit_agg(val) FROM empty_noinit")) {
                assertTrue(rs.next());
                // PG 18: no INITCOND + empty group = NULL, FINALFUNC not called
                assertNull(rs.getObject(1));
            }
        }
    }

    // ---- Aggregate with type cast in argument ----

    @Test
    void aggregateWithTypeCastArg() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION cast_sf(state bigint, val bigint) RETURNS bigint AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE cast_sum(bigint) ("
                    + "SFUNC = cast_sf, STYPE = bigint, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT cast_sum(val::bigint) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(150L, rs.getLong(1));
            }
        }
    }

    // ---- Aggregate with computed expression arg ----

    @Test
    void aggregateWithComputedArg() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION comp_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE comp_sum(integer) ("
                    + "SFUNC = comp_sf, STYPE = integer, INITCOND = '0')");
            // Pass val*2 as argument
            try (ResultSet rs = stmt.executeQuery("SELECT comp_sum(val * 2) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(300, rs.getInt(1));  // (10+20+30+40+50)*2
            }
        }
    }

    // ---- CREATE AGGREGATE return message ----

    @Test
    void createAggregateReturnsNoError() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION msg_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            // Should not throw
            assertDoesNotThrow(() ->
                stmt.execute("CREATE AGGREGATE msg_sum(integer) ("
                        + "SFUNC = msg_sf, STYPE = integer, INITCOND = '0')"));
        }
    }

    // ---- Aggregate in ORDER BY ----

    @Test
    void aggregateInOrderBy() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION ord_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE ord_sum(integer) ("
                    + "SFUNC = ord_sf, STYPE = integer, INITCOND = '0')");
            // Order groups by aggregate result descending
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT grp, ord_sum(val) FROM nums GROUP BY grp ORDER BY ord_sum(val) DESC")) {
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertEquals(120, rs.getInt(2));  // b: 30+40+50 (larger, first)
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals(30, rs.getInt(2));   // a: 10+20
                assertFalse(rs.next());
            }
        }
    }

    // ---- Aggregate over view ----

    @Test
    void aggregateOverView() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE VIEW nums_view AS SELECT * FROM nums WHERE grp = 'b'");
            stmt.execute("CREATE FUNCTION view_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE view_sum(integer) ("
                    + "SFUNC = view_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery("SELECT view_sum(val) FROM nums_view")) {
                assertTrue(rs.next());
                assertEquals(120, rs.getInt(1));  // 30+40+50
            }
        }
    }

    // ---- Aggregate as window function ----

    @Test
    void aggregateAsWindowFunction() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION win_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE win_sum(integer) ("
                    + "SFUNC = win_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT val, win_sum(val) OVER (PARTITION BY grp) FROM nums ORDER BY id")) {
                // Group 'a': sum = 30, Group 'b': sum = 120
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1)); assertEquals(30, rs.getInt(2));
                assertTrue(rs.next());
                assertEquals(20, rs.getInt(1)); assertEquals(30, rs.getInt(2));
                assertTrue(rs.next());
                assertEquals(30, rs.getInt(1)); assertEquals(120, rs.getInt(2));
                assertTrue(rs.next());
                assertEquals(40, rs.getInt(1)); assertEquals(120, rs.getInt(2));
                assertTrue(rs.next());
                assertEquals(50, rs.getInt(1)); assertEquals(120, rs.getInt(2));
                assertFalse(rs.next());
            }
        }
    }

    // ---- HAVING with mixed user-defined + built-in aggregate ----

    @Test
    void havingWithMixedAggregates() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION mix_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE mix_sum(integer) ("
                    + "SFUNC = mix_sf, STYPE = integer, INITCOND = '0')");
            // HAVING uses built-in count, SELECT uses user-defined aggregate
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT grp, mix_sum(val) FROM nums GROUP BY grp HAVING count(*) > 2")) {
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertEquals(120, rs.getInt(2));
                assertFalse(rs.next());  // 'a' has only 2 rows
            }
        }
    }

    // ---- Aggregate on subquery ----

    @Test
    void aggregateOnDerivedTable() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION der_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE der_sum(integer) ("
                    + "SFUNC = der_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT der_sum(doubled) FROM (SELECT val * 2 AS doubled FROM nums) sub")) {
                assertTrue(rs.next());
                assertEquals(300, rs.getInt(1));
            }
        }
    }

    // ---- Boolean state type ----

    @Test
    void aggregateWithBooleanState() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Track whether all values are positive
            stmt.execute("CREATE FUNCTION all_pos_sf(state boolean, val integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN state AND val > 0; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE all_positive(integer) ("
                    + "SFUNC = all_pos_sf, STYPE = boolean, INITCOND = 'true')");
            try (ResultSet rs = stmt.executeQuery("SELECT all_positive(val) FROM nums")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));  // all values 10-50 are positive
            }
            // Add a negative value and recheck
            stmt.execute("INSERT INTO nums VALUES (6, -5, 'c')");
            try (ResultSet rs = stmt.executeQuery("SELECT all_positive(val) FROM nums")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
            }
        }
    }

    // ---- Aggregate in UNION ----

    @Test
    void aggregateInUnion() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION union_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE union_sum(integer) ("
                    + "SFUNC = union_sf, STYPE = integer, INITCOND = '0')");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT union_sum(val) FROM nums WHERE grp = 'a' "
                    + "UNION ALL "
                    + "SELECT union_sum(val) FROM nums WHERE grp = 'b' "
                    + "ORDER BY 1")) {
                assertTrue(rs.next());
                assertEquals(30, rs.getInt(1));   // a: 10+20
                assertTrue(rs.next());
                assertEquals(120, rs.getInt(1));  // b: 30+40+50
                assertFalse(rs.next());
            }
        }
    }

    // ---- Aggregate persists across statements in same session ----

    @Test
    void aggregatePersistsAcrossStatements() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION persist_sf(state integer, val integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN state + val; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE AGGREGATE persist_sum(integer) ("
                    + "SFUNC = persist_sf, STYPE = integer, INITCOND = '0')");
        }
        // New statement, same connection scope — aggregate should still exist
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT persist_sum(val) FROM nums")) {
                assertTrue(rs.next());
                assertEquals(150, rs.getInt(1));
            }
        }
    }
}
