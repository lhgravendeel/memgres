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
}
