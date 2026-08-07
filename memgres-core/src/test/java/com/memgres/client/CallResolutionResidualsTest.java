package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The rest of what deciding a call needs: how many arguments a signature accepts, what the
 * statement says an argument is, when a name cannot be chosen, and what a NULL argument makes of
 * the call.
 *
 * <p>Arity was read as an exact count, so a signature whose tail carries defaults was matched only
 * by a call passing every one of them — and a call passing fewer went unjudged entirely. An
 * argument's type was read from a literal, a cast or a base table's column and from nothing else,
 * so an array constructor and a scalar subquery said nothing about themselves. A name that no
 * argument could choose between was reported only where some argument had been written without a
 * type. And a routine memgres answers without reading its arguments answered anyway when the
 * argument was NULL, though every one of them is strict.
 */
class CallResolutionResidualsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE crr_t (n numeric, i int, b bigint, s text)");
            st.execute("INSERT INTO crr_t VALUES (2, 2, 2, 'ab')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @AfterEach
    void releaseLocks() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("SELECT pg_advisory_unlock_all()");
        }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void refused(String state, String message, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> scalar(sql), "should be refused: " + sql);
        assertEquals(state, e.getSQLState(), sql);
        assertEquals("ERROR: " + message,
                e.getMessage() == null ? null : e.getMessage().split("\n")[0], sql);
    }

    private static void runs(String sql) {
        assertDoesNotThrow(() -> scalar(sql), sql);
    }

    // ------------------------------------------------------- how many arguments a signature takes

    /**
     * A signature whose tail carries defaults is still that signature's when a call passes fewer.
     * Judging arity as an exact count left every such call unjudged, so a wrong type reached it.
     */
    @Test
    void aSignatureWithDefaultsIsStillJudgedWhenFewerArePassed() {
        refused("42883", "function pg_terminate_backend(numeric) does not exist",
                "SELECT pg_terminate_backend(1.5)");
        // And the calls that do pass are unaffected, whichever of its forms they use.
        runs("SELECT pg_terminate_backend(1)");
        runs("SELECT pg_terminate_backend(1, 100)");
        runs("SELECT lpad('abc', 5)");
        runs("SELECT lpad('abc', 5, 'x')");
        runs("SELECT array_to_string(ARRAY[1,2], ',')");
        runs("SELECT array_to_string(ARRAY[1,2], ',', 'n')");
        runs("SELECT regexp_replace('abc', 'b', 'x')");
        runs("SELECT regexp_replace('abc', 'b', 'x', 'g')");
        runs("SELECT round(1.5)");
        runs("SELECT substr('abcdef', 2)");
    }

    // ------------------------------------------------------- what the statement says an argument is

    /** An array constructor is of the type of its elements, and says so. */
    @Test
    void anArrayConstructorSaysWhatItIs() {
        refused("42883", "function array_fill(integer, numeric[]) does not exist",
                "SELECT array_fill(1, ARRAY[2.5])");
        refused("42883", "function array_fill(integer, numeric[], integer[]) does not exist",
                "SELECT array_fill(1, ARRAY[2.5], ARRAY[1])");
        runs("SELECT array_fill(1, ARRAY[2])");
        runs("SELECT array_fill(1, ARRAY[2], ARRAY[1])");
        runs("SELECT array_length(ARRAY[1,2], 1)");
        runs("SELECT array_to_string(ARRAY['a','b'], ',')");
    }

    /** So does a column a subquery or a CTE produced, and a scalar subquery's one column. */
    @Test
    void aDerivedColumnSaysWhatItIs() {
        refused("42883", "function pg_advisory_lock(numeric) does not exist",
                "SELECT pg_advisory_lock(n) FROM (SELECT 2::numeric AS n) t");
        refused("42883", "function pg_advisory_lock(numeric) does not exist",
                "WITH c AS (SELECT 2::numeric AS n) SELECT pg_advisory_lock(n) FROM c");
        refused("42883", "function pg_advisory_lock(numeric) does not exist",
                "SELECT pg_advisory_lock((SELECT n FROM crr_t))");
        // The same written against a column that does reach still runs.
        runs("SELECT pg_advisory_lock(i) FROM (SELECT 2::int AS i) t");
        runs("WITH c AS (SELECT 2::int AS i) SELECT pg_advisory_lock(i) FROM c");
        runs("SELECT pg_advisory_lock((SELECT i FROM crr_t))");
        runs("SELECT left('abcde', (SELECT 4))");
    }

    /**
     * A column the engine could not type is a text one as far as the catalogue is concerned, and
     * so is every column that really is text. That answer is left alone: reading it as a
     * declaration said a number was a string and resolved the call against the wrong type.
     */
    @Test
    void aColumnTheEngineCouldNotTypeIsLeftAlone() throws Exception {
        assertEquals("14", scalar("SELECT val FROM (SELECT 3 + 4 + 7 AS val) sub WHERE val > 10"));
    }

    // ------------------------------------------------------- a name that cannot be chosen

    /**
     * A call reaching more than one signature and matching none of them exactly is not a call
     * PostgreSQL can choose, whether or not any argument was written without a type.
     */
    @Test
    void aCallThatChoosesNothingIsRefused() {
        SQLException e = assertThrows(SQLException.class, () -> scalar("SELECT to_hex(10::smallint)"));
        assertEquals("42725", e.getSQLState());
        assertTrue(e.getMessage().contains("function to_hex(smallint) is not unique"), e.getMessage());
        // The forms it does declare are unaffected.
        runs("SELECT to_hex(10::int)");
        runs("SELECT to_hex(10::bigint)");
    }

    /**
     * A signature written over "whatever was passed" is not one of two answers to choose between:
     * array_agg is declared over both anyarray and anynonarray, and which one a call means follows
     * from whether its argument is an array.
     */
    @Test
    void aPolymorphicNameIsStillChoosable() {
        runs("SELECT array_agg(i) FROM crr_t");
        runs("SELECT array_agg(s) FROM crr_t");
        runs("SELECT array_agg(b) FROM crr_t");
        runs("SELECT max(i) FROM crr_t");
        runs("SELECT coalesce(1::int, 2::int)");
        runs("SELECT greatest(1::int, 2::bigint)");
    }

    /** A type PostgreSQL does not have is one its signatures say nothing about. */
    @Test
    void aTypeOfMemgresOwnIsNotJudgedAgainstPostgresSignatures() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS hstore");
            st.execute("DROP TABLE IF EXISTS crr_h");
            st.execute("CREATE TABLE crr_h (h hstore)");
            st.execute("INSERT INTO crr_h VALUES ('a=>1'::hstore)");
            st.execute("UPDATE crr_h SET h['b'] = '2'");
        }
        assertEquals("2", scalar("SELECT h -> 'b' FROM crr_h"));
    }

    // ------------------------------------------------------- a NULL argument

    /**
     * A routine memgres answers without reading its arguments is still strict, so a NULL argument
     * is the whole call. Left to the implementation the answer was whatever it was going to be
     * anyway — false, the empty array, the one role, the empty string a void function prints.
     */
    @Test
    void aNullArgumentToAStrictRoutineIsTheWholeCall() throws Exception {
        for (String sql : new String[]{
                "SELECT pg_advisory_lock(NULL)", "SELECT pg_advisory_unlock(NULL)",
                "SELECT pg_try_advisory_lock(NULL)", "SELECT pg_advisory_xact_lock(NULL)",
                "SELECT pg_advisory_lock(NULL, NULL)", "SELECT pg_advisory_lock(1, NULL)",
                "SELECT pg_sleep(NULL)", "SELECT pg_sleep_for(NULL)", "SELECT setseed(NULL)",
                "SELECT pg_terminate_backend(NULL)", "SELECT pg_get_userbyid(NULL)",
                "SELECT pg_blocking_pids(NULL)", "SELECT pg_column_size(NULL)",
                "SELECT pg_table_is_visible(NULL)", "SELECT current_schemas(NULL)",
                "SELECT make_time(NULL, NULL, NULL)", "SELECT random_normal(NULL, NULL)",
                "SELECT txid_snapshot_xmin(NULL)", "SELECT pg_encoding_to_char(NULL)"}) {
            assertNull(scalar(sql), sql);
        }
    }

    /** The same routines still answer when they are given something. */
    @Test
    void thoseRoutinesStillAnswerWhenGivenAValue() throws Exception {
        assertEquals("", scalar("SELECT pg_advisory_lock(1)"));
        assertEquals("true", scalar("SELECT pg_advisory_unlock(1)::text"));
        assertEquals("01:02:03", scalar("SELECT make_time(1, 2, 3)::text"));
        assertEquals("UTF8", scalar("SELECT pg_encoding_to_char(6)"));
        assertEquals("{public}", scalar("SELECT current_schemas(false)::text"));
        assertEquals("{}", scalar("SELECT int4multirange()::text"));
    }

    /**
     * A multirange holds ranges, and NULL is not one. A lone NULL is the variadic tail itself
     * being NULL, which makes the call NULL; a NULL beside other ranges is a member the value
     * cannot hold, and dropping it built a multirange the caller never asked for.
     */
    @Test
    void aNullRangeIsNoMemberOfAMultirange() throws Exception {
        assertNull(scalar("SELECT int4multirange(NULL)"));
        refused("22004", "multirange values cannot contain null members",
                "SELECT int4multirange(NULL, int4range(1,5))");
        refused("22004", "multirange values cannot contain null members",
                "SELECT nummultirange(numrange(1,5), NULL)");
        assertEquals("{[1,5)}", scalar("SELECT int4multirange(int4range(1,5))::text"));
    }

    // ------------------------------------------------------- a series over numbers with a fraction

    /**
     * A bound or a step with a fraction is the numeric form of the series. Truncating it to a
     * bigint answered a series of whole numbers for a series that has none — and the same call
     * written in FROM already answered the fractions.
     */
    @Test
    void aSeriesOverFractionsKeepsThem() throws Exception {
        assertEquals("1.5", scalar("SELECT generate_series(1.5, 3.5)::text"));
        assertEquals("1.5,2.5,3.5",
                scalar("SELECT string_agg(g::text, ',') FROM generate_series(1.5, 3.5) g"));
        assertEquals("1.0,1.5,2.0",
                scalar("SELECT string_agg(g::text, ',') FROM generate_series(1.0, 2.0, 0.5) g"));
        assertEquals("numeric", scalar("SELECT pg_typeof(generate_series(1.5, 3.5))::text"));
        // A series over whole numbers is unchanged.
        assertEquals("1,2,3", scalar("SELECT string_agg(g::text, ',') FROM generate_series(1, 3) g"));
        assertEquals("3,2,1", scalar("SELECT string_agg(g::text, ',') FROM generate_series(3, 1, -1) g"));
    }
}
