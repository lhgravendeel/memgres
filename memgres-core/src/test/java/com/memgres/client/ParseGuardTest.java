package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Values taken from SQL text, read with a guard.
 *
 * <p>Each of these used to reach the client as {@code XX000 Internal error} carrying a Java
 * exception message, which tells an application the database is broken rather than that its SQL
 * was wrong. The worst of them did more than that: a {@code $N} number too large to be one threw
 * out of the extended-protocol Describe, and the escape left the connection permanently one
 * response ahead of its client — every later statement handed back the previous statement's
 * result set, silently.
 */
class ParseGuardTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static SQLException refused(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> scalar(sql), sql);
        assertNotEquals("XX000", e.getSQLState(), sql + " → " + e.getMessage());
        assertFalse(e.getMessage().contains("Internal error"), sql + " → " + e.getMessage());
        return e;
    }

    /** An exponent marker with no digits behind it is not a number. */
    @Test
    void anExponentNeedsItsDigits() throws Exception {
        for (String sql : new String[]{"SELECT 1e", "SELECT 1.5e", "SELECT 1E+"}) {
            SQLException e = refused(sql);
            assertEquals("42601", e.getSQLState(), sql);
            assertTrue(e.getMessage().contains("trailing junk after numeric literal"), sql);
        }
        assertEquals("100000", scalar("SELECT 1e5"));
        assertEquals("0.0015", scalar("SELECT 1.5e-3"));
    }

    /** A parameter number too large to be one is named, and the connection survives it. */
    @Test
    void aParameterNumberTooLargeIsNamed() throws Exception {
        SQLException e = refused("SELECT $99999999999999");
        assertEquals("42601", e.getSQLState());
        assertTrue(e.getMessage().contains("parameter number too large"));
        // The connection must still be in step: the next statement gets its own answer.
        assertEquals("ok", scalar("SELECT 'ok' AS marker"));
        assertEquals("2", scalar("SELECT 2"));
    }

    /** NULLIF and COALESCE are grammar, so the wrong argument count is a syntax error. */
    @Test
    void theSpecialFormsNeedTheirArguments() throws Exception {
        assertEquals("42601", refused("SELECT NULLIF(1)").getSQLState());
        assertEquals("42601", refused("SELECT COALESCE()").getSQLState());
        assertEquals("1", scalar("SELECT NULLIF(1,2)"));
        assertNull(scalar("SELECT NULLIF(1,1)"));
        assertEquals("2", scalar("SELECT COALESCE(NULL,2)"));
    }

    /** A SIMILAR TO pattern the engine cannot compile is an invalid regular expression. */
    @Test
    void similarToReportsAnInvalidPattern() throws Exception {
        SQLException unbalanced = refused("SELECT 'abc' SIMILAR TO 'a('");
        assertEquals("2201B", unbalanced.getSQLState());
        assertEquals("ERROR: invalid regular expression: parentheses () not balanced",
                unbalanced.getMessage());
        SQLException quantifier = refused("SELECT 'abc' SIMILAR TO '*abc'");
        assertEquals("2201B", quantifier.getSQLState());
        assertEquals("ERROR: invalid regular expression: quantifier operand invalid",
                quantifier.getMessage());
        assertEquals("2201B", refused("SELECT 'abc' SIMILAR TO 'a)'").getSQLState());
        // A brace that begins no quantifier is the character it is, not a repetition.
        assertEquals("t", scalar("SELECT 'a{b}' SIMILAR TO 'a{b}'"));
        assertEquals("t", scalar("SELECT 'aaa' SIMILAR TO 'a{2,3}'"));
        assertEquals("t", scalar("SELECT 'abc' SIMILAR TO '(a|b)%'"));
    }

    /** A Julian day beyond what a long holds is a date out of range. */
    @Test
    void aJulianDayOutOfRangeIsADateError() {
        SQLException e = refused("SELECT 'J999999999999999999999999'::date");
        assertEquals("22008", e.getSQLState());
        assertTrue(e.getMessage().contains("date/time field value out of range"));
    }

    /** COST and ROWS take a positive number, and anything else is refused where it stands. */
    @Test
    void routineCostIsValidated() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE FUNCTION zz_pg_fn() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$");
        }
        try {
            assertEquals("22023", refused("ALTER FUNCTION zz_pg_fn() COST -1").getSQLState());
            assertTrue(refused("ALTER FUNCTION zz_pg_fn() COST -1").getMessage()
                    .contains("COST must be positive"));
            assertEquals("22023", refused("ALTER FUNCTION zz_pg_fn() ROWS -5").getSQLState());
            assertEquals("42601", refused("ALTER FUNCTION zz_pg_fn() COST abc").getSQLState());
            assertEquals("22023", refused("ALTER FUNCTION zz_pg_fn() COST 0").getSQLState());
            try (Statement st = conn.createStatement()) {
                st.execute("ALTER FUNCTION zz_pg_fn() COST 5");
            }
        } finally {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP FUNCTION IF EXISTS zz_pg_fn()");
            }
        }
    }

    /** Writing an expression to a generated column is refused, not a cast failure. */
    @Test
    void writingToAGeneratedColumnIsRefused() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TEMP TABLE zz_pg_gen (a int, g int GENERATED ALWAYS AS (a * 2) STORED)");
            st.execute("INSERT INTO zz_pg_gen (a) VALUES (1)");
        }
        SQLException e = refused("UPDATE zz_pg_gen SET g = a + 1 RETURNING g");
        assertEquals("428C9", e.getSQLState());
        assertTrue(e.getMessage().contains("can only be updated to DEFAULT"));
    }

    /** generate_series is strict: a NULL bound or step produces no rows. */
    @Test
    void generateSeriesIsStrict() throws Exception {
        assertEquals("0", scalar("SELECT count(*) FROM generate_series(NULL::numeric, 10::numeric, 1)"));
        assertEquals("0", scalar("SELECT count(*) FROM generate_series(1, NULL::int, 1)"));
        assertEquals("0", scalar("SELECT count(*) FROM generate_series(1, 10, NULL::int)"));
        assertEquals("10", scalar("SELECT count(*) FROM generate_series(1::numeric, 10::numeric, 1)"));
    }
}
