package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a value a type cannot read is answered with.
 *
 * <p>A client is told which of its values is wrong and which type could not take it. Three
 * different answers stood in for that: an internal error, where the parse failure was let out
 * unhandled and reported as though the server had gone wrong; a quiet substitution, where the
 * value was taken as zero or the call answered NULL; and a complaint naming whichever type
 * happened to do the reading rather than the one the parameter was declared as.
 */
class ArgumentTypeErrorsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
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

    /** Asserts that {@code sql} is refused as bad input for {@code type}. */
    private static void badInputFor(String type, String value, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> scalar(sql), "should have been refused: " + sql);
        assertEquals("22P02", e.getSQLState(), sql);
        assertEquals("ERROR: invalid input syntax for type " + type + ": \"" + value + "\"",
                e.getMessage() == null ? null : e.getMessage().split("\n")[0], sql);
    }

    // ------------------------------------------------------- the advisory lock keys

    /**
     * An advisory lock key was read with a parser whose failure nobody caught, so a key that was
     * not a number came back as an internal error — which says nothing about the value and gives
     * a client nothing to act on.
     */
    @Test
    void anAdvisoryLockKeyThatIsNotANumberIsBadInput() {
        for (String fn : new String[]{"pg_advisory_lock", "pg_advisory_unlock", "pg_try_advisory_lock",
                "pg_advisory_lock_shared", "pg_advisory_xact_lock", "pg_try_advisory_xact_lock",
                "pg_advisory_unlock_shared"}) {
            badInputFor("bigint", "x", "SELECT " + fn + "('x')");
        }
        // The two-key form takes an integer apiece, so that is the type it names.
        badInputFor("integer", "x", "SELECT pg_advisory_lock('x', 'y')");
    }

    /** And a key that is one still locks and unlocks, in either form. */
    @Test
    void anAdvisoryLockStillWorks() throws Exception {
        assertEquals("", scalar("SELECT pg_advisory_lock(42)"));
        assertEquals("true", scalar("SELECT pg_advisory_unlock(42)::text"));
        assertEquals("", scalar("SELECT pg_advisory_lock('42')"));
        assertEquals("true", scalar("SELECT pg_advisory_unlock(42)::text"));
        assertEquals("true", scalar("SELECT pg_try_advisory_lock(1, 2)::text"));
        assertEquals("true", scalar("SELECT pg_advisory_unlock(1, 2)::text"));
    }

    // ------------------------------------------------------- arguments that were evaluated and dropped

    /**
     * These functions have nothing to do with their argument — memgres has no other backend to
     * signal, no seed to keep, one role to name — but the argument still has a type, and a value
     * that is not of it was accepted as though it were.
     */
    @Test
    void anArgumentThatIsNotUsedIsStillOfItsType() {
        badInputFor("double precision", "x", "SELECT pg_sleep('x')");
        badInputFor("double precision", "x", "SELECT setseed('x')");
        badInputFor("integer", "x", "SELECT pg_terminate_backend('x')");
        badInputFor("integer", "x", "SELECT pg_cancel_backend('x')");
        badInputFor("integer", "x", "SELECT pg_blocking_pids('x')");
        badInputFor("oid", "x", "SELECT pg_get_userbyid('x')");
        SQLException e = assertThrows(SQLException.class, () -> scalar("SELECT pg_sleep_for('x')"));
        assertEquals("22007", e.getSQLState());
    }

    /** The values those functions do take still reach them. */
    @Test
    void theArgumentsThoseFunctionsTakeStillWork() throws Exception {
        assertEquals("", scalar("SELECT pg_sleep(0)"));
        assertEquals("", scalar("SELECT setseed(0.5)"));
        assertEquals("", scalar("SELECT pg_sleep_for(interval '0 seconds')"));
        assertEquals("false", scalar("SELECT pg_terminate_backend(1)::text"));
        assertEquals("memgres", scalar("SELECT pg_get_userbyid(10)"));
    }

    // ------------------------------------------------------- substr's position

    /**
     * {@code substring} has a (text, text) form, where the second argument is a pattern to match
     * with. {@code substr} has no such form — its second argument is a position — so treating one
     * that is not a number as a pattern answered NULL for a call PostgreSQL refuses.
     */
    @Test
    void substrTakesAPositionAndSubstringTakesAPattern() throws Exception {
        badInputFor("integer", "x", "SELECT substr('abc', 'x')");
        badInputFor("integer", "x", "SELECT substr('abc', 'x', 'y')");
        assertEquals("oob", scalar("SELECT substring('foobar', 'o.b')"));
        assertEquals("bcd", scalar("SELECT substr('abcdef', 2, 3)"));
        assertEquals("bcdef", scalar("SELECT substr('abcdef', 2)"));
        assertEquals("bcd", scalar("SELECT substring('abcdef' from 2 for 3)"));
        assertEquals("oob", scalar("SELECT substring('foobar' from '%#\"o_b#\"%' for '#')"));
    }

    // ------------------------------------------------------- the type that could not read it

    /** A narrow numeric type names itself rather than the one whose reader it borrowed. */
    @Test
    void theTypeNamedIsTheOneThatCouldNotReadIt() {
        badInputFor("smallint", "x", "SELECT 'x'::smallint");
        badInputFor("real", "x", "SELECT 'x'::real");
        badInputFor("oid", "x", "SELECT 'x'::oid");
        badInputFor("xid", "x", "SELECT 'x'::xid");
        badInputFor("integer", "x", "SELECT 'x'::int");
        badInputFor("bigint", "x", "SELECT 'x'::bigint");
    }

    /** A transaction ID that could not be read used to come back as transaction zero. */
    @Test
    void anUnreadableTransactionIdIsNotTransactionZero() throws Exception {
        assertEquals("100", scalar("SELECT '100'::xid::text"));
        SQLException e = assertThrows(SQLException.class, () -> scalar("SELECT 'x'::xid"));
        assertEquals("22P02", e.getSQLState());
    }

    // ------------------------------------------------------- the range an OID covers

    /**
     * An OID is an unsigned 32-bit number, so it reaches 4294967295 and reads a negative as the
     * value that far below zero wraps to. Reading one as a signed integer refused every OID above
     * two billion as out of range and kept a negative one negative, neither of which an OID can be.
     */
    @Test
    void anOidIsUnsigned() throws Exception {
        assertEquals("4294967295", scalar("SELECT '4294967295'::oid::text"));
        assertEquals("2147483648", scalar("SELECT '2147483648'::oid::text"));
        assertEquals("4294967295", scalar("SELECT '-1'::oid::text"));
        // The ones the catalogs hand out are unchanged, and still compare as numbers.
        assertEquals("16384", scalar("SELECT '16384'::oid::text"));
        assertEquals("0", scalar("SELECT '0'::oid::text"));
        assertEquals("true", scalar("SELECT ('16384'::oid = 16384)::text"));
        // Past the top of the range it is the value the type cannot hold, not one it cannot read.
        SQLException e = assertThrows(SQLException.class, () -> scalar("SELECT '4294967296'::oid"));
        assertEquals("22003", e.getSQLState());
    }

    // ------------------------------------------------------- what the refusal must not reach

    /** generate_series still says what it says about two arguments it cannot type. */
    @Test
    void generateSeriesStillReportsAnUntypedPair() throws Exception {
        assertEquals("5", scalar("SELECT count(*)::text FROM generate_series(1, 5)"));
        SQLException e = assertThrows(SQLException.class,
                () -> scalar("SELECT * FROM generate_series('x', 'y')"));
        assertEquals("42725", e.getSQLState());
    }

    /** A count argument is reported against integer, whatever width read it on the way there. */
    @Test
    void aCountArgumentIsReportedAgainstInteger() {
        badInputFor("integer", "x", "SELECT lpad('a', 'x')");
        badInputFor("integer", "x", "SELECT repeat('a', 'x')");
        badInputFor("integer", "x", "SELECT left('abc', 'x')");
    }

    /** Whitespace around a number is not what makes it unreadable. */
    @Test
    void surroundingSpaceIsStillANumber() throws Exception {
        assertEquals("42", scalar("SELECT ' 42 '::bigint::text"));
    }
}
