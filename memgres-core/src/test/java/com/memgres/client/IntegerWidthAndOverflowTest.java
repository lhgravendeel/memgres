package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integer arithmetic is done in the wider of its two operand types, and checked against that
 * type's bounds.
 *
 * <p>There was an arm for two smallints and an arm for two integers and nothing for a mixed pair,
 * so a smallint beside a wider integer fell through to the float8 path and {@code 5::int2 /
 * 2::int4} answered 2.5 where PostgreSQL answers 2.
 *
 * <p>Beside it: a sign in front of a numeric literal is part of the literal, as it is in
 * PostgreSQL's own grammar. Negating the literal afterwards read {@code 2147483648} first, which
 * is a bigint, so {@code (-2147483648) - 1} answered a number instead of raising.
 */
class IntegerWidthAndOverflowTest {

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
        return assertThrows(SQLException.class, () -> scalar(sql), sql);
    }

    /** The result type is the wider of the two operands, and integer division stays integer. */
    @Test
    void theWiderOperandDecidesTheType() throws Exception {
        assertEquals("2", scalar("SELECT 5::int2 / 2::int4"));
        assertEquals("3", scalar("SELECT 7 / 2"));
        assertEquals("3", scalar("SELECT 7::int8 / 2::int2"));
        assertEquals("smallint", scalar("SELECT pg_typeof(5::int2 + 2::int2)::text"));
        assertEquals("integer", scalar("SELECT pg_typeof(5::int2 + 2::int4)::text"));
        assertEquals("bigint", scalar("SELECT pg_typeof(5::int2 + 2::int8)::text"));
        assertEquals("bigint", scalar("SELECT pg_typeof(5::int4 + 2::int8)::text"));
    }

    /** And the bounds checked are that type's own. */
    @Test
    void overflowIsRaisedInTheResultType() {
        assertEquals("22003", refused("SELECT 32767::int2 + 1::int2").getSQLState());
        assertTrue(refused("SELECT 32767::int2 + 1::int2").getMessage().contains("smallint out of range"));
        assertEquals("22003", refused("SELECT (-32768)::int2 * (-1)::int2").getSQLState());
        assertEquals("22003", refused("SELECT 2147483647::int4 + 1::int2").getSQLState());
        assertTrue(refused("SELECT 2147483647::int4 + 1::int2").getMessage().contains("integer out of range"));
        assertEquals("22003", refused("SELECT 9223372036854775807 + 1").getSQLState());
        assertTrue(refused("SELECT 9223372036854775807 + 1").getMessage().contains("bigint out of range"));
    }

    /** Division by zero is still division by zero, not an overflow. */
    @Test
    void divisionByZeroKeepsItsOwnError() {
        assertEquals("22012", refused("SELECT 1/0").getSQLState());
        assertEquals("22012", refused("SELECT 1::int2/0::int2").getSQLState());
    }

    /** A sign in front of a numeric literal belongs to the literal. */
    @Test
    void aSignBelongsToTheLiteral() throws Exception {
        assertEquals("integer", scalar("SELECT pg_typeof(-2147483648)::text"));
        assertEquals("22003", refused("SELECT (-2147483648) - 1").getSQLState());
        assertEquals("22003", refused("SELECT (-2147483648) * (-1)").getSQLState());
        assertEquals("22003", refused("SELECT abs(-2147483648)").getSQLState());
        // Flipped either way, so a double negation folds to the bigint it needs to be.
        assertEquals("2147483648", scalar("SELECT -(-2147483648)"));
        // A sign in front of anything else is still the operator.
        assertEquals("-5", scalar("SELECT -(2+3)"));
    }
}
