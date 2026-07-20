package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for fix/bit-bytea-money: H22, H23, H24, L1, L2, L6.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BitByteaMoneyTest {

    private static Memgres memgres;
    private static Connection conn;

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

    private String query(String sql) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected a row for: " + sql);
            return rs.getString(1);
        }
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private String getSqlState(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    // ========== H22: bit/varbit ==========

    @Test @Order(1)
    void bitShiftLeftIsBinary() throws SQLException {
        // B'1010' << 1 should give 0100, not decimal shift
        assertEquals("0100", query("SELECT B'1010' << 1"));
    }

    @Test @Order(2)
    void bitShiftRightIsBinary() throws SQLException {
        assertEquals("0101", query("SELECT B'1010' >> 1"));
    }

    @Test @Order(3)
    void intToBitCast() throws SQLException {
        assertEquals("0101", query("SELECT 5::bit(4)"));
    }

    @Test @Order(4)
    void negativeToBitCastTwosComplement() throws SQLException {
        assertEquals("11111111", query("SELECT (-1)::bit(8)"));
    }

    @Test @Order(5)
    void bitTruncation() throws SQLException {
        assertEquals("101", query("SELECT B'10101'::bit(3)"));
    }

    @Test @Order(6)
    void bitPadding() throws SQLException {
        assertEquals("10000", query("SELECT B'10'::bit(5)"));
    }

    @Test @Order(7)
    void varbitMaxLengthEnforcement() {
        assertEquals("22001", getSqlState("SELECT B'10101'::varbit(3)"));
    }

    @Test @Order(8)
    void invalidBitDigitRejected() {
        assertEquals("22P02", getSqlState("SELECT '102'::bit(3)"));
    }

    @Test @Order(9)
    void octetLengthBitString() throws SQLException {
        assertEquals("2", query("SELECT octet_length(B'1010101010101010')"));
    }

    @Test @Order(10)
    void bitLengthBitString() throws SQLException {
        assertEquals("4", query("SELECT bit_length(B'1010')"));
    }

    @Test @Order(11)
    void bitToIntegerCast() throws SQLException {
        assertEquals("5", query("SELECT B'0101'::integer"));
    }

    // ========== H23: bytea ==========

    @Test @Order(20)
    void byteaEscapeFormatParsing() throws SQLException {
        // '\000\047'::bytea should parse octal escapes to raw bytes
        String result = query("SELECT '\\000\\047'::bytea");
        assertEquals("\\x0027", result);
    }

    @Test @Order(21)
    void md5OfBytea() throws SQLException {
        // md5 should hash raw bytes, not the text form
        assertEquals("93b885adfe0da089cdf634904fd59f71", query("SELECT md5('\\x00'::bytea)"));
    }

    @Test @Order(22)
    void encodeEscapeSingleBackslash() throws SQLException {
        // encode escape should use single backslash for non-printable
        String result = query("SELECT encode('\\x000141'::bytea, 'escape')");
        assertEquals("\\000\\001A", result);
    }

    @Test @Order(23)
    void hexWithWhitespace() throws SQLException {
        // PG allows whitespace in hex bytea literals
        String result = query("SELECT '\\xde ad'::bytea");
        assertEquals("\\xdead", result);
    }

    @Test @Order(24)
    void ltrimBytea() throws SQLException {
        String result = query("SELECT ltrim('\\x001234'::bytea, '\\x00'::bytea)");
        assertEquals("\\x1234", result);
    }

    // ========== H24: money ==========

    @Test @Order(30)
    void moneyWithSeparators() throws SQLException {
        assertEquals("$1,234.56", query("SELECT '$1,234.56'::money"));
    }

    @Test @Order(31)
    void moneyParenthesisNegation() throws SQLException {
        assertEquals("-$123.45", query("SELECT '($123.45)'::money"));
    }

    @Test @Order(32)
    void moneyAddition() throws SQLException {
        assertEquals("$30.00", query("SELECT '$10.00'::money + '$20.00'::money"));
    }

    @Test @Order(33)
    void moneyDivMoneyReturnsFloat8() throws SQLException {
        // money / money should return float8, not money
        String result = query("SELECT '$100.00'::money / '$25.00'::money");
        // Wire protocol may format 4.0 as "4" — check it's not money-formatted
        assertFalse(result.contains("$"), "Expected float8, not money");
        assertEquals(4.0, Double.parseDouble(result), 0.001);
    }

    @Test @Order(34)
    void moneyDivNumericReturnsMoney() throws SQLException {
        assertEquals("$25.00", query("SELECT '$100.00'::money / 4"));
    }

    @Test @Order(35)
    void invalidMoneyInput() {
        assertEquals("22P02", getSqlState("SELECT 'abc'::money"));
    }

    @Test @Order(36)
    void moneySum() throws SQLException {
        exec("CREATE TABLE money_test_h24 (amount money)");
        exec("INSERT INTO money_test_h24 VALUES ('$10.00'), ('$20.00'), ('$30.00')");
        String result = query("SELECT sum(amount) FROM money_test_h24");
        assertEquals("$60.00", result);
        exec("DROP TABLE money_test_h24");
    }

    @Test @Order(37)
    void float8ToMoneyCastRejected() {
        // PG does not allow direct float8::money cast
        assertEquals("42846", getSqlState("SELECT 1234.56::float8::money"));
    }

    // ========== L1: SQLSTATE fixes ==========

    @Test @Order(40)
    void getByteOutOfRange2202E() {
        assertEquals("2202E", getSqlState("SELECT get_byte('\\x01'::bytea, 5)"));
    }

    @Test @Order(41)
    void setByteOutOfRange2202E() {
        assertEquals("2202E", getSqlState("SELECT set_byte('\\x01'::bytea, 5, 0)"));
    }

    @Test @Order(42)
    void getBitOutOfRange2202E() {
        assertEquals("2202E", getSqlState("SELECT get_bit('\\x01'::bytea, 20)"));
    }

    @Test @Order(43)
    void setBitOutOfRange2202E() {
        assertEquals("2202E", getSqlState("SELECT set_bit('\\x01'::bytea, 20, 1)"));
    }

    @Test @Order(44)
    void negativeSubstringLength22011() {
        assertEquals("22011", getSqlState("SELECT substring('hello' from 1 for -1)"));
    }

    @Test @Order(45)
    void chrZero54000() {
        assertEquals("54000", getSqlState("SELECT chr(0)"));
    }

    // ========== L2: bytea substring negative-start clamping ==========

    @Test @Order(50)
    void byteaSubstringNegativeStart() throws SQLException {
        // substring(bytea from -1 for 3): start=-1, count=3, end=-1+3=2
        // effective range after clamping: [0, 1) in 0-based = first byte only
        String result = query("SELECT substring('\\x123456'::bytea from -1 for 3)");
        assertEquals("\\x12", result);
    }

    @Test @Order(51)
    void byteaSubstringZeroStart() throws SQLException {
        // substring(bytea from 0 for 3): start=0, count=3, end=0+3=3
        // effective range: [0, 2) in 0-based = first 2 bytes
        String result = query("SELECT substring('\\x0102030405'::bytea from 0 for 3)");
        assertEquals("\\x0102", result);
    }

    // ========== L6: multidim array casts and literal inference ==========

    @Test @Order(60)
    void multidimArrayCastToInt8() throws SQLException {
        String result = query("SELECT ARRAY[[1,2],[3,4]]::int8[]");
        assertEquals("{{1,2},{3,4}}", result);
    }

    @Test @Order(61)
    void arrayLiteralWithNaN() throws SQLException {
        String result = query("SELECT ARRAY[1.5,'NaN']::float8[]");
        assertEquals("{1.5,NaN}", result);
    }
}
