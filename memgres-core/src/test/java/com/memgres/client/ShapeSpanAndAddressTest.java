package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A shape, a span and an address are values, and their text is only how they print.
 *
 * <p>Each of these types has a reader of its own, and the reader has to consume the whole
 * literal: a box written with three corners is not a box, and a near-number is not a number. The
 * reader runs on the way into a column too, so what a table holds is the shape rather than the
 * characters -- which is what makes a box equal to the same box written the other way round.
 * Comparison is the type's own as well: two circles by the area they enclose, two lines by their
 * coefficients scaled, two paths by how many points they hold, and coordinates to PostgreSQL's
 * own tolerance rather than to the last bit.
 *
 * <p>A span carries its fields in the widths they are stored in and says so when they will not
 * fit, and each interval style writes a sign the way its own reader will read it back. An
 * encoding argument decides what a string of bytes spells, and the codecs are PostgreSQL's own
 * rather than the JDK's.
 */
class ShapeSpanAndAddressTest {

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

    private static String one(String sql) throws SQLException {
        List<String> rows = rows(sql);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) out.add(rs.getString(1));
            return out;
        }
    }

    private static void run(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** A literal names one shape, and everything written has to be part of it. */
    @Test
    void aGeometricLiteralIsReadRightThrough() {
        assertEquals("22P02", stateOf("SELECT '(1,2),(3,4),(5,6)'::box"));
        assertEquals("22P02", stateOf("SELECT '{1,2,3,4}'::line"));
        assertEquals("22P02", stateOf("SELECT '<(1,2),3,4>'::circle"));
        assertEquals("22P02", stateOf("SELECT '(1,2)x'::point"));
        assertEquals("22P02", stateOf("SELECT '((1,2))'::point"));
        assertEquals("22P02", stateOf("SELECT '[(0,0),(1,1)]'::box"));
        assertEquals("22P02", stateOf("SELECT '<(1,2),-3>'::circle"));
        assertEquals("22P02", stateOf("SELECT '[]'::path"));
    }

    /** A near-number is the caller's mistake, not this engine's. */
    @Test
    void aNearNumberIsNotANumber() {
        assertEquals("22P02", stateOf("SELECT '[(1e,2),(3,4)]'::lseg"));
        assertEquals("22P02", stateOf("SELECT '((1e,2),(3,4),(5,6))'::polygon"));
        assertEquals("22P02", stateOf("SELECT '<(1e,2),3>'::circle"));
        assertEquals("22P02", stateOf("SELECT '(1d,2)'::point"));
    }

    /** Every spelling PostgreSQL accepts is accepted, whitespace and all. */
    @Test
    void everySpellingOfAShapeIsRead() throws SQLException {
        assertEquals("(1,2)", one("SELECT ' (1,2) '::point::text"));
        assertEquals("(1,2)", one("SELECT '1,2'::point::text"));
        assertEquals("[(0,0),(1,1)]", one("SELECT '(0,0),(1,1)'::lseg::text"));
        assertEquals("(1,1),(0,0)", one("SELECT '0,0,1,1'::box::text"));
        assertEquals("((0,0),(1,1),(2,2))", one("SELECT '((0,0),(1,1),(2,2))'::path::text"));
        assertEquals("((0,0),(1,1),(2,2))", one("SELECT '(0,0),(1,1),(2,2)'::polygon::text"));
        assertEquals("<(1,2),3>", one("SELECT '((1,2),3)'::circle::text"));
        assertEquals("<(1,2),3>", one("SELECT '1,2,3'::circle::text"));
        assertEquals("(NaN,0)", one("SELECT '(NaN,0)'::point::text"));
        assertEquals("(Infinity,0)", one("SELECT '(inf,0)'::point::text"));
    }

    /**
     * A column holds the shape, so the same box written either way round is one value and
     * compares equal to itself.
     */
    @Test
    void aColumnHoldsTheShapeAndNotTheCharacters() throws SQLException {
        run("DROP TABLE IF EXISTS zzss_g CASCADE");
        run("CREATE TABLE zzss_g (id int, b box)");
        run("INSERT INTO zzss_g VALUES (1, '(0,0),(1,1)'), (4, '(0,1),(1,0)')");
        assertEquals(List.of("(1,1),(0,0)", "(1,1),(0,0)"),
                rows("SELECT b::text FROM zzss_g ORDER BY id"));
        assertEquals(List.of("true", "true"),
                rows("SELECT (b = '(1,1),(0,0)'::box)::text FROM zzss_g ORDER BY id"));
        run("DROP TABLE zzss_g");
    }

    /** Each type says what "the same" means for it. */
    @Test
    void eachShapeHasItsOwnEquality() throws SQLException {
        assertEquals("true", one("SELECT ('<(0,0),1>'::circle = '<(9,9),1>'::circle)::text"));
        assertEquals("true", one("SELECT ('{1,1,0}'::line = '{2,2,0}'::line)::text"));
        assertEquals("false", one("SELECT ('[(0,0),(1,1)]'::path < '[(0,0),(2,2)]'::path)::text"));
        assertEquals("true", one("SELECT ('(NaN,0)'::point ~= '(NaN,0)'::point)::text"));
    }

    /** A line has three spellings, and two points nearer than the tolerance name no line. */
    @Test
    void aLineIsWrittenTheWayPostgresqlWritesIt() throws SQLException {
        assertEquals("{-1,0,0}", one("SELECT '[(0,0),(0,1)]'::line::text"));
        assertEquals("22P02", stateOf("SELECT '[(0,0),(0,0)]'::line"));
        assertEquals("22P02", stateOf("SELECT '[(0,0),(0.0000005,0)]'::line"));
        // The constructor names it a bad argument where the reader names it bad syntax.
        assertEquals("22023", stateOf("SELECT line(point(0,0), point(0,0))"));
    }

    /** The tolerance is PostgreSQL's, and it is used where PostgreSQL uses it. */
    @Test
    void oneToleranceServesEveryPredicate() throws SQLException {
        assertEquals("<(1.3333333333333333,0.6666666666666666),1.308077670527261>",
                one("SELECT circle('((0,0),(2,0),(2,2))'::polygon)::text"));
        assertEquals("0", one("SELECT slope(point '(1,0)', point '(0,0)')::text"));
        assertEquals("Infinity", one("SELECT slope(point '(0,0)', point '(0.0000005,5)')::text"));
        // Only an exactly zero divisor is a division by zero.
        assertEquals("(1000000,1000000)",
                one("SELECT (point '(1,1)' / point '(0.000001,0)')::text"));
    }

    /** PostgreSQL declares no operator whose own operand is xml. */
    @Test
    void xmlHasNoOperatorsOfItsOwn() throws SQLException {
        assertEquals("42883", stateOf("SELECT '<a/>'::xml = '<a/>'::xml"));
        assertEquals("42883", stateOf("SELECT '<a/>'::xml < '<b/>'::xml"));
        assertEquals("42883",
                stateOf("SELECT count(DISTINCT x) FROM (VALUES ('<a/>'::xml)) v(x)"));
        // It still reaches the polymorphic rows, which is how this one resolves.
        assertEquals("a<a/>", one("SELECT 'a' || '<a/>'::xml"));
    }

    /** Whether nothing is a document is not a question with a yes or a no. */
    @Test
    void nothingIsNeitherADocumentNorNotOne() throws SQLException {
        assertNull(one("SELECT (NULL::xml IS DOCUMENT)::text"));
        assertEquals("true", one("SELECT ('<a/>'::xml IS DOCUMENT)::text"));
    }

    /** A declaration is written only where it says something. */
    @Test
    void xmlrootWritesADeclarationOnlyWhenItSaysSomething() throws SQLException {
        assertEquals("<a/>", one("SELECT xmlroot('<a/>'::xml, version '1.0')::text"));
        assertEquals("<?xml version=\"2.0\"?><a/>",
                one("SELECT xmlroot('<a/>'::xml, version '2.0')::text"));
        assertEquals("<?xml version=\"1.0\" standalone=\"yes\"?><a/>",
                one("SELECT xmlroot('<a/>'::xml, version '1.0', standalone yes)::text"));
    }

    /** An xml value goes into a forest as the markup it is, and a name is escaped in upper hex. */
    @Test
    void xmlforestEmbedsWhatIsAlreadyXml() throws SQLException {
        assertEquals("<f><root><row/></root></f>",
                one("SELECT xmlforest('<root><row/></root>'::xml AS f)::text"));
        assertEquals("<foo att_x003C_r=\"a&amp;b\"/>",
                one("SELECT xmlelement(name foo, xmlattributes('a&b' as \"att<r\"))::text"));
        assertEquals("42601",
                stateOf("SELECT xmlelement(name foo, xmlattributes(1 as a, 2 as a))"));
    }

    /** XMLTABLE reads its column clauses rather than skipping past them. */
    @Test
    void xmltableHonoursItsColumnClauses() throws SQLException {
        run("DROP TABLE IF EXISTS zzss_x CASCADE");
        run("CREATE TABLE zzss_x (x xml)");
        run("INSERT INTO zzss_x VALUES"
                + " ('<root><row><a>1</a></row><row><a>2</a></row></root>')");
        assertEquals(List.of("1", "2"), rows("SELECT t.n::text FROM zzss_x d,"
                + " xmltable('/root/row' PASSING d.x COLUMNS n FOR ORDINALITY,"
                + " a int PATH 'a') t ORDER BY t.n"));
        run("DELETE FROM zzss_x");
        run("INSERT INTO zzss_x VALUES ('<root><row/></root>')");
        assertEquals("42", one("SELECT t.a::text FROM zzss_x d, xmltable('/root/row'"
                + " PASSING d.x COLUMNS a int PATH 'a' DEFAULT 42) t"));
        assertEquals("22004", stateOf("SELECT t.a FROM zzss_x d, xmltable('/root/row'"
                + " PASSING d.x COLUMNS a int PATH 'a' NOT NULL) t"));
        run("DELETE FROM zzss_x");
        run("INSERT INTO zzss_x VALUES ('<root><row><a>zz</a></row></root>')");
        assertEquals("22P02", stateOf("SELECT t.a FROM zzss_x d, xmltable('/root/row'"
                + " PASSING d.x COLUMNS a int PATH 'a') t"));
        run("DROP TABLE zzss_x");
    }

    /** A span too large to hold is refused rather than wrapped. */
    @Test
    void aSpanIsHeldInTheWidthsItIsStoredIn() throws SQLException {
        assertEquals("22008", stateOf("SELECT make_interval(years => 200000000)"));
        assertEquals("22008", stateOf("SELECT make_interval(weeks => 400000000)"));
        assertEquals("22008", stateOf("SELECT make_interval(years => 178956970, months => 8)"));
        // The infinity marker is on a field a caller cannot reach, so every month count is usable.
        assertEquals("178956970 years 7 mons",
                one("SELECT make_interval(months => 2147483647)::text"));
        assertEquals("true", one("SELECT (interval '1000000000 mons' > interval '1 day')::text"));
        assertEquals("true", one("SELECT (interval '2000000000 mons'"
                + " > interval '1000000000 mons')::text"));
    }

    /** A value is built out of every argument, so an argument that is nothing leaves nothing. */
    @Test
    void theMakeFamilyIsStrict() throws SQLException {
        assertNull(one("SELECT make_interval(NULL)::text"));
        assertNull(one("SELECT make_date(NULL, 1, 1)::text"));
        assertNull(one("SELECT make_date(2020, NULL, 1)::text"));
        assertNull(one("SELECT make_time(NULL, 1, 1)::text"));
        assertNull(one("SELECT make_timestamptz(2020, 1, 1, 0, 0, 0, NULL)::text"));
        assertNull(one("SELECT to_number('123', NULL)::text"));
    }

    /** Each style writes a sign the way its own reader will read it back. */
    @Test
    void eachIntervalStyleRoundTripsItsSign() throws SQLException {
        run("SET intervalstyle = 'iso_8601'");
        assertEquals("PT-0.5S", one("SELECT interval '-0.5 seconds'::text"));
        assertEquals("PT-1H-0.5S", one("SELECT interval '-1 hour -0.5 seconds'::text"));
        assertEquals("P-1DT-0.25S", one("SELECT interval '-1 day -0.25 seconds'::text"));
        assertEquals("P-1Y-2M", one("SELECT interval '-1 year -2 mons'::text"));
        run("SET intervalstyle = 'sql_standard'");
        assertEquals("-0-1", one("SELECT interval '-1 month'::text"));
        assertEquals("-1-1", one("SELECT interval '-13 months'::text"));
        assertEquals("0-10", one("SELECT interval '1 year -2 months'::text"));
        assertEquals("-1 2:03:00", one("SELECT interval '-1 day -2 hours -3 minutes'::text"));
        assertEquals("+0-0 +1 -2:00:00", one("SELECT interval '1 day -2 hours'::text"));
        assertEquals("-1-0 -1 -1:00:00", one("SELECT interval '-1 year -1 day -1 hour'::text"));
        run("SET intervalstyle = 'postgres_verbose'");
        assertEquals("@ 1 mon ago", one("SELECT interval '-1 month'::text"));
        assertEquals("@ 1 day 2 hours ago", one("SELECT interval '-1 day -2 hours'::text"));
        assertEquals("@ 1 day -2 hours", one("SELECT interval '1 day -2 hours'::text"));
        run("SET intervalstyle = 'postgres'");
    }

    /**
     * The style decides how an ambiguously signed literal is read as well as how one is
     * printed: under the SQL standard one sign stands for every field that follows it.
     */
    @Test
    void theStyleDecidesHowASignIsRead() throws SQLException {
        assertEquals("-10 mons", one("SELECT interval '-1 year 2 months'::text"));
        run("SET intervalstyle = 'sql_standard'");
        assertEquals("-1-2", one("SELECT interval '-1 year 2 months'::text"));
        run("SET intervalstyle = 'postgres'");
    }

    /** A range created by CREATE TYPE is a type, and answers to the statements that name one. */
    @Test
    void aRangeCreatedByCreateTypeIsAType() throws SQLException {
        run("DROP TYPE IF EXISTS zzss_r CASCADE");
        run("DROP TYPE IF EXISTS zzss_r2 CASCADE");
        run("CREATE TYPE zzss_r AS RANGE (SUBTYPE = int)");
        run("DROP TABLE IF EXISTS zzss_rt CASCADE");
        run("CREATE TABLE zzss_rt (i int)");
        run("ALTER TABLE zzss_rt ADD COLUMN b zzss_r");
        assertEquals("zzss_r", one("SELECT format_type(atttypid, atttypmod) FROM pg_attribute"
                + " WHERE attrelid = 'zzss_rt'::regclass AND attname = 'b'"));
        run("ALTER TYPE zzss_r RENAME TO zzss_r2");
        // The multirange keeps the name it was created with: renaming a range renames the range.
        assertEquals(List.of("zzss_r2", "zzss_r_multirange"),
                rows("SELECT typname FROM pg_type WHERE typname IN"
                        + " ('zzss_r','zzss_r2','zzss_r_multirange','zzss_r2_multirange')"
                        + " ORDER BY typname"));
        assertEquals("[1,5)", one("SELECT '[1,5)'::zzss_r2::text"));
        run("DROP TABLE zzss_rt");
        run("DROP TYPE zzss_r2");
    }

    /** An address is read by the spellings PostgreSQL lists, and by no others. */
    @Test
    void anAddressIsReadByItsSpellings() throws SQLException {
        assertEquals("10.0.0.0/8", one("SELECT '10/8'::inet::text"));
        assertEquals("10.1.0.0/16", one("SELECT '10.1/16'::inet::text"));
        assertEquals("22P02", stateOf("SELECT '192.168.1.5 '::inet"));
        assertEquals("22P02", stateOf("SELECT ' 192.168.1.5'::inet"));
        assertEquals("08:00:2b:01:02:03", one("SELECT '0800-2b01-0203'::macaddr::text"));
        assertEquals("08:00:2b:01:02:03:04:05",
                one("SELECT '08002b:0102030405'::macaddr8::text"));
        assertEquals("08:00:2b:01:02:03:04:05",
                one("SELECT '08002b-0102030405'::macaddr8::text"));
        assertEquals("08:00:2b:01:02:03:04:05",
                one("SELECT '0800-2b01-0203-0405'::macaddr8::text"));
        // A group written with a sign is a number out of range, not a spelling nobody knows.
        assertEquals("22003", stateOf("SELECT '00:11:22:33:44:-6'::macaddr"));
    }

    /** Minus one is the one negative mask length that names a length: all of it. */
    @Test
    void aMaskLengthOfMinusOneIsTheWholeAddress() throws SQLException {
        assertEquals("192.168.1.5/32",
                one("SELECT set_masklen('192.168.1.5/24'::inet, -1)::text"));
        assertEquals("32", one("SELECT masklen(set_masklen('192.168.1.5/24'::inet, -1))::text"));
    }

    /** The operator exists for the pair; it is the values that will not go together. */
    @Test
    void combiningAddressesOfDifferentWidthsIsAValueError() {
        assertEquals("22023", stateOf("SELECT '192.168.1.5'::inet & '::1'::inet"));
        assertEquals("22023", stateOf("SELECT '192.168.1.5'::inet | '::1'::inet"));
        assertEquals("22023", stateOf("SELECT '192.168.1.5'::inet - '::1'::inet"));
    }

    /** base64 is written a line at a time, and read with the padding that says how it ends. */
    @Test
    void base64IsWrittenAndReadAsPostgresqlWritesIt() throws SQLException {
        assertEquals("77", one("SELECT length(encode(decode(repeat('61', 57),"
                + " 'hex'), 'base64'))::text"));
        assertEquals("1353", one("SELECT length(encode(decode(repeat('61', 1000),"
                + " 'hex'), 'base64'))::text"));
        assertEquals("61626364", one("SELECT encode(decode(E'YWJj\\nZA==', 'base64'), 'hex')"));
        assertEquals("22023", stateOf("SELECT decode('YWJjZA','base64')"));
        assertEquals("22023", stateOf("SELECT decode('!!!!','base64')"));
    }

    /** hex ignores the whitespace between digits and names a character that is not one. */
    @Test
    void hexIgnoresWhitespaceAndNamesABadDigit() throws SQLException {
        assertEquals("1234", one("SELECT encode(decode('12 34', 'hex'), 'hex')"));
        assertEquals("22023", stateOf("SELECT decode('xy','hex')"));
        assertEquals("22023", stateOf("SELECT decode('123','hex')"));
    }

    /** escape writes only the zero byte, the high-bit bytes and the backslash. */
    @Test
    void escapeWritesOnlyWhatHasToBeWritten() throws SQLException {
        assertEquals("c3a9", one("SELECT encode(decode('é', 'escape'), 'hex')"));
        assertEquals("5", one("SELECT length(encode('\\x0102030405'::bytea, 'escape'))::text"));
        assertEquals("\\000", one("SELECT encode('\\x00'::bytea,'escape')"));
        assertEquals("\\377", one("SELECT encode('\\xff'::bytea,'escape')"));
    }

    /** The encoding named is the one that says what the bytes spell. */
    @Test
    void theEncodingNamedIsTheOneUsed() throws SQLException {
        assertEquals("é", one("SELECT convert_from('\\xe9'::bytea, 'LATIN1')"));
        assertEquals("\\x616263", one("SELECT convert_to('abc','SQL_ASCII')::text"));
        assertEquals("c3a9",
                one("SELECT encode(convert('\\xe9'::bytea, 'LATIN1', 'UTF8'),'hex')"));
        assertEquals("KarACl", one("SELECT to_ascii('Karél', 'LATIN1')"));
        assertEquals("4", one("SELECT length('jose', 'UTF8')::text"));
        assertNull(one("SELECT convert('abc'::bytea, NULL, 'UTF8')::text"));
        assertNull(one("SELECT encode('\\x0102'::bytea, NULL)"));
        assertNull(one("SELECT decode('abcd', NULL)::text"));
    }

    /** A name that names no encoding is reported the way its call site reports it. */
    @Test
    void anUnknownEncodingIsReportedByItsCallSite() {
        assertEquals("22023", stateOf("SELECT convert('abc'::bytea, 'BOGUSENC', 'UTF8')"));
        assertEquals("22023", stateOf("SELECT convert('abc'::bytea, 'UTF8', 'BOGUSENC')"));
        assertEquals("42704", stateOf("SELECT to_ascii('abc', 'NOSUCHENC')"));
        assertEquals("22023", stateOf("SELECT length('jose', 'BOGUS')"));
    }

    /** A digest is bytes, so the documented hex idiom spells the digest and not the hex of it. */
    @Test
    void aDigestIsBytes() throws SQLException {
        assertEquals("bytea", one("SELECT pg_typeof(sha256('abc'::bytea))::text"));
        assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                one("SELECT encode(sha256('abc'::bytea),'hex')"));
    }

    /** The index is checked before an offset is worked out from it. */
    @Test
    void aBitIndexIsCheckedBeforeItIsUsed() throws SQLException {
        assertEquals("2202E", stateOf("SELECT get_bit('\\xff'::bytea, -1)"));
        assertEquals("2202E", stateOf("SELECT get_bit('\\x00ff'::bytea, -1)"));
        assertEquals("2202E", stateOf("SELECT get_bit('\\xff'::bytea, 8)"));
        // get_byte is declared over an int alone, so a wider index names no function to call.
        assertEquals("42883", stateOf("SELECT get_byte('\\x0102'::bytea, 4294967296)"));
        assertNull(one("SELECT get_byte('\\x0102'::bytea, NULL)::text"));
        assertNull(one("SELECT get_bit('\\xff'::bytea, NULL)::text"));
    }
}
