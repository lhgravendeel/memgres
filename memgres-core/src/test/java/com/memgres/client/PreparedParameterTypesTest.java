package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a driver is told the parameters of a prepared statement are.
 *
 * <p>PostgreSQL answers Describe with the types it resolved the parameters to while reading the
 * statement, so {@code ParameterMetaData} names the column's type rather than a placeholder. A
 * tool that reads it to decide how to bind — jOOQ, some Spring and MyBatis paths — binds wrongly
 * when every parameter comes back as text.
 *
 * <p>The type a parameter takes is whatever the statement says it has to be: the column it is
 * compared with, the column it is written into, the operand beside it, or the argument position
 * of a function whose signature settles it. Where the statement says nothing, the parameter stays
 * unresolved and text is what a driver reads — the same answer PostgreSQL gives.
 *
 * <p>Some positions settle on nothing <em>because</em> the name means too many things, and that is
 * an answer too: PostgreSQL refuses the statement rather than describing a parameter it did not
 * resolve.
 */
class PreparedParameterTypesTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TYPE pp_enum AS ENUM ('a','b')");
            st.execute("CREATE DOMAIN pp_dom AS int CHECK (VALUE > 0)");
            st.execute("CREATE TABLE pp_t ("
                    + "i int PRIMARY KEY, b bigint, si smallint, n numeric(10,2), r real,"
                    + " d double precision, s text, v varchar(20), ch char(4), bo boolean,"
                    + " dt date, ts timestamp, tz timestamptz, tm time, u uuid, jb jsonb,"
                    + " by bytea, ip inet, ia int[], ta text[], e pp_enum, dm pp_dom)");
            st.execute("CREATE TABLE pp_c (id int, t_i int, note text)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** The types a driver reads for the parameters of {@code sql}, comma-separated. */
    private static String describe(String sql) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ParameterMetaData md = ps.getParameterMetaData();
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= md.getParameterCount(); i++) {
                if (i > 1) sb.append(", ");
                sb.append(md.getParameterTypeName(i));
            }
            return sb.toString();
        }
    }

    private static SQLException refusalOf(String sql) {
        return assertThrows(SQLException.class, () -> describe(sql), sql);
    }

    // ------------------------------------------------------------ SECTION A
    // A parameter compared with a column is of that column's type.

    @Test
    void aParameterTakesTheTypeOfTheColumnItIsComparedWith() throws Exception {
        assertEquals("int4", describe("SELECT * FROM pp_t WHERE i = ?"));
        assertEquals("int8", describe("SELECT * FROM pp_t WHERE b = ?"));
        assertEquals("int2", describe("SELECT * FROM pp_t WHERE si = ?"));
        assertEquals("numeric", describe("SELECT * FROM pp_t WHERE n = ?"));
        assertEquals("float4", describe("SELECT * FROM pp_t WHERE r = ?"));
        assertEquals("float8", describe("SELECT * FROM pp_t WHERE d = ?"));
        assertEquals("bool", describe("SELECT * FROM pp_t WHERE bo = ?"));
        assertEquals("date", describe("SELECT * FROM pp_t WHERE dt = ?"));
        assertEquals("timestamp", describe("SELECT * FROM pp_t WHERE ts = ?"));
        assertEquals("timestamptz", describe("SELECT * FROM pp_t WHERE tz = ?"));
        assertEquals("uuid", describe("SELECT * FROM pp_t WHERE u = ?"));
        assertEquals("jsonb", describe("SELECT * FROM pp_t WHERE jb = ?"));
        assertEquals("inet", describe("SELECT * FROM pp_t WHERE ip = ?"));
        assertEquals("_int4", describe("SELECT * FROM pp_t WHERE ia = ?"));
        assertEquals("pp_enum", describe("SELECT * FROM pp_t WHERE e = ?"));
    }

    /**
     * A varchar column carries no comparison operator of its own — text's are used for it — and
     * a domain carries the operators of the type it is built on, so a parameter beside either
     * resolves to the type the operator PostgreSQL chose actually takes.
     */
    @Test
    void aParameterTakesTheTypeTheChosenOperatorTakes() throws Exception {
        assertEquals("text", describe("SELECT * FROM pp_t WHERE v = ?"));
        assertEquals("int4", describe("SELECT * FROM pp_t WHERE dm = ?"));
        assertEquals("int4", describe("SELECT * FROM pp_t WHERE dm > ?"));
    }

    /** ANY takes the array over the type, where IN takes a list of them. */
    @Test
    void anyTakesAnArrayAndInTakesAList() throws Exception {
        assertEquals("_int4", describe("SELECT * FROM pp_t WHERE i = ANY(?)"));
        assertEquals("_int4", describe("SELECT * FROM pp_t WHERE i = ALL(?)"));
        assertEquals("int4, int4", describe("SELECT * FROM pp_t WHERE i IN (?, ?)"));
        assertEquals("int4, int4", describe("SELECT * FROM pp_t WHERE i BETWEEN ? AND ?"));
    }

    /** A subscript is not compared with what it indexes: an array is indexed by an integer. */
    @Test
    void aSubscriptIsAnIntegerAndNotTheArray() throws Exception {
        assertEquals("int4", describe("SELECT ia[?] FROM pp_t"));
    }

    @Test
    void aRowIsComparedElementByElement() throws Exception {
        assertEquals("int4, text", describe("SELECT * FROM pp_t WHERE (i, s) = (?, ?)"));
    }

    @Test
    void aParameterIsResolvedThroughEveryClauseThatSaysWhatItIs() throws Exception {
        assertEquals("int4", describe("SELECT CASE WHEN i = ? THEN 1 ELSE 2 END FROM pp_t"));
        assertEquals("int8", describe("SELECT count(*) FROM pp_t GROUP BY i HAVING count(*) > ?"));
        assertEquals("int4", describe("SELECT greatest(i, ?) FROM pp_t"));
        assertEquals("int4", describe("SELECT nullif(i, ?) FROM pp_t"));
        assertEquals("int4", describe("SELECT sum(i + ?) FROM pp_t"));
        assertEquals("int4", describe("SELECT lag(i, ?) OVER (ORDER BY i) FROM pp_t"));
        assertEquals("int4", describe("INSERT INTO pp_c (id) SELECT ?"));
        assertEquals("int8, int8", describe("SELECT * FROM pp_t ORDER BY i LIMIT ? OFFSET ?"));
        assertEquals("int8, int8",
                describe("SELECT * FROM pp_t ORDER BY i OFFSET ? ROWS FETCH FIRST ? ROWS ONLY"));
    }

    /** A parameter written with a cast is of the type it was cast to, declared types included. */
    @Test
    void aCastSettlesTheParameterInsideIt() throws Exception {
        assertEquals("int8", describe("SELECT * FROM pp_t WHERE i = ?::bigint"));
        assertEquals("pp_enum", describe("SELECT * FROM pp_t WHERE e = ?::pp_enum"));
        assertEquals("uuid", describe("SELECT * FROM pp_t WHERE u = ?::uuid"));
    }

    /** A signature settles an argument position; the preferred type of its category settles it. */
    @Test
    void aFunctionSignatureSettlesItsArguments() throws Exception {
        assertEquals("float8", describe("SELECT abs(?)"));
        assertEquals("numeric", describe("SELECT round(?, 2)"));
        assertEquals("text", describe("SELECT upper(?)"));
        assertEquals("text", describe("SELECT length(?)"));
    }

    /** What a moment is added to is a length of time, and what is taken from one is a moment. */
    @Test
    void arithmeticOnAnInstantSettlesTheOtherOperand() throws Exception {
        assertEquals("interval", describe("SELECT ts + ? FROM pp_t"));
        assertEquals("timestamp", describe("SELECT ts - ? FROM pp_t"));
    }

    /** Where the statement says nothing, the parameter keeps the type it had. */
    @Test
    void aParameterWithNothingToGoOnStaysText() throws Exception {
        assertEquals("text", describe("SELECT ?"));
        assertEquals("text, text, text", describe("SELECT ?, ?, ?"));
        assertEquals("text, text", describe("SELECT substring(s from ? for ?) FROM pp_t"));
    }

    // ------------------------------------------------------------ SECTION B
    // A statement that resolves to no one call is refused, not described.

    @Test
    void aNameThatMeansMoreThanOneThingIsRefused() {
        assertEquals("42725", refusalOf("SELECT sum(?) FROM pp_t").getSQLState());
        assertEquals("42725", refusalOf("SELECT to_char(?, 'YYYY')").getSQLState());
        assertEquals("42725", refusalOf("SELECT * FROM generate_series(?, ?)").getSQLState());
        assertEquals("42725", refusalOf("SELECT dt + ? FROM pp_t").getSQLState());
    }

    /** A signature written over "whatever was passed" has nothing to read a parameter's type from. */
    @Test
    void aParameterWithNoTypeToTakeIsRefused() {
        SQLException e = refusalOf("SELECT concat(s, ?) FROM pp_t");
        assertEquals("42P18", e.getSQLState());
        assertTrue(e.getMessage().contains("$1"), e.getMessage());
    }

    /** A comparison a parameter leaves without an operator is refused before anything runs. */
    @Test
    void aComparisonWithNoOperatorIsRefused() {
        // A subquery is read on its own, so nothing outside settles a parameter in its output:
        // it is text by then, and there is no integer = text.
        assertEquals("42883", refusalOf("SELECT i FROM pp_t WHERE i = (SELECT ?)").getSQLState());
        // ts - ? is the time between two moments, and no moment is comparable with a length of one.
        assertEquals("42883", refusalOf("SELECT * FROM pp_t WHERE ts > ts - ?").getSQLState());
    }

    /** A kind of value no signature takes there. */
    @Test
    void argumentsOfAKindTheNameDoesNotTakeAreRefused() {
        assertEquals("42883",
                refusalOf("SELECT * FROM pp_t WHERE (i, b) OVERLAPS (?, ?)").getSQLState());
    }

    /**
     * A parameter left standing where the statement has no place for it.
     *
     * <p>A driver turns every {@code ?} into a parameter, so the jsonb existence operator written
     * as {@code ?} becomes one too and the statement has two parameters where it meant one
     * operator. Dropping the leftover silently accepted a statement that means nothing.
     */
    @Test
    void aParameterWithNowhereToStandIsASyntaxError() {
        SQLException e = refusalOf("SELECT * FROM pp_t WHERE jb ? ?");
        assertEquals("42601", e.getSQLState());
        assertTrue(e.getMessage().contains("$1"), e.getMessage());
    }
}
