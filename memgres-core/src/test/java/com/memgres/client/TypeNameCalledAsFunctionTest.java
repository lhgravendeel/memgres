package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A type name written like a function call, which PostgreSQL reads as a cast.
 *
 * <p>PostgreSQL resolves {@code typename(x)} as a coercion whenever no pg_proc row of that name
 * matches, for every type whose name is a legal function name — the built-ins, a user's domain, a
 * user's enum. memgres implemented that for a handful of names and answered 42883 for the rest,
 * so {@code int4('42')}, {@code bool('t')}, {@code uuid(...)}, {@code jsonb(...)} and
 * {@code tsvector(...)} were refused while the CAST spelling beside them worked.
 *
 * <p>Every answer here was measured against PostgreSQL 18 on localhost:5432/memgrestest, and the
 * refusals as much as the values: a name PostgreSQL's grammar reads as a type ({@code numeric},
 * {@code varchar}, {@code integer}) is not enabled, and neither is a call whose argument
 * PostgreSQL has no conversion from — {@code date(42)}, {@code uuid(42)},
 * {@code int4(point '(1,2)')}.
 */
class TypeNameCalledAsFunctionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE DOMAIN fes_dom AS integer");
        exec("CREATE TYPE fes_enum AS ENUM ('a','b')");
        exec("CREATE TABLE fes_t (i integer, t text, d date, p point, n numeric)");
        exec("INSERT INTO fes_t VALUES (7, '42', DATE '2020-01-02', point '(1,2)', 3.7)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    /** The type name the driver is told the single column has. */
    private static String columnType(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.getMetaData().getColumnTypeName(1);
        }
    }

    private static SQLException failure(String sql) {
        return assertThrows(SQLException.class, () -> {
            try (Statement st = conn.createStatement()) { st.execute(sql); }
        }, "expected " + sql + " to fail");
    }

    private static void assertFails(String sql, String sqlState, String messageStart)
            throws SQLException {
        SQLException e = failure(sql);
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        String first = e.getMessage().split("\n")[0];
        assertTrue(first.contains(messageStart),
                sql + " -> expected \"" + messageStart + "\" in \"" + first + "\"");
    }

    // ---- the numeric type names -------------------------------------------------

    @Test
    void integerTypeNamesResolveAsCasts() throws Exception {
        assertEquals("42", scalar("SELECT int4(42)"));
        assertEquals("43", scalar("SELECT int4(42.6)"));
        assertEquals("42", scalar("SELECT int4('42')"));
        assertNull(scalar("SELECT int4(NULL)"));
        assertEquals("42", scalar("SELECT int2(42)"));
        assertEquals("42", scalar("SELECT int8(42)"));
        assertEquals("1.5", scalar("SELECT float4(1.5)"));
        assertEquals("1.5", scalar("SELECT float8(1.5)"));
        assertEquals("42", scalar("SELECT oid('42')"));
        assertEquals("t", scalar("SELECT bool('t')"));
    }

    @Test
    void characterTypeNamesResolveAsCasts() throws Exception {
        assertEquals("ab", scalar("SELECT bpchar('ab')"));
        assertEquals("ab", scalar("SELECT name('ab')"));
    }

    @Test
    void otherBuiltInTypeNamesResolveAsCasts() throws Exception {
        assertEquals("2020-01-02", scalar("SELECT date(TIMESTAMP '2020-01-02 03:04:05')"));
        assertEquals("1.2.3.4", scalar("SELECT inet('1.2.3.4')"));
        assertEquals("1.2.3.0/24", scalar("SELECT cidr('1.2.3.0/24')"));
        assertEquals("08:00:2b:01:02:03", scalar("SELECT macaddr('08:00:2b:01:02:03')"));
        assertEquals("11111111-1111-1111-1111-111111111111",
                scalar("SELECT uuid('11111111-1111-1111-1111-111111111111')"));
        assertEquals("{\"a\": 1}", scalar("SELECT jsonb('{\"a\":1}')::text"));
        assertEquals("'a' 'b'", scalar("SELECT tsvector('a b')::text"));
        assertEquals("101", scalar("SELECT varbit('101')::text"));
        assertEquals("100", scalar("SELECT xid('100')::text"));
        assertEquals("0/16B3748", scalar("SELECT pg_lsn('0/16B3748')::text"));
        assertEquals("1 2", scalar("SELECT oidvector('1 2')::text"));
        assertEquals("616263", scalar("SELECT encode(bytea('abc'), 'hex')"));
        assertEquals("<a/>", scalar("SELECT xml('<a/>')::text"));
    }

    /** void prints nothing at all, whatever it is handed. */
    @Test
    void voidCallAnswersWithNothing() throws Exception {
        assertEquals("", scalar("SELECT void('x')"));
    }

    // ---- the type each call answers in -----------------------------------------

    @Test
    void aCoercionColumnCarriesTheTypeThatWasWritten() throws Exception {
        assertEquals("integer", scalar("SELECT pg_typeof(int4('42'))::text"));
        assertEquals("boolean", scalar("SELECT pg_typeof(bool('t'))::text"));
        assertEquals("date", scalar("SELECT pg_typeof(date(TIMESTAMP '2020-01-02'))::text"));
        assertEquals("jsonb", scalar("SELECT pg_typeof(jsonb('{}'))::text"));
        assertEquals("uuid",
                scalar("SELECT pg_typeof(uuid('11111111-1111-1111-1111-111111111111'))::text"));
        assertEquals("tsvector", scalar("SELECT pg_typeof(tsvector('a b'))::text"));
        // and the driver is told the same thing before any row arrives
        assertEquals("int4", columnType("SELECT int4('42')"));
        assertEquals("bool", columnType("SELECT bool('t')"));
        assertEquals("jsonb", columnType("SELECT jsonb('{}')"));
    }

    // ---- the qualified spellings ------------------------------------------------

    @Test
    void pgCatalogQualifiedCoercionsResolve() throws Exception {
        assertEquals("2", scalar("SELECT pg_catalog.int4(1.9)"));
        assertEquals("2020-01-02", scalar("SELECT pg_catalog.date('2020-01-02')"));
        assertEquals("t", scalar("SELECT pg_catalog.bool('t')"));
    }

    /**
     * The typmod-applying rows, which only the qualified spelling reaches because the bare name of
     * every type that has one is a keyword. The modifier is the packed one the catalogs hold, so
     * anything below the four-byte header means no modifier at all.
     */
    @Test
    void qualifiedTypmodFormsApplyThePackedModifier() throws Exception {
        assertEquals("1.5", scalar("SELECT pg_catalog.numeric(1.5, 2)"));
        assertEquals("abcdef", scalar("SELECT pg_catalog.varchar('abcdef'::varchar, 3, true)"));
        assertEquals("abc", scalar("SELECT pg_catalog.varchar('abcdef'::varchar, 7, true)"));
    }

    @Test
    void aQualifierThatNamesNoSchemaIsTheMissingSchema() throws Exception {
        assertFails("SELECT nosuchschema_fes.int4(1.9)", "3F000",
                "schema \"nosuchschema_fes\" does not exist");
    }

    // ---- a user's own types -----------------------------------------------------

    @Test
    void aDomainAndAnEnumAreCallableByName() throws Exception {
        assertEquals("1", scalar("SELECT fes_dom(1)"));
        assertEquals("1", scalar("SELECT public.fes_dom('1')"));
        assertEquals("a", scalar("SELECT fes_enum('a')"));
        assertEquals("fes_enum", columnType("SELECT fes_enum('a')"));
    }

    @Test
    void anEnumIsReachedFromAStringAndNothingElse() throws Exception {
        assertFails("SELECT fes_enum('zzz')", "22P02",
                "invalid input value for enum fes_enum: \"zzz\"");
        assertFails("SELECT fes_enum(1)", "42883", "function fes_enum(integer) does not exist");
    }

    // ---- what must keep being refused -------------------------------------------

    /**
     * PostgreSQL performs the coercion only where it declares a conversion. An integer is not a
     * date, a uuid or an inet, and a point is not an int4 — each is 42883 there, and the argument
     * is named by the type it was written as rather than by the text its value looks like.
     */
    @Test
    void aCoercionPostgresDoesNotDeclareIsRefused() throws Exception {
        assertFails("SELECT date(42)", "42883", "function date(integer) does not exist");
        assertFails("SELECT uuid(42)", "42883", "function uuid(integer) does not exist");
        assertFails("SELECT inet(42)", "42883", "function inet(integer) does not exist");
        assertFails("SELECT int4(point '(1,2)')", "42883",
                "function int4(point) does not exist");
        assertFails("SELECT int4(p) FROM fes_t", "42883",
                "function int4(point) does not exist");
    }

    @Test
    void aCoercionTakesExactlyOneArgument() throws Exception {
        assertFails("SELECT int4()", "42883", "function int4() does not exist");
        assertFails("SELECT int4(1, 2)", "42883",
                "function int4(integer, integer) does not exist");
    }

    @Test
    void aPolymorphicPseudoTypeIsNotACoercion() throws Exception {
        assertFails("SELECT anyelement(1)", "42883",
                "function anyelement(integer) does not exist");
        assertFails("SELECT nosuchtype_fes(1)", "42883",
                "function nosuchtype_fes(integer) does not exist");
    }

    /**
     * A type name PostgreSQL's grammar reads as a type before it reaches the parenthesis is a
     * syntax error there — {@code SELECT numeric(42)} is 42601 — and never a coercion. memgres
     * parses the same text as a call and so cannot reach 42601 from here; what matters is that
     * none of these names starts answering, because that would accept SQL PostgreSQL rejects.
     * Only the refusal is asserted, since the SQLSTATE memgres reaches (42883) is not the one
     * PostgreSQL reports and pinning it would record a divergence as if it were the answer.
     */
    @Test
    void grammarTypeKeywordsAreNotEnabledAsCoercions() {
        for (String sql : new String[]{
                "SELECT numeric(42)", "SELECT varchar('ab')", "SELECT integer(1)",
                "SELECT boolean('t')", "SELECT bigint(1)", "SELECT smallint(1)",
                "SELECT real(1)", "SELECT bit('101')", "SELECT time('12:00:00')",
                "SELECT char('a')", "SELECT dec(1)", "SELECT int(1)"}) {
            failure(sql);
        }
    }

    @Test
    void aSchemaThatHoldsNoSuchTypeStaysRefused() throws Exception {
        assertFails("SELECT public.int4(1.9)", "42883",
                "function public.int4(numeric) does not exist");
    }

    // ---- the contexts a call can be written in ----------------------------------

    @Test
    void aCoercionResolvesInEveryClause() throws Exception {
        assertEquals("42", scalar("SELECT int4(t) FROM fes_t"));
        assertEquals("7", scalar("SELECT int8(i) FROM fes_t"));
        assertEquals("2020-01-02", scalar("SELECT date(d) FROM fes_t"));
        assertEquals("4", scalar("SELECT int4(n) FROM fes_t"));
        assertEquals("42", scalar("SELECT a FROM (SELECT int4('42') AS a) s"));
        assertEquals("42", scalar("WITH c AS (SELECT int4('42') AS a) SELECT a FROM c"));
        assertEquals("42", scalar("SELECT (SELECT int4('42'))"));
        assertEquals("7", scalar("SELECT i FROM fes_t WHERE int4(t) = 42"));
        assertEquals("1", scalar("SELECT count(*) FROM fes_t GROUP BY int4(t)"));
        assertEquals("t", scalar("SELECT tsvector('a b') @@ tsquery('a')"));
    }

    @Test
    void aCoercionResolvesInAViewAndInAPreparedStatement() throws Exception {
        exec("CREATE VIEW fes_v AS SELECT int4('42') AS a, bool('t') AS b,"
                + " date('2020-01-02') AS c");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT a, b, c FROM fes_v")) {
            assertTrue(rs.next());
            assertEquals("42", rs.getString(1));
            assertEquals("t", rs.getString(2));
            assertEquals("2020-01-02", rs.getString(3));
            assertEquals("int4", rs.getMetaData().getColumnTypeName(1));
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT int4(?)")) {
            ps.setString(1, "42");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("42", rs.getString(1));
            }
        }
        exec("DROP VIEW fes_v");
    }

    // ---- the geometric constructors ---------------------------------------------

    @Test
    void aGeometricConstructorAnswersInTheShapeItBuilds() throws Exception {
        assertEquals("point", scalar("SELECT pg_typeof(point(1,2))::text"));
        assertEquals("point", scalar("SELECT pg_typeof(point('(1,2)'))::text"));
        assertEquals("box", scalar("SELECT pg_typeof(box('(1,1),(0,0)'))::text"));
        assertEquals("circle", scalar("SELECT pg_typeof(circle('<(0,0),5>'))::text"));
        assertEquals("lseg", scalar("SELECT pg_typeof(lseg('[(0,0),(1,1)]'))::text"));
        assertEquals("line", scalar("SELECT pg_typeof(line('{1,2,3}'))::text"));
        assertEquals("path", scalar("SELECT pg_typeof(path('[(0,0),(1,1)]'))::text"));
        assertEquals("polygon", scalar("SELECT pg_typeof(polygon('((0,0),(1,1),(2,0))'))::text"));
        assertEquals("point", columnType("SELECT point(1,2)"));
        assertEquals("(1,2)", scalar("SELECT point(1,2)::text"));
        assertEquals("(1,1),(0,0)",
                scalar("SELECT box(point '(0,0)', point '(1,1)')::text"));
    }

    /**
     * Every geometric constructor needs an argument, and a single one has to be something a shape
     * can be read out of. memgres read the first of an empty argument list and reported the
     * resulting internal error to the client; {@code point(42)} answered (NaN,NaN).
     */
    @Test
    void aGeometricConstructorWithNoUsableArgumentIsRefused() throws Exception {
        assertFails("SELECT point()", "42883", "function point() does not exist");
        assertFails("SELECT box()", "42883", "function box() does not exist");
        assertFails("SELECT circle()", "42883", "function circle() does not exist");
        assertFails("SELECT lseg()", "42883", "function lseg() does not exist");
        assertFails("SELECT line()", "42883", "function line() does not exist");
        assertFails("SELECT path()", "42883", "function path() does not exist");
        assertFails("SELECT polygon()", "42883", "function polygon() does not exist");
        assertFails("SELECT point(42)", "42883", "function point(integer) does not exist");
        assertFails("SELECT circle(42)", "42883", "function circle(integer) does not exist");
        assertFails("SELECT path(42)", "42883", "function path(integer) does not exist");
        assertFails("SELECT polygon(42)", "42883", "function polygon(integer) does not exist");
        assertFails("SELECT point('abc')", "22P02",
                "invalid input syntax for type point: \"abc\"");
    }

    /** The shapes PostgreSQL does build from another shape keep working. */
    @Test
    void aShapeBuiltFromAnotherShapeStillResolves() throws Exception {
        assertEquals("(0.8535533905932737,0.8535533905932737),"
                        + "(0.14644660940672627,0.14644660940672627)",
                scalar("SELECT box(circle '<(0.5,0.5),0.5>')::text"));
        assertEquals("(1,2)", scalar("SELECT point(box '(1,2),(1,2)')::text"));
        assertEquals("<(0.5,0.5),0.7071067811865476>",
                scalar("SELECT circle(box '(1,1),(0,0)')::text"));
        assertEquals("((0,0),(0,1),(1,1),(1,0))",
                scalar("SELECT polygon(box '(1,1),(0,0)')::text"));
        assertEquals("[(1,1),(0,0)]", scalar("SELECT lseg(box '(1,1),(0,0)')::text"));
    }
}
