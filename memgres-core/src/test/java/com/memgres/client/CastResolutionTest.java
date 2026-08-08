package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Whether a cast exists, and what type it produces.
 *
 * <p>A cast was "render the source to text and feed the text to the target's input function", so
 * whether {@code x::T} was allowed depended on whether the text happened to parse rather than on
 * whether PostgreSQL has the conversion. {@code true::int8} answered 1 and
 * {@code '(1,1)'::point::money} answered a price; where the text did not parse, the complaint was
 * 22P02 about the input rather than the 42846 PostgreSQL raises about the types.
 *
 * <p>Beside it: {@code float(p)} names two different types depending on p, and a cast over a cast
 * is named after the type it ends at.
 */
class CastResolutionTest {

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

    private static String label(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.getMetaData().getColumnLabel(1);
        }
    }

    private static SQLException refused(String sql) {
        return assertThrows(SQLException.class, () -> scalar(sql), sql);
    }

    // ------------------------------------------------------- a cast that does not exist

    /** A conversion PostgreSQL has no pg_cast row for is refused as a conversion, not as input. */
    @Test
    void aCastPostgresDoesNotHaveIsRefused() {
        String[] missing = {
                "SELECT true::int8",
                "SELECT 1::int::json",
                "SELECT '(1,1)'::point::money",
                "SELECT '2020-01-01'::date::interval",
                "SELECT 1::int::date",
                "SELECT DATE '2020-01-01'::int",
                "SELECT interval '1 day'::int",
                "SELECT true::date",
                "SELECT 1::point",
                "SELECT '1234'::money::int",
                "SELECT '1234'::money::float8",
                "SELECT 'a'::tsvector::tsquery",
                "SELECT 'a'::tsquery::tsvector",
        };
        for (String sql : missing) {
            SQLException e = refused(sql);
            assertEquals("42846", e.getSQLState(), sql);
            assertTrue(e.getMessage().contains("cannot cast type"), sql + " → " + e.getMessage());
        }
    }

    /** Everything PostgreSQL does have a path for still works, including through text. */
    @Test
    void everyCastPostgresHasStillWorks() throws Exception {
        assertEquals("1", scalar("SELECT 1::text"));
        assertEquals("1", scalar("SELECT '1'::int"));
        assertEquals("1", scalar("SELECT 1::int8"));
        assertEquals("1", scalar("SELECT true::int"));
        assertEquals("1.5", scalar("SELECT 1.5::numeric"));
        assertEquals("1", scalar("SELECT 1::int::oid"));
        assertEquals("x", scalar("SELECT 'x'::varchar::name"));
        assertEquals("1 day", scalar("SELECT '1 day'::interval::text"));
        assertEquals("2020-01-01 00:00:00", scalar("SELECT '2020-01-01'::date::timestamp"));
        assertEquals("{\"a\": 1}", scalar("SELECT '{\"a\":1}'::json::jsonb"));
    }

    /** An untyped literal has no source type to judge, so it casts to whatever it can parse as. */
    @Test
    void anUntypedLiteralIsStillCastableToAnything() throws Exception {
        assertEquals("(1,1)", scalar("SELECT '(1,1)'::point::text"));
        assertEquals("2020-01-01", scalar("SELECT '2020-01-01'::date::text"));
    }

    // ------------------------------------------------------- float(p) is two types

    /** float(p) is a real up to 24 bits of mantissa and a double precision above it. */
    @Test
    void floatPrecisionPicksTheType() throws Exception {
        assertEquals("real", scalar("SELECT pg_typeof(1::float(24))::text"));
        assertEquals("double precision", scalar("SELECT pg_typeof(1::float(25))::text"));
        assertEquals("double precision", scalar("SELECT pg_typeof(1::float)::text"));
        assertEquals("float4", label("SELECT 'inf'::float(24)"));
        assertEquals("float8", label("SELECT 'inf'::float(25)"));
    }

    /** And a precision outside 1..53 is not a width, it is a refusal. */
    @Test
    void floatPrecisionHasBounds() {
        assertEquals("22023", refused("SELECT 1::float(0)").getSQLState());
        assertEquals("22023", refused("SELECT 1::float(54)").getSQLState());
        assertTrue(refused("SELECT 1::float(0)").getMessage().contains("at least 1 bit"));
        assertTrue(refused("SELECT 1::float(54)").getMessage().contains("less than 54 bits"));
    }

    /** real reads every spelling of infinity and NaN that double precision reads. */
    @Test
    void realReadsTheSameSpellingsAsDoublePrecision() throws Exception {
        assertEquals("Infinity", scalar("SELECT 'inf'::float(24)"));
        assertEquals("Infinity", scalar("SELECT 'Infinity'::float(24)"));
        assertEquals("-Infinity", scalar("SELECT '-inf'::float(24)"));
        assertEquals("NaN", scalar("SELECT 'nan'::float(24)"));
        assertEquals("22P02", refused("SELECT 'zzz'::float(24)").getSQLState());
    }

    // ------------------------------------------------------- what a cast column is called

    /** A cast over a cast is named after the type it ends at, not the one it passed through. */
    @Test
    void aCastOverACastIsNamedAfterTheOuterType() throws Exception {
        assertEquals("oid", label("SELECT 1::int::oid"));
        assertEquals("jsonb", label("SELECT '{\"a\":1}'::json::jsonb"));
        assertEquals("name", label("SELECT 'x'::varchar::name"));
        assertEquals("timestamp", label("SELECT '2020-01-01'::date::timestamp"));
        // A column keeps its own name through a single cast, as it does in PostgreSQL.
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TEMP TABLE zz_cast_t (a int)");
        }
        assertEquals("a", label("SELECT a::bigint FROM zz_cast_t"));
        assertEquals("int4", label("SELECT 1::int"));
    }
}
