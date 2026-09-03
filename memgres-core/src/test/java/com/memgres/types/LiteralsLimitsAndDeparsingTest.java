package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a literal may say, what a type may hold, and how a stored expression is written back.
 *
 * <p>A number written in another base is named by that base when it is written with no digits in
 * it, and a number run into a word is refused with the whole word named. A type declared by its
 * mantissa is the type that mantissa picks, and is held to that type's own range. A value carried
 * as text still has fields of its own, so text that is not an aclitem is not one.
 *
 * <p>A default is never echoed as it was written: PostgreSQL prints the tree it parsed, in which
 * the sequence a {@code nextval} names has become a regclass and is spelled the way a relation
 * name is spelled — bare where the search path reaches it.
 */
class LiteralsLimitsAndDeparsingTest {

    private static Memgres memgres;
    private static Connection conn;

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
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** A base marker with no digits behind it is a number of that base with nothing in it. */
    @Test
    void aNumberWrittenInAnotherBase() throws SQLException {
        assertEquals("255", one("SELECT 0xff"));
        assertEquals("5", one("SELECT 0b101"));
        assertEquals("15", one("SELECT 0o17"));
        assertTrue(messageOf("SELECT 0x").contains("invalid hexadecimal integer at or near \"0x\""));
        assertTrue(messageOf("SELECT 0b").contains("invalid binary integer at or near \"0b\""));
        assertTrue(messageOf("SELECT 0o").contains("invalid octal integer at or near \"0o\""));
        // A number run into a word names the whole word, not the letter it ran into.
        assertTrue(messageOf("SELECT 123abc")
                .contains("trailing junk after numeric literal at or near \"123abc\""));
        assertTrue(messageOf("SELECT 0xg")
                .contains("trailing junk after numeric literal at or near \"0xg\""));
    }

    /** float(p) names a type by its mantissa, and that type's range is what holds. */
    @Test
    void aFloatNamedByItsMantissa() throws SQLException {
        assertEquals("1.5", one("SELECT ('1.5'::float(24))::text"));
        assertEquals("real", one("SELECT pg_typeof('1.5'::float(24))::text"));
        assertTrue(messageOf("SELECT '1e40'::float(24)")
                .contains("\"1e40\" is out of range for type real"));
        assertTrue(messageOf("SELECT '1e400'::float(53)")
                .contains("\"1e400\" is out of range for type double precision"));
    }

    /** An aclitem has fields, and text that is not one of them is not an aclitem. */
    @Test
    void anAclItemIsReadFieldByField() throws SQLException {
        assertEquals("memgres=arwdDxt/memgres", one("SELECT 'memgres=arwdDxt/memgres'::aclitem::text"));
        assertEquals("=r/memgres", one("SELECT '=r/memgres'::aclitem::text"));
        assertTrue(messageOf("SELECT 'garbage'::aclitem")
                .contains("unrecognized key word: \"garbage\""));
        assertTrue(messageOf("SELECT 'a=b'::aclitem")
                .contains("invalid mode character: must be one of \"arwdDxtXUCTcsAm\""));
    }

    /** The distance between two points is scaled so it does not overflow on the way. */
    @Test
    void aDistanceIsMeasuredWithoutOverflowing() throws SQLException {
        assertEquals("1.4142135623730951e+308",
                one("SELECT ('(0,0)'::point <-> '(1e308,1e308)'::point)::text"));
        assertEquals("5", one("SELECT ('(0,0)'::point <-> '(3,4)'::point)::text"));
        assertEquals("4.242640687119286",
                one("SELECT ('(0,0),(1,1)'::box <-> '(3,3),(4,4)'::box)::text"));
    }

    /** The sequence a nextval names is written back as a regclass. */
    @Test
    void aSequenceDefaultIsWrittenAsARegclass() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SEQUENCE zld_s");
            s.execute("CREATE TABLE zld_t (a int DEFAULT nextval('zld_s'),"
                    + " b int DEFAULT nextval('zld_s'::regclass),"
                    + " c int DEFAULT nextval('public.zld_s'))");
        }
        for (String col : new String[]{"a", "b", "c"}) {
            assertEquals("nextval('zld_s'::regclass)",
                    one("SELECT column_default FROM information_schema.columns"
                            + " WHERE table_name='zld_t' AND column_name='" + col + "'"), col);
        }
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE zld_t");
            s.execute("DROP SEQUENCE zld_s");
        }
    }

    /** A type name in a function body may say which schema to look in. */
    @Test
    void aQualifiedTypeNameInsideAFunctionBody() {
        assertNull(stateOf("CREATE FUNCTION zld_f() RETURNS text LANGUAGE sql"
                + " AS $$ SELECT 'x'::pg_catalog.text $$"));
        assertEquals("42704", stateOf("CREATE FUNCTION zld_g() RETURNS text LANGUAGE sql"
                + " AS $$ SELECT 'x'::pg_catalog.nosuchtype $$"));
        assertNull(stateOf("DROP FUNCTION zld_f()"));
    }

    /** A domain is its base type with everything the declaration said about it. */
    @Test
    void aDomainCarriesItsBaseTypesModifier() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE DOMAIN zld_dn AS numeric(3,2)");
            s.execute("CREATE DOMAIN zld_db AS bit(4)");
            s.execute("CREATE DOMAIN zld_dv AS varchar(3)");
        }
        assertEquals("1.23", one("SELECT (1.234::zld_dn)::text"));
        assertEquals("22003", stateOf("SELECT 12345.6::zld_dn"));
        assertEquals("1010", one("SELECT ('101'::zld_db)::text"));
        assertEquals("abc", one("SELECT ('abcdef'::zld_dv)::text"));
        try (Statement s = conn.createStatement()) {
            s.execute("DROP DOMAIN zld_dn");
            s.execute("DROP DOMAIN zld_db");
            s.execute("DROP DOMAIN zld_dv");
        }
    }

    /** A cast is registered in the context it was declared with, not always as explicit. */
    @Test
    void aCastIsRegisteredInTheContextItWasDeclaredWith() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE zld_ct AS (a int)");
            s.execute("CREATE FUNCTION zld_cf(int) RETURNS zld_ct LANGUAGE sql"
                    + " AS $$ SELECT ROW($1)::zld_ct $$");
            s.execute("CREATE CAST (int AS zld_ct) WITH FUNCTION zld_cf(int) AS ASSIGNMENT");
            s.execute("CREATE FUNCTION zld_cf2(bigint) RETURNS zld_ct LANGUAGE sql"
                    + " AS $$ SELECT ROW($1::int)::zld_ct $$");
            s.execute("CREATE CAST (bigint AS zld_ct) WITH FUNCTION zld_cf2(bigint) AS IMPLICIT");
        }
        assertEquals("a", one("SELECT castcontext FROM pg_cast"
                + " WHERE castsource='int4'::regtype AND casttarget='zld_ct'::regtype"));
        assertEquals("i", one("SELECT castcontext FROM pg_cast"
                + " WHERE castsource='int8'::regtype AND casttarget='zld_ct'::regtype"));
    }
}
