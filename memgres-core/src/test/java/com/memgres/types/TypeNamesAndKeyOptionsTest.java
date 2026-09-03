package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * How a type is named, and what may be written after a column in a key.
 *
 * <p>A type has more than one name and PostgreSQL picks between them by what was asked. A name may
 * say which schema to look in, and a built-in answers to its own schema's name. An array has no
 * name of its own — it is its element's name with brackets after it. {@code format_type} tells a
 * modifier of none apart from no modifier written, and answers with a different name for each. And
 * every relation that holds rows is a type as much as a composite declared outright is.
 *
 * <p>The other half is the key element. A partition key is written the way an index column is —
 * the column, then a collation, then an operator class — and the two that follow the column are
 * judged on their own terms rather than being read as part of its name.
 */
class TypeNamesAndKeyOptionsTest {

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

    /** A built-in answers to its own schema's name, and to no other schema's. */
    @Test
    void aTypeNameMaySayWhichSchemaToLookIn() throws SQLException {
        assertEquals("integer", one("SELECT 'pg_catalog.int4'::regtype::text"));
        assertEquals("integer", one("SELECT 'pg_catalog.\"int4\"'::regtype::text"));
        assertEquals("integer", one("SELECT '\"pg_catalog\".int4'::regtype::text"));
        assertEquals("numeric", one("SELECT 'pg_catalog.numeric'::regtype::text"));
        assertEquals("23", one("SELECT 'pg_catalog.int4'::regtype::oid::text"));
        // A schema that is not there at all is the fault, rather than the type not being in it.
        assertEquals("3F000", stateOf("SELECT 'nosuchschema.int4'::regtype"));
    }

    /** An array is named after its element, with the brackets the catalogue's name leaves out. */
    @Test
    void anArrayIsNamedAfterItsElement() throws SQLException {
        assertEquals("integer[]", one("SELECT 'int4[]'::regtype::text"));
        assertEquals("integer[]", one("SELECT '_int4'::regtype::text"));
        assertEquals("integer[]", one("SELECT 'pg_catalog._int4'::regtype::text"));
        assertEquals("smallint[]", one("SELECT 'int2[]'::regtype::text"));
        assertEquals("boolean[]", one("SELECT 'bool[]'::regtype::text"));
        assertEquals("character varying[]", one("SELECT 'varchar[]'::regtype::text"));
        assertEquals("timestamp with time zone[]", one("SELECT 'timestamptz[]'::regtype::text"));
        assertEquals("name[]", one("SELECT 'name[]'::regtype::text"));
    }

    /**
     * The SQL names mean a width of one where the catalogue's mean no width at all, so asking with
     * a modifier of none is a different question from asking with none written.
     */
    @Test
    void formatTypeTellsNoModifierFromAModifierOfNone() throws SQLException {
        assertEquals("character", one("SELECT format_type('bpchar'::regtype, NULL)"));
        assertEquals("bpchar", one("SELECT format_type('bpchar'::regtype, -1)"));
        assertEquals("character(10)", one("SELECT format_type('bpchar'::regtype, 14)"));
        assertEquals("bit", one("SELECT format_type('bit'::regtype, NULL)"));
        assertEquals("\"bit\"", one("SELECT format_type('bit'::regtype, -1)"));
        assertEquals("bit(5)", one("SELECT format_type('bit'::regtype, 5)"));
        assertEquals("bpchar[]", one("SELECT format_type('_bpchar'::regtype, -1)"));
        // Every other type answers with the same name either way.
        assertEquals("integer", one("SELECT format_type('int4'::regtype, -1)"));
        assertEquals("character varying", one("SELECT format_type('varchar'::regtype, -1)"));
    }

    /** A relation that holds rows has a type of its own name; a sequence holds none and has none. */
    @Test
    void aRelationsRowsAreAType() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zrt_t (a int, b text)");
            s.execute("CREATE VIEW zrt_v AS SELECT 1 AS x");
            s.execute("CREATE SEQUENCE zrt_s");
        }
        assertEquals("zrt_t", one("SELECT 'zrt_t'::regtype::text"));
        assertEquals("zrt_t", one("SELECT 'public.zrt_t'::regtype::text"));
        assertEquals("zrt_v", one("SELECT 'zrt_v'::regtype::text"));
        assertEquals("zrt_t", one("SELECT format_type('zrt_t'::regtype, -1)"));
        assertEquals("same", one("SELECT CASE WHEN 'zrt_t'::regtype::oid ="
                + " (SELECT reltype FROM pg_class WHERE relname='zrt_t')"
                + " THEN 'same' ELSE 'other' END"));
        assertEquals("42704", stateOf("SELECT 'zrt_s'::regtype"));
        try (Statement s = conn.createStatement()) {
            s.execute("DROP VIEW zrt_v");
            s.execute("DROP SEQUENCE zrt_s");
            s.execute("DROP TABLE zrt_t");
        }
    }

    /** A collation and an operator class may follow the column of a partition key. */
    @Test
    void aPartitionKeyElementMayCarryACollationAndAClass() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zpk_a (s text) PARTITION BY RANGE (s COLLATE \"C\")");
            s.execute("CREATE TABLE zpk_b (s text) PARTITION BY RANGE (s text_pattern_ops)");
            s.execute("CREATE TABLE zpk_c (s text)"
                    + " PARTITION BY RANGE (s COLLATE \"C\" text_pattern_ops)");
        }
        assertEquals("RANGE (s COLLATE \"C\")",
                one("SELECT pg_get_partkeydef('zpk_a'::regclass)"));
        assertEquals("RANGE (s text_pattern_ops)",
                one("SELECT pg_get_partkeydef('zpk_b'::regclass)"));
        assertEquals("RANGE (s COLLATE \"C\" text_pattern_ops)",
                one("SELECT pg_get_partkeydef('zpk_c'::regclass)"));
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE zpk_a, zpk_b, zpk_c");
        }
    }

    /**
     * What is judged against the relation is the column alone; the collation and the class are
     * each judged on their own terms, and each has its own fault to report.
     */
    @Test
    void whichFaultAPartitionKeyElementHas() {
        assertEquals("42703",
                stateOf("CREATE TABLE zpk_d (s text) PARTITION BY RANGE (nosuchcol COLLATE \"C\")"));
        assertEquals("42704",
                stateOf("CREATE TABLE zpk_e (s text) PARTITION BY RANGE (s nosuch_ops)"));
        assertEquals("42804",
                stateOf("CREATE TABLE zpk_f (s int) PARTITION BY RANGE (s COLLATE \"C\")"));
    }

    /** A default is evaluated with nothing in scope, on a domain as much as on a column. */
    @Test
    void aDomainDefaultIsHeldToTheSameRuleAsAColumnsIs() {
        assertEquals("0A000", stateOf("CREATE DOMAIN zdd_1 AS int DEFAULT (SELECT 1)"));
        assertNull(stateOf("CREATE DOMAIN zdd_2 AS int DEFAULT (1 + 1)"));
        assertNull(stateOf("DROP DOMAIN zdd_2"));
    }

    /** A collation this server does not have is one it cannot drop. */
    @Test
    void droppingACollationNobodyHas() {
        assertEquals("42704", stateOf("DROP COLLATION zdc_nosuch"));
        assertNull(stateOf("DROP COLLATION IF EXISTS zdc_nosuch"));
    }

    /** A channel is named by an identifier, and is held to an identifier's length. */
    @Test
    void aNotifyChannelIsNamedByAnIdentifier() {
        assertEquals("22023", stateOf("SELECT pg_notify(repeat('a', 64), 'x')"));
        assertNull(stateOf("SELECT pg_notify(repeat('a', 63), 'x')"));
    }
}
