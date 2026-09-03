package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * How big a value is, what a reference points at, and how an object's name is read.
 *
 * <p>A type of fixed width takes that width and no more: only a value whose length varies carries
 * the four-byte header that says how long it is. A numeric is that header, two bytes of its own,
 * and two more for each base-ten-thousand digit it is made of — a digit group of zeroes at either
 * end is not stored at all.
 *
 * <p>The privilege functions read the name they are given the way a statement's parser reads one:
 * folded to lower case unless it is quoted, and split only on a dot outside the quotes.
 */
class ObjectsSizesAndPrivilegeNamesTest {

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

    /** A fixed-width type takes its own width, with no header in front of it. */
    @Test
    void howBigAFixedWidthValueIs() throws SQLException {
        assertEquals("4|1|8|2", one("SELECT pg_column_size(1::int)::text || '|'"
                + " || pg_column_size(true)::text || '|' || pg_column_size(1::bigint)::text"
                + " || '|' || pg_column_size(1::smallint)::text"));
        assertEquals("4|8|4", one("SELECT pg_column_size(1::real)::text || '|'"
                + " || pg_column_size(1::float8)::text || '|'"
                + " || pg_column_size('2020-01-01'::date)::text"));
        assertEquals("8|16|16", one("SELECT pg_column_size(now())::text || '|'"
                + " || pg_column_size('1 day'::interval)::text || '|'"
                + " || pg_column_size(gen_random_uuid())::text"));
    }

    /** A value whose length varies carries a header saying how long it is. */
    @Test
    void howBigAVaryingValueIs() throws SQLException {
        assertEquals("7|5|9", one("SELECT pg_column_size('abc'::text)::text || '|'"
                + " || pg_column_size('a'::varchar(5))::text || '|'"
                + " || pg_column_size('a'::char(5))::text"));
        // A numeric is the header, two bytes of its own, and two for each group of four figures.
        assertEquals("6|10|12|8", one("SELECT pg_column_size(0::numeric)::text || '|'"
                + " || pg_column_size(1.5::numeric)::text || '|'"
                + " || pg_column_size(12345.6789::numeric)::text || '|'"
                + " || pg_column_size(1e10::numeric)::text"));
        assertEquals("36", one("SELECT pg_column_size(ARRAY[1,2,3])::text"));
    }

    /** A reference says which catalogue and which row, and this says what that is. */
    @Test
    void whatAnObjectReferencePointsAt() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zos (a int)");
        }
        assertEquals("table|public|zos|public.zos",
                one("SELECT type || '|' || schema || '|' || name || '|' || identity"
                        + " FROM pg_identify_object('pg_class'::regclass::oid,"
                        + " 'zos'::regclass::oid, 0)"));
        assertEquals("type|pg_catalog|int4",
                one("SELECT type || '|' || schema || '|' || name"
                        + " FROM pg_identify_object('pg_type'::regclass::oid,"
                        + " 'int4'::regtype::oid, 0)"));
        assertEquals("schema|public",
                one("SELECT type || '|' || name FROM pg_identify_object("
                        + "'pg_namespace'::regclass::oid,"
                        + " (SELECT oid FROM pg_namespace WHERE nspname='public'), 0)"));
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE zos");
        }
    }

    /** The name is read as an identifier, not taken as the text it was written with. */
    @Test
    void howAPrivilegeFunctionReadsAName() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ZOS_HT (a int)");
        }
        assertEquals("true", one("SELECT has_table_privilege('ZOS_HT','SELECT')::text"));
        assertEquals("true", one("SELECT has_table_privilege('zos_ht','SELECT')::text"));
        // A quoted name holding a dot is one name, not a schema and a name.
        assertEquals("42P01", stateOf("SELECT has_table_privilege('\"zos_a.dotted\"','SELECT')"));
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE ZOS_HT");
        }
    }

    /** A VALUES list is one relation, so every row of it has the same columns. */
    @Test
    void everyRowOfAValuesListIsTheSameShape() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zos_v (a int, b text)");
        }
        assertEquals("42601", stateOf("INSERT INTO zos_v VALUES (1,'x'),(2)"));
        assertEquals("42601", stateOf("SELECT * FROM (VALUES (1,2),(3)) v"));
        // A single short row is a row that stops before the rest, which is allowed.
        assertNull(stateOf("INSERT INTO zos_v VALUES (1)"));
        assertNull(stateOf("INSERT INTO zos_v VALUES (1,'x'),(2,'y')"));
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE zos_v");
        }
    }
}
