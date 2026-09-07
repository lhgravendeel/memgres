package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a shape, a document and an inherited column each hold.
 *
 * <p>A child takes its parent's columns as columns of its own: sharing the object made an ALTER of
 * either relation reach both. A child also carries them in whatever order its own declaration
 * settled, so a parent reads them by name -- taken by position, a child of two parents handed the
 * first parent's value to the second parent's column. And a unique index belongs to the relation it
 * is built on, which inheritance never carries down.
 *
 * <p>A line is {@code Ax + By + C = 0}, which describes a line only where A and B are not both
 * zero; a coordinate is a double precision, so a number too large for one is out of range rather
 * than an infinity; a polygon is closed, so an open path has no inside to become one; and "same as"
 * is declared over the four shapes that have a place of their own and not over a segment.
 *
 * <p>A lexeme too long for a tsvector is refused when it is written as a literal and left out when
 * it is found in a document, a quoted lexeme is the whole of what its quotes hold, and a phrase
 * distance is a count the operator can carry.
 */
class WhatAShapeAndADocumentHoldTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
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

    /** A child's column is its own, and a parent reads its columns by name. */
    @Test
    void whatAnInheritedColumnBelongsTo() throws SQLException {
        exec("CREATE TABLE zws_p (a text)");
        exec("CREATE TABLE zws_c () INHERITS (zws_p)");
        exec("ALTER TABLE zws_c ALTER COLUMN a SET STORAGE PLAIN");
        assertEquals("x", one("SELECT at.attstorage::text FROM pg_attribute at"
                + " JOIN pg_class c ON c.oid=at.attrelid"
                + " WHERE c.relname='zws_p' AND at.attname='a'"));
        assertEquals("p", one("SELECT at.attstorage::text FROM pg_attribute at"
                + " JOIN pg_class c ON c.oid=at.attrelid"
                + " WHERE c.relname='zws_c' AND at.attname='a'"));
        exec("DROP TABLE zws_c");
        exec("DROP TABLE zws_p");
        // Two parents contribute two columns, and each reads its own.
        exec("CREATE TABLE zws_ip1 (a int)");
        exec("CREATE TABLE zws_ip2 (b text)");
        exec("CREATE TABLE zws_ic () INHERITS (zws_ip1, zws_ip2)");
        exec("INSERT INTO zws_ic VALUES (7, 'hello')");
        assertEquals("7", one("SELECT a::text FROM zws_ip1"));
        assertEquals("hello", one("SELECT b FROM zws_ip2"));
        exec("DROP TABLE zws_ic");
        exec("DROP TABLE zws_ip2");
        exec("DROP TABLE zws_ip1");
    }

    /** A unique index is the relation's own, and a child's rows are not its to hold unique. */
    @Test
    void whoseRowsAUniqueIndexHolds() throws SQLException {
        exec("CREATE TABLE zws_pu (a int)");
        exec("INSERT INTO zws_pu VALUES (1),(2)");
        exec("CREATE TABLE zws_pc () INHERITS (zws_pu)");
        exec("INSERT INTO zws_pc VALUES (1)");
        assertNull(stateOf("ALTER TABLE zws_pu ADD CONSTRAINT zws_puu UNIQUE (a)"));
        exec("DROP TABLE zws_pc");
        exec("DROP TABLE zws_pu");
    }

    /** A shape holds only what its own definition can describe. */
    @Test
    void whatAShapeMayBe() throws SQLException {
        assertEquals("22P02", stateOf("SELECT '{0,0,1}'::line"));
        assertTrue(messageOf("SELECT '{0,0,1}'::line")
                .contains("invalid line specification: A and B cannot both be zero"));
        assertEquals("{1,0,1}", one("SELECT '{1,0,1}'::line::text"));
        assertEquals("22003", stateOf("SELECT '(1e400,0)'::point"));
        assertTrue(messageOf("SELECT '(1e400,0)'::point")
                .contains("\"1e400\" is out of range for type double precision"));
        assertEquals("(1,0)", one("SELECT '(1,0)'::point::text"));
        // An infinity written as one is still an infinity.
        assertEquals("(Infinity,0)", one("SELECT '(inf,0)'::point::text"));
        // A polygon is closed, so an open path is not one.
        assertEquals("22023", stateOf("SELECT CAST(CAST('[(0,0),(1,0),(1,1)]' AS path) AS polygon)"));
        assertTrue(messageOf("SELECT CAST(CAST('[(0,0),(1,0),(1,1)]' AS path) AS polygon)")
                .contains("open path cannot be converted to polygon"));
        assertEquals("((0,0),(1,0),(1,1))",
                one("SELECT CAST(CAST('((0,0),(1,0),(1,1))' AS path) AS polygon)::text"));
        // "Same as" is not declared over a segment.
        assertEquals("42883", stateOf("SELECT '[(0,0),(1,1)]'::lseg ~= '[(0,0),(1,1)]'::lseg"));
        assertTrue(messageOf("SELECT '[(0,0),(1,1)]'::lseg ~= '[(0,0),(1,1)]'::lseg")
                .contains("operator does not exist: lseg ~= lseg"));
        assertEquals("true", one("SELECT ('(1,1)'::point ~= '(1,1)'::point)::text"));
    }

    /** What a text-search value may hold, and how it is read back. */
    @Test
    void whatADocumentMayHold() throws SQLException {
        // A quoted lexeme is the whole of what its quotes hold.
        assertEquals("'a:b'", one("SELECT '''a:b'''::tsquery::text"));
        assertEquals("'a':B", one("SELECT 'a:B'::tsquery::text"));
        // A comma promises another position, so the literal cannot end there.
        assertEquals("42601", stateOf("SELECT 'a:1,'::tsvector"));
        assertTrue(messageOf("SELECT 'a:1,'::tsvector")
                .contains("syntax error in tsvector: \"a:1,\""));
        assertEquals("'a':1", one("SELECT 'a:1'::tsvector::text"));
        // A word too long is refused where it is written and dropped where it is found.
        assertEquals("54000", stateOf("SELECT ('a'||repeat('b',5000))::tsvector"));
        assertTrue(messageOf("SELECT ('a'||repeat('b',5000))::tsvector")
                .contains("word is too long (5001 bytes, max 2046 bytes)"));
        assertEquals("0", one("SELECT length(to_tsvector('english', repeat('k',2047))::text)::text"));
        assertEquals("2049",
                one("SELECT length(to_tsvector('english', repeat('k',2045))::text)::text"));
        // A phrase distance is a count the operator can carry.
        assertEquals("22023", stateOf("SELECT tsquery_phrase('a'::tsquery,'b'::tsquery,-1)"));
        assertEquals("22023", stateOf("SELECT tsquery_phrase('a'::tsquery,'b'::tsquery,16385)"));
        assertTrue(messageOf("SELECT tsquery_phrase('a'::tsquery,'b'::tsquery,-1)")
                .contains("distance in phrase operator must be an integer value"
                        + " between zero and 16384 inclusive"));
        assertEquals("'a' <3> 'b'",
                one("SELECT tsquery_phrase('a'::tsquery,'b'::tsquery,3)::text"));
    }

    /** A serialisation answers with the type its clause names. */
    @Test
    void whatASerialisationAnswersWith() throws SQLException {
        assertEquals("varchar", columnTypeOf("SELECT XMLSERIALIZE(CONTENT '<a>x</a>'::xml"
                + " AS varchar(20))"));
        assertEquals("text", columnTypeOf("SELECT XMLSERIALIZE(CONTENT '<a>x</a>'::xml AS text)"));
        assertEquals("<a>x</a>", one("SELECT XMLSERIALIZE(CONTENT '<a>x</a>'::xml AS text)"));
    }

    private static String columnTypeOf(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.getMetaData().getColumnTypeName(1);
        }
    }
}
