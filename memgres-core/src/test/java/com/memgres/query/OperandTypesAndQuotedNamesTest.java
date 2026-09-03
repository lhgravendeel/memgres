package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a bare literal becomes beside a typed value, and what a quote means inside one.
 *
 * <p>A quoted literal has no type of its own, so what it becomes is decided by what it stands
 * beside: next to a bit string it is a bit string. Where neither side settles the question there
 * is no operator at all, and PostgreSQL says so rather than reading one side as something it is
 * not.
 *
 * <p>Inside a quoted lexeme two quotes stand for one, so a lexeme may hold a quote of its own.
 */
class OperandTypesAndQuotedNamesTest {

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

    /** A bare literal beside a bit string is read as a bit string. */
    @Test
    void aLiteralBesideABitString() throws SQLException {
        assertEquals("true", one("SELECT (B'101' = '101')::text"));
        assertEquals("false", one("SELECT (B'101' = '110')::text"));
        assertEquals("true", one("SELECT (B'101' = B'101')::text"));
        // A value that is already of another type is that type, and there is no operator for it.
        assertTrue(messageOf("SELECT B'101' = 5").contains("operator does not exist: bit = integer"));
        assertTrue(messageOf("SELECT B'101' = 'x'::text")
                .contains("operator does not exist: bit = text"));
    }

    /** Where neither side is a number there is no # to apply. */
    @Test
    void anOperatorThatNeedsNumbers() throws SQLException {
        assertEquals("6", one("SELECT (5 # 3)::text"));
        assertEquals("011", one("SELECT (B'101' # B'110')::text"));
        assertTrue(messageOf("SELECT 'a'::text # 'b'::text")
                .contains("operator does not exist: text # text"));
    }

    /** A shift is declared over one pair of types and not another. */
    @Test
    void whichOperandsAShiftTakes() throws SQLException {
        assertEquals("0", one("SELECT (1::bigint >> 1::int)::text"));
        assertEquals("4", one("SELECT (1::int << 2)::text"));
        // PostgreSQL declares int4 >> int4 and int8 >> int4, and nothing for int4 >> int8.
        assertTrue(messageOf("SELECT 1::int >> 1::bigint")
                .contains("operator does not exist: integer >> bigint"));
        // The same spellings belong to the network types, which do have entries for them.
        assertEquals("true", one("SELECT ('10.0.0.0/8'::inet >> '10.1.0.0/16'::inet)::text"));
    }

    /** A part of a bit string is a bit string. */
    @Test
    void whatAPartOfABitStringIs() throws SQLException {
        assertEquals("bit", one("SELECT pg_typeof(substring(B'1010' FROM 1 FOR 2))::text"));
        assertEquals("text", one("SELECT pg_typeof(substring('abc' FROM 1 FOR 2))::text"));
        assertEquals("1000", one("SELECT (B'1010' & substring(B'1101' FROM 1 FOR 4))::text"));
        exec("CREATE TABLE zop_b (bfix bit(4), bvar varbit(8))");
        exec("INSERT INTO zop_b VALUES (B'1010', B'11010011')");
        assertEquals("1000",
                one("SELECT (bfix & substring(bvar::bit(4) FROM 1 FOR 4))::text FROM zop_b"));
        exec("DROP TABLE zop_b");
    }

    /** Two quotes inside a quoted lexeme stand for one. */
    @Test
    void aQuoteInsideALexeme() throws SQLException {
        assertEquals("'it''s'", one("SELECT ('''it''''s''')::tsquery::text"));
        assertEquals("'it''s':*", one("SELECT ('''it''''s'':*')::tsquery::text"));
        assertEquals("'a' & 'b'", one("SELECT ('''a'' & ''b''')::tsquery::text"));
    }

    /** GROUP in front of a grantee says nothing more than the name does. */
    @Test
    void theOldSpellingOfAGrantee() throws SQLException {
        exec("CREATE ROLE zop_a");
        exec("CREATE TABLE zop_t (a int)");
        assertNull(stateOf("GRANT SELECT ON zop_t TO GROUP zop_a"));
        assertEquals("true", one("SELECT has_table_privilege('zop_a','zop_t','SELECT')::text"));
        assertNull(stateOf("REVOKE SELECT ON zop_t FROM GROUP zop_a"));
        exec("DROP TABLE zop_t");
        exec("DROP ROLE zop_a");
    }

    /** A relation named for its XML is looked for the way any relation reference is. */
    @Test
    void whichRelationsMayBeWrittenOutAsXml() throws SQLException {
        exec("CREATE TABLE zop_x (a int)");
        assertEquals("true", one("SELECT (table_to_xml('zop_x', false, false, '')"
                + " IS NOT NULL)::text"));
        assertEquals("true", one("SELECT (table_to_xml('public.zop_x', false, false, '')"
                + " IS NOT NULL)::text"));
        // A catalogue relation is a relation, and asking for one is a question with an answer.
        assertEquals("true", one("SELECT (table_to_xml('pg_class', false, false, '')"
                + " IS NOT NULL)::text"));
        assertEquals("42P01", stateOf("SELECT table_to_xml('zop_nosuch', false, false, '')"));
        exec("DROP TABLE zop_x");
    }

    /** A type is named as an identifier here too, schema and all. */
    @Test
    void aQualifiedTypeNameInAPrivilegeQuestion() throws SQLException {
        exec("CREATE ROLE zop_r");
        exec("CREATE TYPE zop_ty AS (a int)");
        assertEquals("true", one("SELECT has_type_privilege('zop_r','public.zop_ty','USAGE')::text"));
        assertEquals("true", one("SELECT has_type_privilege('zop_r','zop_ty','USAGE')::text"));
        assertEquals("true",
                one("SELECT has_type_privilege('zop_r','pg_catalog.int4','USAGE')::text"));
        assertEquals("42704", stateOf("SELECT has_type_privilege('zop_r','zop_nosuch','USAGE')"));
        exec("DROP TYPE zop_ty");
        exec("DROP ROLE zop_r");
    }
}
