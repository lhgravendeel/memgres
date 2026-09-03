package com.memgres.errors;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which fault a statement has, and where PostgreSQL says it is.
 *
 * <p>Two statements can be wrong in the same place and be wrong in different ways, and the answer
 * is the first fault found rather than the worst one. A table that already has a primary key has
 * no room for a second, whatever the index behind it would have been called — so the key is judged
 * before the name. A word the grammar reserves cannot stand where a name belongs, which is what
 * separates an option nobody has from a statement that will not parse. And a word that only ever
 * opens a clause is read as opening one, so what is wrong is whatever came after it.
 *
 * <p>Where a type is named in a complaint, it is named the way PostgreSQL names it: an array by
 * its element with brackets, an integer as {@code integer} and not as the alias that was written.
 */
class WhichFaultAStatementHasTest {

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

    /** These name whoever is asking, or everyone, rather than a role there is to drop. */
    @Test
    void aSpecialRoleSpecifierIsNoRoleToDrop() {
        for (String written : new String[]{"current_user", "session_user", "current_role", "public"}) {
            assertEquals("22023", stateOf("DROP ROLE " + written), written);
            assertTrue(messageOf("DROP ROLE " + written)
                    .contains("cannot use special role specifier in DROP ROLE"), written);
        }
    }

    /**
     * The production that names role options takes a plain identifier, so a keyword never reaches
     * it: a word nobody knows is an option nobody has, and a reserved one will not parse at all.
     */
    @Test
    void whichWordsMayNameARoleOption() {
        assertTrue(messageOf("CREATE ROLE zwf_a BOGUS").contains("unrecognized role option \"bogus\""));
        assertTrue(messageOf("CREATE ROLE zwf_b WITH BOGUS")
                .contains("unrecognized role option \"bogus\""));
        assertTrue(messageOf("CREATE ROLE zwf_c IF NOT EXISTS")
                .contains("syntax error at or near \"IF\""));
        assertTrue(messageOf("CREATE ROLE zwf_d NOT").contains("syntax error at or near \"NOT\""));
    }

    /** A table has one primary key, and that is settled before the name of a second is. */
    @Test
    void aSecondPrimaryKeyIsJudgedBeforeItsName() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zwf_pk (a int PRIMARY KEY, b int)");
            s.execute("CREATE TABLE zwf_taken (x int)");
        }
        assertEquals("42P16",
                stateOf("ALTER TABLE zwf_pk ADD CONSTRAINT zwf_taken PRIMARY KEY (b)"));
        assertEquals("42P16", stateOf("ALTER TABLE zwf_pk ADD CONSTRAINT zwf_new PRIMARY KEY (b)"));
        // A UNIQUE key has no such rule, so the name is what is wrong with it.
        assertEquals("42P07", stateOf("ALTER TABLE zwf_pk ADD CONSTRAINT zwf_taken UNIQUE (b)"));
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE zwf_pk");
            s.execute("DROP TABLE zwf_taken");
        }
    }

    /** A word written where a strategy belongs is read as one, and refused as the strategy it is not. */
    @Test
    void anUnrecognizedPartitioningStrategy() {
        assertEquals("22023", stateOf("CREATE TABLE zwf_pb (a int) PARTITION BY BOGUS (a)"));
        assertTrue(messageOf("CREATE TABLE zwf_pb (a int) PARTITION BY BOGUS (a)")
                .contains("unrecognized partitioning strategy \"bogus\""));
    }

    /** A clause-opening word is read as opening its clause, so the fault is what follows it. */
    @Test
    void aStatementThatEndsInTheMiddleOfAClause() {
        assertTrue(messageOf("SELECT 1 GROUP").contains("syntax error at end of input"));
        assertTrue(messageOf("SELECT 1 ORDER").contains("syntax error at end of input"));
        assertTrue(messageOf("SELECT 1 GROUP x").contains("syntax error at or near \"x\""));
        assertTrue(messageOf("SELECT 1 ORDER x").contains("syntax error at or near \"x\""));
        // The field of an EXTRACT is written as a name, and a reserved word cannot be one.
        assertTrue(messageOf("SELECT EXTRACT(FROM now())")
                .contains("syntax error at or near \"FROM\""));
        assertTrue(messageOf("SELECT EXTRACT(SELECT FROM now())")
                .contains("syntax error at or near \"SELECT\""));
    }

    /** An array is named after its element, in a complaint as much as anywhere else. */
    @Test
    void howATypeIsNamedInAComplaint() {
        assertTrue(messageOf("SELECT '{1}'::int[]::int")
                .contains("cannot cast type integer[] to integer"));
        assertTrue(messageOf("SELECT ARRAY['a']::int")
                .contains("cannot cast type text[] to integer"));
        assertTrue(messageOf("CREATE FUNCTION zwf_rt() RETURNS int LANGUAGE sql AS $$ SELECT 'a' $$")
                .contains("declared to return integer"));
    }

    /** There is no cast from a type to an array of it, whatever the value happens to be. */
    @Test
    void aScalarIsNotAnArrayOfItself() {
        assertEquals("42846", stateOf("SELECT 1::int[]"));
        assertEquals("42846", stateOf("SELECT 1.5::numeric[]"));
        assertEquals("42846", stateOf("SELECT true::bool[]"));
        assertTrue(messageOf("SELECT now()::timestamp[]")
                .contains("cannot cast type timestamp with time zone to timestamp without time zone[]"));
        // Text is read by the array's own input function, and null is null whatever its type.
        assertEquals("22P02", stateOf("SELECT '1'::int[]"));
        assertNull(stateOf("SELECT NULL::int[]"));
        assertNull(stateOf("SELECT '{1}'::int[]"));
    }
}
