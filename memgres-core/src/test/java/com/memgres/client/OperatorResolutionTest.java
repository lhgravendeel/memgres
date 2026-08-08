package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An operator exists for the types it is written over, or it does not.
 *
 * <p>Operators used to be chosen from the runtime classes of the two values, so every one of them
 * had an accidental domain: {@code 1 || 2} concatenated, {@code money + 1} added, {@code date LIKE
 * '2020%'} matched. An application whose query has a type error learned nothing, because there was
 * nothing for the wrong pair to fail against.
 */
class OperatorResolutionTest {

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

    private static SQLException refused(String sql) {
        return assertThrows(SQLException.class, () -> scalar(sql), sql);
    }

    /** A pair the operator table has no row for does not exist. */
    @Test
    void aPairWithNoEntryDoesNotExist() {
        for (String sql : new String[]{
                "SELECT 1 || 2",
                "SELECT 1 || 2 + 3",
                "SELECT '2020-01-01'::date LIKE '2020%'",
                "SELECT 'abc' LIKE 5",
                "SELECT '1.00'::money + 1",
                "SELECT 'fox:1'::tsvector @@ 'fox'::text",
                "SELECT '{1,2}'::int[] = '{1,2}'::bigint[]"}) {
            SQLException e = refused(sql);
            assertEquals("42883", e.getSQLState(), sql);
            assertTrue(e.getMessage().contains("operator does not exist"), sql + " → " + e.getMessage());
        }
    }

    /** The message names the two types the query wrote. */
    @Test
    void theMessageNamesBothOperandTypes() {
        assertTrue(refused("SELECT 1 || 2").getMessage().contains("integer || integer"));
        assertTrue(refused("SELECT '2020-01-01'::date LIKE '2020%'").getMessage()
                .contains("date ~~ unknown"));
    }

    /** Nothing typed on either side, and more than one shape to choose from, is ambiguous. */
    @Test
    void twoUntypedOperandsCanBeAmbiguous() {
        for (String sql : new String[]{"SELECT NULL + NULL", "SELECT NULL - NULL",
                "SELECT 'a' * 'b'", "SELECT '{a,b}' @> '{a}'"}) {
            SQLException e = refused(sql);
            assertEquals("42725", e.getSQLState(), sql);
            assertTrue(e.getMessage().contains("operator is not unique"), sql);
        }
    }

    /** Where a candidate takes text on both sides, the unknowns are read as text and resolve. */
    @Test
    void untypedOperandsResolveThroughText() throws Exception {
        assertEquals("t", scalar("SELECT 'abc' LIKE 'a%'"));
        assertEquals("t", scalar("SELECT 'x' ~ 'x'"));
        assertEquals("ab", scalar("SELECT 'a' || 'b'"));
        assertNull(scalar("SELECT NULL || NULL"));
        assertNull(scalar("SELECT NULL = NULL"));
    }

    /** A polymorphic row unifies an array with its own element type. */
    @Test
    void arrayOperatorsStillResolve() throws Exception {
        assertEquals("{1,2,3}", scalar("SELECT ARRAY[1,2] || 3"));
        assertEquals("{3,1,2}", scalar("SELECT 3 || ARRAY[1,2]"));
        assertEquals("{1,2,1,2}", scalar("SELECT ARRAY[1,2] || ARRAY[1,2]"));
        assertEquals("{a,b}", scalar("SELECT ARRAY['a']::text[] || 'b'::char(1)"));
        assertEquals("t", scalar("SELECT 1 = ANY(ARRAY[1,2])"));
    }

    /** An operator named with its schema resolves to the same one. */
    @Test
    void aQualifiedOperatorIsTheOperatorItNames() throws Exception {
        assertEquals("3", scalar("SELECT 1 OPERATOR(pg_catalog.+) 2"));
        assertEquals("ab", scalar("SELECT 'a' OPERATOR(pg_catalog.||) 'b'"));
    }

    /** A schema on the search path that does not exist is skipped, not complained about. */
    @Test
    void aMissingSchemaOnThePathIsNotAnError() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("SET search_path TO zz_op_nosuch, public");
        }
        try {
            assertEquals("3", scalar("SELECT 1 + 2"));
            assertEquals("3", scalar("SELECT 1 OPERATOR(pg_catalog.+) 2"));
        } finally {
            try (Statement st = conn.createStatement()) {
                st.execute("RESET search_path");
            }
        }
    }
}
