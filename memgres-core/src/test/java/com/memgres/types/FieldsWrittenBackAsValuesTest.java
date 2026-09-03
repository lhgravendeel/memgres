package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A value written back into a composite, and a bound written back into a range.
 *
 * <p>A row is written as its fields with commas between them, so a field holding a comma or a
 * quote is written in quotes: assembled with commas alone, a field holding one became two fields
 * and the row could not be read back at all.
 *
 * <p>A range's bound is written by its own type's output function, era and all, which is what
 * makes a date before the common era read back as the date it is.
 */
class FieldsWrittenBackAsValuesTest {

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

    /** A field is written so that it reads back as the one field it is. */
    @Test
    void aFieldHoldingWhatSeparatesFields() throws SQLException {
        exec("CREATE TYPE zfw_addr AS (street text, city text)");
        exec("CREATE TABLE zfw_ct (a zfw_addr)");
        exec("INSERT INTO zfw_ct VALUES (ROW('S','Ede'))");
        exec("UPDATE zfw_ct SET a.street = 'One, Two'");
        assertEquals("One, Two", one("SELECT (a).street FROM zfw_ct"));
        assertEquals("Ede", one("SELECT (a).city FROM zfw_ct"));
        exec("UPDATE zfw_ct SET a.street = 'has \"quote\"'");
        assertEquals("has \"quote\"", one("SELECT (a).street FROM zfw_ct"));
        assertEquals("(\"has \"\"quote\"\"\",Ede)", one("SELECT a::text FROM zfw_ct"));
        exec("DROP TABLE zfw_ct");
        exec("DROP TYPE zfw_addr");
    }

    /** A bound is written the way its own type is written. */
    @Test
    void aBoundBeforeTheCommonEra() throws SQLException {
        assertEquals("[\"4713-01-01 BC\",2020-01-01)",
                one("SELECT '[4713-01-01 BC,2020-01-01)'::daterange::text"));
        assertEquals("4713-01-01 BC", one("SELECT '4713-01-01 BC'::date::text"));
        // A bound in the common era needs no quotes and keeps none.
        assertEquals("[2020-01-01,2020-02-01)",
                one("SELECT daterange('2020-01-01','2020-02-01')::text"));
    }
}
