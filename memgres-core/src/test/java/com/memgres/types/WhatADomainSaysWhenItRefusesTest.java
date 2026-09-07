package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.util.PSQLException;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a domain says when it refuses a value.
 *
 * <p>A domain that refuses a value says which of its constraints the value broke, and names the
 * domain, its schema and that constraint in the error's own fields. Rewritten on the way out of a
 * prepared statement's parameter binding, the complaint became "invalid input syntax" -- a
 * sentence about how the value was spelled, for a value that was spelled perfectly well.
 */
class WhatADomainSaysWhenItRefusesTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE DOMAIN zwd_pos AS int CHECK (VALUE > 0)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP DOMAIN zwd_pos");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static PSQLException refusalOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return (PSQLException) e;
        }
    }

    /** A parameter the domain refuses is refused by the domain, in its own words. */
    @Test
    void whatAParameterTheDomainRefusesSays() throws SQLException {
        exec("PREPARE zwd_p (zwd_pos) AS SELECT $1 AS v");
        PSQLException refusal = refusalOf("EXECUTE zwd_p(-1)");
        assertNotNull(refusal);
        assertEquals("23514", refusal.getSQLState());
        assertEquals("value for domain zwd_pos violates check constraint \"zwd_pos_check\"",
                refusal.getServerErrorMessage().getMessage());
        assertEquals("zwd_pos_check", refusal.getServerErrorMessage().getConstraint());
        assertEquals("zwd_pos", refusal.getServerErrorMessage().getDatatype());
        assertEquals("public", refusal.getServerErrorMessage().getSchema());
        // One the domain accepts goes through.
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("EXECUTE zwd_p(3)")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
        exec("DEALLOCATE zwd_p");
    }

    /** A value the declared type cannot read at all is still a complaint about its spelling. */
    @Test
    void whatAValueTheTypeCannotReadSays() throws SQLException {
        exec("PREPARE zwd_q (int) AS SELECT $1 AS v");
        PSQLException refusal = refusalOf("EXECUTE zwd_q('x')");
        assertNotNull(refusal);
        assertEquals("22P02", refusal.getSQLState());
        exec("DEALLOCATE zwd_q");
    }

    /** A column's domain names its schema too. */
    @Test
    void whatAColumnsDomainSays() throws SQLException {
        exec("CREATE TABLE zwd_t (a zwd_pos)");
        PSQLException refusal = refusalOf("INSERT INTO zwd_t VALUES (-1)");
        assertNotNull(refusal);
        assertEquals("23514", refusal.getSQLState());
        assertEquals("zwd_pos_check", refusal.getServerErrorMessage().getConstraint());
        assertEquals("public", refusal.getServerErrorMessage().getSchema());
        exec("DROP TABLE zwd_t");
    }
}
