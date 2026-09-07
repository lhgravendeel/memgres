package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.util.PSQLException;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a missing expression is called.
 *
 * <p>PostgreSQL names the token it found where the expression should have been -- the THEN of an
 * IF with no condition, the LOOP of a WHILE with none, the semicolon after a bare RETURN -- rather
 * than saying the body ends too soon, or accepting the statement outright.
 */
class WhatAMissingExpressionIsCalledTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static void assertMissing(String at, String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            fail("expected a refusal: " + sql);
        } catch (SQLException e) {
            assertEquals("42601", e.getSQLState(), sql);
            assertEquals("missing expression at or near \"" + at + "\"",
                    ((PSQLException) e).getServerErrorMessage().getMessage(), sql);
        }
    }

    /** Each place the grammar needs an expression names what it found instead. */
    @Test
    void whereTheExpressionShouldHaveBeen() throws SQLException {
        assertMissing("THEN", "CREATE FUNCTION zxe_a() RETURNS int LANGUAGE plpgsql"
                + " AS $$ BEGIN IF THEN END $$");
        assertMissing("THEN", "CREATE FUNCTION zxe_b() RETURNS int LANGUAGE plpgsql"
                + " AS $$ BEGIN IF THEN END IF; RETURN 1; END $$");
        assertMissing("THEN", "CREATE FUNCTION zxe_c() RETURNS int LANGUAGE plpgsql"
                + " AS $$ BEGIN IF true THEN RETURN 1; ELSIF THEN RETURN 2; END IF; RETURN 3; END $$");
        assertMissing("LOOP", "CREATE FUNCTION zxe_d() RETURNS int LANGUAGE plpgsql"
                + " AS $$ BEGIN WHILE LOOP END LOOP; RETURN 1; END $$");
        assertMissing(";", "CREATE FUNCTION zxe_e() RETURNS int LANGUAGE plpgsql"
                + " AS $$ BEGIN RETURN; END $$");
        // A body that writes its expressions is created as before.
        exec("CREATE FUNCTION zxe_ok() RETURNS int LANGUAGE plpgsql"
                + " AS $$ BEGIN IF true THEN RETURN 1; END IF; RETURN 2; END $$");
        exec("DROP FUNCTION zxe_ok()");
    }
}
