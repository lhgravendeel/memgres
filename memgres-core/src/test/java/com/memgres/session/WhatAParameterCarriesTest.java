package com.memgres.session;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a prepared parameter carries, and which procedure a bare name reaches.
 *
 * <p>A parameter is of the type the PREPARE declared for it, and that declaration is the only
 * thing that says what its value is. Read without it, a composite handed to EXECUTE was a record
 * of no particular type: naming a field of it identified no column, and once that was fixed the
 * field still had no type to be described with, so the column came back as text.
 *
 * <p>A bare procedure name is looked for along the search path and nowhere else, as any
 * unqualified routine reference is. Read from the register by name alone, a procedure in a schema
 * the session could not name was called anyway.
 */
class WhatAParameterCarriesTest {

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

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** A composite parameter carries its type, so a field of it has one too. */
    @Test
    void whatACompositeParameterCarries() throws SQLException {
        exec("CREATE TYPE zwx_c AS (a int, b text)");
        exec("PREPARE zwx_p (zwx_c) AS SELECT ($1).a AS v");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE zwx_p(ROW(3,'z'))")) {
            assertEquals("int4", rs.getMetaData().getColumnTypeName(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
        // The other field is the other field, and its type is its own.
        exec("DEALLOCATE zwx_p");
        exec("PREPARE zwx_q (zwx_c) AS SELECT ($1).b AS v");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE zwx_q(ROW(3,'z'))")) {
            assertEquals("text", rs.getMetaData().getColumnTypeName(1));
            assertTrue(rs.next());
            assertEquals("z", rs.getString(1));
        }
        exec("DEALLOCATE zwx_q");
        exec("DROP TYPE zwx_c");
    }

    /** A bare name reaches a procedure only where the search path reaches its schema. */
    @Test
    void whichProcedureABareNameReaches() throws SQLException {
        exec("CREATE SCHEMA zwx_sch");
        exec("CREATE PROCEDURE zwx_sch.zwx_sp() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$");
        assertEquals("42883", stateOf("CALL zwx_sp()"));
        assertNull(stateOf("CALL zwx_sch.zwx_sp()"));
        // With the schema on the path, the bare name reaches it.
        exec("SET search_path = zwx_sch, public");
        assertNull(stateOf("CALL zwx_sp()"));
        exec("SET search_path = public");
        exec("DROP PROCEDURE zwx_sch.zwx_sp()");
        exec("DROP SCHEMA zwx_sch");
    }
}
