package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Where a CREATE lands when the search path names nowhere.
 *
 * <p>An object nobody put a schema on lands in the first schema the search path names that can be
 * created in, and a path that names none selects nowhere at all. Read from the schema a bare name
 * resolves to, a function, view or sequence created under an empty path landed in public -- a
 * schema the reader had deliberately excluded.
 */
class WhereACreateLandsTest {

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
        if (conn != null) {
            exec("SET search_path TO public");
            conn.close();
        }
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

    /** An empty path selects nothing to create in, whatever kind of object is written. */
    @Test
    void whatAnEmptyPathSelects() throws SQLException {
        exec("SET search_path TO ''");
        assertEquals("3F000",
                stateOf("CREATE FUNCTION zwy_f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$"));
        assertEquals("3F000", stateOf("CREATE TABLE zwy_t (a int)"));
        assertEquals("3F000", stateOf("CREATE VIEW zwy_v AS SELECT 1 AS a"));
        assertEquals("3F000", stateOf("CREATE SEQUENCE zwy_s"));
        assertEquals("3F000", stateOf("CREATE TYPE zwy_c AS (a int)"));
        assertEquals("3F000",
                stateOf("CREATE PROCEDURE zwy_p() LANGUAGE sql AS $$ SELECT 1 $$"));
        // A qualified name says where to put it, so the path has nothing to decide.
        assertNull(stateOf("CREATE TABLE public.zwy_t2 (a int)"));
        exec("SET search_path TO public");
        exec("DROP TABLE zwy_t2");
        // And with a path again, an unqualified name lands where it names.
        assertNull(stateOf("CREATE SEQUENCE zwy_s2"));
        exec("DROP SEQUENCE zwy_s2");
    }
}
