package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a tool learns about the server by reading the catalog. A function memgres evaluates but
 * never records in pg_proc works when called and is invisible to everything that asks first — the
 * JDBC driver's getFunctions, ::regproc, psql's \df. The same holds for the array and row types a
 * driver consults to decide how to decode a column. Expectations captured from a live
 * PostgreSQL 18.0 server.
 *
 * <p>D1 pg_proc, D2 array and range types, D3 getTypeInfo, D8 getUDTs.
 */
class CatalogProcAndTypeRegistryTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cpt_t (id int PRIMARY KEY, name text)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static int count(String sql) throws SQLException {
        return Integer.parseInt(one(sql));
    }

    // ---- D1: pg_proc lists the functions memgres has ----

    @Test
    void aBuiltInFunctionAppearsInPgProc() throws Exception {
        assertTrue(count("SELECT count(*) FROM pg_proc WHERE proname = 'upper'") >= 1);
        assertTrue(count("SELECT count(*) FROM pg_proc WHERE proname = 'abs'") >= 1);
        assertTrue(count("SELECT count(*) FROM pg_proc WHERE proname = 'age'") >= 1);
        assertTrue(count("SELECT count(*) FROM pg_proc WHERE proname = 'to_char'") >= 1);
    }

    @Test
    void theBuiltInsLiveInPgCatalog() throws Exception {
        assertEquals("pg_catalog", one(
                "SELECT n.nspname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace"
              + " WHERE p.proname = 'upper' LIMIT 1"));
    }

    /**
     * A name in pg_proc is what ::regproc resolves through. The name has to be one the server
     * carries only once — PG itself refuses an ambiguous one, since upper() covers text as well
     * as ranges.
     */
    @Test
    void aBuiltInResolvesAsRegproc() throws Exception {
        assertEquals("pg_backend_pid", one("SELECT 'pg_backend_pid'::regproc::text"));
    }

    @Test
    void theDriverCanListFunctions() throws Exception {
        int found = 0;
        try (ResultSet rs = conn.getMetaData().getFunctions(null, "pg_catalog", "upper")) {
            while (rs.next()) found++;
        }
        assertTrue(found >= 1, "getFunctions should report upper()");
    }

    @Test
    void aUserFunctionIsStillListed() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE FUNCTION cpt_f(a int) RETURNS int"
                    + " LANGUAGE sql AS $$ SELECT a $$");
        }
        assertEquals(1, count("SELECT count(*) FROM pg_proc WHERE proname = 'cpt_f'"));
        try (Statement s = conn.createStatement()) {
            s.execute("DROP FUNCTION cpt_f(int)");
        }
    }

    // ---- D2: array and range types ----

    @Test
    void theCommonArrayTypesAreRegistered() throws Exception {
        String[] arrays = {"_int4", "_text", "_bool", "_numeric", "_uuid", "_jsonb",
                           "_bit", "_box", "_oid", "_xml", "_macaddr"};
        for (String name : arrays) {
            assertEquals(1, count("SELECT count(*) FROM pg_type WHERE typname = '" + name + "'"),
                    name + " should be registered");
        }
    }

    @Test
    void anArrayTypePointsAtItsElementType() throws Exception {
        assertEquals("int4", one(
                "SELECT e.typname FROM pg_type a JOIN pg_type e ON e.oid = a.typelem"
              + " WHERE a.typname = '_int4'"));
    }

    /** A range is its own kind of type, and a multirange another. */
    @Test
    void rangeTypesAreClassifiedAsRanges() throws Exception {
        assertEquals("r", one("SELECT typtype::text FROM pg_type WHERE typname = 'int4range'"));
        assertEquals("r", one("SELECT typtype::text FROM pg_type WHERE typname = 'numrange'"));
        assertEquals("m", one("SELECT typtype::text FROM pg_type WHERE typname = 'int4multirange'"));
        assertEquals("m", one("SELECT typtype::text FROM pg_type WHERE typname = 'datemultirange'"));
    }

    @Test
    void ordinaryTypesAreStillBaseTypes() throws Exception {
        assertEquals("b", one("SELECT typtype::text FROM pg_type WHERE typname = 'int4'"));
        assertEquals("b", one("SELECT typtype::text FROM pg_type WHERE typname = 'text'"));
    }

    // ---- D3: getTypeInfo ----

    @Test
    void theDriverCanMapTheCommonTypes() throws Exception {
        java.util.Set<String> names = new java.util.HashSet<String>();
        try (ResultSet rs = conn.getMetaData().getTypeInfo()) {
            while (rs.next()) names.add(rs.getString("TYPE_NAME"));
        }
        for (String expected : new String[]{"int4", "text", "bool", "numeric", "uuid", "jsonb"}) {
            assertTrue(names.contains(expected), "getTypeInfo should include " + expected);
        }
    }

    // ---- D8: getUDTs ----

    /** A table is also a row type, and that is what getUDTs reports. */
    @Test
    void aTableIsRegisteredAsARowType() throws Exception {
        assertEquals("c", one("SELECT typtype::text FROM pg_type WHERE typname = 'cpt_t'"));
    }

    @Test
    void theDriverCanListUserDefinedTypes() throws Exception {
        int found = 0;
        try (ResultSet rs = conn.getMetaData().getUDTs(null, "public", "cpt%", null)) {
            while (rs.next()) found++;
        }
        assertTrue(found >= 1, "getUDTs should report the row type of cpt_t");
    }

    @Test
    void aCompositeTypeIsStillItsOwnRowType() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TYPE IF EXISTS cpt_c CASCADE");
            s.execute("CREATE TYPE cpt_c AS (a int, b text)");
        }
        assertEquals("c", one("SELECT typtype::text FROM pg_type WHERE typname = 'cpt_c'"));
        assertEquals(1, count("SELECT count(*) FROM pg_type WHERE typname = 'cpt_c'"));
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TYPE cpt_c");
        }
    }
}
