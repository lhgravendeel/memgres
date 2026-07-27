package com.memgres.ddl;

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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Object definitions and session settings PostgreSQL 18 accepts: a view that refers to itself, a
 * range type, renaming a composite type, reading the time zone back by the name it is set with,
 * and calling a function that supplies all of its own arguments. Expectations captured from a live
 * PostgreSQL 18.0 server.
 *
 * <p>B4 CREATE RECURSIVE VIEW, B6 CREATE TYPE AS RANGE, B7 ALTER TYPE RENAME TO,
 * B8 SHOW/RESET TIME ZONE, B9 all-default function arguments.
 */
class TypeViewDdlAndSessionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
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

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static String state(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql));
        return e.getSQLState();
    }

    // ---- B4: CREATE RECURSIVE VIEW ----

    /** The recursive form is shorthand for a view whose body is a WITH RECURSIVE query. */
    @Test
    void aRecursiveViewCounts() throws Exception {
        exec("DROP VIEW IF EXISTS tvs_v CASCADE");
        exec("CREATE RECURSIVE VIEW tvs_v (n) AS"
                + " SELECT 1 UNION ALL SELECT n+1 FROM tvs_v WHERE n < 3");
        assertEquals("3", one("SELECT count(*) FROM tvs_v"));
        assertEquals("3", one("SELECT max(n) FROM tvs_v"));
        exec("DROP VIEW tvs_v");
    }

    @Test
    void aRecursiveViewIsAnOrdinaryViewInTheCatalog() throws Exception {
        exec("DROP VIEW IF EXISTS tvs_v2 CASCADE");
        exec("CREATE RECURSIVE VIEW tvs_v2 (n) AS"
                + " SELECT 1 UNION ALL SELECT n+1 FROM tvs_v2 WHERE n < 2");
        assertEquals("v", one("SELECT relkind::text FROM pg_class WHERE relname='tvs_v2'"));
        exec("DROP VIEW tvs_v2");
    }

    // ---- B6: CREATE TYPE ... AS RANGE ----

    @Test
    void aRangeTypeCanBeCreatedAndUsed() throws Exception {
        exec("DROP TYPE IF EXISTS tvs_r CASCADE");
        exec("CREATE TYPE tvs_r AS RANGE (subtype = int8)");
        assertEquals("tvs_r", one("SELECT 'tvs_r'::regtype::text"));
        exec("DROP TYPE tvs_r");
    }

    @Test
    void aRangeTypeShowsAsARangeInTheCatalog() throws Exception {
        exec("DROP TYPE IF EXISTS tvs_r2 CASCADE");
        exec("CREATE TYPE tvs_r2 AS RANGE (subtype = int8)");
        assertEquals("r", one("SELECT typtype::text FROM pg_type WHERE typname='tvs_r2'"));
        exec("DROP TYPE tvs_r2");
    }

    // ---- B7: ALTER TYPE ... RENAME TO ----

    @Test
    void aCompositeTypeCanBeRenamed() throws Exception {
        exec("DROP TYPE IF EXISTS tvs_c CASCADE");
        exec("DROP TYPE IF EXISTS tvs_c2 CASCADE");
        exec("CREATE TYPE tvs_c AS (a int)");
        exec("ALTER TYPE tvs_c RENAME TO tvs_c2");
        assertEquals("tvs_c2", one("SELECT typname FROM pg_type WHERE typname='tvs_c2'"));
        assertEquals("0", one("SELECT count(*) FROM pg_type WHERE typname='tvs_c'"));
        exec("DROP TYPE tvs_c2");
    }

    @Test
    void anEnumTypeCanBeRenamedToo() throws Exception {
        exec("DROP TYPE IF EXISTS tvs_e CASCADE");
        exec("DROP TYPE IF EXISTS tvs_e2 CASCADE");
        exec("CREATE TYPE tvs_e AS ENUM ('a','b')");
        exec("ALTER TYPE tvs_e RENAME TO tvs_e2");
        assertEquals("a", one("SELECT 'a'::tvs_e2::text"));
        exec("DROP TYPE tvs_e2");
    }

    /** Attribute-level changes keep working. */
    @Test
    void compositeAttributesCanStillBeChanged() throws Exception {
        exec("DROP TYPE IF EXISTS tvs_c3 CASCADE");
        exec("CREATE TYPE tvs_c3 AS (a int)");
        exec("ALTER TYPE tvs_c3 ADD ATTRIBUTE b text");
        exec("ALTER TYPE tvs_c3 RENAME ATTRIBUTE b TO c");
        exec("ALTER TYPE tvs_c3 DROP ATTRIBUTE c");
        assertEquals("tvs_c3", one("SELECT typname FROM pg_type WHERE typname='tvs_c3'"));
        exec("DROP TYPE tvs_c3");
    }

    // ---- B8: SHOW / RESET TIME ZONE ----

    @Test
    void theTimeZoneCanBeReadBackByTheNameItIsSetWith() throws Exception {
        exec("SET TIME ZONE 'UTC'");
        assertEquals("UTC", one("SHOW TIME ZONE"));
        exec("SET TIME ZONE 'Europe/Amsterdam'");
        assertEquals("Europe/Amsterdam", one("SHOW TIME ZONE"));
        exec("RESET TIME ZONE");
    }

    @Test
    void showTimezoneAsOneWordStillWorks() throws Exception {
        exec("SET TIME ZONE 'UTC'");
        assertEquals("UTC", one("SHOW timezone"));
        exec("RESET TIME ZONE");
    }

    @Test
    void resetTimeZoneUndoesTheSetting() throws Exception {
        String before = one("SHOW TIME ZONE");
        exec("SET TIME ZONE 'Europe/Amsterdam'");
        exec("RESET TIME ZONE");
        assertEquals(before, one("SHOW TIME ZONE"));
    }

    @Test
    void anUnknownSettingIsStillRejected() {
        assertEquals("42704", state("SHOW no_such_setting_at_all"));
    }

    // ---- B9: calling a function that supplies all its arguments ----

    @Test
    void aFunctionWithAllDefaultsCanBeCalledWithNoArguments() throws Exception {
        exec("CREATE OR REPLACE FUNCTION tvs_f(a int DEFAULT 1) RETURNS int"
                + " LANGUAGE sql AS $$ SELECT a $$");
        assertEquals("1", one("SELECT tvs_f()"));
        assertEquals("5", one("SELECT tvs_f(5)"));
        exec("DROP FUNCTION tvs_f(int)");
    }

    @Test
    void aFunctionWithSomeDefaultsCanOmitTheDefaultedOnes() throws Exception {
        exec("CREATE OR REPLACE FUNCTION tvs_g(a int, b int DEFAULT 10) RETURNS int"
                + " LANGUAGE sql AS $$ SELECT a + b $$");
        assertEquals("11", one("SELECT tvs_g(1)"));
        assertEquals("3", one("SELECT tvs_g(1,2)"));
        exec("DROP FUNCTION tvs_g(int,int)");
    }

    @Test
    void aVariadicFunctionWithDefaultsCanBeCalledWithNoArguments() throws Exception {
        exec("CREATE OR REPLACE FUNCTION tvs_h(a int DEFAULT 1, VARIADIC b int[] DEFAULT '{}')"
                + " RETURNS int LANGUAGE sql AS $$ SELECT a $$");
        assertEquals("1", one("SELECT tvs_h()"));
        assertEquals("2", one("SELECT tvs_h(2)"));
        assertEquals("2", one("SELECT tvs_h(2,3,4)"));
        exec("DROP FUNCTION tvs_h(int,int[])");
    }
}
