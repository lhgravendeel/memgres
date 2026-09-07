package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What an aggregate's declaration records, and what a date template's week fields mean.
 *
 * <p>MSFUNC, MINVFUNC, MSTYPE and MINITCOND are the moving-aggregate half of a declaration -- how
 * a sliding window frame adds a row and takes one away -- and SORTOP names the operator that puts
 * two values in order. Consumed by the parser to keep it moving and then thrown away, an aggregate
 * declared with all of them recorded none, so pg_aggregate said the server could not compute it
 * over a frame at all. PARALLEL went the same way, and pg_proc called every aggregate unsafe.
 *
 * <p>An ISO week date says which day is meant by a year, a week within it and a day within that.
 * Week one is the week the fourth of January falls in, so ISO 2006 begins on the second. Read as
 * nothing at all, every such date came back as the first of January. The two calendars may not be
 * mixed in one template: PostgreSQL refuses {@code IYYY-MM-DD} rather than reading half of each.
 */
class WhatADeclarationAndATemplateSayTest {

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

    /** The whole of what CREATE AGGREGATE was written with reaches the catalogue. */
    @Test
    void whatAnAggregateDeclarationRecords() throws SQLException {
        exec("CREATE FUNCTION zwd_sum(int,int) RETURNS int LANGUAGE sql IMMUTABLE PARALLEL SAFE"
                + " AS $$ SELECT $1+$2 $$");
        exec("CREATE AGGREGATE zwd_agg(int) (SFUNC=zwd_sum, STYPE=int, INITCOND='0',"
                + " MSFUNC=zwd_sum, MINVFUNC=zwd_sum, MSTYPE=int, MINITCOND='0', SORTOP= <,"
                + " PARALLEL=SAFE)");
        assertEquals("zwd_sum|zwd_sum|integer|0|<(integer,integer)",
                one("SELECT aggmtransfn::text || '|' || aggminvtransfn::text || '|'"
                        + " || aggmtranstype::regtype::text || '|' || aggminitval || '|'"
                        + " || aggsortop::regoperator::text FROM pg_aggregate"
                        + " WHERE aggfnoid::regproc::text = 'zwd_agg'"));
        assertEquals("s", one("SELECT proparallel FROM pg_proc WHERE proname='zwd_agg'"));
        exec("DROP AGGREGATE zwd_agg(int)");
        // An aggregate written without any of them records none of them, and is unsafe.
        exec("CREATE AGGREGATE zwd_plain(int) (SFUNC=zwd_sum, STYPE=int)");
        assertEquals("-|-|0",
                one("SELECT aggmtransfn::text || '|' || aggminvtransfn::text || '|'"
                        + " || aggsortop::text FROM pg_aggregate"
                        + " WHERE aggfnoid::regproc::text = 'zwd_plain'"));
        assertEquals("u", one("SELECT proparallel FROM pg_proc WHERE proname='zwd_plain'"));
        exec("DROP AGGREGATE zwd_plain(int)");
        exec("DROP FUNCTION zwd_sum(int,int)");
    }

    /** An ISO week date names a day by its year, its week and its day within the week. */
    @Test
    void whatAnIsoWeekDateNames() throws SQLException {
        assertEquals("2006-10-19", one("SELECT to_date('2006-42-4', 'IYYY-IW-ID')::text"));
        assertEquals("2006-01-02", one("SELECT to_date('2006-01-1', 'IYYY-IW-ID')::text"));
        assertEquals("2006-01-08", one("SELECT to_date('2006-01-7', 'IYYY-IW-ID')::text"));
        assertEquals("2007-01-01", one("SELECT to_date('2006-53-1', 'IYYY-IW-ID')::text"));
        assertEquals("2005-01-02", one("SELECT to_date('2004-53-7', 'IYYY-IW-ID')::text"));
        // Without a day, the week's Monday; without a week, the ordinary first of January.
        assertEquals("2006-10-16", one("SELECT to_date('2006-42', 'IYYY-IW')::text"));
        assertEquals("2006-01-01", one("SELECT to_date('2006', 'IYYY')::text"));
        // A day of the ISO year is counted from the Monday that year begins on.
        assertEquals("2006-04-11", one("SELECT to_date('2006-100', 'IYYY-IDDD')::text"));
        assertEquals("2006-01-02", one("SELECT to_date('2006-1', 'IYYY-IDDD')::text"));
        // A two-digit ISO year is widened like any other.
        assertEquals("2006-10-19", one("SELECT to_date('06-42-4', 'IY-IW-ID')::text"));
        // The ordinary week number counts from the first of January, and the day says nothing.
        assertEquals("2006-10-15", one("SELECT to_date('2006 42 4', 'YYYY WW D')::text"));
        assertEquals("2006-01-01", one("SELECT to_date('2006 1 4', 'YYYY WW D')::text"));
    }

    /** The two calendars may not be mixed in one template. */
    @Test
    void whichTemplatesAreRefused() {
        for (String written : new String[]{"IYYY-MM-DD", "IYYY-DDD"}) {
            assertEquals("22007", stateOf("SELECT to_date('2006-10-15', '" + written + "')"),
                    written);
        }
        assertEquals("22007", stateOf("SELECT to_date('2020-42-4', 'YYYY-IW-ID')"));
        assertEquals("22007", stateOf("SELECT to_date('2020-100', 'YYYY-IDDD')"));
        // One calendar at a time is read as it always was.
        assertEquals("2020-10-15", one2("SELECT to_date('2020-10-15', 'YYYY-MM-DD')::text"));
    }

    private static String one2(String sql) {
        try {
            return one(sql);
        } catch (SQLException e) {
            throw new AssertionError(sql, e);
        }
    }
}
