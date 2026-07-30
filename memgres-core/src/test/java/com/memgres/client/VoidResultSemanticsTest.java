package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A void result is not a NULL. PostgreSQL sends an empty value of type void, so a client reads back
 * an empty string and {@code IS NULL} is false.
 *
 * <p>memgres agreed for the blocking advisory-lock functions — they were listed by name where a
 * function's result type is decided — and disagreed for every other void function: pg_sleep,
 * pg_sleep_for, pg_stat_clear_snapshot and pg_stat_reset answered SQL NULL, while setseed,
 * pg_notify and the stat functions declared their result text or unknown. The list is now read off
 * the signature table, which already carried prorettype 2278 for all of them.
 *
 * <p>Three advisory functions turned out to be missing from that table altogether, so replacing the
 * hand-written list with it briefly took their void typing away — which is why they are asserted
 * here too.
 */
class VoidResultSemanticsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** The value, whether the driver saw a NULL, and the column type the server described. */
    private static String describe(String expr) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT " + expr)) {
            ResultSetMetaData md = rs.getMetaData();
            assertTrue(rs.next(), "one row for " + expr);
            String v = rs.getString(1);
            return "[" + v + "] wasNull=" + rs.wasNull() + " type=" + md.getColumnTypeName(1);
        }
    }

    private static String typeOf(String expr) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_typeof(" + expr + ")::text")) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    @Test
    void aVoidResultIsAnEmptyValueAndNotANull() throws Exception {
        String[] calls = {
                "pg_sleep(0)",
                "pg_sleep_for('0 seconds')",
                "pg_stat_clear_snapshot()",
                "pg_stat_reset()",
                "setseed(0.5)",
                "pg_notify('vrs_t', 'x')",
                "pg_advisory_lock(9392001)",
        };
        for (String call : calls) {
            assertEquals("[] wasNull=false type=void", describe(call),
                    call + " must answer an empty void value, not SQL NULL");
        }
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT pg_advisory_unlock_all()");
        }
    }

    @Test
    void everyVoidFunctionDeclaresTheVoidType() throws Exception {
        String[] calls = {
                "pg_sleep(0)", "setseed(0.75)", "pg_stat_clear_snapshot()",
                "pg_stat_reset()", "pg_notify('vrs_t', 'y')",
                "pg_advisory_lock(9392002)", "pg_advisory_lock_shared(9392003)",
                "pg_advisory_xact_lock_shared(9392004)", "pg_advisory_unlock_all()",
        };
        for (String call : calls) {
            assertEquals("void", typeOf(call), call + " is declared to return void");
        }
    }

    @Test
    void aFunctionThatReturnsSomethingIsUntouched() throws Exception {
        // The rule fires only where every signature of the name returns void, so the unlock
        // functions — which answer whether a lock was held — keep their boolean.
        assertEquals("boolean", typeOf("pg_advisory_unlock(9392090)"));
        assertEquals("boolean", typeOf("pg_advisory_unlock_shared(9392091)"));
        assertEquals("boolean", typeOf("pg_try_advisory_lock(9392092)"));
        assertEquals("integer", typeOf("abs(-1)"));
        assertEquals("text", typeOf("current_setting('search_path')"));

        try (Statement s = conn.createStatement()) {
            s.execute("SELECT pg_advisory_unlock_all()");
        }
    }

    @Test
    void theCatalogAgreesAboutWhatTheseReturn() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT DISTINCT proname, format_type(prorettype, NULL) FROM pg_proc "
                             + "WHERE proname IN ('pg_advisory_lock_shared',"
                             + "'pg_advisory_xact_lock_shared','pg_advisory_unlock_all',"
                             + "'pg_advisory_unlock_shared') ORDER BY proname")) {
            StringBuilder sb = new StringBuilder();
            while (rs.next()) sb.append(rs.getString(1)).append('=').append(rs.getString(2)).append(' ');
            assertEquals("pg_advisory_lock_shared=void pg_advisory_unlock_all=void "
                            + "pg_advisory_unlock_shared=boolean pg_advisory_xact_lock_shared=void ",
                    sb.toString(),
                    "these three were absent from the signature table entirely");
        }
    }
}
