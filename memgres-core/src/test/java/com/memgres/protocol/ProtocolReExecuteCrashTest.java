package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Issue #7: EXECUTE p_limit(1) crashes with "no field structure" when the SQL
 * has a leading comment. The JDBC driver uses extended protocol (Parse/Describe/Execute).
 * During Describe, inferColumns doesn't strip leading comments, so it fails to
 * recognize the EXECUTE statement → sends NoData → Execute sends DataRows
 * without RowDescription → JDBC driver crashes.
 */
class ProtocolReExecuteCrashTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        // Default mode (extended protocol) to match verification harness
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    /**
     * Core bug: SQL-level EXECUTE with a leading line comment fails because
     * inferColumns doesn't strip comments before checking startsWith("EXECUTE").
     * Describe sends NoData, then Execute sends DataRows → "no field structure" crash.
     */
    @Test void execute_with_leading_comment_no_crash() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE prt_t(id int PRIMARY KEY, a int)");
            s.execute("INSERT INTO prt_t VALUES (1,10),(2,20),(3,30)");
            s.execute("PREPARE p_lim(int) AS SELECT id, a FROM prt_t ORDER BY id LIMIT $1");
        }
        try {
            // First execute without comment, should work
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("EXECUTE p_lim(2)")) {
                int n = 0; while (rs.next()) n++;
                assertEquals(2, n);
            }

            // Execute with leading comment: this is the exact pattern from the verification
            // harness that causes the crash: "-- comment\nEXECUTE p_lim(1)"
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("-- reuse with different value\nEXECUTE p_lim(1)")) {
                int n = 0; while (rs.next()) n++;
                assertEquals(1, n, "EXECUTE with leading comment must not crash");
            }

            // Execute with block comment
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("/* reuse */ EXECUTE p_lim(3)")) {
                int n = 0; while (rs.next()) n++;
                assertEquals(3, n, "EXECUTE with leading block comment must not crash");
            }
        } finally {
            try (Statement s = conn.createStatement()) {
                try { s.execute("DEALLOCATE p_lim"); } catch (Exception ignored) {}
                s.execute("DROP TABLE prt_t");
            }
        }
    }

    /**
     * Same issue applies to FETCH with leading comments; inferColumns also
     * checks startsWith("FETCH") without stripping comments.
     */
    @Test void fetch_with_leading_comment_no_crash() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE prt_f(id int PRIMARY KEY, val text)");
            s.execute("INSERT INTO prt_f VALUES (1,'a'),(2,'b'),(3,'c')");
        }
        try {
            try (Statement s = conn.createStatement()) {
                s.execute("BEGIN");
                s.execute("DECLARE cur_test CURSOR FOR SELECT * FROM prt_f ORDER BY id");
            }
            // FETCH with leading comment
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("-- fetch next row\nFETCH 1 FROM cur_test")) {
                assertTrue(rs.next(), "FETCH with leading comment must return a row");
                assertEquals("1", rs.getString(1));
            }
            try (Statement s = conn.createStatement()) {
                s.execute("CLOSE cur_test");
                s.execute("COMMIT");
            }
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE prt_f");
            }
        }
    }
}
