package com.memgres.notify;

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
 * A notification travels through a fixed-size queue slot, so PostgreSQL bounds both the channel
 * name and the payload rather than truncating either. Expectations captured from a live
 * PostgreSQL 18.0 server.
 *
 * <p>N56 NOTIFY payload and channel-name validation.
 */
class NotifyInputValidationTest {

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

    private static String expr(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static String state(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql));
        return e.getSQLState();
    }

    @Test
    void anOversizedPayloadIsRejected() {
        assertEquals("22023", state("SELECT pg_notify('nvc', repeat('x', 9000))"));
        assertEquals("22023", state("SELECT pg_notify('nvc', repeat('x', 8000))"));
    }

    @Test
    void anEmptyChannelNameIsRejected() {
        assertEquals("22023", state("SELECT pg_notify(NULL, 'x')"));
        assertEquals("22023", state("SELECT pg_notify('', 'x')"));
    }

    /** Just inside the limit is accepted, and pg_notify returns void rather than null. */
    @Test
    void aPayloadInsideTheLimitIsAccepted() throws Exception {
        assertEquals("f", expr("SELECT (pg_notify('nvc', repeat('x', 7999)) IS NULL)"));
        assertEquals("f", expr("SELECT (pg_notify('nvc','ok') IS NULL)"));
    }

    @Test
    void theStatementFormIsBoundedTheSameWay() throws Exception {
        exec("LISTEN nvc");
        try {
            assertEquals("22023", state("NOTIFY nvc, '" + repeat(9000) + "'"));
            exec("NOTIFY nvc, 'ok'");
            assertEquals("1", expr("SELECT count(*)::text FROM pg_listening_channels()"));
        } finally {
            exec("UNLISTEN *");
        }
    }

    private static String repeat(int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) sb.append('x');
        return sb.toString();
    }
}
