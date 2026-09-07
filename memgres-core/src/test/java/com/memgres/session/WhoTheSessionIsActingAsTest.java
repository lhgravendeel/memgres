package com.memgres.session;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What is_superuser reports.
 *
 * <p>is_superuser is a preset: PostgreSQL computes it from whoever the session is acting as, it is
 * not a value anyone stored. Read out of the table it was seeded in, it stayed "on" across a SET
 * ROLE to a role that is nothing of the kind -- so a session that had just given up its powers
 * still said it had them.
 */
class WhoTheSessionIsActingAsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE ROLE zwq_plain LOGIN");
        exec("CREATE ROLE zwq_super LOGIN SUPERUSER");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("RESET ROLE");
            exec("DROP ROLE zwq_plain");
            exec("DROP ROLE zwq_super");
            conn.close();
        }
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

    /** The setting follows the role the session is acting as. */
    @Test
    void whatIsSuperuserFollows() throws SQLException {
        assertEquals("on", one("SELECT current_setting('is_superuser')"));
        exec("SET ROLE zwq_plain");
        assertEquals("off", one("SELECT current_setting('is_superuser')"));
        // SHOW reads the same preset, so the two agree.
        assertEquals("off", one("SHOW is_superuser"));
        // A role that is a superuser says so.
        exec("SET ROLE zwq_super");
        assertEquals("on", one("SELECT current_setting('is_superuser')"));
        exec("RESET ROLE");
        assertEquals("on", one("SELECT current_setting('is_superuser')"));
    }
}
