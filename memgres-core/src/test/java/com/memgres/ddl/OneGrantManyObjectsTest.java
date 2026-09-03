package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * One GRANT may name several objects, and says the same thing about each of them.
 *
 * <p>Read as one object, the comma between two of them was a syntax error, so a statement that
 * granted on three tables granted on none -- and the REVOKE written the same way took nothing
 * back.
 */
class OneGrantManyObjectsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
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

    private static String privilege(String relation, String what) throws SQLException {
        return one("SELECT has_table_privilege('zog_r','" + relation + "','" + what + "')::text");
    }

    /** Every table named takes the privilege, and gives it up again. */
    @Test
    void severalTablesInOneStatement() throws SQLException {
        exec("CREATE TABLE zog_a (i int)");
        exec("CREATE TABLE zog_b (i int)");
        exec("CREATE TABLE zog_c (i int)");
        exec("CREATE ROLE zog_r");
        exec("GRANT SELECT ON zog_a, zog_b, public.zog_c TO zog_r");
        assertEquals("true", privilege("zog_a", "SELECT"));
        assertEquals("true", privilege("zog_b", "SELECT"));
        assertEquals("true", privilege("zog_c", "SELECT"));
        assertEquals("false", privilege("zog_a", "UPDATE"));
        exec("REVOKE SELECT ON zog_a, zog_b FROM zog_r");
        assertEquals("false", privilege("zog_a", "SELECT"));
        assertEquals("false", privilege("zog_b", "SELECT"));
        // The one that was not named keeps what it was given.
        assertEquals("true", privilege("zog_c", "SELECT"));
        // The word TABLE changes nothing about how many may be named.
        exec("GRANT UPDATE ON TABLE zog_a, zog_b TO zog_r");
        assertEquals("true", privilege("zog_a", "UPDATE"));
        assertEquals("true", privilege("zog_b", "UPDATE"));
        exec("DROP TABLE zog_a");
        exec("DROP TABLE zog_b");
        exec("DROP TABLE zog_c");
        exec("DROP ROLE zog_r");
    }

    /** A sequence list is read the same way. */
    @Test
    void severalSequencesInOneStatement() throws SQLException {
        exec("CREATE SEQUENCE zog_s1");
        exec("CREATE SEQUENCE zog_s2");
        exec("CREATE ROLE zog_sr");
        exec("GRANT USAGE ON SEQUENCE zog_s1, zog_s2 TO zog_sr");
        assertEquals("true", one("SELECT has_sequence_privilege('zog_sr','zog_s1','USAGE')::text"));
        assertEquals("true", one("SELECT has_sequence_privilege('zog_sr','zog_s2','USAGE')::text"));
        exec("DROP SEQUENCE zog_s1");
        exec("DROP SEQUENCE zog_s2");
        exec("DROP ROLE zog_sr");
    }
}
