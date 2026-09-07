package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Who row security binds, and how far a cursor moves.
 *
 * <p>A superuser, and a role created with BYPASSRLS, always bypass row security. FORCE takes away
 * the exemption that owning the table gives, and nobody else's: read as though it bound every
 * role, a superuser reading a table it had just forced saw none of its own rows, and a role given
 * BYPASSRLS was filtered by every policy it was meant to be free of.
 *
 * <p>{@code row_security = off} says the reader would rather be refused than quietly shown less
 * than the whole relation, so it is a refusal for anybody the policies would really have applied
 * to -- an owner who has forced them on themselves included. Read as a bypass for the owner, the
 * setting meant nothing to the role most likely to set it.
 *
 * <p>And a fetch's count is how far the cursor goes, not how many rows it found: a fetch of three
 * that found one row still went three places. Stopping at the row it found left the cursor short
 * of where PostgreSQL leaves it, so the next fetch answered with a row already read.
 */
class WhoRowSecurityBindsTest {

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

    private static java.util.List<String> rows(String sql) throws SQLException {
        java.util.List<String> out = new java.util.ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
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

    /** A superuser is never filtered, forced or not. */
    @Test
    void whatASuperuserSees() throws SQLException {
        exec("CREATE TABLE zwy_ow (id int)");
        exec("INSERT INTO zwy_ow VALUES (1),(2),(3),(4)");
        exec("ALTER TABLE zwy_ow ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE zwy_ow FORCE ROW LEVEL SECURITY");
        assertEquals("4", one("SELECT count(*)::text FROM zwy_ow"));
        exec("CREATE POLICY zwy_p ON zwy_ow FOR SELECT USING (id = 1)");
        assertEquals("4", one("SELECT count(*)::text FROM zwy_ow"));
        exec("DROP TABLE zwy_ow");
    }

    /** A role given BYPASSRLS is free of every policy. */
    @Test
    void whatABypassRoleSees() throws SQLException {
        exec("CREATE ROLE zwy_b LOGIN BYPASSRLS");
        exec("CREATE TABLE zwy_bt (id int)");
        exec("INSERT INTO zwy_bt VALUES (1),(2)");
        exec("ALTER TABLE zwy_bt ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY zwy_bp ON zwy_bt FOR SELECT USING (id = 1)");
        exec("GRANT SELECT ON zwy_bt TO zwy_b");
        exec("SET ROLE zwy_b");
        assertEquals(java.util.Arrays.asList("1", "2"),
                rows("SELECT id::text FROM zwy_bt ORDER BY id"));
        exec("RESET ROLE");
        exec("DROP TABLE zwy_bt");
        exec("DROP ROLE zwy_b");
    }

    /** An owner who is not a superuser is bound by FORCE, and refused with row_security off. */
    @Test
    void whatAnOwnerSees() throws SQLException {
        exec("CREATE ROLE zwy_o LOGIN");
        exec("GRANT CREATE ON SCHEMA public TO zwy_o");
        exec("SET ROLE zwy_o");
        exec("CREATE TABLE zwy_t (id int)");
        exec("INSERT INTO zwy_t VALUES (1),(2)");
        exec("ALTER TABLE zwy_t ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY zwy_sel ON zwy_t FOR SELECT USING (id = 1)");
        // Owning the table is an exemption until the owner gives it up.
        assertEquals("2", one("SELECT count(*)::text FROM zwy_t"));
        exec("ALTER TABLE zwy_t FORCE ROW LEVEL SECURITY");
        assertEquals("1", one("SELECT count(*)::text FROM zwy_t"));
        // With row security off, a reader who would be filtered is refused instead.
        exec("SET row_security = off");
        assertEquals("42501", stateOf("SELECT count(*) FROM zwy_t"));
        exec("ALTER TABLE zwy_t NO FORCE ROW LEVEL SECURITY");
        assertEquals("2", one("SELECT count(*)::text FROM zwy_t"));
        exec("SET row_security = on");
        exec("DROP TABLE zwy_t");
        exec("RESET ROLE");
        exec("REVOKE CREATE ON SCHEMA public FROM zwy_o");
        exec("DROP ROLE zwy_o");
    }

    /** A fetch moves as far as it was told, whether or not it found rows on the way. */
    @Test
    void howFarAFetchMoves() throws SQLException {
        exec("CREATE TABLE zwy_c (i int)");
        exec("INSERT INTO zwy_c VALUES (1),(2),(3),(4),(5)");
        exec("BEGIN");
        exec("DECLARE zwy_x SCROLL CURSOR FOR SELECT i FROM zwy_c ORDER BY i");
        assertEquals(java.util.Arrays.asList("1", "2"), rows("FETCH 2 FROM zwy_x"));
        // Two places back from the second row is before the first, so one row comes out.
        assertEquals(java.util.Arrays.asList("1"), rows("FETCH BACKWARD 2 FROM zwy_x"));
        // And the cursor is before the first row, so everything follows.
        assertEquals(java.util.Arrays.asList("1", "2", "3", "4", "5"),
                rows("FETCH ALL FROM zwy_x"));
        // The same forward: three places on from the fourth row is past the end.
        assertEquals(java.util.Arrays.asList("4"), rows("FETCH ABSOLUTE 4 FROM zwy_x"));
        assertEquals(java.util.Arrays.asList("5"), rows("FETCH FORWARD 3 FROM zwy_x"));
        assertEquals(java.util.Arrays.asList("5"), rows("FETCH PRIOR FROM zwy_x"));
        exec("COMMIT");
        exec("DROP TABLE zwy_c");
    }
}
