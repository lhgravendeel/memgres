package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The security statements resolve what they name, and act on every name they were given.
 *
 * <p>GRANT and REVOKE looked up a table and nothing else, so a grant on a sequence, a function, a
 * type, a language or a database was recorded against a name nothing answered to and reported
 * success — a typo granted nothing and said it had. CREATE ROLE and ALTER ROLE stopped reading at
 * the first word they did not know and reported success with that word and everything after it
 * discarded, so a mistyped option was silently not applied and two contradicting options were
 * resolved last-one-wins rather than refused. And a statement naming several roles acted on one
 * of them: DROP OWNED BY and REASSIGN OWNED BY each read a name and advanced over the rest.
 *
 * <p>A role is not dropped while something names it, either: a grant depends on the role, and
 * PostgreSQL says which object the privileges are on.
 */
class SecurityDdlResolvesWhatItNamesTest {

    static Memgres memgres;
    static Connection conn;

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

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int c = 1; c <= rs.getMetaData().getColumnCount(); c++) {
                    if (c > 1) row.append('/');
                    row.append(rs.getString(c));
                }
                out.add(row.toString());
            }
            return out;
        }
    }

    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getMessage();
        }
    }

    /** A grant names an object, and the object has to be there and be of the kind named. */
    @Test
    void aGrantResolvesTheObjectItNames() throws SQLException {
        exec("CREATE TABLE zsd_t (a int)");
        exec("CREATE ROLE zsd_r NOLOGIN");
        try {
            assertTrue(messageOf("GRANT SELECT ON SEQUENCE zsd_t TO zsd_r")
                    .contains("\"zsd_t\" is not a sequence"));
            assertTrue(messageOf("GRANT EXECUTE ON FUNCTION zsd_nosuch(int) TO zsd_r")
                    .contains("function zsd_nosuch(integer) does not exist"));
            assertEquals("42704", stateOf("GRANT USAGE ON TYPE zsd_nosuchtype TO zsd_r"));
            assertEquals("42704", stateOf("GRANT USAGE ON LANGUAGE zsd_nosuchlang TO zsd_r"));
            assertEquals("3D000", stateOf("GRANT CREATE ON DATABASE zsd_nosuchdb TO zsd_r"));
            assertEquals("3F000",
                    stateOf("GRANT SELECT ON ALL TABLES IN SCHEMA zsd_nosuchsc TO zsd_r"));
            assertEquals("3F000", stateOf("REVOKE USAGE ON SCHEMA zsd_nosuchsc FROM zsd_r"));
            // The signature says which routine of a name the statement meant.
            exec("CREATE FUNCTION zsd_f(int) RETURNS int LANGUAGE sql AS $$ SELECT $1 $$");
            assertTrue(messageOf("GRANT EXECUTE ON FUNCTION zsd_f(text) TO zsd_r")
                    .contains("function zsd_f(text) does not exist"));
            assertNull(stateOf("GRANT EXECUTE ON FUNCTION zsd_f(int) TO zsd_r"));
        } finally {
            exec("DROP FUNCTION IF EXISTS zsd_f(int)");
            exec("DROP TABLE zsd_t CASCADE");
            exec("DROP OWNED BY zsd_r");
            exec("DROP ROLE zsd_r");
        }
    }

    /** A role option is an option, and two that contradict each other are refused. */
    @Test
    void aRoleOptionIsOneTheServerKnows() throws SQLException {
        assertTrue(messageOf("CREATE ROLE zsd_o NOLOGIN NOSUCHOPTION")
                .contains("unrecognized role option \"nosuchoption\""));
        exec("CREATE ROLE zsd_o NOLOGIN");
        try {
            assertTrue(messageOf("ALTER ROLE zsd_o WITH SUPERUSER NOSUPERUSER")
                    .contains("conflicting or redundant options"));
            assertEquals("42601", stateOf("ALTER ROLE zsd_o WITH NOSUCHOPTION"));
            assertEquals("22007", stateOf("ALTER ROLE zsd_o VALID UNTIL 'garbage'"));
            assertTrue(messageOf("ALTER ROLE zsd_o SET zsd_nosuchguc = 1")
                    .contains("unrecognized configuration parameter \"zsd_nosuchguc\""));
            // The ones the server does know still work.
            assertNull(stateOf("ALTER ROLE zsd_o SET work_mem = '4MB'"));
            assertNull(stateOf("ALTER ROLE zsd_o WITH CREATEDB NOSUPERUSER"));
        } finally {
            exec("DROP ROLE zsd_o");
        }
    }

    /** A statement that names several roles acts on all of them. */
    @Test
    void aStatementActsOnEveryRoleItNames() throws SQLException {
        exec("CREATE ROLE zsd_a NOLOGIN");
        exec("CREATE ROLE zsd_b NOLOGIN");
        exec("CREATE ROLE zsd_c NOLOGIN");
        exec("CREATE TABLE zsd_u (a int)");
        exec("GRANT SELECT ON zsd_u TO zsd_a");
        exec("GRANT SELECT ON zsd_u TO zsd_b");
        try {
            exec("DROP OWNED BY zsd_a, zsd_b");
            assertEquals(List.of(), rows("SELECT grantee::text"
                    + " FROM information_schema.role_table_grants"
                    + " WHERE table_name='zsd_u' AND grantee IN ('zsd_a','zsd_b')"));
            exec("DROP ROLE zsd_a, zsd_b, zsd_c");
            assertEquals(List.of(), rows("SELECT rolname::text FROM pg_roles"
                    + " WHERE rolname IN ('zsd_a','zsd_b','zsd_c')"));
        } finally {
            exec("DROP TABLE zsd_u CASCADE");
        }
    }

    /** A role is not dropped while a grant names it, and the refusal says what the grant is on. */
    @Test
    void aRoleIsNotDroppedWhileAGrantNamesIt() throws SQLException {
        exec("CREATE TABLE zsd_v (a int)");
        exec("CREATE ROLE zsd_h NOLOGIN");
        exec("GRANT SELECT ON zsd_v TO zsd_h");
        try {
            assertEquals("2BP01", stateOf("DROP ROLE zsd_h"));
            assertTrue(messageOf("DROP ROLE zsd_h").contains("privileges for table zsd_v"));
            // Taking the grant away is what makes the role droppable.
            exec("REVOKE SELECT ON zsd_v FROM zsd_h");
            assertNull(stateOf("DROP ROLE zsd_h"));
        } finally {
            exec("DROP TABLE zsd_v CASCADE");
            exec("DROP ROLE IF EXISTS zsd_h");
        }
    }

    /** REVOKE ON ALL TABLES IN SCHEMA takes away what the matching GRANT gave. */
    @Test
    void revokingOnAllTablesTakesAwayWhatGrantingOnAllTablesGave() throws SQLException {
        exec("CREATE TABLE zsd_w (a int)");
        exec("CREATE ROLE zsd_m NOLOGIN");
        try {
            exec("GRANT SELECT ON ALL TABLES IN SCHEMA public TO zsd_m");
            assertEquals(List.of("true"),
                    rows("SELECT has_table_privilege('zsd_m','zsd_w','SELECT')::text"));
            exec("REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM zsd_m");
            assertEquals(List.of("false"),
                    rows("SELECT has_table_privilege('zsd_m','zsd_w','SELECT')::text"));
        } finally {
            exec("DROP TABLE zsd_w CASCADE");
            exec("DROP OWNED BY zsd_m");
            exec("DROP ROLE zsd_m");
        }
    }

    /** A role already holds its own rights, so a membership in itself is refused. */
    @Test
    void aRoleIsNotMadeAMemberOfItself() throws SQLException {
        exec("CREATE ROLE zsd_s NOLOGIN");
        try {
            assertEquals("0LP01", stateOf("GRANT zsd_s TO zsd_s"));
            assertTrue(messageOf("GRANT zsd_s TO zsd_s")
                    .contains("role \"zsd_s\" is a member of role \"zsd_s\""));
            // ADMIN OPTION belongs to a membership grant, not to a privilege grant.
            exec("CREATE TABLE zsd_y (a int)");
            assertEquals("42601", stateOf("GRANT SELECT ON zsd_y TO zsd_s WITH ADMIN OPTION"));
        } finally {
            exec("DROP TABLE IF EXISTS zsd_y CASCADE");
            exec("DROP OWNED BY zsd_s");
            exec("DROP ROLE zsd_s");
        }
    }
}
