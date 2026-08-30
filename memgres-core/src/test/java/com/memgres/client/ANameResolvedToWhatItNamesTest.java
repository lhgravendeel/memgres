package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A name resolved to the thing it names, rather than to whatever was nearest.
 *
 * <p>A role OID could not be turned back into a role name: pg_get_userbyid answered with the role
 * the server started as whatever it was asked about, pg_tables.tableowner was that same name
 * written in, and regrole passed its argument through untouched — so a catalogue read gave every
 * relation to one role, an OID cast to regrole printed the number the reader already had, and a
 * role name cast to one resolved to nothing a join could match.
 *
 * <p>A name written without a schema means what the search path says it means. to_regclass looked
 * through every schema instead, so it answered for relations the writer could not have named
 * without qualifying them — and a caller using it to ask whether a name resolves was told yes for
 * names that do not.
 *
 * <p>A schema written in front of a window function is not part of its name, and matched with the
 * qualifier still attached pg_catalog.row_number() fell through every arm that computes one and
 * every row came back null.
 *
 * <p>And the case a name folds to is the server's business, not the machine's: folded through the
 * JVM's default locale, a server started under a Turkish one read the type "int" as "ınt" and
 * refused every definition that named it.
 */
class ANameResolvedToWhatItNamesTest {

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

    private static String one(String sql) throws SQLException {
        List<String> rows = rows(sql);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
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

    /** A role OID names the role it was handed out for, and says so wherever it is read. */
    @Test
    void aRoleOidNamesTheRoleItWasHandedOutFor() throws SQLException {
        exec("CREATE ROLE znr_a NOLOGIN");
        exec("CREATE ROLE znr_b NOLOGIN");
        exec("CREATE TABLE znr_t (a int)");
        exec("ALTER TABLE znr_t OWNER TO znr_a");
        try {
            assertEquals("znr_a", one("SELECT pg_get_userbyid((SELECT oid FROM pg_roles"
                    + " WHERE rolname = 'znr_a'))"));
            assertEquals("znr_b", one("SELECT pg_get_userbyid((SELECT oid FROM pg_roles"
                    + " WHERE rolname = 'znr_b'))"));
            assertEquals("znr_a", one("SELECT tableowner FROM pg_tables"
                    + " WHERE tablename = 'znr_t'"));
            // An OID nobody was given still reads as an OID, and PostgreSQL says which.
            assertEquals("unknown (OID=999999)", one("SELECT pg_get_userbyid(999999)"));
        } finally {
            exec("DROP TABLE znr_t");
            exec("DROP ROLE znr_a, znr_b");
        }
    }

    /** regrole turns a name into an OID and an OID back into a name. */
    @Test
    void aRegroleReadsBothWays() throws SQLException {
        exec("CREATE ROLE znr_c NOLOGIN");
        try {
            assertEquals("znr_c", one("SELECT (SELECT oid FROM pg_roles"
                    + " WHERE rolname = 'znr_c')::regrole::text"));
            assertEquals("t", one("SELECT 'znr_c'::regrole::oid ="
                    + " (SELECT oid FROM pg_roles WHERE rolname = 'znr_c')"));
            // A name nothing answers to is refused, and the form that asks answers null.
            try (Statement st = conn.createStatement()) {
                st.execute("SELECT 'znr_nosuch'::regrole");
                fail("a role that is not there is refused");
            } catch (SQLException e) {
                assertEquals("42704", e.getSQLState());
                assertTrue(e.getMessage().contains("role \"znr_nosuch\" does not exist"),
                        e.getMessage());
            }
            assertEquals("t", one("SELECT to_regrole('znr_nosuch') IS NULL"));
            assertEquals("znr_c", one("SELECT to_regrole('znr_c')::text"));
        } finally {
            exec("DROP ROLE znr_c");
        }
    }

    /** A bare name means what the search path says it means, and nothing where it says nothing. */
    @Test
    void aBareNameIsResolvedAlongTheSearchPath() throws SQLException {
        exec("CREATE SCHEMA znr_s");
        exec("CREATE TABLE znr_s.znr_u (a int)");
        try {
            assertEquals("t", one("SELECT to_regclass('znr_u') IS NULL"));
            exec("SET search_path = znr_s, public");
            assertEquals("f", one("SELECT to_regclass('znr_u') IS NULL"));
            assertEquals("znr_s", one("SELECT current_schema()"));
            // A qualified name is answered whether or not the path reaches the schema.
            exec("SET search_path = public");
            assertEquals("f", one("SELECT to_regclass('znr_s.znr_u') IS NULL"));
        } finally {
            exec("SET search_path = public");
            exec("DROP SCHEMA znr_s CASCADE");
        }
    }

    /** A window function written with its schema is the same function. */
    @Test
    void aSchemaQualifiedWindowFunctionIsTheSameFunction() throws SQLException {
        exec("CREATE TABLE znr_w (a int, g int)");
        exec("INSERT INTO znr_w VALUES (1,1),(2,1),(3,2)");
        try {
            assertEquals(List.of("1", "2", "3"),
                    rows("SELECT pg_catalog.row_number() OVER (ORDER BY a) FROM znr_w"));
            assertEquals(List.of("1", "2", "1"),
                    rows("SELECT pg_catalog.rank() OVER (PARTITION BY g ORDER BY a) FROM znr_w"));
            assertEquals(List.of("3", "3", "3"),
                    rows("SELECT pg_catalog.count(*) OVER () FROM znr_w"));
        } finally {
            exec("DROP TABLE znr_w");
        }
    }

    /** A connection that has not named itself has no name, and RESET gives it none. */
    @Test
    void aConnectionThatNamedItselfNothingHasNoName() throws SQLException {
        exec("SET application_name = 'znr_probe'");
        assertEquals("znr_probe", one("SELECT current_setting('application_name')"));
        exec("RESET application_name");
        assertEquals("", one("SELECT current_setting('application_name')"));
        assertEquals("/", one("SELECT boot_val, reset_val FROM pg_settings"
                + " WHERE name = 'application_name'"));
    }
}
