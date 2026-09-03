package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * How the catalogue names an object, what it ships, and what a revocation leaves behind.
 *
 * <p>{@code pg_describe_object} says what kind of relation it is looking at and which column of it,
 * and answers nothing at all -- NULL -- for an object that is not there. Calling every relation a
 * table named views and sequences as things the database does not hold.
 *
 * <p>The server ships five text search templates and three memberships of {@code pg_monitor}, and a
 * database nobody has granted anything in still has all of them.
 *
 * <p>{@code REVOKE ADMIN OPTION FOR} takes the right to hand a membership on and leaves the
 * membership; taking the whole of it away left a role that had only been told to stop granting
 * with no membership at all.
 */
class HowAnObjectIsNamedAndHeldTest {

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

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int width = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 1; i <= width; i++) {
                    if (i > 1) line.append('|');
                    line.append(rs.getString(i));
                }
                out.add(line.toString());
            }
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

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** An object is described by what kind of thing it is. */
    @Test
    void whatKindOfThingAnObjectIsDescribedAs() throws SQLException {
        exec("CREATE TABLE zho_t (a int, b text)");
        exec("CREATE VIEW zho_v AS SELECT 1 AS x");
        exec("CREATE SEQUENCE zho_s");
        exec("CREATE INDEX zho_ix ON zho_t (a)");
        exec("CREATE SCHEMA zho_sc");
        assertEquals("table zho_t", describe("'zho_t'::regclass::oid", 0));
        assertEquals("column a of table zho_t", describe("'zho_t'::regclass::oid", 1));
        assertEquals("column b of table zho_t", describe("'zho_t'::regclass::oid", 2));
        assertEquals("view zho_v", describe("'zho_v'::regclass::oid", 0));
        assertEquals("sequence zho_s", describe("'zho_s'::regclass::oid", 0));
        assertEquals("index zho_ix", describe("'zho_ix'::regclass::oid", 0));
        assertEquals("type integer",
                one("SELECT pg_describe_object('pg_type'::regclass::oid,"
                        + " 'int4'::regtype::oid, 0)"));
        assertEquals("schema zho_sc",
                one("SELECT pg_describe_object('pg_namespace'::regclass::oid,"
                        + " 'zho_sc'::regnamespace::oid, 0)"));
        // An object that is not there is described by nothing at all.
        assertNull(one("SELECT pg_describe_object(0,0,0)"));
        assertNull(one("SELECT pg_describe_object(1259, 999999, 0)"));
        exec("DROP INDEX zho_ix");
        exec("DROP SEQUENCE zho_s");
        exec("DROP VIEW zho_v");
        exec("DROP TABLE zho_t");
        exec("DROP SCHEMA zho_sc");
    }

    private static String describe(String oidExpr, int subId) throws SQLException {
        return one("SELECT pg_describe_object('pg_class'::regclass::oid, " + oidExpr + ", "
                + subId + ")");
    }

    /** The separator string_agg puts between two values is the second one's. */
    @Test
    void whichSeparatorRunsTwoValuesTogether() throws SQLException {
        assertEquals("1+2",
                one("SELECT string_agg(v::text, d) FROM (VALUES (1,'-'),(2,'+')) t(v,d)"));
        assertEquals("1+2*3",
                one("SELECT string_agg(v::text, d) FROM (VALUES (1,'-'),(2,'+'),(3,'*')) t(v,d)"));
        // One separator for the whole group is still one separator.
        assertEquals("1,2", one("SELECT string_agg(v::text, ',') FROM (VALUES (1),(2)) t(v)"));
    }

    /** The templates a text search dictionary may be made from. */
    @Test
    void theTextSearchTemplatesTheServerShips() throws SQLException {
        assertEquals(java.util.Arrays.asList(
                        "ispell|dispell_init|dispell_lexize",
                        "simple|dsimple_init|dsimple_lexize",
                        "snowball|dsnowball_init|dsnowball_lexize",
                        "synonym|dsynonym_init|dsynonym_lexize",
                        "thesaurus|thesaurus_init|thesaurus_lexize"),
                rows("SELECT tmplname, tmplinit::text, tmpllexize::text FROM pg_ts_template"
                        + " ORDER BY tmplname"));
    }

    /** The memberships the server ships, and the ones a revocation leaves. */
    @Test
    void whichMembershipsAreHeld() throws SQLException {
        assertEquals(java.util.Arrays.asList(
                        "pg_monitor|pg_read_all_settings", "pg_monitor|pg_read_all_stats",
                        "pg_monitor|pg_stat_scan_tables"),
                rows("SELECT b.rolname, a.rolname FROM pg_auth_members m"
                        + " JOIN pg_roles a ON a.oid=m.roleid JOIN pg_roles b ON b.oid=m.member"
                        + " ORDER BY 1,2"));
        exec("CREATE ROLE zho_r");
        exec("CREATE ROLE zho_r2");
        exec("GRANT zho_r2 TO zho_r WITH ADMIN OPTION");
        assertEquals("true", memberAdmin());
        exec("REVOKE ADMIN OPTION FOR zho_r2 FROM zho_r");
        // The membership is still there; only the right to hand it on has gone.
        assertEquals("false", memberAdmin());
        exec("REVOKE zho_r2 FROM zho_r");
        assertEquals("0", one("SELECT count(*)::text FROM pg_auth_members m"
                + " JOIN pg_roles a ON a.oid=m.member WHERE a.rolname='zho_r'"));
        exec("DROP ROLE zho_r");
        exec("DROP ROLE zho_r2");
    }

    private static String memberAdmin() throws SQLException {
        return one("SELECT admin_option::text FROM pg_auth_members m"
                + " JOIN pg_roles a ON a.oid=m.member JOIN pg_roles b ON b.oid=m.roleid"
                + " WHERE a.rolname='zho_r' AND b.rolname='zho_r2'");
    }

    /** A membership that would close a cycle names the two roles the way PostgreSQL does. */
    @Test
    void whichRolesACycleNames() throws SQLException {
        exec("CREATE ROLE zho_a");
        exec("CREATE ROLE zho_b");
        exec("GRANT zho_b TO zho_a");
        assertTrue(messageOf("GRANT zho_a TO zho_b")
                .contains("role \"zho_a\" is a member of role \"zho_b\""));
        assertTrue(messageOf("GRANT zho_a TO zho_a")
                .contains("role \"zho_a\" is a member of role \"zho_a\""));
        exec("REVOKE zho_b FROM zho_a");
        exec("DROP ROLE zho_a");
        exec("DROP ROLE zho_b");
    }

    /** A word the grammar wants is a keyword and not a name. */
    @Test
    void whatDiscardNames() {
        assertTrue(messageOf("DISCARD \"ALL\"").contains("syntax error at or near \"\"ALL\"\""));
        assertNull(stateOf("DISCARD ALL"));
        assertNull(stateOf("DISCARD PLANS"));
        assertTrue(messageOf("DISCARD NOTHING").contains("syntax error at or near \"NOTHING\""));
    }

    /** A statistics target is a signed integer, and one PostgreSQL will hold. */
    @Test
    void whatAStatisticsTargetMayBe() throws SQLException {
        exec("CREATE TABLE zho_st (a int, b int)");
        exec("CREATE STATISTICS zho_s ON a, b FROM zho_st");
        assertNull(stateOf("ALTER STATISTICS zho_s SET STATISTICS 100"));
        assertNull(stateOf("ALTER STATISTICS zho_s SET STATISTICS -1"));
        assertEquals("22023", stateOf("ALTER STATISTICS zho_s SET STATISTICS -5"));
        assertTrue(messageOf("ALTER STATISTICS zho_s SET STATISTICS -5")
                .contains("statistics target -5 is too low"));
        // A number too wide for the grammar's integer is a syntax error where the digits stand,
        // and it is the digits that are named, not the sign in front of them.
        assertEquals("42601", stateOf("ALTER STATISTICS zho_s SET STATISTICS 99999999999"));
        assertTrue(messageOf("ALTER STATISTICS zho_s SET STATISTICS 99999999999")
                .contains("syntax error at or near \"99999999999\""));
        assertTrue(messageOf("ALTER STATISTICS zho_s SET STATISTICS -99999999999")
                .contains("syntax error at or near \"99999999999\""));
        exec("DROP STATISTICS zho_s");
        exec("DROP TABLE zho_st");
    }

    /** An operator is resolved before the clause it stands in is asked to be a condition. */
    @Test
    void whichComplaintComesFirst() {
        assertEquals("42883", stateOf("SELECT 1 WHERE 1::int >> 1::bigint"));
        assertTrue(messageOf("SELECT 1 WHERE 1::int >> 1::bigint")
                .contains("operator does not exist: integer >> bigint"));
        assertEquals("42804", stateOf("SELECT 1 WHERE 1::int >> 1::int"));
    }

    /** A pattern the engine refuses is refused in PostgreSQL's words. */
    @Test
    void howARefusedPatternIsWorded() {
        assertTrue(messageOf("SELECT 'abc' ~ 'a{2,1}'")
                .contains("invalid regular expression: invalid repetition count(s)"));
        assertTrue(messageOf("SELECT 'abc' ~ '[z-a]'")
                .contains("invalid regular expression: invalid character range"));
    }
}
