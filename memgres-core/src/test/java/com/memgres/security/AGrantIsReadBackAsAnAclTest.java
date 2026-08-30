package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A grant is read back as an ACL, and a policy keeps what kind of policy it is.
 *
 * <p>An ACL is not the list of grants somebody wrote. PostgreSQL keeps the column null while
 * nobody has written one — which is what says the defaults apply — and materialises it the first
 * time a GRANT or a REVOKE names the object: with the owner holding everything that kind of
 * object has, and PUBLIC's own entry ahead of it where PUBLIC holds something by default. So the
 * first grant of one privilege to one role produces three entries and not one. Assembled from the
 * grants alone, the owner was absent from every ACL memgres reported, the privileges came out in
 * the order they were granted rather than PostgreSQL's, and a sequence, a function and a type
 * reported no ACL at all.
 *
 * <p>Two of the things here are security defects rather than reporting ones. ALTER POLICY rebuilt
 * the policy through a constructor with no parameter for its kind, so every altered RESTRICTIVE
 * policy came back PERMISSIVE — which stops withholding the rows it was written to withhold and
 * starts admitting them. And ALTER ROLE ... RENAME TO dropped the role and created a new one,
 * leaving every grant, every membership, everything it owned and every default privilege written
 * for it keyed under a name nothing answered to.
 */
class AGrantIsReadBackAsAnAclTest {

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

    /** ALTER POLICY changes what a policy admits, not what kind of policy it is. */
    @Test
    void alteringAPolicyDoesNotMakeItPermissive() throws SQLException {
        exec("CREATE ROLE zag_r NOLOGIN");
        exec("CREATE TABLE zag_t (id int, owner text)");
        exec("ALTER TABLE zag_t ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY zag_p ON zag_t AS RESTRICTIVE FOR SELECT USING (owner = 'x')");
        try {
            assertEquals("f", one("SELECT polpermissive FROM pg_policy"
                    + " WHERE polrelid='zag_t'::regclass"));
            exec("ALTER POLICY zag_p ON zag_t USING (owner = 'y')");
            assertEquals("f", one("SELECT polpermissive FROM pg_policy"
                    + " WHERE polrelid='zag_t'::regclass"));
            exec("ALTER POLICY zag_p ON zag_t TO zag_r");
            assertEquals("f", one("SELECT polpermissive FROM pg_policy"
                    + " WHERE polrelid='zag_t'::regclass"));
            exec("ALTER POLICY zag_p ON zag_t RENAME TO zag_p2");
            assertEquals("zag_p2/f", one("SELECT polname, polpermissive FROM pg_policy"
                    + " WHERE polrelid='zag_t'::regclass"));
            assertEquals("RESTRICTIVE", one("SELECT permissive FROM pg_policies"
                    + " WHERE tablename='zag_t'"));
        } finally {
            exec("DROP TABLE zag_t CASCADE");
            exec("DROP ROLE zag_r");
        }
    }

    /** A rename gives the role a new name and changes nothing else about it. */
    @Test
    void renamingARoleKeepsWhatTheRoleHolds() throws SQLException {
        exec("CREATE ROLE zag_a NOLOGIN");
        exec("CREATE ROLE zag_b NOLOGIN");
        exec("CREATE TABLE zag_u (id int)");
        exec("GRANT SELECT ON zag_u TO zag_a");
        exec("GRANT zag_a TO zag_b");
        try {
            exec("ALTER ROLE zag_a RENAME TO zag_a2");
            assertEquals("1", one("SELECT count(*)::int FROM information_schema.role_table_grants"
                    + " WHERE grantee='zag_a2' AND table_name='zag_u'"));
            assertEquals("1", one("SELECT count(*)::int FROM pg_auth_members m"
                    + " JOIN pg_roles r ON m.roleid=r.oid JOIN pg_roles g ON m.member=g.oid"
                    + " WHERE r.rolname='zag_a2' AND g.rolname='zag_b'"));
            assertTrue(one("SELECT relacl::text FROM pg_class WHERE relname='zag_u'")
                    .contains("zag_a2=r/"));
        } finally {
            exec("DROP TABLE zag_u CASCADE");
            exec("DROP OWNED BY zag_a2");
            exec("DROP ROLE zag_a2, zag_b");
        }
    }

    /**
     * An ACL is null until something writes one, and then it holds the owner's own entry with
     * every privilege that kind of object has, written in PostgreSQL's order.
     */
    @Test
    void anAclMaterialisesWithTheOwnersOwnEntry() throws SQLException {
        exec("CREATE ROLE zag_g NOLOGIN");
        exec("CREATE TABLE zag_v (a int, b int)");
        try {
            assertEquals("null", one("SELECT relacl::text FROM pg_class WHERE relname='zag_v'"));
            exec("GRANT SELECT ON zag_v TO zag_g");
            assertEquals("{memgres=arwdDxtm/memgres,zag_g=r/memgres}",
                    one("SELECT relacl::text FROM pg_class WHERE relname='zag_v'"));
            // The letters are in PostgreSQL's order, not the order they were granted in.
            exec("GRANT UPDATE, INSERT ON zag_v TO zag_g");
            assertEquals("{memgres=arwdDxtm/memgres,zag_g=arw/memgres}",
                    one("SELECT relacl::text FROM pg_class WHERE relname='zag_v'"));
            // A grant onward is marked where it was made, and the owner's own carries no mark.
            exec("GRANT DELETE ON zag_v TO zag_g WITH GRANT OPTION");
            assertEquals("{memgres=arwdDxtm/memgres,zag_g=arwd*/memgres}",
                    one("SELECT relacl::text FROM pg_class WHERE relname='zag_v'"));
            // Revoking every grant away leaves the owner's entry standing, not a null column.
            exec("REVOKE ALL ON zag_v FROM zag_g");
            assertEquals("{memgres=arwdDxtm/memgres}",
                    one("SELECT relacl::text FROM pg_class WHERE relname='zag_v'"));
        } finally {
            exec("DROP TABLE zag_v CASCADE");
            exec("DROP OWNED BY zag_g");
            exec("DROP ROLE zag_g");
        }
    }

    /** Each kind of object has its own privileges, and PUBLIC holds some of them by default. */
    @Test
    void everyKindOfObjectReportsItsOwnAcl() throws SQLException {
        exec("CREATE ROLE zag_k NOLOGIN");
        exec("CREATE SEQUENCE zag_s");
        exec("CREATE SCHEMA zag_sc");
        exec("CREATE FUNCTION zag_f() RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$");
        exec("CREATE TYPE zag_ty AS (x int)");
        try {
            exec("GRANT USAGE ON SEQUENCE zag_s TO zag_k");
            assertEquals("{memgres=rwU/memgres,zag_k=U/memgres}",
                    one("SELECT relacl::text FROM pg_class WHERE relname='zag_s'"));
            exec("GRANT USAGE ON SCHEMA zag_sc TO zag_k");
            assertEquals("{memgres=UC/memgres,zag_k=U/memgres}",
                    one("SELECT nspacl::text FROM pg_namespace WHERE nspname='zag_sc'"));
            // PUBLIC holds EXECUTE on a function and USAGE on a type without being granted them,
            // and its entry stands first.
            exec("GRANT ALL ON FUNCTION zag_f() TO zag_k");
            assertEquals("{=X/memgres,memgres=X/memgres,zag_k=X/memgres}",
                    one("SELECT proacl::text FROM pg_proc WHERE proname='zag_f'"));
            exec("GRANT ALL ON TYPE zag_ty TO zag_k");
            assertEquals("{=U/memgres,memgres=U/memgres,zag_k=U/memgres}",
                    one("SELECT typacl::text FROM pg_type WHERE typname='zag_ty'"));
        } finally {
            exec("DROP SEQUENCE zag_s");
            exec("DROP SCHEMA zag_sc");
            exec("DROP FUNCTION zag_f()");
            exec("DROP TYPE zag_ty");
            exec("DROP OWNED BY zag_k");
            exec("DROP ROLE zag_k");
        }
    }

    /** A column's ACL holds the column-level grants written on it, and nothing else. */
    @Test
    void aColumnHoldsTheGrantsWrittenOnTheColumn() throws SQLException {
        exec("CREATE ROLE zag_c NOLOGIN");
        exec("CREATE TABLE zag_w (a int, b int)");
        try {
            // Each privilege carries the column list written after it, which need not be the
            // list written after the privilege before it.
            exec("GRANT SELECT (b), INSERT (a), UPDATE (a), REFERENCES (a) ON zag_w TO zag_c");
            assertEquals(List.of("a/{zag_c=awx/memgres}", "b/{zag_c=r/memgres}"),
                    rows("SELECT attname, attacl::text FROM pg_attribute"
                            + " WHERE attrelid='zag_w'::regclass AND attnum>0 ORDER BY attnum"));
            // A grant on the whole relation is a grant on each of its columns, and the view
            // lists it that way beside the column-level ones.
            exec("GRANT SELECT ON zag_w TO zag_c");
            assertEquals(List.of("a/SELECT", "b/SELECT"),
                    rows("SELECT column_name, privilege_type"
                            + " FROM information_schema.column_privileges"
                            + " WHERE table_name='zag_w' AND grantee='zag_c'"
                            + " AND privilege_type='SELECT' ORDER BY 1"));
        } finally {
            exec("DROP TABLE zag_w CASCADE");
            exec("DROP OWNED BY zag_c");
            exec("DROP ROLE zag_c");
        }
    }

    /** A grant of ALL is a grant of each privilege, which is how the views list it. */
    @Test
    void aGrantOfAllIsListedAsEachPrivilege() throws SQLException {
        exec("CREATE ROLE zag_l NOLOGIN");
        exec("CREATE TABLE zag_x (a int)");
        try {
            exec("GRANT ALL ON zag_x TO zag_l");
            assertEquals(List.of("DELETE/NO", "INSERT/NO", "REFERENCES/NO", "SELECT/NO",
                            "TRIGGER/NO", "TRUNCATE/NO", "UPDATE/NO"),
                    rows("SELECT privilege_type, is_grantable"
                            + " FROM information_schema.role_table_grants"
                            + " WHERE table_name='zag_x' AND grantee='zag_l' ORDER BY 1"));
            // A role holds one of each privilege however many statements gave it, and it is
            // grantable if any of them made it so.
            exec("GRANT SELECT ON zag_x TO zag_l WITH GRANT OPTION");
            assertEquals(List.of("SELECT/YES"),
                    rows("SELECT privilege_type, is_grantable"
                            + " FROM information_schema.role_table_grants"
                            + " WHERE table_name='zag_x' AND grantee='zag_l'"
                            + " AND privilege_type='SELECT'"));
        } finally {
            exec("DROP TABLE zag_x CASCADE");
            exec("DROP OWNED BY zag_l");
            exec("DROP ROLE zag_l");
        }
    }
}
