package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category C (rules + inheritance):
 *  - CREATE RULE DO INSTEAD / DO ALSO rewrite
 *  - pg_rules view
 *  - ALTER TABLE INHERIT / NO INHERIT
 *  - CHECK NO INHERIT
 *  - ONLY qualifier in SELECT / UPDATE / DELETE
 *  - pg_inherits view contents
 */
class Round15RulesInheritanceTest {

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
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // A. CREATE RULE DO INSTEAD / DO ALSO
    // =========================================================================

    @Test
    void rule_do_instead_redirects_insert() throws SQLException {
        exec("CREATE TABLE r15_r_a (id int)");
        exec("CREATE TABLE r15_r_b (id int)");
        exec("CREATE RULE r15_rule_redir AS ON INSERT TO r15_r_a "
                + "DO INSTEAD INSERT INTO r15_r_b VALUES (NEW.id)");
        exec("INSERT INTO r15_r_a VALUES (1)");

        int a = scalarInt("SELECT count(*)::int FROM r15_r_a");
        int b = scalarInt("SELECT count(*)::int FROM r15_r_b");
        assertEquals(0, a, "DO INSTEAD must redirect, leaving r15_r_a empty");
        assertEquals(1, b, "DO INSTEAD must insert into r15_r_b");
    }

    @Test
    void rule_do_also_writes_both() throws SQLException {
        exec("CREATE TABLE r15_ra (id int)");
        exec("CREATE TABLE r15_rb (id int)");
        exec("CREATE RULE r15_rule_also AS ON INSERT TO r15_ra "
                + "DO ALSO INSERT INTO r15_rb VALUES (NEW.id)");
        exec("INSERT INTO r15_ra VALUES (1)");

        int a = scalarInt("SELECT count(*)::int FROM r15_ra");
        int b = scalarInt("SELECT count(*)::int FROM r15_rb");
        assertEquals(1, a);
        assertEquals(1, b);
    }

    // =========================================================================
    // B. pg_rules view
    // =========================================================================

    @Test
    void pg_rules_lists_user_rule() throws SQLException {
        exec("CREATE TABLE r15_pr (id int)");
        exec("CREATE TABLE r15_pr_dest (id int)");
        exec("CREATE RULE r15_rule_pr AS ON INSERT TO r15_pr "
                + "DO INSTEAD INSERT INTO r15_pr_dest VALUES (NEW.id)");

        int n = scalarInt(
                "SELECT count(*)::int FROM pg_rules WHERE rulename='r15_rule_pr'");
        assertEquals(1, n, "pg_rules must expose user-created rule");
    }

    @Test
    void pg_rules_has_standard_columns() throws SQLException {
        // Should have: schemaname, tablename, rulename, definition
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT schemaname, tablename, rulename, definition "
                     + "FROM pg_rules WHERE rulename='r15_rule_pr'")) {
            if (rs.next()) {
                assertNotNull(rs.getString("schemaname"));
                assertNotNull(rs.getString("tablename"));
                assertEquals("r15_rule_pr", rs.getString("rulename"));
                assertNotNull(rs.getString("definition"));
            }
        }
    }

    // =========================================================================
    // C. Inheritance — INHERITS clause and pg_inherits
    // =========================================================================

    @Test
    void create_table_inherits_populates_pg_inherits() throws SQLException {
        exec("CREATE TABLE r15_parent (a int)");
        exec("CREATE TABLE r15_child () INHERITS (r15_parent)");
        int n = scalarInt("SELECT count(*)::int FROM pg_inherits "
                + "WHERE inhrelid = 'r15_child'::regclass::oid "
                + "AND inhparent = 'r15_parent'::regclass::oid");
        assertEquals(1, n, "pg_inherits must contain child→parent row");
    }

    @Test
    void only_qualifier_excludes_inherited_rows() throws SQLException {
        exec("CREATE TABLE r15_pr_o (a int)");
        exec("CREATE TABLE r15_ch_o () INHERITS (r15_pr_o)");
        exec("INSERT INTO r15_pr_o VALUES (1)");
        exec("INSERT INTO r15_ch_o VALUES (2)");

        int allRows = scalarInt("SELECT count(*)::int FROM r15_pr_o");
        assertEquals(2, allRows, "SELECT without ONLY should include child rows");

        int onlyRows = scalarInt("SELECT count(*)::int FROM ONLY r15_pr_o");
        assertEquals(1, onlyRows, "SELECT ONLY should exclude child rows");
    }

    @Test
    void update_only_skips_child_tables() throws SQLException {
        exec("CREATE TABLE r15_pu_o (a int, tag text)");
        exec("CREATE TABLE r15_cu_o () INHERITS (r15_pu_o)");
        exec("INSERT INTO r15_pu_o VALUES (1, 'p')");
        exec("INSERT INTO r15_cu_o VALUES (1, 'c')");

        exec("UPDATE ONLY r15_pu_o SET tag='TAGGED' WHERE a=1");

        int taggedP = scalarInt("SELECT count(*)::int FROM ONLY r15_pu_o WHERE tag='TAGGED'");
        int taggedC = scalarInt("SELECT count(*)::int FROM r15_cu_o WHERE tag='TAGGED'");
        assertEquals(1, taggedP, "ONLY update should affect parent rows");
        assertEquals(0, taggedC, "ONLY update must NOT affect child rows");
    }

    // =========================================================================
    // D. ALTER TABLE INHERIT / NO INHERIT
    // =========================================================================

    @Test
    void alter_table_inherit_adds_inheritance() throws SQLException {
        exec("CREATE TABLE r15_ai_par (a int)");
        exec("CREATE TABLE r15_ai_ch (a int)");  // no initial INHERITS
        exec("ALTER TABLE r15_ai_ch INHERIT r15_ai_par");

        int n = scalarInt("SELECT count(*)::int FROM pg_inherits "
                + "WHERE inhrelid = 'r15_ai_ch'::regclass::oid "
                + "AND inhparent = 'r15_ai_par'::regclass::oid");
        assertEquals(1, n, "ALTER TABLE … INHERIT must create pg_inherits row");
    }

    @Test
    void alter_table_no_inherit_removes_inheritance() throws SQLException {
        exec("CREATE TABLE r15_ni_par (a int)");
        exec("CREATE TABLE r15_ni_ch () INHERITS (r15_ni_par)");
        exec("ALTER TABLE r15_ni_ch NO INHERIT r15_ni_par");

        int n = scalarInt("SELECT count(*)::int FROM pg_inherits "
                + "WHERE inhrelid = 'r15_ni_ch'::regclass::oid "
                + "AND inhparent = 'r15_ni_par'::regclass::oid");
        assertEquals(0, n, "ALTER TABLE … NO INHERIT must remove pg_inherits row");
    }

    // =========================================================================
    // E. CHECK constraint with NO INHERIT
    // =========================================================================

    @Test
    void check_no_inherit_only_on_parent() throws SQLException {
        exec("CREATE TABLE r15_cn_par (a int CHECK (a > 0) NO INHERIT)");
        exec("CREATE TABLE r15_cn_ch () INHERITS (r15_cn_par)");

        // Parent must reject
        try {
            exec("INSERT INTO r15_cn_par VALUES (-1)");
            fail("parent CHECK (a>0) should reject a=-1");
        } catch (SQLException e) { /* expected */ }

        // Child should accept because NO INHERIT means the constraint does not propagate
        try {
            exec("INSERT INTO r15_cn_ch VALUES (-1)");
        } catch (SQLException e) {
            fail("child should NOT inherit NO INHERIT check; insert should succeed");
        }
    }

    @Test
    void check_constraint_connoinherit_flag() throws SQLException {
        exec("CREATE TABLE r15_noin (a int CHECK (a > 0) NO INHERIT)");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_constraint c "
                        + "JOIN pg_class r ON c.conrelid = r.oid "
                        + "WHERE r.relname='r15_noin' AND c.connoinherit");
        assertTrue(n >= 1, "pg_constraint.connoinherit should be true for NO INHERIT check");
    }

    // =========================================================================
    // F. DROP RULE
    // =========================================================================

    @Test
    void drop_rule_removes_from_pg_rules() throws SQLException {
        exec("CREATE TABLE r15_dr (id int)");
        exec("CREATE TABLE r15_dr_dest (id int)");
        exec("CREATE RULE r15_rule_dr AS ON INSERT TO r15_dr "
                + "DO INSTEAD INSERT INTO r15_dr_dest VALUES (NEW.id)");
        exec("DROP RULE r15_rule_dr ON r15_dr");

        int n = scalarInt(
                "SELECT count(*)::int FROM pg_rules WHERE rulename='r15_rule_dr'");
        assertEquals(0, n, "DROP RULE must remove pg_rules row");
    }
}
