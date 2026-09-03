package com.memgres.rls;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for C7: Row-Level Security enforcement.
 */
class RlsEnforcementTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        // Since PostgreSQL 15 the public schema grants CREATE to nobody, and memgres now says
        // so too. These tests are about something else, so the grant is made once here rather
        // than in every one of them.
        try (java.sql.Statement grantStmt = conn.createStatement()) {
            grantStmt.execute("GRANT CREATE ON SCHEMA public TO PUBLIC");
        }
    }

    @AfterAll
    static void stop() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private List<String> query(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            List<String> rows = new ArrayList<>();
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append(",");
                    sb.append(rs.getString(i));
                }
                rows.add(sb.toString());
            }
            return rows;
        }
    }

    private int execUpdate(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { return st.executeUpdate(sql); }
    }

    // === Default-deny ===

    @Test
    void defaultDeny_noPolicies_nonOwnerSeesNothing() throws Exception {
        exec("CREATE TABLE dd_t(id int, v text)");
        exec("INSERT INTO dd_t VALUES (1,'a'),(2,'b'),(3,'c')");
        exec("ALTER TABLE dd_t ENABLE ROW LEVEL SECURITY");
        exec("CREATE ROLE dd_user LOGIN");
        exec("GRANT ALL ON dd_t TO dd_user");
        try {
            exec("SET ROLE dd_user");
            assertEquals(List.of("0"), query("SELECT count(*)::int FROM dd_t"));
            assertEquals(0, execUpdate("DELETE FROM dd_t"));
            assertEquals(0, execUpdate("UPDATE dd_t SET v = 'x'"));
            SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO dd_t VALUES (4,'d')"));
            assertEquals("42501", ex.getSQLState());
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE dd_t");
            exec("DROP ROLE dd_user");
        }
    }

    @Test
    void defaultDeny_ownerBypassesRls() throws Exception {
        exec("CREATE TABLE dd_own(id int)");
        exec("INSERT INTO dd_own VALUES (1),(2),(3)");
        exec("ALTER TABLE dd_own ENABLE ROW LEVEL SECURITY");
        try {
            // Owner sees all rows even with no policies
            assertEquals(List.of("3"), query("SELECT count(*)::int FROM dd_own"));
        } finally {
            exec("DROP TABLE dd_own");
        }
    }

    // === DELETE policy ===

    @Test
    void deletePolicy_onlyDeleteOwnRows() throws Exception {
        exec("CREATE TABLE dp_t(id int, owner_name text)");
        exec("INSERT INTO dp_t VALUES (1,'dp_alice'),(2,'dp_bob'),(3,'dp_alice')");
        exec("ALTER TABLE dp_t ENABLE ROW LEVEL SECURITY");
        exec("CREATE ROLE dp_alice LOGIN");
        exec("CREATE ROLE dp_bob LOGIN");
        exec("GRANT ALL ON dp_t TO dp_alice, dp_bob");
        exec("CREATE POLICY dp_sel ON dp_t FOR SELECT USING (owner_name = current_user)");
        exec("CREATE POLICY dp_del ON dp_t FOR DELETE USING (owner_name = current_user)");
        try {
            exec("SET ROLE dp_alice");
            assertEquals(List.of("2"), query("SELECT count(*)::int FROM dp_t"));
            assertEquals(0, execUpdate("DELETE FROM dp_t WHERE id = 2")); // bob's row
            assertEquals(1, execUpdate("DELETE FROM dp_t WHERE id = 1")); // alice's row
            exec("RESET ROLE");

            exec("SET ROLE dp_bob");
            assertEquals(List.of("1"), query("SELECT count(*)::int FROM dp_t"));
            assertEquals(1, execUpdate("DELETE FROM dp_t")); // bob's row only
            exec("RESET ROLE");

            // alice's id=3 remains
            assertEquals(List.of("1"), query("SELECT count(*)::int FROM dp_t"));
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE dp_t");
            exec("DROP ROLE dp_alice");
            exec("DROP ROLE dp_bob");
        }
    }

    // === UPDATE WITH CHECK ===

    @Test
    void updateWithCheck_rejectsViolation() throws Exception {
        exec("CREATE TABLE uc_t(id int, owner_name text)");
        exec("INSERT INTO uc_t VALUES (1,'uc_user'),(2,'other')");
        exec("ALTER TABLE uc_t ENABLE ROW LEVEL SECURITY");
        exec("CREATE ROLE uc_user LOGIN");
        exec("GRANT ALL ON uc_t TO uc_user");
        exec("CREATE POLICY uc_all ON uc_t FOR ALL USING (owner_name = current_user) WITH CHECK (owner_name = current_user)");
        try {
            exec("SET ROLE uc_user");
            assertEquals(List.of("1"), query("SELECT count(*)::int FROM uc_t"));
            // Try to change owner_name → WITH CHECK should reject
            SQLException ex = assertThrows(SQLException.class,
                () -> exec("UPDATE uc_t SET owner_name = 'stolen' WHERE id = 1"));
            assertEquals("42501", ex.getSQLState());
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE uc_t");
            exec("DROP ROLE uc_user");
        }
    }

    @Test
    void updateUsing_filtersInvisibleRows() throws Exception {
        exec("CREATE TABLE uu_t(id int, owner_name text)");
        exec("INSERT INTO uu_t VALUES (1,'uu_user'),(2,'other')");
        exec("ALTER TABLE uu_t ENABLE ROW LEVEL SECURITY");
        exec("CREATE ROLE uu_user LOGIN");
        exec("GRANT ALL ON uu_t TO uu_user");
        exec("CREATE POLICY uu_upd ON uu_t FOR ALL USING (owner_name = current_user) WITH CHECK (owner_name = current_user)");
        try {
            exec("SET ROLE uu_user");
            // Update non-owner_name column on own row → OK
            assertEquals(1, execUpdate("UPDATE uu_t SET id = 10 WHERE id = 1"));
            // Update row not visible → 0 affected
            assertEquals(0, execUpdate("UPDATE uu_t SET id = 20 WHERE id = 2"));
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE uu_t");
            exec("DROP ROLE uu_user");
        }
    }

    // === Owner vs FORCE ===

    @Test
    void ownerForce_ownerFilteredWhenForced() throws Exception {
        exec("CREATE TABLE of_t(id int, val text)");
        exec("INSERT INTO of_t VALUES (1,'a'),(2,'b')");
        exec("ALTER TABLE of_t ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY of_sel ON of_t FOR SELECT USING (id = 1)");
        try {
            // Owner bypass
            assertEquals(List.of("2"), query("SELECT count(*)::int FROM of_t"));
            // FORCE RLS
            exec("ALTER TABLE of_t FORCE ROW LEVEL SECURITY");
            assertEquals(List.of("1"), query("SELECT count(*)::int FROM of_t"));
            // NO FORCE restores bypass
            exec("ALTER TABLE of_t NO FORCE ROW LEVEL SECURITY");
            assertEquals(List.of("2"), query("SELECT count(*)::int FROM of_t"));
        } finally {
            exec("DROP TABLE of_t");
        }
    }

    // === Superuser bypass by rolsuper ===

    @Test
    void superuserBypass_rolsuperAttribute() throws Exception {
        exec("CREATE ROLE su_role SUPERUSER LOGIN");
        exec("CREATE TABLE su_t(id int)");
        exec("INSERT INTO su_t VALUES (1),(2)");
        exec("ALTER TABLE su_t ENABLE ROW LEVEL SECURITY");
        exec("GRANT ALL ON su_t TO su_role");
        try {
            exec("SET ROLE su_role");
            assertEquals(List.of("2"), query("SELECT count(*)::int FROM su_t"));
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE su_t");
            exec("DROP ROLE su_role");
        }
    }

    @Test
    void superuserForce_superuserFilteredWhenForced() throws Exception {
        exec("CREATE ROLE suf_role SUPERUSER LOGIN");
        exec("CREATE TABLE suf_t(id int)");
        exec("INSERT INTO suf_t VALUES (1),(2)");
        exec("ALTER TABLE suf_t ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY suf_sel ON suf_t FOR SELECT USING (id = 1)");
        exec("GRANT ALL ON suf_t TO suf_role");
        try {
            exec("SET ROLE suf_role");
            // Superuser bypasses
            assertEquals(List.of("2"), query("SELECT count(*)::int FROM suf_t"));
            exec("RESET ROLE");
            exec("ALTER TABLE suf_t FORCE ROW LEVEL SECURITY");
            exec("SET ROLE suf_role");
            // FORCE: superuser is filtered
            assertEquals(List.of("1"), query("SELECT count(*)::int FROM suf_t"));
        } finally {
            exec("RESET ROLE");
            exec("ALTER TABLE suf_t NO FORCE ROW LEVEL SECURITY");
            exec("DROP TABLE suf_t");
            exec("DROP ROLE suf_role");
        }
    }

    // === SET row_security=off ===

    @Test
    void rowSecurityOff_ownerBypasses() throws Exception {
        exec("CREATE TABLE rso_t(id int)");
        exec("INSERT INTO rso_t VALUES (1),(2)");
        exec("ALTER TABLE rso_t ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE rso_t FORCE ROW LEVEL SECURITY");
        exec("CREATE POLICY rso_sel ON rso_t FOR SELECT USING (id = 1)");
        try {
            // Owner with FORCE sees filtered
            assertEquals(List.of("1"), query("SELECT count(*)::int FROM rso_t"));
            // row_security=off bypasses for owner
            exec("SET row_security = off");
            assertEquals(List.of("2"), query("SELECT count(*)::int FROM rso_t"));
        } finally {
            exec("SET row_security = on");
            exec("ALTER TABLE rso_t NO FORCE ROW LEVEL SECURITY");
            exec("DROP TABLE rso_t");
        }
    }

    @Test
    void rowSecurityOff_nonOwnerErrors() throws Exception {
        exec("CREATE TABLE rsoe_t(id int)");
        exec("INSERT INTO rsoe_t VALUES (1),(2)");
        exec("ALTER TABLE rsoe_t ENABLE ROW LEVEL SECURITY");
        exec("CREATE ROLE rsoe_user LOGIN");
        exec("GRANT ALL ON rsoe_t TO rsoe_user");
        exec("CREATE POLICY rsoe_sel ON rsoe_t FOR SELECT USING (id = 1)");
        try {
            try (Connection c2 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple", "rsoe_user", "")) {
                c2.setAutoCommit(true);
                try (Statement st = c2.createStatement()) {
                    st.execute("SET row_security = off");
                    SQLException ex = assertThrows(SQLException.class,
                        () -> st.executeQuery("SELECT * FROM rsoe_t"));
                    // PostgreSQL reports this as a refusal to read the relation.
                    assertEquals("42501", ex.getSQLState());
                }
            }
        } finally {
            exec("DROP TABLE rsoe_t");
            exec("DROP ROLE rsoe_user");
        }
    }

    // === UPDATE falls back to USING when no WITH CHECK ===

    @Test
    void updateFallbackUsing_noWithCheck() throws Exception {
        exec("CREATE TABLE ufu_t(id int, owner_name text)");
        exec("INSERT INTO ufu_t VALUES (1,'ufu_user'),(2,'other')");
        exec("ALTER TABLE ufu_t ENABLE ROW LEVEL SECURITY");
        exec("CREATE ROLE ufu_user LOGIN");
        exec("GRANT ALL ON ufu_t TO ufu_user");
        // Policy with USING only (no WITH CHECK) — PG uses USING for both read and write check
        exec("CREATE POLICY ufu_pol ON ufu_t FOR ALL USING (owner_name = current_user)");
        try {
            exec("SET ROLE ufu_user");
            // Can update own row's non-policy column
            assertEquals(1, execUpdate("UPDATE ufu_t SET id = 10 WHERE id = 1"));
            // Changing owner_name violates USING (used as fallback for WITH CHECK)
            SQLException ex = assertThrows(SQLException.class,
                () -> exec("UPDATE ufu_t SET owner_name = 'hacked' WHERE id = 10"));
            assertEquals("42501", ex.getSQLState());
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE ufu_t");
            exec("DROP ROLE ufu_user");
        }
    }

    // === DELETE without WHERE + RLS ===

    @Test
    void deleteAllWithRls_onlyDeletesVisibleRows() throws Exception {
        exec("CREATE TABLE daw_t(id int, owner_name text)");
        exec("INSERT INTO daw_t VALUES (1,'daw_a'),(2,'daw_b'),(3,'daw_a')");
        exec("ALTER TABLE daw_t ENABLE ROW LEVEL SECURITY");
        exec("CREATE ROLE daw_a LOGIN");
        exec("GRANT ALL ON daw_t TO daw_a");
        exec("CREATE POLICY daw_sel ON daw_t FOR SELECT USING (owner_name = current_user)");
        exec("CREATE POLICY daw_del ON daw_t FOR DELETE USING (owner_name = current_user)");
        try {
            exec("SET ROLE daw_a");
            assertEquals(2, execUpdate("DELETE FROM daw_t")); // only daw_a's rows
            exec("RESET ROLE");
            assertEquals(List.of("1"), query("SELECT count(*)::int FROM daw_t")); // daw_b remains
        } finally {
            exec("RESET ROLE");
            exec("DROP TABLE daw_t");
            exec("DROP ROLE daw_a");
        }
    }
}
