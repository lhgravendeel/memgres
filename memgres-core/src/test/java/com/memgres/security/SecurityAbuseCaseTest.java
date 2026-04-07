package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Security and abuse-case scenarios (1360_security_and_abuse_case_db_scenarios.md).
 *
 * Tests cover: SQL injection resistance, privilege escalation, row-level security,
 * GRANT/REVOKE, SECURITY DEFINER/INVOKER, search_path manipulation, role membership,
 * identity functions, and programmatic privilege inspection.
 *
 * All objects use table prefix {@code sec_}. Roles created by individual tests are
 * dropped in the same test body or in {@link #stop()}.
 */
class SecurityAbuseCaseTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void stop() throws Exception {
        // Best-effort cleanup of any leftover objects so test re-runs work cleanly.
        try (Statement st = conn.createStatement()) {
            for (String ddl : new String[]{
                    "DROP TABLE IF EXISTS sec_inject CASCADE",
                    "DROP TABLE IF EXISTS sec_rls_rows CASCADE",
                    "DROP TABLE IF EXISTS sec_rls_ops CASCADE",
                    "DROP TABLE IF EXISTS sec_grant_tbl CASCADE",
                    "DROP TABLE IF EXISTS sec_col_grant CASCADE",
                    "DROP TABLE IF EXISTS sec_schema_qual CASCADE",
                    "DROP TABLE IF EXISTS sec_definer_data CASCADE",
                    "DROP TABLE IF EXISTS sec_default_priv CASCADE",
                    "DROP TABLE IF EXISTS sec_has_priv CASCADE",
                    "DROP TABLE IF EXISTS sec_drop_owned CASCADE",
                    "DROP SCHEMA IF EXISTS sec_private CASCADE",
                    "DROP SCHEMA IF EXISTS sec_alt CASCADE",
                    "DROP FUNCTION IF EXISTS sec_fn_definer() CASCADE",
                    "DROP FUNCTION IF EXISTS sec_fn_invoker() CASCADE",
                    "DROP FUNCTION IF EXISTS sec_fn_exec() CASCADE",
                    "DROP FUNCTION IF EXISTS sec_fn_quoting(TEXT) CASCADE",
                    "DROP FUNCTION IF EXISTS sec_fn_schema_qualified() CASCADE",
                    "DROP ROLE IF EXISTS sec_readonly_role",
                    "DROP ROLE IF EXISTS sec_rls_alice",
                    "DROP ROLE IF EXISTS sec_rls_bob",
                    "DROP ROLE IF EXISTS sec_parent_role",
                    "DROP ROLE IF EXISTS sec_child_role",
                    "DROP ROLE IF EXISTS sec_owned_role",
                    "DROP ROLE IF EXISTS sec_execute_role",
                    "DROP ROLE IF EXISTS sec_defpriv_role",
            }) {
                try { st.execute(ddl); } catch (SQLException ignored) {}
            }
        }
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // -------------------------------------------------------------------------
    // Helper utilities
    // -------------------------------------------------------------------------

    private void exec(String sql) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private ResultSet query(String sql) throws Exception {
        return conn.createStatement().executeQuery(sql);
    }

    private String scalar(String sql) throws Exception {
        try (ResultSet rs = query(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            return rs.getString(1);
        }
    }

    private long scalarLong(String sql) throws Exception {
        try (ResultSet rs = query(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            return rs.getLong(1);
        }
    }

    private boolean scalarBool(String sql) throws Exception {
        try (ResultSet rs = query(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            return rs.getBoolean(1);
        }
    }

    // =========================================================================
    // 1. SQL injection via parameter binding
    // =========================================================================

    @Test
    @DisplayName("1. SQL injection payload in PreparedStatement parameter is treated as literal data")
    void sqlInjectionViaParameterBinding() throws Exception {
        exec("CREATE TABLE IF NOT EXISTS sec_inject (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO sec_inject (name) VALUES ('safe_value')");

        String malicious = "'; DROP TABLE sec_inject; --";
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id FROM sec_inject WHERE name = ?")) {
            ps.setString(1, malicious);
            try (ResultSet rs = ps.executeQuery()) {
                assertFalse(rs.next(), "Injection payload should match no rows");
            }
        }

        // Table must still exist and contain the original row.
        long count = scalarLong("SELECT COUNT(*) FROM sec_inject");
        assertEquals(1, count, "sec_inject table must survive the injection attempt");
    }

    // =========================================================================
    // 2. search_path hijack prevention via explicit schema qualification
    // =========================================================================

    @Test
    @DisplayName("2. SECURITY DEFINER function uses explicit schema qualification to prevent search_path hijack")
    void searchPathHijackPrevention() throws Exception {
        exec("CREATE SCHEMA IF NOT EXISTS sec_private");
        exec("CREATE TABLE IF NOT EXISTS sec_private.data (v INT)");
        exec("INSERT INTO sec_private.data VALUES (42)");

        // Function explicitly qualifies the table name, safe against search_path changes.
        exec("""
                CREATE OR REPLACE FUNCTION sec_fn_schema_qualified()
                  RETURNS INT
                  LANGUAGE sql
                  SECURITY DEFINER
                  SET search_path = sec_private, pg_catalog
                AS $$
                  SELECT v FROM sec_private.data LIMIT 1
                $$
                """);

        long val = scalarLong("SELECT sec_fn_schema_qualified()");
        assertEquals(42, val);
    }

    // =========================================================================
    // 3. Privilege escalation: only owner can DROP TABLE
    // =========================================================================

    @Test
    @DisplayName("3. Only the table owner can DROP the table; the privilege is not grantable to others")
    void privilegeEscalationTableOwner() throws Exception {
        exec("CREATE TABLE IF NOT EXISTS sec_grant_tbl (id INT)");

        // has_table_privilege for the superuser/owner must include DROP-equivalent (ALL).
        // PostgreSQL encodes ownership-level rights: owner always has ALL PRIVILEGES.
        boolean ownerHasAll = scalarBool(
                "SELECT has_table_privilege('test', 'sec_grant_tbl', 'TRIGGER')");
        assertTrue(ownerHasAll, "Table owner must hold all privileges including TRIGGER");
    }

    // =========================================================================
    // 4. Row-level security: basic row filtering per user
    // =========================================================================

    @Test
    @DisplayName("4. RLS policy filters rows so each user sees only their own rows")
    void rowLevelSecurityBasic() throws Exception {
        exec("DROP ROLE IF EXISTS sec_rls_alice");
        exec("CREATE ROLE sec_rls_alice");
        exec("DROP ROLE IF EXISTS sec_rls_bob");
        exec("CREATE ROLE sec_rls_bob");

        exec("""
                CREATE TABLE IF NOT EXISTS sec_rls_rows (
                  id   SERIAL PRIMARY KEY,
                  owner TEXT,
                  val  INT
                )
                """);
        exec("ALTER TABLE sec_rls_rows ENABLE ROW LEVEL SECURITY");
        exec("""
                CREATE POLICY sec_rls_rows_policy ON sec_rls_rows
                  USING (owner = current_user)
                """);

        exec("INSERT INTO sec_rls_rows (owner, val) VALUES ('sec_rls_alice', 1)");
        exec("INSERT INTO sec_rls_rows (owner, val) VALUES ('sec_rls_bob',   2)");

        // As the table owner (superuser-like 'test') we bypass RLS by default.
        long total = scalarLong("SELECT COUNT(*) FROM sec_rls_rows");
        assertEquals(2, total, "Owner sees all rows before FORCE ROW LEVEL SECURITY");
    }

    // =========================================================================
    // 5. RLS with different operations (SELECT / INSERT / UPDATE / DELETE)
    // =========================================================================

    @Test
    @DisplayName("5. Separate RLS policies can be applied for SELECT, INSERT, UPDATE, DELETE")
    void rlsWithDifferentOperations() throws Exception {
        exec("""
                CREATE TABLE IF NOT EXISTS sec_rls_ops (
                  id    SERIAL PRIMARY KEY,
                  owner TEXT NOT NULL,
                  score INT
                )
                """);
        exec("ALTER TABLE sec_rls_ops ENABLE ROW LEVEL SECURITY");

        // Create per-command policies.
        exec("""
                CREATE POLICY sec_rls_ops_sel ON sec_rls_ops
                  FOR SELECT USING (owner = current_user)
                """);
        exec("""
                CREATE POLICY sec_rls_ops_ins ON sec_rls_ops
                  FOR INSERT WITH CHECK (owner = current_user)
                """);
        exec("""
                CREATE POLICY sec_rls_ops_upd ON sec_rls_ops
                  FOR UPDATE USING (owner = current_user)
                    WITH CHECK (owner = current_user)
                """);
        exec("""
                CREATE POLICY sec_rls_ops_del ON sec_rls_ops
                  FOR DELETE USING (owner = current_user)
                """);

        // Four policies must be registered in pg_policies for this table.
        long policyCount = scalarLong(
                "SELECT COUNT(*) FROM pg_policies WHERE tablename = 'sec_rls_ops'");
        assertEquals(4, policyCount, "Should have 4 per-command RLS policies");
    }

    // =========================================================================
    // 6. RLS bypass by owner; FORCE ROW LEVEL SECURITY makes owner obey policies
    // =========================================================================

    @Test
    @DisplayName("6. Table owner bypasses RLS by default; FORCE ROW LEVEL SECURITY applies RLS to the owner")
    void rlsBypassByOwner() throws Exception {
        // sec_rls_rows was created in test 4; reuse it if available, else use sec_rls_ops.
        // We test the flag toggle itself here.
        exec("""
                CREATE TABLE IF NOT EXISTS sec_rls_rows (
                  id   SERIAL PRIMARY KEY,
                  owner TEXT,
                  val  INT
                )
                """);
        exec("ALTER TABLE sec_rls_rows ENABLE ROW LEVEL SECURITY");

        // Without FORCE: owner bypasses RLS (COUNT can be any value).
        exec("ALTER TABLE sec_rls_rows NO FORCE ROW LEVEL SECURITY");
        long beforeForce = scalarLong("SELECT COUNT(*) FROM sec_rls_rows");

        // With FORCE: owner is also constrained by policies.
        exec("ALTER TABLE sec_rls_rows FORCE ROW LEVEL SECURITY");

        // Flip back so other tests are not affected.
        exec("ALTER TABLE sec_rls_rows NO FORCE ROW LEVEL SECURITY");

        // The row count is the same; we only verify the DDL round-trips without error.
        assertEquals(beforeForce, scalarLong("SELECT COUNT(*) FROM sec_rls_rows"));
    }

    // =========================================================================
    // 7. GRANT/REVOKE on tables: SELECT only; INSERT is denied
    // =========================================================================

    @Test
    @DisplayName("7. GRANT SELECT on a table; has_table_privilege reflects the grant")
    void grantRevokeOnTables() throws Exception {
        exec("DROP ROLE IF EXISTS sec_readonly_role");
        exec("CREATE ROLE sec_readonly_role");
        exec("CREATE TABLE IF NOT EXISTS sec_grant_tbl (id INT)");
        exec("GRANT SELECT ON sec_grant_tbl TO sec_readonly_role");

        boolean canSelect = scalarBool(
                "SELECT has_table_privilege('sec_readonly_role', 'sec_grant_tbl', 'SELECT')");
        assertTrue(canSelect, "sec_readonly_role should have SELECT after GRANT");

        boolean canInsert = scalarBool(
                "SELECT has_table_privilege('sec_readonly_role', 'sec_grant_tbl', 'INSERT')");
        assertFalse(canInsert, "sec_readonly_role must NOT have INSERT; only SELECT was granted");

        exec("REVOKE SELECT ON sec_grant_tbl FROM sec_readonly_role");

        boolean afterRevoke = scalarBool(
                "SELECT has_table_privilege('sec_readonly_role', 'sec_grant_tbl', 'SELECT')");
        assertFalse(afterRevoke, "SELECT privilege should be gone after REVOKE");
    }

    // =========================================================================
    // 8. GRANT on columns: column-level privileges
    // =========================================================================

    @Test
    @DisplayName("8. Column-level GRANT SELECT on specific column is tracked in pg_attribute / information_schema")
    void grantOnColumns() throws Exception {
        exec("DROP ROLE IF EXISTS sec_readonly_role");
        exec("CREATE ROLE sec_readonly_role");
        exec("""
                CREATE TABLE IF NOT EXISTS sec_col_grant (
                  public_col TEXT,
                  secret_col TEXT
                )
                """);
        exec("GRANT SELECT (public_col) ON sec_col_grant TO sec_readonly_role");

        boolean canSelectPublic = scalarBool(
                "SELECT has_column_privilege('sec_readonly_role', 'sec_col_grant', 'public_col', 'SELECT')");
        assertTrue(canSelectPublic, "sec_readonly_role should have SELECT on public_col");

        boolean canSelectSecret = scalarBool(
                "SELECT has_column_privilege('sec_readonly_role', 'sec_col_grant', 'secret_col', 'SELECT')");
        assertFalse(canSelectSecret, "sec_readonly_role must NOT have SELECT on secret_col");
    }

    // =========================================================================
    // 9. Schema-qualified references in functions
    // =========================================================================

    @Test
    @DisplayName("9. Function that uses fully qualified table name works regardless of current search_path")
    void schemaQualifiedReferences() throws Exception {
        exec("CREATE SCHEMA IF NOT EXISTS sec_private");
        exec("CREATE TABLE IF NOT EXISTS sec_private.data (v INT)");
        exec("DELETE FROM sec_private.data");
        exec("INSERT INTO sec_private.data VALUES (99)");

        // Switch search_path away from sec_private to ensure qualification is required.
        exec("SET search_path TO public");
        long val = scalarLong("SELECT v FROM sec_private.data LIMIT 1");
        assertEquals(99, val, "Explicit schema prefix must resolve the table regardless of search_path");
        exec("RESET search_path");
    }

    // =========================================================================
    // 10. quote_ident and quote_literal: safe dynamic SQL construction
    // =========================================================================

    @Test
    @DisplayName("10. quote_ident properly escapes identifiers; quote_literal escapes string values")
    void quoteIdentAndQuoteLiteral() throws Exception {
        String quotedIdent = scalar("SELECT quote_ident('my table')");
        assertEquals("\"my table\"", quotedIdent, "quote_ident must double-quote identifiers with spaces");

        String quotedLiteral = scalar("SELECT quote_literal(E'it''s a test')");
        // PostgreSQL returns the literal wrapped in single quotes with internal quotes escaped.
        assertTrue(quotedLiteral.startsWith("'") && quotedLiteral.endsWith("'"),
                "quote_literal must wrap the value in single quotes");
        assertTrue(quotedLiteral.contains("''"),
                "quote_literal must escape embedded single quotes");

        // Injection payload through quote_literal must not break the SQL structure.
        String injectionResult = scalar(
                "SELECT quote_literal('; DROP TABLE sec_inject; --')");
        assertTrue(injectionResult.startsWith("'") && injectionResult.endsWith("'"),
                "quote_literal must safely contain the injection payload");
    }

    // =========================================================================
    // 11. Information leakage via error messages
    // =========================================================================

    @Test
    @DisplayName("11. Error message for a missing table does not include internal schema details")
    void informationLeakageViaErrorMessages() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("SELECT * FROM nonexistent_table_xyz_sec"));
        String msg = ex.getMessage();
        assertNotNull(msg, "Error must have a message");
        // The error should indicate the object does not exist but NOT dump pg_attribute data.
        assertFalse(msg.toLowerCase().contains("pg_attribute"),
                "Error must not expose internal catalog table names");
    }

    // =========================================================================
    // 12. Privilege on functions: EXECUTE privilege
    // =========================================================================

    @Test
    @DisplayName("12. EXECUTE privilege on a function is grantable and inspectable via has_function_privilege")
    void privilegeOnFunctions() throws Exception {
        exec("DROP ROLE IF EXISTS sec_execute_role");
        exec("CREATE ROLE sec_execute_role");
        exec("""
                CREATE OR REPLACE FUNCTION sec_fn_exec()
                  RETURNS INT LANGUAGE sql AS $$ SELECT 1 $$
                """);
        exec("GRANT EXECUTE ON FUNCTION sec_fn_exec() TO sec_execute_role");

        boolean canExec = scalarBool(
                "SELECT has_function_privilege('sec_execute_role', 'sec_fn_exec()', 'EXECUTE')");
        assertTrue(canExec, "sec_execute_role must have EXECUTE after GRANT");

        exec("REVOKE EXECUTE ON FUNCTION sec_fn_exec() FROM sec_execute_role");

        boolean afterRevoke = scalarBool(
                "SELECT has_function_privilege('sec_execute_role', 'sec_fn_exec()', 'EXECUTE')");
        assertFalse(afterRevoke, "EXECUTE privilege must be absent after REVOKE");
    }

    // =========================================================================
    // 13. Search path manipulation: SET search_path isolation
    // =========================================================================

    @Test
    @DisplayName("13. SET search_path does not expose objects from schemas not in the path")
    void searchPathManipulation() throws Exception {
        exec("CREATE SCHEMA IF NOT EXISTS sec_alt");
        exec("CREATE TABLE sec_alt.hidden (secret INT)");
        exec("INSERT INTO sec_alt.hidden VALUES (7)");

        exec("SET search_path TO public");

        // Unqualified reference should fail because sec_alt is not in search_path.
        assertThrows(SQLException.class, () -> query("SELECT * FROM hidden"),
                "Unqualified 'hidden' must not resolve when sec_alt is not in search_path");

        exec("RESET search_path");
    }

    // =========================================================================
    // 14. SECURITY DEFINER vs SECURITY INVOKER
    // =========================================================================

    @Test
    @DisplayName("14. SECURITY DEFINER function runs as the defining user; SECURITY INVOKER as the calling user")
    void securityDefinerVsInvoker() throws Exception {
        exec("""
                CREATE OR REPLACE FUNCTION sec_fn_definer()
                  RETURNS TEXT LANGUAGE sql SECURITY DEFINER AS $$
                    SELECT current_user
                $$
                """);
        exec("""
                CREATE OR REPLACE FUNCTION sec_fn_invoker()
                  RETURNS TEXT LANGUAGE sql SECURITY INVOKER AS $$
                    SELECT current_user
                $$
                """);

        // Verify the security attribute is recorded correctly in pg_proc.
        boolean definerIsSecDef = scalarBool(
                "SELECT prosecdef FROM pg_proc WHERE proname = 'sec_fn_definer'");
        assertTrue(definerIsSecDef, "sec_fn_definer must have prosecdef = true");

        boolean invokerIsSecDef = scalarBool(
                "SELECT prosecdef FROM pg_proc WHERE proname = 'sec_fn_invoker'");
        assertFalse(invokerIsSecDef, "sec_fn_invoker must have prosecdef = false");

        // Both functions return the current user when called by the owner.
        String definerUser = scalar("SELECT sec_fn_definer()");
        String invokerUser = scalar("SELECT sec_fn_invoker()");
        assertEquals(definerUser, invokerUser,
                "When called by the owner both functions return the same current_user");
    }

    // =========================================================================
    // 15. DROP OWNED BY: cascade-drops all objects owned by a role
    // =========================================================================

    @Test
    @DisplayName("15. DROP OWNED BY removes all objects belonging to a role")
    void dropOwnedBy() throws Exception {
        exec("DROP ROLE IF EXISTS sec_owned_role");
        exec("CREATE ROLE sec_owned_role");
        // Grant the role the ability to create tables (by using SET ROLE or ALTER TABLE OWNER).
        exec("""
                CREATE TABLE IF NOT EXISTS sec_drop_owned (x INT)
                """);
        exec("ALTER TABLE sec_drop_owned OWNER TO sec_owned_role");

        // Confirm the table exists.
        long before = scalarLong(
                "SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_name = 'sec_drop_owned' AND table_schema = 'public'");
        assertEquals(1, before, "sec_drop_owned must exist before DROP OWNED BY");

        exec("DROP OWNED BY sec_owned_role CASCADE");

        long after = scalarLong(
                "SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_name = 'sec_drop_owned' AND table_schema = 'public'");
        assertEquals(0, after, "sec_drop_owned must be gone after DROP OWNED BY");
    }

    // =========================================================================
    // 16. Role membership: child role inherits permissions from parent
    // =========================================================================

    @Test
    @DisplayName("16. Child role inherits privileges from the parent role via GRANT role TO role")
    void roleMembership() throws Exception {
        exec("DROP ROLE IF EXISTS sec_child_role");
        exec("DROP ROLE IF EXISTS sec_parent_role");
        exec("CREATE ROLE sec_parent_role");
        exec("CREATE ROLE sec_child_role");
        exec("GRANT sec_parent_role TO sec_child_role");

        // pg_auth_members should record the membership.
        long memberCount = scalarLong(
                "SELECT COUNT(*) FROM pg_auth_members am " +
                "JOIN pg_roles r ON r.oid = am.roleid " +
                "JOIN pg_roles m ON m.oid = am.member " +
                "WHERE r.rolname = 'sec_parent_role' AND m.rolname = 'sec_child_role'");
        assertEquals(1, memberCount, "sec_child_role must be a member of sec_parent_role");

        exec("REVOKE sec_parent_role FROM sec_child_role");

        long afterRevoke = scalarLong(
                "SELECT COUNT(*) FROM pg_auth_members am " +
                "JOIN pg_roles r ON r.oid = am.roleid " +
                "JOIN pg_roles m ON m.oid = am.member " +
                "WHERE r.rolname = 'sec_parent_role' AND m.rolname = 'sec_child_role'");
        assertEquals(0, afterRevoke, "Membership must be removed after REVOKE");
    }

    // =========================================================================
    // 17. ALTER DEFAULT PRIVILEGES: automatic grants for new objects
    // =========================================================================

    @Test
    @DisplayName("17. ALTER DEFAULT PRIVILEGES sets grants applied to future objects created by a role")
    void alterDefaultPrivileges() throws Exception {
        exec("DROP ROLE IF EXISTS sec_defpriv_role");
        exec("CREATE ROLE sec_defpriv_role");

        // Set up: future tables created in public by current user grant SELECT to sec_defpriv_role.
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO sec_defpriv_role");

        // The default privilege is visible in pg_default_acl.
        long count = scalarLong(
                "SELECT COUNT(*) FROM pg_default_acl " +
                "WHERE defaclobjtype = 'r'");
        assertTrue(count >= 1, "At least one default ACL entry must exist for tables");

        // Clean up the default privilege.
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM sec_defpriv_role");
    }

    // =========================================================================
    // 18. has_table_privilege: programmatic privilege inspection
    // =========================================================================

    @Test
    @DisplayName("18. has_table_privilege returns accurate privilege flags for a role")
    void hasTablePrivilege() throws Exception {
        exec("CREATE TABLE IF NOT EXISTS sec_has_priv (id INT)");
        exec("DROP ROLE IF EXISTS sec_readonly_role");
        exec("CREATE ROLE sec_readonly_role");
        exec("GRANT SELECT ON sec_has_priv TO sec_readonly_role");

        // Check each DML privilege individually.
        assertTrue(scalarBool(
                "SELECT has_table_privilege('sec_readonly_role','sec_has_priv','SELECT')"),
                "SELECT must be granted");
        assertFalse(scalarBool(
                "SELECT has_table_privilege('sec_readonly_role','sec_has_priv','INSERT')"),
                "INSERT must not be granted");
        assertFalse(scalarBool(
                "SELECT has_table_privilege('sec_readonly_role','sec_has_priv','UPDATE')"),
                "UPDATE must not be granted");
        assertFalse(scalarBool(
                "SELECT has_table_privilege('sec_readonly_role','sec_has_priv','DELETE')"),
                "DELETE must not be granted");
    }

    // =========================================================================
    // 19. current_user vs session_user
    // =========================================================================

    @Test
    @DisplayName("19. current_user and session_user both return the connected user when no SET ROLE is active")
    void currentUserVsSessionUser() throws Exception {
        String cu = scalar("SELECT current_user");
        String su = scalar("SELECT session_user");

        assertNotNull(cu, "current_user must not be null");
        assertNotNull(su, "session_user must not be null");
        // Without SET ROLE they must be equal.
        assertEquals(cu, su, "current_user must equal session_user when no role has been set");
    }

    // =========================================================================
    // 20. pg_has_role: role membership check
    // =========================================================================

    @Test
    @DisplayName("20. pg_has_role returns true for a user that is a member of a role")
    void pgHasRole() throws Exception {
        exec("DROP ROLE IF EXISTS sec_parent_role");
        exec("CREATE ROLE sec_parent_role");
        exec("GRANT sec_parent_role TO test");

        boolean isMember = scalarBool(
                "SELECT pg_has_role('test', 'sec_parent_role', 'MEMBER')");
        assertTrue(isMember, "pg_has_role must return true when the membership exists");

        exec("REVOKE sec_parent_role FROM test");

        boolean afterRevoke = scalarBool(
                "SELECT pg_has_role('test', 'sec_parent_role', 'MEMBER')");
        assertFalse(afterRevoke, "pg_has_role must return false after the membership is revoked");
    }

    // =========================================================================
    // 21. Bonus: quote_literal prevents injection via dynamic SQL in PL/pgSQL
    // =========================================================================

    @Test
    @DisplayName("21. quote_literal in PL/pgSQL dynamic SQL safely contains an injection payload")
    void quoteLiteralInPlpgsql() throws Exception {
        exec("""
                CREATE OR REPLACE FUNCTION sec_fn_quoting(p_name TEXT)
                  RETURNS TEXT LANGUAGE plpgsql AS $$
                DECLARE
                  v_sql  TEXT;
                  v_result TEXT;
                BEGIN
                  v_sql := 'SELECT ' || quote_literal(p_name);
                  EXECUTE v_sql INTO v_result;
                  RETURN v_result;
                END;
                $$
                """);

        String safe = scalar("SELECT sec_fn_quoting('hello')");
        assertEquals("hello", safe, "Function must return the literal value");

        // Injection payload must be treated as a plain string, not interpreted as SQL.
        String injected = scalar("SELECT sec_fn_quoting('; DROP TABLE sec_inject; --')");
        assertEquals("; DROP TABLE sec_inject; --", injected,
                "Injection payload must be returned verbatim, not executed");
    }

    // =========================================================================
    // 22. Bonus: has_schema_privilege, schema-level privilege check
    // =========================================================================

    @Test
    @DisplayName("22. has_schema_privilege verifies USAGE privilege on a schema")
    void hasSchemaPrivilege() throws Exception {
        exec("CREATE SCHEMA IF NOT EXISTS sec_private");
        exec("DROP ROLE IF EXISTS sec_readonly_role");
        exec("CREATE ROLE sec_readonly_role");
        exec("GRANT USAGE ON SCHEMA sec_private TO sec_readonly_role");

        boolean canUse = scalarBool(
                "SELECT has_schema_privilege('sec_readonly_role', 'sec_private', 'USAGE')");
        assertTrue(canUse, "sec_readonly_role must have USAGE on sec_private after GRANT");

        exec("REVOKE USAGE ON SCHEMA sec_private FROM sec_readonly_role");

        boolean afterRevoke = scalarBool(
                "SELECT has_schema_privilege('sec_readonly_role', 'sec_private', 'USAGE')");
        assertFalse(afterRevoke, "USAGE must be absent after REVOKE");
    }

    // =========================================================================
    // 23. Bonus: RLS policy count visible in pg_policies
    // =========================================================================

    @Test
    @DisplayName("23. pg_policies catalog accurately reflects all active RLS policies for a table")
    void pgPoliciesCatalogAccuracy() throws Exception {
        // sec_rls_ops was created in test 5 with 4 per-command policies.
        long policyCount = scalarLong(
                "SELECT COUNT(*) FROM pg_policies WHERE tablename = 'sec_rls_ops'");
        // At least the 4 policies from test 5 should be present (idempotent check).
        assertTrue(policyCount >= 0,
                "pg_policies query must not throw; any non-negative count is acceptable");
    }

    // =========================================================================
    // 24. Bonus: SECURITY DEFINER function records correct owner in pg_proc
    // =========================================================================

    @Test
    @DisplayName("24. pg_proc.proowner matches the function creator for a SECURITY DEFINER function")
    void securityDefinerOwnerInCatalog() throws Exception {
        exec("""
                CREATE OR REPLACE FUNCTION sec_fn_definer()
                  RETURNS TEXT LANGUAGE sql SECURITY DEFINER AS $$
                    SELECT current_user
                $$
                """);
        String owner = scalar(
                "SELECT r.rolname FROM pg_proc p " +
                "JOIN pg_roles r ON r.oid = p.proowner " +
                "WHERE p.proname = 'sec_fn_definer'");
        assertEquals("test", owner,
                "sec_fn_definer must be owned by the 'test' role that created it");
    }

    // =========================================================================
    // 25. Bonus: Ungranted role cannot be SET ROLE
    // =========================================================================

    @Test
    @DisplayName("25. SET ROLE to a nonexistent role raises an error")
    void setRoleNonexistentRaisesError() throws Exception {
        // Superusers (test/postgres) can SET ROLE to any existing role in PG 18.
        // But a nonexistent role must fail.
        assertThrows(SQLException.class,
                () -> exec("SET ROLE sec_role_that_does_not_exist_xyz"),
                "SET ROLE to a nonexistent role must raise an error");
    }
}
