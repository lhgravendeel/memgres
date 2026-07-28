package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The has_*_privilege family answers a question callers use to decide whether to attempt an
 * operation, so the failure mode matters: answering "true" about a table, column, schema or
 * privilege name that does not exist turns a typo into apparent permission, and answering "false"
 * about a role that does not exist hides a grant that is really there. PostgreSQL resolves the
 * role, then the object, then the privilege name, and raises on the first that does not resolve.
 */
class PrivilegeIntrospectionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE pit_pv (i int, j text)");
        exec("CREATE VIEW pit_vw AS SELECT i FROM pit_pv");
        exec("CREATE SEQUENCE pit_seq");
        exec("CREATE SCHEMA pit_s");
        exec("CREATE ROLE pit_r");
        exec("CREATE FUNCTION pit_f(int) RETURNS int LANGUAGE sql AS 'SELECT $1'");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void assertAnswer(String expected, String sql) throws SQLException {
        assertEquals(expected, scalar(sql), sql);
    }

    private static void assertFails(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    // ---- Privilege names ----

    @Test
    void aPrivilegeNameTheObjectCannotCarryIsRejected() {
        assertFails("22023", "unrecognized privilege type: \"NOSUCHPRIV\"",
                "SELECT has_table_privilege('pit_pv', 'NOSUCHPRIV')");
        assertFails("22023", "unrecognized privilege type: \"USAGE\"",
                "SELECT has_table_privilege('pit_pv', 'USAGE')");
        assertFails("22023", "unrecognized privilege type: \"ALL\"",
                "SELECT has_table_privilege('pit_pv', 'ALL')");
        assertFails("22023", "unrecognized privilege type: \"\"",
                "SELECT has_table_privilege('pit_pv', '')");
        assertFails("22023", "unrecognized privilege type: \"DELETE\"",
                "SELECT has_column_privilege('pit_pv', 'i', 'DELETE')");
        assertFails("22023", "unrecognized privilege type: \"DELETE\"",
                "SELECT has_any_column_privilege('pit_pv', 'DELETE')");
        assertFails("22023", "unrecognized privilege type: \"SELECT\"",
                "SELECT has_schema_privilege('pit_s', 'SELECT')");
        assertFails("22023", "unrecognized privilege type: \"NOSUCHPRIV\"",
                "SELECT has_database_privilege(current_database(), 'NOSUCHPRIV')");
        assertFails("22023", "unrecognized privilege type: \"INSERT\"",
                "SELECT has_sequence_privilege('pit_seq', 'INSERT')");
        assertFails("22023", "unrecognized privilege type: \"SELECT\"",
                "SELECT has_function_privilege('pit_f(int)', 'SELECT')");
        assertFails("22023", "unrecognized privilege type: \"NOSUCHPRIV\"",
                "SELECT has_language_privilege('sql', 'NOSUCHPRIV')");
        assertFails("22023", "unrecognized privilege type: \"NOSUCHPRIV\"",
                "SELECT has_type_privilege('int4', 'NOSUCHPRIV')");
        assertFails("22023", "unrecognized privilege type: \"NOSUCHPRIV\"",
                "SELECT has_tablespace_privilege('pg_default', 'NOSUCHPRIV')");
        assertFails("22023", "unrecognized privilege type: \"NOSUCHPRIV\"",
                "SELECT has_parameter_privilege('work_mem', 'NOSUCHPRIV')");
        assertFails("22023", "unrecognized privilege type: \"NOSUCHPRIV\"",
                "SELECT pg_has_role('pit_r', 'NOSUCHPRIV')");
    }

    @Test
    void aPrivilegeListIsSplitOnCommasAndTrimmed() throws Exception {
        assertAnswer("t", "SELECT has_table_privilege('pit_pv', 'INSERT, UPDATE')");
        assertAnswer("t", "SELECT has_table_privilege('pit_pv', 'select , insert')");
        assertAnswer("t", "SELECT has_table_privilege('pit_pv', ' SELECT ')");
        assertAnswer("t", "SELECT has_table_privilege('pit_pv', 'SELECT WITH GRANT OPTION')");
        // A trailing comma leaves an empty name, and the doubled space is not the suffix.
        assertFails("22023", "unrecognized privilege type: \"\"",
                "SELECT has_table_privilege('pit_pv', 'SELECT,')");
        assertFails("22023", "unrecognized privilege type: \"SELECT WITH  GRANT OPTION\"",
                "SELECT has_table_privilege('pit_pv', 'SELECT WITH  GRANT OPTION')");
    }

    // ---- Objects that do not exist ----

    @Test
    void anObjectThatDoesNotExistIsReportedRatherThanAnswered() {
        assertFails("42P01", "relation \"pit_no_such_table\" does not exist",
                "SELECT has_table_privilege('pit_no_such_table', 'SELECT')");
        assertFails("42P01", "relation \"public.pit_no_such_table\" does not exist",
                "SELECT has_table_privilege('public.pit_no_such_table', 'SELECT')");
        assertFails("42P01", "relation \"pit_no_such_table\" does not exist",
                "SELECT has_any_column_privilege('pit_no_such_table', 'SELECT')");
        assertFails("42703", "column \"nosuchcol\" of relation \"pit_pv\" does not exist",
                "SELECT has_column_privilege('pit_pv', 'nosuchcol', 'SELECT')");
        assertFails("3F000", "schema \"pit_no_such_schema\" does not exist",
                "SELECT has_schema_privilege('pit_no_such_schema', 'USAGE')");
        assertFails("3D000", "database \"pit_no_such_db\" does not exist",
                "SELECT has_database_privilege('pit_no_such_db', 'CONNECT')");
        assertFails("42P01", "relation \"pit_no_such_seq\" does not exist",
                "SELECT has_sequence_privilege('pit_no_such_seq', 'USAGE')");
        assertFails("42809", "\"pit_pv\" is not a sequence",
                "SELECT has_sequence_privilege('pit_pv', 'USAGE')");
        assertFails("42883", "function \"pit_no_such_fn(int)\" does not exist",
                "SELECT has_function_privilege('pit_no_such_fn(int)', 'EXECUTE')");
        assertFails("42704", "language \"nosuchlang\" does not exist",
                "SELECT has_language_privilege('nosuchlang', 'USAGE')");
        assertFails("42704", "type \"pit_nosuchtype\" does not exist",
                "SELECT has_type_privilege('pit_nosuchtype', 'USAGE')");
        assertFails("42704", "tablespace \"pit_nosuchts\" does not exist",
                "SELECT has_tablespace_privilege('pit_nosuchts', 'CREATE')");
        assertFails("42704", "server \"pit_nosuchsrv\" does not exist",
                "SELECT has_server_privilege('pit_nosuchsrv', 'USAGE')");
        assertFails("42704", "foreign-data wrapper \"pit_nosuchfdw\" does not exist",
                "SELECT has_foreign_data_wrapper_privilege('pit_nosuchfdw', 'USAGE')");
    }

    @Test
    void aRoleThatDoesNotExistIsReportedBeforeTheObject() {
        assertFails("42704", "role \"pit_no_such_role\" does not exist",
                "SELECT has_table_privilege('pit_no_such_role', 'pit_pv', 'SELECT')");
        assertFails("42704", "role \"pit_no_such_role\" does not exist",
                "SELECT has_table_privilege('pit_no_such_role', 'pit_no_such_table', 'NOSUCHPRIV')");
        assertFails("42704", "role \"pit_no_such_role\" does not exist",
                "SELECT has_column_privilege('pit_no_such_role', 'pit_pv', 'i', 'SELECT')");
        assertFails("42704", "role \"pit_no_such_role\" does not exist",
                "SELECT has_schema_privilege('pit_no_such_role', 'pit_s', 'USAGE')");
        assertFails("42704", "role \"pit_no_such_role\" does not exist",
                "SELECT has_database_privilege('pit_no_such_role', current_database(), 'CONNECT')");
        assertFails("42704", "role \"pit_no_such_role\" does not exist",
                "SELECT has_sequence_privilege('pit_no_such_role', 'pit_seq', 'USAGE')");
        assertFails("42704", "role \"pit_no_such_role\" does not exist",
                "SELECT has_function_privilege('pit_no_such_role', 'pit_f(int)', 'EXECUTE')");
        assertFails("42704", "role \"pit_no_such_role\" does not exist",
                "SELECT pg_has_role('pit_no_such_role', 'pit_r', 'USAGE')");
        assertFails("42704", "role \"pit_no_such_role\" does not exist",
                "SELECT pg_has_role('memgres', 'pit_no_such_role', 'USAGE')");
        // PUBLIC is a role name to has_*_privilege but not to pg_has_role.
        assertFails("42704", "role \"public\" does not exist",
                "SELECT pg_has_role('public', 'USAGE')");
    }

    // ---- Answers that must keep working ----

    @Test
    void theOwnerStillHoldsEveryPrivilege() throws Exception {
        assertAnswer("t", "SELECT has_table_privilege('pit_pv', 'SELECT')");
        assertAnswer("t", "SELECT has_table_privilege('pit_pv', 'MAINTAIN')");
        assertAnswer("t", "SELECT has_table_privilege('pit_vw', 'SELECT')");
        assertAnswer("t", "SELECT has_table_privilege('pit_seq', 'SELECT')");
        assertAnswer("t", "SELECT has_table_privilege(current_user, 'pg_class', 'SELECT')");
        assertAnswer("t", "SELECT has_column_privilege('pit_pv', 'i', 'SELECT')");
        assertAnswer("t", "SELECT has_any_column_privilege('pit_pv', 'REFERENCES')");
        assertAnswer("t", "SELECT has_schema_privilege('pit_s', 'CREATE')");
        assertAnswer("t", "SELECT has_database_privilege(current_database(), 'CREATE')");
        assertAnswer("t", "SELECT has_sequence_privilege('pit_seq', 'USAGE')");
        assertAnswer("t", "SELECT has_function_privilege('pit_f(int)', 'EXECUTE')");
        assertAnswer("t", "SELECT has_function_privilege('now()', 'EXECUTE')");
        assertAnswer("t", "SELECT has_type_privilege('integer', 'USAGE')");
        assertAnswer("t", "SELECT has_parameter_privilege('work_mem', 'ALTER SYSTEM')");
    }

    @Test
    void aRoleWithoutGrantsIsToldNoRatherThanYes() throws Exception {
        assertAnswer("f", "SELECT has_table_privilege('pit_r', 'pit_pv', 'DELETE')");
        assertAnswer("f", "SELECT has_column_privilege('pit_r', 'pit_pv', 'j', 'SELECT')");
        assertAnswer("f", "SELECT has_schema_privilege('pit_r', 'pit_s', 'USAGE')");
        assertAnswer("f", "SELECT has_sequence_privilege('pit_r', 'pit_seq', 'USAGE')");
        // PG 15 dropped PUBLIC's CREATE on the public schema; USAGE it kept.
        assertAnswer("t", "SELECT has_schema_privilege('pit_r', 'public', 'USAGE')");
        assertAnswer("f", "SELECT has_schema_privilege('pit_r', 'public', 'CREATE')");
        // PUBLIC holds CONNECT and TEMPORARY on a database, but never CREATE.
        assertAnswer("t", "SELECT has_database_privilege('pit_r', current_database(), 'CONNECT')");
        assertAnswer("f", "SELECT has_database_privilege('pit_r', current_database(), 'CREATE')");
    }

    @Test
    void aNullArgumentMakesTheAnswerUnknown() throws Exception {
        assertNull(scalar("SELECT has_table_privilege(NULL, 'SELECT')"));
        assertNull(scalar("SELECT has_table_privilege('pit_pv', NULL)"));
        assertNull(scalar("SELECT has_table_privilege(NULL, 'pit_pv', 'SELECT')"));
        assertNull(scalar("SELECT has_column_privilege('pit_pv', NULL, 'SELECT')"));
        assertNull(scalar("SELECT has_any_column_privilege(NULL, 'SELECT')"));
        assertNull(scalar("SELECT has_schema_privilege(NULL, 'USAGE')"));
        assertNull(scalar("SELECT has_database_privilege(NULL, 'CONNECT')"));
        assertNull(scalar("SELECT has_sequence_privilege(NULL, 'USAGE')"));
        assertNull(scalar("SELECT has_function_privilege(NULL, 'EXECUTE')"));
        assertNull(scalar("SELECT pg_has_role(NULL, 'USAGE')"));
    }

    @Test
    void anAttnumOutsideTheTableIsUnknownRatherThanAnError() throws Exception {
        assertAnswer("t", "SELECT has_column_privilege('pit_pv', 1::int2, 'SELECT')");
        assertAnswer("t", "SELECT has_column_privilege('pit_pv', 2::int2, 'SELECT')");
        // Negative attnums are system columns, whose privileges follow the table's.
        assertAnswer("t", "SELECT has_column_privilege('pit_pv', (-1)::int2, 'SELECT')");
        assertNull(scalar("SELECT has_column_privilege('pit_pv', 0::int2, 'SELECT')"));
        assertNull(scalar("SELECT has_column_privilege('pit_pv', 99::int2, 'SELECT')"));
    }

    // ---- GRANT validation ----

    @Test
    void aPrivilegeNoColumnCanCarryIsRejected() throws Exception {
        exec("CREATE TABLE pit_gt (i int)");
        assertFails("0LP01", "invalid privilege type DELETE for column",
                "GRANT DELETE (i) ON pit_gt TO PUBLIC");
        assertFails("0LP01", "invalid privilege type TRUNCATE for column",
                "GRANT TRUNCATE (i) ON pit_gt TO PUBLIC");
        assertFails("0LP01", "invalid privilege type TRIGGER for column",
                "GRANT TRIGGER (i) ON pit_gt TO PUBLIC");
        assertFails("0LP01", "invalid privilege type MAINTAIN for column",
                "GRANT MAINTAIN (i) ON pit_gt TO PUBLIC");
        assertFails("0LP01", "invalid privilege type USAGE for column",
                "GRANT USAGE (i) ON pit_gt TO PUBLIC");
        // The ones a column can carry still go through.
        exec("GRANT SELECT (i), INSERT (i), UPDATE (i), REFERENCES (i) ON pit_gt TO PUBLIC");
    }

    @Test
    void aNameThatIsNoPrivilegeAtAllIsASyntaxError() throws Exception {
        exec("CREATE TABLE pit_gu (i int)");
        assertFails("42601", "unrecognized privilege type \"nosuchpriv\"",
                "GRANT NOSUCHPRIV ON pit_gu TO PUBLIC");
        assertFails("42601", "unrecognized privilege type \"nosuchpriv\"",
                "GRANT NOSUCHPRIV (i) ON pit_gu TO PUBLIC");
        // USAGE is a sequence right, so a relation rejects it as a table privilege;
        // the rest never reach the relation-specific check.
        assertFails("0LP01", "invalid privilege type USAGE for table",
                "GRANT USAGE ON pit_gu TO PUBLIC");
        assertFails("0LP01", "invalid privilege type EXECUTE for relation",
                "GRANT EXECUTE ON pit_gu TO PUBLIC");
        assertFails("0LP01", "invalid privilege type CONNECT for relation",
                "GRANT CONNECT ON pit_gu TO PUBLIC");
    }

    @Test
    void aGrantOptionCannotBeHandedToPublic() throws Exception {
        exec("CREATE TABLE pit_gv (i int)");
        assertFails("0LP01", "grant options can only be granted to roles",
                "GRANT SELECT ON pit_gv TO PUBLIC WITH GRANT OPTION");
        assertFails("0LP01", "grant options can only be granted to roles",
                "GRANT SELECT ON pit_gv TO pit_r, PUBLIC WITH GRANT OPTION");
        // Naming only real roles is still fine.
        exec("GRANT SELECT ON pit_gv TO pit_r WITH GRANT OPTION");
        assertAnswer("t",
                "SELECT has_table_privilege('pit_r', 'pit_gv', 'SELECT WITH GRANT OPTION')");
    }

    @Test
    void alterDefaultPrivilegesResolvesItsSchemaAndRole() {
        assertFails("3F000", "schema \"pit_nosuch\" does not exist",
                "ALTER DEFAULT PRIVILEGES IN SCHEMA pit_nosuch GRANT SELECT ON TABLES TO PUBLIC");
        assertFails("3F000", "schema \"pit_nosuch\" does not exist",
                "ALTER DEFAULT PRIVILEGES IN SCHEMA pit_nosuch REVOKE SELECT ON TABLES FROM PUBLIC");
        assertFails("42704", "role \"pit_nosuchrole\" does not exist",
                "ALTER DEFAULT PRIVILEGES FOR ROLE pit_nosuchrole IN SCHEMA public"
                        + " GRANT SELECT ON TABLES TO PUBLIC");
    }

    @Test
    void aGrantToPublicReachesEveryRole() throws Exception {
        exec("CREATE TABLE pit_gp (i int, j text)");
        assertAnswer("f", "SELECT has_table_privilege('pit_r', 'pit_gp', 'SELECT')");
        exec("GRANT SELECT (i) ON pit_gp TO PUBLIC");
        assertAnswer("t", "SELECT has_column_privilege('pit_r', 'pit_gp', 'i', 'SELECT')");
        assertAnswer("f", "SELECT has_column_privilege('pit_r', 'pit_gp', 'j', 'SELECT')");
        assertAnswer("t", "SELECT has_any_column_privilege('pit_r', 'pit_gp', 'SELECT')");
        assertAnswer("f", "SELECT has_table_privilege('pit_r', 'pit_gp', 'SELECT')");
        exec("GRANT ALL ON pit_gp TO PUBLIC");
        assertAnswer("t", "SELECT has_table_privilege('pit_r', 'pit_gp', 'DELETE')");
        assertAnswer("t", "SELECT has_table_privilege('pit_r', 'pit_gp', 'SELECT, INSERT')");
    }
}
