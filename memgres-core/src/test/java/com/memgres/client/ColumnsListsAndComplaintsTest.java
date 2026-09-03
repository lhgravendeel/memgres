package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What the catalogue says its own columns are, what a list of options reads as, and how a
 * complaint names a string.
 *
 * <p>A reader asks pg_attribute what a catalogue column is and binds by the answer, so an OID
 * column has to say oid and a list of ACL items has to say aclitem[].
 *
 * <p>An array handed to a function is a list of values, not the text a list prints as: read as one
 * string, the first option kept the bracket the printing put there.
 *
 * <p>And a string constant is named in a complaint with the quotes that made it one.
 */
class ColumnsListsAndComplaintsTest {

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

    /** The catalogue names its own columns by their types. */
    @Test
    void whatTheDefaultPrivilegeColumnsAre() throws SQLException {
        assertEquals(java.util.Arrays.asList(
                        "defaclacl|aclitem[]", "defaclnamespace|oid",
                        "defaclobjtype|\"char\"", "defaclrole|oid"),
                rows("SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute"
                        + " WHERE attrelid='pg_default_acl'::regclass"
                        + " AND attname IN ('defaclacl','defaclnamespace','defaclobjtype',"
                        + "'defaclrole') ORDER BY attname"));
    }

    /** An array of options is read as the values it holds. */
    @Test
    void aListOfOptionsReadApart() throws SQLException {
        assertEquals(java.util.Arrays.asList("autovacuum_enabled|false", "fillfactor|70"),
                rows("SELECT option_name, option_value FROM pg_options_to_table("
                        + "ARRAY['fillfactor=70','autovacuum_enabled=false']) ORDER BY 1"));
    }

    /** A string constant is named with the quotes that made it one. */
    @Test
    void howAComplaintNamesAString() {
        assertTrue(messageOf("SET work_mem '4MB' rubbish")
                .contains("syntax error at or near \"'4MB'\""), messageOf("SET work_mem '4MB' x"));
        assertTrue(messageOf("SET datestyle 'ISO' junk here")
                .contains("syntax error at or near \"'ISO'\""));
    }

    /** Default privileges are about relations to come, so no column of one may be named. */
    @Test
    void defaultPrivilegesOverColumns() throws SQLException {
        exec("CREATE SCHEMA zcl_s");
        exec("CREATE ROLE zcl_a");
        assertEquals("0LP01", stateOf("ALTER DEFAULT PRIVILEGES IN SCHEMA zcl_s"
                + " GRANT SELECT (a) ON TABLES TO zcl_a"));
        assertTrue(messageOf("ALTER DEFAULT PRIVILEGES IN SCHEMA zcl_s"
                + " GRANT SELECT (a) ON TABLES TO zcl_a")
                .contains("default privileges cannot be set for columns"));
        // Without a column list it is the statement PostgreSQL has.
        assertNull(stateOf("ALTER DEFAULT PRIVILEGES IN SCHEMA zcl_s"
                + " GRANT SELECT ON TABLES TO zcl_a"));
        exec("DROP SCHEMA zcl_s");
        exec("DROP ROLE zcl_a");
    }
}
