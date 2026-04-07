package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the exact queries a DB tool sends when inspecting table columns.
 * The error is: "column seq.seqtypid does not exist" because pg_sequence table
 * is missing the seqtypid column.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DbToolColumnIntrospectionTest {

    static Memgres memgres;
    static Connection conn;
    static String tableOid;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE test_cols(id serial PRIMARY KEY, name text NOT NULL, val int DEFAULT 42)");
            // Get the OID of the table for subsequent queries
            try (ResultSet rs = s.executeQuery("SELECT 'test_cols'::regclass::oid")) {
                rs.next();
                tableOid = rs.getString(1);
            }
        }
    }
    @AfterAll static void tearDown() throws Exception {
        if (conn != null) { try (Statement s = conn.createStatement()) { s.execute("DROP TABLE test_cols"); } conn.close(); }
        if (memgres != null) memgres.close();
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData(); int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) { List<String> row = new ArrayList<>(); for (int i = 1; i <= cols; i++) row.add(rs.getString(i)); rows.add(row); }
            return rows;
        }
    }

    // Query 1: Schema+table lookup by OID
    @Test @Order(1) void schema_table_lookup_by_oid() throws SQLException {
        List<List<String>> rows = query("""
            SELECT nsp.nspname AS schema, rel.relname AS table
            FROM pg_catalog.pg_class rel
                JOIN pg_catalog.pg_namespace nsp ON rel.relnamespace = nsp.oid::oid
            WHERE rel.oid = """ + tableOid + "::oid");
        assertEquals(1, rows.size());
        assertEquals("test_cols", rows.get(0).get(1));
    }

    // Query 2: The actual column introspection (this is the one that fails)
    @Test @Order(2) void column_introspection_with_pg_sequence() throws SQLException {
        List<List<String>> rows = query("""
            SELECT DISTINCT att.attname as name, att.attnum as OID, pg_catalog.format_type(ty.oid,NULL) AS datatype,
            att.attnotnull as not_null,
            CASE WHEN att.atthasdef OR att.attidentity != '' OR ty.typdefault IS NOT NULL THEN True
            ELSE False END as has_default_val, des.description, seq.seqtypid
            FROM pg_catalog.pg_attribute att
                JOIN pg_catalog.pg_type ty ON ty.oid=atttypid
                JOIN pg_catalog.pg_namespace tn ON tn.oid=ty.typnamespace
                JOIN pg_catalog.pg_class cl ON cl.oid=att.attrelid
                JOIN pg_catalog.pg_namespace na ON na.oid=cl.relnamespace
                LEFT OUTER JOIN pg_catalog.pg_type et ON et.oid=ty.typelem
                LEFT OUTER JOIN pg_catalog.pg_attrdef def ON adrelid=att.attrelid AND adnum=att.attnum
                LEFT OUTER JOIN (pg_catalog.pg_depend JOIN pg_catalog.pg_class cs ON classid='pg_class'::regclass AND objid=cs.oid AND cs.relkind='S') ON refobjid=att.attrelid AND refobjsubid=att.attnum
                LEFT OUTER JOIN pg_catalog.pg_namespace ns ON ns.oid=cs.relnamespace
                LEFT OUTER JOIN pg_catalog.pg_index pi ON pi.indrelid=att.attrelid AND indisprimary
                LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=att.attrelid AND des.objsubid=att.attnum AND des.classoid='pg_class'::regclass)
                LEFT OUTER JOIN pg_catalog.pg_sequence seq ON cs.oid=seq.seqrelid
            WHERE
                att.attrelid = """ + tableOid + """
            ::oid
                AND att.attnum > 0
                AND att.attisdropped IS FALSE
            ORDER BY att.attnum
            """);
        // Should return columns: id, name, val
        assertTrue(rows.size() >= 3, "Should return at least 3 columns, got " + rows.size());
    }

    // Minimal test: pg_sequence must have seqtypid column
    @Test @Order(3) void pg_sequence_has_seqtypid_column() throws SQLException {
        // Just verify the column exists, even if the table is empty
        List<List<String>> rows = query("SELECT seqtypid FROM pg_catalog.pg_sequence LIMIT 0");
        assertNotNull(rows);
    }

    // Minimal test: pg_sequence has seqrelid column
    @Test @Order(4) void pg_sequence_has_seqrelid_column() throws SQLException {
        List<List<String>> rows = query("SELECT seqrelid FROM pg_catalog.pg_sequence LIMIT 0");
        assertNotNull(rows);
    }

    // pg_depend must have classid, objid, refobjid, refobjsubid columns
    @Test @Order(5) void pg_depend_has_required_columns() throws SQLException {
        query("SELECT classid, objid, refobjid, refobjsubid FROM pg_catalog.pg_depend LIMIT 0");
    }

    // pg_attribute must have atthasdef and attidentity columns
    @Test @Order(6) void pg_attribute_has_atthasdef_and_attidentity() throws SQLException {
        List<List<String>> rows = query("""
            SELECT attname, atthasdef, attidentity
            FROM pg_catalog.pg_attribute
            WHERE attrelid = """ + tableOid + "::oid AND attnum > 0 AND NOT attisdropped ORDER BY attnum");
        assertTrue(rows.size() >= 3);
        // id column (serial) should have atthasdef=true
        assertEquals("t", rows.get(0).get(1), "id (serial) should have atthasdef=true");
    }
}
