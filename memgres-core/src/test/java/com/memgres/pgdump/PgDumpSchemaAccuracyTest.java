package com.memgres.pgdump;

import com.memgres.engine.util.IO;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 4: Schema Reconstruction Accuracy.
 *
 * Restores the reference pg_dump into a fresh Memgres instance, then verifies
 * the reconstructed schema matches the original by querying pg_get_* catalog
 * functions, pg_constraint, pg_indexes, pg_description, pg_sequence, etc.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PgDumpSchemaAccuracyTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        // Restore the reference dump
        String rawSql = IO.readString(findFixture("pgdump-fixtures/reference-dump.sql"), StandardCharsets.UTF_8);
        restoreDump(conn, rawSql);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static Path findFixture(String name) {
        Path p = Paths.get("src/test/resources", name);
        if (Files.exists(p)) return p;
        p = Paths.get("memgres-core/src/test/resources", name);
        if (Files.exists(p)) return p;
        throw new RuntimeException("Fixture not found: " + name);
    }

    static String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static List<String> queryColumn(String sql) throws SQLException {
        List<String> results = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) results.add(rs.getString(1));
        }
        return results;
    }

    static List<Map<String, String>> queryRows(String sql) throws SQLException {
        List<Map<String, String>> rows = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            while (rs.next()) {
                Map<String, String> row = new LinkedHashMap<>();
                for (int c = 1; c <= cols; c++) {
                    row.put(md.getColumnName(c).toLowerCase(), rs.getString(c));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    // === Dump restore engine ===

    static void restoreDump(Connection c, String rawSql) throws Exception {
        CopyManager cm = new CopyManager(c.unwrap(BaseConnection.class));
        rawSql = rawSql.replace("\r\n", "\n").replace("\r", "\n");
        String[] lines = rawSql.split("\n", -1);
        StringBuilder currentStmt = new StringBuilder();
        int errors = 0;

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (currentStmt.isEmpty() && (line.trim().startsWith("--") || line.trim().isEmpty())) continue;

            currentStmt.append(line).append("\n");
            String soFar = currentStmt.toString().trim();

            // COPY FROM stdin block
            if (soFar.toUpperCase().startsWith("COPY ") && soFar.endsWith(";")
                    && soFar.toUpperCase().contains("FROM STDIN")) {
                String copyCmd = soFar.substring(0, soFar.length() - 1);
                StringBuilder copyData = new StringBuilder();
                i++;
                while (i < lines.length) {
                    if (lines[i].equals("\\.")) break;
                    copyData.append(lines[i]).append("\n");
                    i++;
                }
                try {
                    cm.copyIn(copyCmd, new StringReader(copyData.toString()));
                } catch (Exception e) {
                    errors++;
                    System.err.println("COPY error: " + e.getMessage());
                }
                currentStmt.setLength(0);
                continue;
            }

            // Regular statement terminated by ;
            if (isStatementComplete(soFar)) {
                try (Statement s = c.createStatement()) {
                    s.execute(soFar);
                } catch (Exception e) {
                    errors++;
                    System.err.println("SQL error: " + e.getMessage() + " => " +
                            soFar.substring(0, Math.min(80, soFar.length())));
                }
                currentStmt.setLength(0);
            }
        }
        if (errors > 0) System.err.println("Restore completed with " + errors + " errors");
    }

    static boolean isStatementComplete(String sql) {
        if (!sql.endsWith(";")) return false;
        // Don't split inside dollar-quoted strings
        int depth = 0;
        boolean inSingleQuote = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' && !inSingleQuote) { inSingleQuote = true; }
            else if (c == '\'' && inSingleQuote) {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') { i++; }
                else { inSingleQuote = false; }
            }
            if (c == '$' && !inSingleQuote) {
                int j = i + 1;
                while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j++;
                if (j < sql.length() && sql.charAt(j) == '$') {
                    String tag = sql.substring(i, j + 1);
                    int end = sql.indexOf(tag, j + 1);
                    if (end < 0) return false; // unclosed dollar-quote
                    i = end + tag.length() - 1;
                }
            }
        }
        return !inSingleQuote;
    }

    // === Test: Tables exist with correct columns ===

    @Test @Order(1)
    void tablesExist() throws SQLException {
        List<String> tables = queryColumn(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' ORDER BY tablename");
        assertTrue(tables.contains("customers"), "customers table missing");
        assertTrue(tables.contains("orders"), "orders table missing");
        assertTrue(tables.contains("order_items"), "order_items table missing");
    }

    // === Test: Column types match original ===

    @Test @Order(2)
    void columnTypes_matchOriginal() throws SQLException {
        // customers table
        List<Map<String, String>> cols = queryRows("""
            SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) as type
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relname = 'customers'
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum""");

        Map<String, String> typeMap = new LinkedHashMap<>();
        for (var row : cols) typeMap.put(row.get("attname"), row.get("type"));

        assertEquals("integer", typeMap.get("id"));
        assertEquals("text", typeMap.get("name"));
        assertEquals("character varying(255)", typeMap.get("email"));
        assertEquals("timestamp with time zone", typeMap.get("created_at"));
        assertEquals("jsonb", typeMap.get("metadata"));
        assertEquals("text[]", typeMap.get("tags"));
        assertEquals("boolean", typeMap.get("active"));
        assertEquals("numeric(5,2)", typeMap.get("score"));
        assertEquals("uuid", typeMap.get("uid"));

        // orders table
        cols = queryRows("""
            SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) as type
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relname = 'orders'
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum""");

        typeMap.clear();
        for (var row : cols) typeMap.put(row.get("attname"), row.get("type"));

        assertEquals("bigint", typeMap.get("id"));
        assertEquals("integer", typeMap.get("customer_id"));
        assertEquals("order_status", typeMap.get("status"));
        assertEquals("numeric(10,2)", typeMap.get("total"));
        assertEquals("text", typeMap.get("notes"));
        assertEquals("timestamp with time zone", typeMap.get("placed_at"));
        assertEquals("timestamp with time zone", typeMap.get("shipped_at"));
    }

    // === Test: Column defaults match original ===

    @Test @Order(3)
    void columnDefaults_matchOriginal() throws SQLException {
        List<Map<String, String>> defaults = queryRows("""
            SELECT a.attname,
                   pg_catalog.pg_get_expr(d.adbin, d.adrelid) as default_expr
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
            WHERE n.nspname = 'public' AND c.relname = 'customers'
              AND a.attnum > 0 AND NOT a.attisdropped AND d.adbin IS NOT NULL
            ORDER BY a.attnum""");

        Map<String, String> defMap = new LinkedHashMap<>();
        for (var row : defaults) defMap.put(row.get("attname"), row.get("default_expr"));

        // id has nextval default
        assertNotNull(defMap.get("id"), "id should have a default");
        assertTrue(defMap.get("id").contains("nextval"), "id default should use nextval");

        // created_at defaults to now(), stored as now() or CURRENT_TIMESTAMP
        assertNotNull(defMap.get("created_at"), "created_at should have a default");
        String createdAtDef = defMap.get("created_at").toLowerCase();
        assertTrue(createdAtDef.contains("now") || createdAtDef.contains("current_timestamp"),
                "created_at default should reference now() or CURRENT_TIMESTAMP, got: " + createdAtDef);

        // metadata defaults to '{}'::jsonb
        assertNotNull(defMap.get("metadata"), "metadata should have a default");

        // active defaults to true
        assertNotNull(defMap.get("active"), "active should have a default");
        assertTrue(defMap.get("active").toLowerCase().contains("true"),
                "active default should be true");
    }

    // === Test: Constraint definitions match ===

    @Test @Order(4)
    void constraintDefinitions_matchOriginal() throws SQLException {
        // Primary keys
        List<Map<String, String>> constraints = queryRows("""
            SELECT conname, pg_catalog.pg_get_constraintdef(con.oid, true) as condef, contype::text
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON con.conrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public'
            ORDER BY c.relname, conname""");

        Map<String, String> conMap = new LinkedHashMap<>();
        Map<String, String> conTypeMap = new LinkedHashMap<>();
        for (var row : constraints) {
            conMap.put(row.get("conname"), row.get("condef"));
            conTypeMap.put(row.get("conname"), row.get("contype"));
        }

        // Primary keys
        assertEquals("p", conTypeMap.get("customers_pkey"), "customers_pkey should be PK");
        assertTrue(conMap.get("customers_pkey").toUpperCase().contains("PRIMARY KEY"),
                "customers_pkey def should contain PRIMARY KEY");

        assertEquals("p", conTypeMap.get("orders_pkey"), "orders_pkey should be PK");
        assertEquals("p", conTypeMap.get("order_items_pkey"), "order_items_pkey should be PK");

        // Unique constraint
        assertEquals("u", conTypeMap.get("customers_email_key"), "customers_email_key should be UNIQUE");
        assertTrue(conMap.get("customers_email_key").toUpperCase().contains("UNIQUE"),
                "customers_email_key should define UNIQUE");

        // Foreign keys
        assertEquals("f", conTypeMap.get("orders_customer_id_fkey"), "orders FK type");
        String ordersFk = conMap.get("orders_customer_id_fkey").toUpperCase();
        assertTrue(ordersFk.contains("REFERENCES"), "orders FK should reference customers");

        assertEquals("f", conTypeMap.get("order_items_order_id_fkey"), "order_items FK type");
        String itemsFk = conMap.get("order_items_order_id_fkey").toUpperCase();
        assertTrue(itemsFk.contains("ON DELETE CASCADE"), "order_items FK should CASCADE");
    }

    // === Test: Index definitions match ===

    @Test @Order(5)
    void indexDefinitions_matchOriginal() throws SQLException {
        List<Map<String, String>> indexes = queryRows("""
            SELECT c2.relname as indexname,
                   pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) as indexdef
            FROM pg_catalog.pg_index i
            JOIN pg_catalog.pg_class c ON i.indrelid = c.oid
            JOIN pg_catalog.pg_class c2 ON c2.oid = i.indexrelid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND NOT i.indisprimary AND NOT i.indisunique
            ORDER BY c2.relname""");

        Map<String, String> idxMap = new LinkedHashMap<>();
        for (var row : indexes) idxMap.put(row.get("indexname"), row.get("indexdef"));

        // Reference dump creates 3 explicit indexes
        assertNotNull(idxMap.get("idx_customers_email"), "idx_customers_email missing");
        assertTrue(idxMap.get("idx_customers_email").toLowerCase().contains("email"),
                "idx_customers_email should index email column");

        assertNotNull(idxMap.get("idx_orders_customer_id"), "idx_orders_customer_id missing");
        assertTrue(idxMap.get("idx_orders_customer_id").toLowerCase().contains("customer_id"),
                "idx_orders_customer_id should index customer_id");

        assertNotNull(idxMap.get("idx_orders_status"), "idx_orders_status missing");
        assertTrue(idxMap.get("idx_orders_status").toLowerCase().contains("status"),
                "idx_orders_status should index status");
    }

    // === Test: View definitions match ===

    @Test @Order(6)
    void viewDefinitions_matchOriginal() throws SQLException {
        // The reference dump creates a customer_summary view
        String viewDef = query1("""
            SELECT pg_catalog.pg_get_viewdef(c.oid, true)
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relname = 'customer_summary'""");

        assertNotNull(viewDef, "customer_summary view not found");
        String upper = viewDef.toUpperCase();
        // The view should reference customers and orders with a LEFT JOIN
        assertTrue(upper.contains("CUSTOMERS"), "view should reference customers");
        assertTrue(upper.contains("ORDERS") || upper.contains("LEFT JOIN"),
                "view should reference orders via join");
        assertTrue(upper.contains("COUNT") || upper.contains("SUM"),
                "view should contain aggregation");
        assertTrue(upper.contains("GROUP BY"), "view should GROUP BY");
    }

    // === Test: Sequence parameters match ===

    @Test @Order(7)
    void sequenceParameters_matchOriginal() throws SQLException {
        // Check sequences via pg_sequence
        List<Map<String, String>> seqs = queryRows("""
            SELECT c.relname, s.seqstart, s.seqincrement, s.seqmax, s.seqmin, s.seqcache, s.seqcycle
            FROM pg_catalog.pg_sequence s
            JOIN pg_catalog.pg_class c ON s.seqrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public'
            ORDER BY c.relname""");

        // Reference dump has 3 sequences
        assertTrue(seqs.size() >= 3, "Expected at least 3 sequences, got " + seqs.size());

        Map<String, Map<String, String>> seqMap = new LinkedHashMap<>();
        for (var row : seqs) seqMap.put(row.get("relname"), row);

        // customers_id_seq
        assertNotNull(seqMap.get("customers_id_seq"), "customers_id_seq missing");
        assertEquals("1", seqMap.get("customers_id_seq").get("seqincrement"));

        // orders_id_seq
        assertNotNull(seqMap.get("orders_id_seq"), "orders_id_seq missing");
        assertEquals("1", seqMap.get("orders_id_seq").get("seqincrement"));

        // order_items_id_seq
        assertNotNull(seqMap.get("order_items_id_seq"), "order_items_id_seq missing");
        assertEquals("1", seqMap.get("order_items_id_seq").get("seqincrement"));

        // Verify setval was applied by calling nextval and checking the result
        // After setval(seq, 5, true), nextval should return 6
        String nextCustomer = query1("SELECT nextval('public.customers_id_seq')");
        assertEquals("6", nextCustomer, "customers_id_seq nextval should be 6 after setval(5, true)");

        String nextOrder = query1("SELECT nextval('public.orders_id_seq')");
        assertEquals("6", nextOrder, "orders_id_seq nextval should be 6 after setval(5, true)");

        String nextItem = query1("SELECT nextval('public.order_items_id_seq')");
        assertEquals("8", nextItem, "order_items_id_seq nextval should be 8 after setval(7, true)");
    }

    // === Test: Enum type preserved ===

    @Test @Order(8)
    void enumType_preserved() throws SQLException {
        // Verify order_status enum exists with correct labels
        List<String> labels = queryColumn("""
            SELECT e.enumlabel
            FROM pg_catalog.pg_enum e
            JOIN pg_catalog.pg_type t ON e.enumtypid = t.oid
            WHERE t.typname = 'order_status'
            ORDER BY e.enumsortorder""");

        assertEquals(Cols.listOf("pending", "confirmed", "shipped", "delivered", "cancelled"), labels);
    }

    // === Test: Table comments preserved ===

    @Test @Order(9)
    void tableComments_preserved() throws SQLException {
        // The reference dump has COMMENT ON TABLE and COMMENT ON COLUMN
        String customersComment = query1("""
            SELECT description FROM pg_catalog.pg_description d
            JOIN pg_catalog.pg_class c ON d.objoid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relname = 'customers'
              AND d.objsubid = 0""");

        assertEquals("Core customer records", customersComment, "customers table comment");

        String ordersComment = query1("""
            SELECT description FROM pg_catalog.pg_description d
            JOIN pg_catalog.pg_class c ON d.objoid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relname = 'orders'
              AND d.objsubid = 0""");

        assertEquals("Customer orders with status tracking", ordersComment, "orders table comment");
    }

    // === Test: Column comments preserved ===

    @Test @Order(10)
    void columnComments_preserved() throws SQLException {
        // COMMENT ON COLUMN customers.metadata
        String metadataComment = query1("""
            SELECT description FROM pg_catalog.pg_description d
            JOIN pg_catalog.pg_class c ON d.objoid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid AND a.attnum = d.objsubid)
            WHERE n.nspname = 'public' AND c.relname = 'customers' AND a.attname = 'metadata'""");

        assertEquals("Arbitrary JSON metadata for extensibility", metadataComment, "metadata column comment");

        // COMMENT ON COLUMN customers.tags
        String tagsComment = query1("""
            SELECT description FROM pg_catalog.pg_description d
            JOIN pg_catalog.pg_class c ON d.objoid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid AND a.attnum = d.objsubid)
            WHERE n.nspname = 'public' AND c.relname = 'customers' AND a.attname = 'tags'""");

        assertEquals("Freeform tags for categorization", tagsComment, "tags column comment");
    }

    // === Test: Index comments preserved ===

    @Test @Order(11)
    void indexComments_preserved() throws SQLException {
        String indexComment = query1("""
            SELECT description FROM pg_catalog.pg_description d
            JOIN pg_catalog.pg_class c ON d.objoid = c.oid
            WHERE c.relname = 'idx_customers_email'
              AND d.objsubid = 0""");

        assertEquals("Fast email lookups", indexComment, "index comment");
    }

    // === Test: Data integrity after restore ===

    @Test @Order(12)
    void dataIntegrity_afterRestore() throws SQLException {
        // Verify row counts
        assertEquals("5", query1("SELECT count(*) FROM customers"), "customers count");
        assertEquals("5", query1("SELECT count(*) FROM orders"), "orders count");
        assertEquals("7", query1("SELECT count(*) FROM order_items"), "order_items count");

        // Verify specific values survived COPY
        List<Map<String, String>> alice = queryRows(
            "SELECT name, email, active, score, uid FROM customers WHERE id = 1");
        assertEquals(1, alice.size());
        assertEquals("Alice Johnson", alice.get(0).get("name"));
        assertEquals("alice@example.com", alice.get(0).get("email"));
        assertEquals("t", alice.get(0).get("active"));
        assertEquals("98.50", alice.get(0).get("score"));
        assertEquals("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", alice.get(0).get("uid"));

        // Verify special characters in customer 4
        List<Map<String, String>> dave = queryRows(
            "SELECT name, metadata FROM customers WHERE id = 4");
        assertEquals(1, dave.size());
        assertEquals("Dave \"The Dev\" O'Brien", dave.get(0).get("name"));

        // Verify NULL values in customer 3
        List<Map<String, String>> carol = queryRows(
            "SELECT tags, score, uid FROM customers WHERE id = 3");
        assertEquals(1, carol.size());
        assertNull(carol.get(0).get("tags"));
        assertNull(carol.get(0).get("score"));
        assertNull(carol.get(0).get("uid"));

        // Verify empty name in customer 5
        assertEquals("", query1("SELECT name FROM customers WHERE id = 5"));

        // Verify enum value
        assertEquals("confirmed", query1("SELECT status FROM orders WHERE id = 1"));
        assertEquals("cancelled", query1("SELECT status FROM orders WHERE id = 5"));
    }

    // === Test: Foreign key actions work ===

    @Test @Order(13)
    void foreignKeyActions_work() throws SQLException {
        // ON DELETE CASCADE: deleting an order should cascade to order_items
        int itemsBefore = Integer.parseInt(query1(
            "SELECT count(*) FROM order_items WHERE order_id = 1"));
        assertTrue(itemsBefore > 0, "order 1 should have items");

        try (Statement s = conn.createStatement()) {
            s.execute("DELETE FROM orders WHERE id = 1");
        }
        assertEquals("0", query1("SELECT count(*) FROM order_items WHERE order_id = 1"),
                "CASCADE should have deleted items for order 1");

        // ON DELETE RESTRICT: can't delete customer with remaining orders
        try (Statement s = conn.createStatement()) {
            assertThrows(SQLException.class, () ->
                s.execute("DELETE FROM customers WHERE id = 2"),
                "RESTRICT should prevent deleting customer with orders");
        }
    }

    // === Test: View is queryable ===

    @Test @Order(14)
    void view_isQueryable() throws SQLException {
        List<Map<String, String>> summary = queryRows(
            "SELECT * FROM customer_summary ORDER BY id");
        assertFalse(summary.isEmpty(), "customer_summary should return rows");

        // Check column names exist
        Map<String, String> first = summary.get(0);
        assertNotNull(first.get("id"));
        assertNotNull(first.get("name"));
        assertNotNull(first.get("email"));
        assertNotNull(first.get("order_count"));
        assertNotNull(first.get("total_spent"));
    }

    // === Test: OWNER TO was applied ===

    @Test @Order(15)
    void ownerTo_applied() throws SQLException {
        // The reference dump sets OWNER TO memgres
        String owner = query1("""
            SELECT pg_catalog.pg_get_userbyid(c.relowner)
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relname = 'customers'""");

        assertEquals("memgres", owner, "customers table should be owned by memgres");
    }

    // === Test: NOT NULL constraints preserved ===

    @Test @Order(16)
    void notNullConstraints_preserved() throws SQLException {
        List<Map<String, String>> cols = queryRows("""
            SELECT a.attname, a.attnotnull
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relname = 'customers'
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum""");

        Map<String, String> notNullMap = new LinkedHashMap<>();
        for (var row : cols) notNullMap.put(row.get("attname"), row.get("attnotnull"));

        assertEquals("t", notNullMap.get("id"), "id should be NOT NULL");
        assertEquals("t", notNullMap.get("name"), "name should be NOT NULL");
        assertEquals("t", notNullMap.get("email"), "email should be NOT NULL");
        assertEquals("t", notNullMap.get("created_at"), "created_at should be NOT NULL");
        assertEquals("f", notNullMap.get("score"), "score should be nullable");
        assertEquals("f", notNullMap.get("uid"), "uid should be nullable");
    }

    // === Test: Sequence OWNED BY preserved ===

    @Test @Order(17)
    void sequenceOwnedBy_preserved() throws SQLException {
        // After restoring ALTER SEQUENCE ... OWNED BY, nextval should auto-increment
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO customers (name, email) VALUES ('New Customer', 'new@test.com')");
        }
        String newId = query1("SELECT id FROM customers WHERE email = 'new@test.com'");
        assertNotNull(newId);
        int id = Integer.parseInt(newId);
        assertTrue(id > 5, "Auto-incremented id should be > 5 (setval was 5)");
    }
}
