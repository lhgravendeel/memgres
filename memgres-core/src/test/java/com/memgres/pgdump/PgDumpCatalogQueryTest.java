package com.memgres.pgdump;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 2: pg_dump Catalog Query Compatibility.
 *
 * Runs the exact catalog queries that pg_dump 18 issues against memgres
 * and verifies they return reasonable results. Each test isolates one
 * category of pg_dump introspection so failures pinpoint the exact gap.
 *
 * Uses the same reference schema as PgDumpFromMemgresTest (Tier 1).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PgDumpCatalogQueryTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        PgDumpFromMemgresTest.populateReferenceSchema(conn);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // === Helper ===

    ResultSet query(String sql) throws SQLException {
        return conn.createStatement().executeQuery(sql);
    }

    String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    int count(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    Set<String> columnSet(String sql) throws SQLException {
        Set<String> result = new LinkedHashSet<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) result.add(rs.getString(1));
        }
        return result;
    }

    // === 1. Server version ===

    @Test @Order(1)
    void serverVersionQuery() throws Exception {
        // pg_dump calls SELECT version() and SHOW server_version_num
        String version = scalar("SELECT version()");
        assertNotNull(version);
        assertTrue(version.contains("PostgreSQL"), "version() should contain 'PostgreSQL'");

        String versionNum = scalar("SHOW server_version_num");
        assertNotNull(versionNum);
        int num = Integer.parseInt(versionNum);
        assertTrue(num >= 100000, "server_version_num should be >= 100000 (PG 10+), got " + num);
    }

    // === 2. Database info ===

    @Test @Order(2)
    void databaseInfoQuery() throws Exception {
        // pg_dump queries pg_database for encoding, collation, etc.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT d.datname, d.encoding, pg_catalog.pg_encoding_to_char(d.encoding) " +
                     "FROM pg_catalog.pg_database d WHERE d.datname = current_database()")) {
            assertTrue(rs.next(), "Should find current database in pg_database");
            assertEquals("memgres", rs.getString("datname"));
            String encoding = rs.getString(3);
            assertNotNull(encoding, "pg_encoding_to_char should return encoding name");
        }
    }

    // === 3. Schema enumeration ===

    @Test @Order(3)
    void schemaEnumeration() throws Exception {
        // pg_dump enumerates non-system schemas
        Set<String> schemas = columnSet(
                "SELECT n.nspname FROM pg_catalog.pg_namespace n " +
                "WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' " +
                "ORDER BY 1");
        assertTrue(schemas.contains("public"), "Should find 'public' schema");

        // All namespaces should have valid OIDs and tableoid
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT tableoid, oid, nspname, nspowner FROM pg_catalog.pg_namespace ORDER BY oid")) {
            int rows = 0;
            while (rs.next()) {
                int tableoid = rs.getInt("tableoid");
                int oid = rs.getInt("oid");
                String name = rs.getString("nspname");
                assertTrue(tableoid > 0, "tableoid should be > 0 for " + name);
                assertTrue(oid > 0, "oid should be > 0 for " + name);
                rows++;
            }
            assertTrue(rows >= 3, "Should have at least 3 namespaces (pg_catalog, public, information_schema)");
        }
    }

    // === 4. Extension enumeration ===

    @Test @Order(4)
    void extensionEnumeration() throws Exception {
        // pg_dump queries extensions joined with namespaces
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT x.tableoid, x.oid, x.extname, n.nspname, x.extrelocatable, x.extversion " +
                     "FROM pg_catalog.pg_extension x " +
                     "JOIN pg_catalog.pg_namespace n ON n.oid = x.extnamespace")) {
            boolean foundPlpgsql = false;
            while (rs.next()) {
                int tableoid = rs.getInt("tableoid");
                assertTrue(tableoid > 0, "extension tableoid should be > 0");
                if ("plpgsql".equals(rs.getString("extname"))) {
                    foundPlpgsql = true;
                    assertEquals("pg_catalog", rs.getString("nspname"));
                }
            }
            assertTrue(foundPlpgsql, "Should find plpgsql extension");
        }
    }

    // === 5. Table enumeration ===

    @Test @Order(5)
    void tableEnumeration() throws Exception {
        // pg_dump enumerates tables, sequences, views in user schemas
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT c.oid, c.relname, c.relkind, c.relnamespace, n.nspname " +
                     "FROM pg_catalog.pg_class c " +
                     "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
                     "WHERE c.relkind IN ('r','p','S','v','m','f') " +
                     "AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') " +
                     "ORDER BY c.relname")) {
            Set<String> tables = new LinkedHashSet<>();
            Set<String> sequences = new LinkedHashSet<>();
            Set<String> views = new LinkedHashSet<>();
            while (rs.next()) {
                String name = rs.getString("relname");
                String kind = rs.getString("relkind");
                int oid = rs.getInt("oid");
                assertTrue(oid > 0, "OID should be > 0 for " + name);
                switch (kind) {
                    case "r" -> tables.add(name);
                    case "S" -> sequences.add(name);
                    case "v" -> views.add(name);
                }
            }
            assertTrue(tables.containsAll(Cols.setOf("customers", "orders", "order_items")),
                    "Should find all user tables, got: " + tables);
            assertTrue(sequences.size() >= 3,
                    "Should find at least 3 sequences (serial columns), got: " + sequences);
            assertTrue(views.contains("customer_summary"),
                    "Should find customer_summary view, got: " + views);
        }
    }

    // === 6. Column details ===

    @Test @Order(6)
    void columnDetails() throws Exception {
        // pg_dump queries pg_attribute + pg_attrdef for each table
        int customersOid = Integer.parseInt(scalar(
                "SELECT oid FROM pg_class WHERE relname = 'customers'"));
        assertTrue(customersOid > 0);

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a.attname, a.atttypid, a.attlen, a.attnum, a.attnotnull, " +
                     "       pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_name, " +
                     "       a.attidentity, a.attgenerated, a.atthasdef, a.attisdropped " +
                     "FROM pg_catalog.pg_attribute a " +
                     "WHERE a.attrelid = " + customersOid +
                     "  AND a.attnum > 0 AND NOT a.attisdropped " +
                     "ORDER BY a.attnum")) {
            Map<String, String> colTypes = new LinkedHashMap<>();
            while (rs.next()) {
                colTypes.put(rs.getString("attname"), rs.getString("type_name"));
                assertFalse(rs.getBoolean("attisdropped"));
            }
            assertTrue(colTypes.containsKey("id"), "Should have 'id' column");
            assertTrue(colTypes.containsKey("name"), "Should have 'name' column");
            assertTrue(colTypes.containsKey("email"), "Should have 'email' column");
            assertTrue(colTypes.containsKey("metadata"), "Should have 'metadata' column");
            assertTrue(colTypes.containsKey("uid"), "Should have 'uid' column");

            // Verify type names are PG-compatible format
            assertTrue(colTypes.get("email").contains("character varying"),
                    "email type should be 'character varying', got: " + colTypes.get("email"));
            assertEquals("uuid", colTypes.get("uid"),
                    "uid type should be 'uuid'");
        }
    }

    // === 7. Constraint definitions ===

    @Test @Order(7)
    void constraintDefinitions() throws Exception {
        // pg_dump queries pg_constraint for PK, FK, UNIQUE, CHECK
        int customersOid = Integer.parseInt(scalar(
                "SELECT oid FROM pg_class WHERE relname = 'customers'"));

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT c.conname, c.contype, " +
                     "       pg_catalog.pg_get_constraintdef(c.oid, true) AS condef " +
                     "FROM pg_catalog.pg_constraint c " +
                     "WHERE c.conrelid = " + customersOid +
                     " ORDER BY c.conname")) {
            Map<String, String> constraints = new LinkedHashMap<>();
            while (rs.next()) {
                constraints.put(rs.getString("conname"), rs.getString("contype"));
                String def = rs.getString("condef");
                assertNotNull(def, "Constraint def should not be null for " + rs.getString("conname"));
            }
            // Should have PK and UNIQUE
            assertTrue(constraints.values().contains("p"), "Should have PRIMARY KEY constraint");
            assertTrue(constraints.values().contains("u"), "Should have UNIQUE constraint");
        }

        // FK on orders table
        int ordersOid = Integer.parseInt(scalar(
                "SELECT oid FROM pg_class WHERE relname = 'orders'"));
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT c.conname, c.contype, " +
                     "       pg_catalog.pg_get_constraintdef(c.oid, true) AS condef " +
                     "FROM pg_catalog.pg_constraint c " +
                     "WHERE c.conrelid = " + ordersOid +
                     "  AND c.contype = 'f'")) {
            assertTrue(rs.next(), "orders should have a FK constraint");
            String def = rs.getString("condef");
            assertTrue(def.toLowerCase().contains("references"),
                    "FK def should contain REFERENCES, got: " + def);
        }
    }

    // === 8. Index definitions ===

    @Test @Order(8)
    void indexDefinitions() throws Exception {
        // pg_dump queries pg_index + pg_class for each table's indexes
        int customersOid = Integer.parseInt(scalar(
                "SELECT oid FROM pg_class WHERE relname = 'customers'"));

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT c2.relname AS indexname, " +
                     "       pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) AS indexdef " +
                     "FROM pg_catalog.pg_index i " +
                     "JOIN pg_catalog.pg_class c2 ON c2.oid = i.indexrelid " +
                     "WHERE i.indrelid = " + customersOid +
                     " ORDER BY c2.relname")) {
            Set<String> indexNames = new LinkedHashSet<>();
            while (rs.next()) {
                String name = rs.getString("indexname");
                String def = rs.getString("indexdef");
                assertNotNull(def, "Index def should not be null for " + name);
                assertTrue(def.toUpperCase().contains("INDEX") || def.toUpperCase().contains("UNIQUE"),
                        "Index def should contain INDEX or UNIQUE: " + def);
                indexNames.add(name);
            }
            assertTrue(indexNames.contains("idx_customers_email"),
                    "Should find idx_customers_email, got: " + indexNames);
        }
    }

    // === 9. View definitions ===

    @Test @Order(9)
    void viewDefinitions() throws Exception {
        // pg_dump uses pg_get_viewdef to reconstruct CREATE VIEW
        String viewOid = scalar(
                "SELECT oid FROM pg_class WHERE relname = 'customer_summary' AND relkind = 'v'");
        assertNotNull(viewOid, "customer_summary view should exist in pg_class");

        String viewdef = scalar("SELECT pg_catalog.pg_get_viewdef(" + viewOid + "::pg_catalog.oid)");
        assertNotNull(viewdef, "pg_get_viewdef should return view definition");
        assertTrue(viewdef.toLowerCase().contains("select"),
                "View def should contain SELECT: " + viewdef);
        assertTrue(viewdef.toLowerCase().contains("customers") || viewdef.toLowerCase().contains("customer"),
                "View def should reference customers table");
    }

    // === 10. Sequence details ===

    @Test @Order(10)
    void sequenceDetails() throws Exception {
        // pg_dump queries pg_sequence for each sequence's properties
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT seqrelid, format_type(seqtypid, NULL) AS seqtype, " +
                     "       seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle " +
                     "FROM pg_catalog.pg_sequence ORDER BY seqrelid")) {
            int seqCount = 0;
            while (rs.next()) {
                seqCount++;
                long start = rs.getLong("seqstart");
                long inc = rs.getLong("seqincrement");
                assertTrue(inc != 0, "Sequence increment should not be 0");
                assertNotNull(rs.getString("seqtype"), "Sequence type should not be null");
            }
            assertTrue(seqCount >= 3, "Should have at least 3 sequences, got " + seqCount);
        }
    }

    // === 11. Comment retrieval ===

    @Test @Order(11)
    void commentRetrieval() throws Exception {
        // pg_dump queries pg_description for comments on all objects
        // Table comment
        int customersOid = Integer.parseInt(scalar(
                "SELECT oid FROM pg_class WHERE relname = 'customers'"));
        String tableComment = scalar(
                "SELECT description FROM pg_catalog.pg_description " +
                "WHERE objoid = " + customersOid +
                " AND classoid = 'pg_class'::regclass AND objsubid = 0");
        assertEquals("Core customer records", tableComment,
                "customers table comment should match");

        // Column comment (objsubid = column attnum)
        String colComment = scalar(
                "SELECT description FROM pg_catalog.pg_description " +
                "WHERE objoid = " + customersOid +
                " AND classoid = 'pg_class'::regclass AND objsubid = 5");
        assertEquals("Arbitrary JSON metadata for extensibility", colComment,
                "metadata column comment should match");

        // Bulk comment query (pg_dump's actual pattern)
        int commentCount = count(
                "SELECT count(*) FROM pg_catalog.pg_description " +
                "WHERE description IS NOT NULL");
        assertTrue(commentCount >= 3, "Should have at least 3 comments (2 table + 1 column + 1 index)");
    }

    // === 12. Ownership ===

    @Test @Order(12)
    void ownershipQuery() throws Exception {
        // pg_dump queries relowner for OWNER TO statements
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT c.relname, pg_catalog.pg_get_userbyid(c.relowner) AS owner " +
                     "FROM pg_catalog.pg_class c " +
                     "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
                     "WHERE n.nspname = 'public' AND c.relkind IN ('r','v','S') " +
                     "ORDER BY c.relname")) {
            int count = 0;
            while (rs.next()) {
                String owner = rs.getString("owner");
                assertNotNull(owner, "Owner should not be null for " + rs.getString("relname"));
                assertEquals("memgres", owner, "Owner should be 'memgres'");
                count++;
            }
            assertTrue(count >= 6, "Should have at least 6 user objects (3 tables + 3 sequences + 1 view)");
        }
    }

    // === 13. Dependency graph ===

    @Test @Order(13)
    void dependencyGraph() throws Exception {
        // pg_dump uses pg_depend to determine dump ordering
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT classid, objid, refclassid, refobjid, deptype " +
                     "FROM pg_catalog.pg_depend " +
                     "WHERE deptype != 'p' AND deptype != 'e' " +
                     "ORDER BY 1, 2")) {
            // Should return without error (even if empty for simple schemas)
            int count = 0;
            while (rs.next()) {
                count++;
                assertNotNull(rs.getString("deptype"));
            }
            // The query itself should succeed; empty is OK for simple schemas
        }
    }

    // === 14. Enum type queries ===

    @Test @Order(14)
    void enumTypeQueries() throws Exception {
        // pg_dump uses pg_type + pg_enum for enum types
        String typeOid = scalar(
                "SELECT t.oid FROM pg_catalog.pg_type t " +
                "JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace " +
                "WHERE t.typname = 'order_status' AND n.nspname = 'public'");
        assertNotNull(typeOid, "order_status type should exist in pg_type");

        // pg_dump's exact enum label query uses PREPARE
        conn.createStatement().execute(
                "PREPARE dumpEnumType(pg_catalog.oid) AS " +
                "SELECT oid, enumlabel FROM pg_catalog.pg_enum " +
                "WHERE enumtypid = $1 ORDER BY enumsortorder");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE dumpEnumType('" + typeOid + "')")) {
            List<String> labels = new ArrayList<>();
            while (rs.next()) {
                labels.add(rs.getString("enumlabel"));
            }
            assertEquals(Cols.listOf("pending", "confirmed", "shipped", "delivered", "cancelled"),
                    labels, "Enum labels should match in order");
        }

        conn.createStatement().execute("DEALLOCATE dumpEnumType");
    }

    // === 15. pg_dump SET preamble ===

    @Test @Order(15)
    void pgDumpPreambleStatements() throws Exception {
        // pg_dump sends many SET commands at the start of a session.
        // All must succeed (no errors) even if they are no-ops for Memgres.
        String[] preamble = {
            "SET statement_timeout = 0",
            "SET lock_timeout = 0",
            "SET idle_in_transaction_session_timeout = 0",
            "SET transaction_timeout = 0",
            "SET client_encoding = 'UTF8'",
            "SET standard_conforming_strings = on",
            "SET check_function_bodies = false",
            "SET xmloption = content",
            "SET client_min_messages = warning",
            "SET row_security = off",
            "SET synchronize_seqscans TO off",
            "SET extra_float_digits TO 3",
            "SET DATESTYLE = ISO",
            "SET INTERVALSTYLE = POSTGRES",
            "SET default_table_access_method = heap",
            "SET default_tablespace = ''"
        };
        try (Statement s = conn.createStatement()) {
            for (String sql : preamble) {
                assertDoesNotThrow(() -> s.execute(sql),
                        "SET preamble should not throw: " + sql);
            }
        }
    }

    // === 16. pg_catalog function calls ===

    @Test @Order(16)
    void catalogFunctions() throws Exception {
        // pg_dump uses various catalog functions throughout its queries
        // pg_is_in_recovery()
        String recovery = scalar("SELECT pg_catalog.pg_is_in_recovery()");
        assertNotNull(recovery);

        // set_config
        String result = scalar(
                "SELECT pg_catalog.set_config('search_path', 'public', false)");
        assertEquals("public", result);

        // current_database
        String db = scalar("SELECT current_database()");
        assertEquals("memgres", db);

        // current_schemas
        String schemas = scalar("SELECT pg_catalog.current_schemas(false)");
        assertNotNull(schemas);

        // has_schema_privilege
        String priv = scalar(
                "SELECT pg_catalog.has_schema_privilege('public', 'USAGE')");
        assertNotNull(priv);

        // pg_get_userbyid
        String user = scalar("SELECT pg_catalog.pg_get_userbyid(10)");
        assertEquals("memgres", user, "OID 10 should be the bootstrap superuser");

        // acldefault
        assertDoesNotThrow(() -> scalar("SELECT pg_catalog.acldefault('n', 10)"));

        // format_type
        String typeName = scalar("SELECT pg_catalog.format_type(23, -1)");
        assertEquals("integer", typeName);
    }

    // === 17. Regclass casts ===

    @Test @Order(17)
    void regclassCasts() throws Exception {
        // pg_dump uses 'tablename'::regclass extensively
        String oid1 = scalar("SELECT 'pg_class'::regclass::integer");
        assertNotNull(oid1, "'pg_class'::regclass should resolve to an OID");
        assertTrue(Integer.parseInt(oid1) > 0);

        String oid2 = scalar("SELECT 'pg_namespace'::regclass::integer");
        assertNotNull(oid2);
        assertEquals("2615", oid2, "pg_namespace OID should be 2615");

        String oid3 = scalar("SELECT 'pg_extension'::regclass::integer");
        assertNotNull(oid3);
        assertEquals("3079", oid3, "pg_extension OID should be 3079");
    }

    // === 18. pg_class full columns ===

    @Test @Order(18)
    void pgClassFullColumns() throws Exception {
        // pg_dump queries many pg_class columns; verify they all exist
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT c.tableoid, c.oid, c.relname, c.relnamespace, c.relkind, " +
                     "       c.relowner, c.relchecks, c.relhasindex, c.relhasrules, " +
                     "       c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, " +
                     "       c.relfrozenxid, c.relminmxid, c.relacl, c.relpersistence, " +
                     "       c.relreplident, c.relpages, c.reltuples, c.relallvisible, " +
                     "       c.relhasoids, c.relispartition, c.relallfrozen " +
                     "FROM pg_catalog.pg_class c " +
                     "WHERE c.relname = 'customers'")) {
            assertTrue(rs.next(), "Should find customers in pg_class");
            assertEquals("customers", rs.getString("relname"));
            assertEquals("r", rs.getString("relkind"));
            assertTrue(rs.getInt("tableoid") > 0, "tableoid must be > 0");
            assertTrue(rs.getInt("oid") > 0, "oid must be > 0");
        }
    }

    // === 19. pg_attribute full columns ===

    @Test @Order(19)
    void pgAttributeFullColumns() throws Exception {
        // pg_dump queries many pg_attribute columns
        int customersOid = Integer.parseInt(scalar(
                "SELECT oid FROM pg_class WHERE relname = 'customers'"));

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a.attrelid, a.attname, a.atttypid, a.attnum, " +
                     "       a.attnotnull, a.atttypmod, a.attlen, a.attisdropped, " +
                     "       a.atthasdef, a.attidentity, a.attgenerated, a.attcollation, " +
                     "       a.attislocal, a.attinhcount, a.attacl, a.attoptions, " +
                     "       a.attstattarget, a.attstorage, a.attcompression, " +
                     "       a.atthasmissing, a.attmissingval, a.attalign " +
                     "FROM pg_catalog.pg_attribute a " +
                     "WHERE a.attrelid = " + customersOid +
                     "  AND a.attnum > 0 AND NOT a.attisdropped " +
                     "ORDER BY a.attnum")) {
            int colCount = 0;
            while (rs.next()) {
                colCount++;
                assertTrue(rs.getInt("attnum") > 0);
                assertNotNull(rs.getString("attname"));
                assertNotNull(rs.getString("attstorage"));
                assertNotNull(rs.getString("attalign"));
            }
            assertEquals(9, colCount, "customers should have 9 columns");
        }
    }

    // === 20. pg_constraint full columns ===

    @Test @Order(20)
    void pgConstraintFullColumns() throws Exception {
        // pg_dump queries pg_constraint with many columns including conkey/confkey
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT c.tableoid, c.oid, c.conname, c.connamespace, c.contype, " +
                     "       c.conrelid, c.confrelid, c.conkey, c.confkey, " +
                     "       c.condeferrable, c.condeferred, c.convalidated, " +
                     "       c.conislocal, c.conindid, c.confupdtype, c.confdeltype, " +
                     "       c.connoinherit, c.conbin, c.conperiod, c.conparentid " +
                     "FROM pg_catalog.pg_constraint c " +
                     "WHERE c.contype = 'f' " +
                     "  AND c.conparentid = 0 " +
                     "ORDER BY c.conrelid, c.conname")) {
            int fkCount = 0;
            while (rs.next()) {
                fkCount++;
                assertNotNull(rs.getString("conname"));
                assertEquals("f", rs.getString("contype"));
                assertTrue(rs.getInt("conrelid") > 0);
                assertTrue(rs.getInt("confrelid") > 0);
            }
            assertEquals(3, fkCount, "Should have 3 FK constraints (orders + order_items + tenant_sessions)");
        }
    }

    // === 21. pg_index full columns ===

    @Test @Order(21)
    void pgIndexFullColumns() throws Exception {
        // pg_dump queries pg_index for each table
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT i.indexrelid, i.indrelid, i.indnatts, i.indisunique, " +
                     "       i.indisprimary, i.indimmediate, i.indisclustered, " +
                     "       i.indisvalid, i.indisready, i.indislive, i.indisreplident, " +
                     "       i.indkey, i.indcollation, i.indclass, " +
                     "       pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) AS indexdef " +
                     "FROM pg_catalog.pg_index i " +
                     "JOIN pg_catalog.pg_class c ON c.oid = i.indrelid " +
                     "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
                     "WHERE n.nspname = 'public' " +
                     "ORDER BY i.indexrelid")) {
            int indexCount = 0;
            while (rs.next()) {
                indexCount++;
                assertTrue(rs.getInt("indexrelid") > 0);
                assertNotNull(rs.getString("indexdef"));
            }
            // 3 PK indexes + 3 explicit indexes + 1 UNIQUE = ~7
            assertTrue(indexCount >= 6, "Should have at least 6 indexes, got " + indexCount);
        }
    }

    // === 22. Trigger / rewrite / policy queries ===

    @Test @Order(22)
    void triggerRewritePolicyQueries() throws Exception {
        // These return empty for our schema but must not error
        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_trigger t " +
                "WHERE NOT t.tgisinternal"),
                "pg_trigger query should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_rewrite"),
                "pg_rewrite query should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_policy"),
                "pg_policy query should not error");
    }

    // === 23. Publication / subscription queries ===

    @Test @Order(23)
    void publicationSubscriptionQueries() throws Exception {
        // pg_dump queries these even if empty
        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_publication"),
                "pg_publication query should not error");

        assertDoesNotThrow(() -> count(
                "SELECT tableoid, oid, pnpubid, pnnspid " +
                "FROM pg_catalog.pg_publication_namespace"),
                "pg_publication_namespace query should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_subscription " +
                "WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())"),
                "pg_subscription query should not error");
    }

    // === 24. init_privs / seclabels / shdepend queries ===

    @Test @Order(24)
    void initPrivsAndSecurityQueries() throws Exception {
        // pg_dump queries these catalog tables for security info
        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_init_privs"),
                "pg_init_privs query should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_seclabels"),
                "pg_seclabels query should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_shdepend"),
                "pg_shdepend query should not error");

        assertDoesNotThrow(() -> count(
                "SELECT DISTINCT attrelid FROM pg_attribute WHERE attacl IS NOT NULL"),
                "Column ACL query should not error");
    }

    // === 25. Text search catalog queries ===

    @Test @Order(25)
    void textSearchCatalogQueries() throws Exception {
        // pg_dump queries text search catalogs
        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_ts_parser"),
                "pg_ts_parser should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_ts_dict"),
                "pg_ts_dict should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_ts_template"),
                "pg_ts_template should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_ts_config"),
                "pg_ts_config should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_ts_config_map"),
                "pg_ts_config_map should not error");
    }

    // === 26. pg_dump's complex pg_depend UNION query ===

    @Test @Order(26)
    void complexDependUnionQuery() throws Exception {
        // pg_dump issues a 3-way UNION ALL on pg_depend + pg_amop + pg_amproc
        assertDoesNotThrow(() -> {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                    "SELECT classid, objid, refclassid, refobjid, deptype " +
                    "FROM pg_depend WHERE deptype != 'p' AND deptype != 'e' " +
                    "UNION ALL " +
                    "SELECT 'pg_opfamily'::regclass AS classid, amopfamily AS objid, " +
                    "       refclassid, refobjid, deptype " +
                    "FROM pg_depend d, pg_amop o " +
                    "WHERE deptype NOT IN ('p', 'e', 'i') " +
                    "  AND classid = 'pg_amop'::regclass AND objid = o.oid " +
                    "  AND NOT (refclassid = 'pg_opfamily'::regclass AND amopfamily = refobjid) " +
                    "UNION ALL " +
                    "SELECT 'pg_opfamily'::regclass AS classid, amprocfamily AS objid, " +
                    "       refclassid, refobjid, deptype " +
                    "FROM pg_depend d, pg_amproc p " +
                    "WHERE deptype NOT IN ('p', 'e', 'i') " +
                    "  AND classid = 'pg_amproc'::regclass AND objid = p.oid " +
                    "  AND NOT (refclassid = 'pg_opfamily'::regclass AND amprocfamily = refobjid) " +
                    "ORDER BY 1, 2")) {
                // Just verify it runs without error
                while (rs.next()) { /* consume */ }
            }
        }, "Complex UNION ALL pg_depend query should not error");
    }

    // === 27. Sequence data query (PG 18) ===

    @Test @Order(27)
    void sequenceDataQuery() throws Exception {
        // PG 18 pg_dump uses pg_get_sequence_data SRF for data-only dumps
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT seqrelid, format_type(seqtypid, NULL), " +
                     "       seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle, " +
                     "       last_value, is_called " +
                     "FROM pg_catalog.pg_sequence, pg_get_sequence_data(seqrelid) " +
                     "ORDER BY seqrelid")) {
            int seqCount = 0;
            while (rs.next()) {
                seqCount++;
                assertNotNull(rs.getString(2), "format_type should not be null");
                // last_value and is_called come from the SRF
                rs.getLong("last_value"); // should not throw
                rs.getBoolean("is_called"); // should not throw
            }
            assertTrue(seqCount >= 3, "Should have at least 3 sequences, got " + seqCount);
        }
    }

    // === 28. Roles query ===

    @Test @Order(28)
    void rolesQuery() throws Exception {
        // pg_dump queries pg_roles for OWNER TO statements
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT oid, rolname, rolsuper, rolinherit, rolcreaterole, " +
                     "       rolcreatedb, rolcanlogin, rolreplication, rolbypassrls " +
                     "FROM pg_catalog.pg_roles ORDER BY oid")) {
            boolean foundMemgres = false;
            while (rs.next()) {
                if ("memgres".equals(rs.getString("rolname"))) {
                    foundMemgres = true;
                    assertEquals(10, rs.getInt("oid"),
                            "memgres role should have OID 10 (bootstrap superuser)");
                    assertTrue(rs.getBoolean("rolsuper"), "memgres should be superuser");
                }
            }
            assertTrue(foundMemgres, "Should find 'memgres' role");
        }
    }

    // === 29. Tablespace query ===

    @Test @Order(29)
    void tablespaceQuery() throws Exception {
        // pg_dump queries pg_tablespace
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT oid, spcname, pg_catalog.pg_get_userbyid(spcowner) AS owner " +
                     "FROM pg_catalog.pg_tablespace")) {
            boolean foundDefault = false;
            while (rs.next()) {
                if ("pg_default".equals(rs.getString("spcname"))) foundDefault = true;
            }
            assertTrue(foundDefault, "Should find pg_default tablespace");
        }
    }

    // === 30. Collation query ===

    @Test @Order(30)
    void collationQuery() throws Exception {
        // pg_dump queries pg_collation
        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_collation"),
                "pg_collation query should not error");
    }

    // === 31. Foreign data wrapper / server queries ===

    @Test @Order(31)
    void foreignDataWrapperQueries() throws Exception {
        // pg_dump queries these even if no FDWs exist
        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_foreign_data_wrapper"),
                "pg_foreign_data_wrapper should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_foreign_server"),
                "pg_foreign_server should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_user_mapping"),
                "pg_user_mapping should not error");
    }

    // === 32. Language / cast / operator queries ===

    @Test @Order(32)
    void languageCastOperatorQueries() throws Exception {
        // pg_dump queries these for extensions and procedural languages
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT oid, lanname, lanowner, lanispl " +
                     "FROM pg_catalog.pg_language ORDER BY oid")) {
            boolean foundPlpgsql = false;
            while (rs.next()) {
                if ("plpgsql".equals(rs.getString("lanname"))) foundPlpgsql = true;
            }
            assertTrue(foundPlpgsql, "Should find plpgsql language");
        }

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_cast"),
                "pg_cast query should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_operator"),
                "pg_operator query should not error");
    }

    // === 33. Event trigger / AM / auth_members queries ===

    @Test @Order(33)
    void miscCatalogQueries() throws Exception {
        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_event_trigger"),
                "pg_event_trigger should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_am"),
                "pg_am should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_auth_members"),
                "pg_auth_members should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_default_acl"),
                "pg_default_acl should not error");
    }

    // === 34. pg_inherits / pg_partitioned_table queries ===

    @Test @Order(34)
    void inheritancePartitioningQueries() throws Exception {
        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_inherits"),
                "pg_inherits should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_partitioned_table"),
                "pg_partitioned_table should not error");
    }

    // === 35. Conversion / transform / statistic_ext queries ===

    @Test @Order(35)
    void advancedCatalogQueries() throws Exception {
        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_conversion"),
                "pg_conversion should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_transform"),
                "pg_transform should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_statistic_ext"),
                "pg_statistic_ext should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_range"),
                "pg_range should not error");

        assertDoesNotThrow(() -> count(
                "SELECT count(*) FROM pg_catalog.pg_largeobject_metadata"),
                "pg_largeobject_metadata should not error");
    }
}
