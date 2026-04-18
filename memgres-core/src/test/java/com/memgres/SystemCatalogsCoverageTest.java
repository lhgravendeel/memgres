package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage tests for pg18-coverage-checklist items 111-113:
 * System Catalogs, Information Schema, and Catalog Functions.
 */
class SystemCatalogsCoverageTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );

        try (Statement stmt = conn.createStatement()) {
            // Set up a rich schema for catalog testing
            stmt.execute("CREATE SCHEMA myschema");
            stmt.execute("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE, active BOOLEAN DEFAULT true)");
            stmt.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users(id) ON DELETE CASCADE, amount NUMERIC(10,2), status TEXT DEFAULT 'pending')");
            stmt.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price NUMERIC CHECK (price >= 0))");
            stmt.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com'), ('Bob', 'bob@test.com')");
            stmt.execute("INSERT INTO orders (user_id, amount) VALUES (1, 99.99)");
            stmt.execute("CREATE SEQUENCE order_seq START WITH 100 INCREMENT BY 1");
            stmt.execute("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = true");
            stmt.execute("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')");
            stmt.execute("CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0)");
            stmt.execute("CREATE OR REPLACE FUNCTION add_nums(a INTEGER, b INTEGER) RETURNS INTEGER AS $$ BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE ROLE app_user LOGIN PASSWORD 'secret'");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String query1(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getInt(1) : -1;
        }
    }

    private boolean queryHasRows(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next();
        }
    }

    // ==================== pg_catalog tables ====================

    // ---- pg_class ----

    @Test
    void testPgClassContainsTables() throws SQLException {
        assertTrue(queryHasRows("SELECT oid, relname FROM pg_class WHERE relname = 'users'"));
    }

    @Test
    void testPgClassRelkindTable() throws SQLException {
        String relkind = query1("SELECT relkind FROM pg_class WHERE relname = 'users'");
        assertEquals("r", relkind);
    }

    @Test
    void testPgClassRelkindView() throws SQLException {
        String relkind = query1("SELECT relkind FROM pg_class WHERE relname = 'active_users'");
        assertEquals("v", relkind);
    }

    @Test
    void testPgClassRelkindSequence() throws SQLException {
        String relkind = query1("SELECT relkind FROM pg_class WHERE relname = 'order_seq'");
        assertEquals("S", relkind);
    }

    @Test
    void testPgClassOidIsPositive() throws SQLException {
        int oid = queryInt("SELECT oid FROM pg_class WHERE relname = 'users'");
        assertTrue(oid > 0);
    }

    @Test
    void testPgClassHasNamespace() throws SQLException {
        int ns = queryInt("SELECT relnamespace FROM pg_class WHERE relname = 'users'");
        assertTrue(ns > 0);
    }

    @Test
    void testPgClassRelowner() throws SQLException {
        int owner = queryInt("SELECT relowner FROM pg_class WHERE relname = 'users'");
        assertTrue(owner > 0);
    }

    @Test
    void testPgClassRelhasindex() throws SQLException {
        assertNotNull(query1("SELECT relhasindex FROM pg_class WHERE relname = 'users'"));
    }

    @Test
    void testPgClassRelpersistence() throws SQLException {
        String rp = query1("SELECT relpersistence FROM pg_class WHERE relname = 'users'");
        assertEquals("p", rp);
    }

    // ---- pg_attribute ----

    @Test
    void testPgAttributeColumns() throws SQLException {
        assertTrue(queryHasRows("SELECT attname FROM pg_attribute WHERE attname = 'name'"));
    }

    @Test
    void testPgAttributeAttnum() throws SQLException {
        int attnum = queryInt("SELECT attnum FROM pg_attribute WHERE attname = 'id' LIMIT 1");
        assertTrue(attnum >= 1);
    }

    @Test
    void testPgAttributeAtttypid() throws SQLException {
        int typid = queryInt("SELECT atttypid FROM pg_attribute WHERE attname = 'name' LIMIT 1");
        assertTrue(typid > 0);
    }

    @Test
    void testPgAttributeAttnotnull() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT attnotnull FROM pg_attribute WHERE attname = 'name' LIMIT 1")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void testPgAttributeAtthasdef() throws SQLException {
        // 'active' column has DEFAULT true
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT atthasdef FROM pg_attribute WHERE attname = 'active' LIMIT 1")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // ---- pg_type ----

    @Test
    void testPgTypeContainsBuiltins() throws SQLException {
        assertTrue(queryHasRows("SELECT oid, typname FROM pg_type WHERE typname = 'int4'"));
    }

    @Test
    void testPgTypeContainsText() throws SQLException {
        assertTrue(queryHasRows("SELECT oid FROM pg_type WHERE typname = 'text'"));
    }

    @Test
    void testPgTypeContainsBool() throws SQLException {
        assertTrue(queryHasRows("SELECT oid FROM pg_type WHERE typname = 'bool'"));
    }

    @Test
    void testPgTypeTyplen() throws SQLException {
        int typlen = queryInt("SELECT typlen FROM pg_type WHERE typname = 'int4'");
        assertEquals(4, typlen);
    }

    @Test
    void testPgTypeTyptype() throws SQLException {
        String typtype = query1("SELECT typtype FROM pg_type WHERE typname = 'int4'");
        assertEquals("b", typtype); // base type
    }

    // ---- pg_namespace ----

    @Test
    void testPgNamespacePublic() throws SQLException {
        assertTrue(queryHasRows("SELECT oid, nspname FROM pg_namespace WHERE nspname = 'public'"));
    }

    @Test
    void testPgNamespaceCustom() throws SQLException {
        assertTrue(queryHasRows("SELECT oid FROM pg_namespace WHERE nspname = 'myschema'"));
    }

    @Test
    void testPgNamespacePgCatalog() throws SQLException {
        assertTrue(queryHasRows("SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog'"));
    }

    // ---- pg_constraint ----

    @Test
    void testPgConstraintPrimaryKey() throws SQLException {
        assertTrue(queryHasRows("SELECT oid, conname FROM pg_constraint WHERE contype = 'p'"));
    }

    @Test
    void testPgConstraintForeignKey() throws SQLException {
        assertTrue(queryHasRows("SELECT conname FROM pg_constraint WHERE contype = 'f'"));
    }

    @Test
    void testPgConstraintUnique() throws SQLException {
        assertTrue(queryHasRows("SELECT conname FROM pg_constraint WHERE contype = 'u'"));
    }

    @Test
    void testPgConstraintCheck() throws SQLException {
        assertTrue(queryHasRows("SELECT conname FROM pg_constraint WHERE contype = 'c'"));
    }

    @Test
    void testPgConstraintConrelid() throws SQLException {
        int relid = queryInt("SELECT conrelid FROM pg_constraint LIMIT 1");
        assertTrue(relid > 0);
    }

    // ---- pg_index ----

    @Test
    void testPgIndexExists() throws SQLException {
        // pg_index should exist even if empty
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM pg_index LIMIT 1")) {
            // Just check it doesn't throw
            assertNotNull(rs.getMetaData());
        }
    }

    // ---- pg_proc ----

    @Test
    void testPgProcContainsFunction() throws SQLException {
        assertTrue(queryHasRows("SELECT oid, proname FROM pg_proc WHERE proname = 'add_nums'"));
    }

    @Test
    void testPgProcPronamespace() throws SQLException {
        int ns = queryInt("SELECT pronamespace FROM pg_proc WHERE proname = 'add_nums'");
        assertTrue(ns > 0);
    }

    // ---- pg_description ----

    @Test
    void testPgDescriptionAccessible() throws SQLException {
        // Should be queryable even if empty
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT objoid, classoid, description FROM pg_description LIMIT 1")) {
            assertNotNull(rs.getMetaData());
        }
    }

    // ---- pg_settings ----

    @Test
    void testPgSettingsServerVersion() throws SQLException {
        String ver = query1("SELECT setting FROM pg_settings WHERE name = 'server_version'");
        assertEquals("18.0", ver);
    }

    @Test
    void testPgSettingsServerVersionNum() throws SQLException {
        String num = query1("SELECT setting FROM pg_settings WHERE name = 'server_version_num'");
        assertEquals("180000", num);
    }

    @Test
    void testPgSettingsServerEncoding() throws SQLException {
        String enc = query1("SELECT setting FROM pg_settings WHERE name = 'server_encoding'");
        assertEquals("UTF8", enc);
    }

    @Test
    void testPgSettingsSearchPath() throws SQLException {
        String sp = query1("SELECT setting FROM pg_settings WHERE name = 'search_path'");
        assertNotNull(sp);
        assertTrue(sp.contains("public"));
    }

    @Test
    void testPgSettingsCategory() throws SQLException {
        String cat = query1("SELECT category FROM pg_settings WHERE name = 'server_version'");
        assertEquals("Preset Options", cat);
    }

    @Test
    void testPgSettingsShortDesc() throws SQLException {
        String desc = query1("SELECT short_desc FROM pg_settings WHERE name = 'server_version'");
        assertNotNull(desc);
        assertFalse(desc.isEmpty());
    }

    @Test
    void testPgSettingsContext() throws SQLException {
        String ctx = query1("SELECT context FROM pg_settings WHERE name = 'server_version'");
        assertEquals("internal", ctx);
    }

    @Test
    void testPgSettingsVartype() throws SQLException {
        String vt = query1("SELECT vartype FROM pg_settings WHERE name = 'server_version'");
        assertEquals("string", vt);
    }

    @Test
    void testPgSettingsHasMultipleRows() throws SQLException {
        int count = queryInt("SELECT COUNT(*) FROM pg_settings");
        assertTrue(count > 10);
    }

    @Test
    void testPgSettingsResetVal() throws SQLException {
        String resetVal = query1("SELECT reset_val FROM pg_settings WHERE name = 'server_version'");
        assertEquals("18.0", resetVal);
    }

    @Test
    void testPgSettingsTimezone() throws SQLException {
        String tz = query1("SELECT setting FROM pg_settings WHERE name = 'timezone'");
        assertEquals("UTC", tz);
    }

    // ---- pg_tables ----

    @Test
    void testPgTablesContainsUsers() throws SQLException {
        assertTrue(queryHasRows("SELECT tablename FROM pg_tables WHERE tablename = 'users'"));
    }

    @Test
    void testPgTablesSchemaname() throws SQLException {
        String schema = query1("SELECT schemaname FROM pg_tables WHERE tablename = 'users'");
        assertEquals("public", schema);
    }

    @Test
    void testPgTablesTableowner() throws SQLException {
        String owner = query1("SELECT tableowner FROM pg_tables WHERE tablename = 'users'");
        assertEquals("memgres", owner);
    }

    // ---- pg_views ----

    @Test
    void testPgViewsContainsView() throws SQLException {
        assertTrue(queryHasRows("SELECT viewname FROM pg_views WHERE viewname = 'active_users'"));
    }

    @Test
    void testPgViewsSchemaname() throws SQLException {
        String schema = query1("SELECT schemaname FROM pg_views WHERE viewname = 'active_users'");
        assertEquals("public", schema);
    }

    // ---- pg_sequences ----

    @Test
    void testPgSequencesContainsSeq() throws SQLException {
        assertTrue(queryHasRows("SELECT sequencename FROM pg_sequences WHERE sequencename = 'order_seq'"));
    }

    @Test
    void testPgSequencesStartValue() throws SQLException {
        String startVal = query1("SELECT start_value FROM pg_sequences WHERE sequencename = 'order_seq'");
        assertEquals("100", startVal);
    }

    @Test
    void testPgSequencesDataType() throws SQLException {
        String dt = query1("SELECT data_type FROM pg_sequences WHERE sequencename = 'order_seq'");
        assertEquals("bigint", dt);
    }

    // ---- pg_am ----

    @Test
    void testPgAmBtree() throws SQLException {
        assertTrue(queryHasRows("SELECT oid, amname FROM pg_am WHERE amname = 'btree'"));
    }

    @Test
    void testPgAmHash() throws SQLException {
        assertTrue(queryHasRows("SELECT amname FROM pg_am WHERE amname = 'hash'"));
    }

    // ---- pg_database ----

    @Test
    void testPgDatabaseExists() throws SQLException {
        assertTrue(queryHasRows("SELECT datname FROM pg_database WHERE datname = 'memgres'"));
    }

    @Test
    void testPgDatabaseEncoding() throws SQLException {
        String enc = query1("SELECT encoding FROM pg_database WHERE datname = 'memgres'");
        assertNotNull(enc);
    }

    // ---- pg_roles / pg_user ----

    @Test
    void testPgRolesContainsMemgres() throws SQLException {
        assertTrue(queryHasRows("SELECT rolname FROM pg_roles WHERE rolname = 'memgres'"));
    }

    @Test
    void testPgRolesContainsCreatedRole() throws SQLException {
        assertTrue(queryHasRows("SELECT rolname FROM pg_roles WHERE rolname = 'app_user'"));
    }

    @Test
    void testPgUserAlias() throws SQLException {
        // pg_user has its own column names (usename, usesuper, etc.)
        assertTrue(queryHasRows("SELECT usename FROM pg_user"));
    }

    // ---- pg_stat_activity ----

    @Test
    void testPgStatActivityAccessible() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM pg_stat_activity LIMIT 1")) {
            assertNotNull(rs.getMetaData());
        }
    }

    // ---- pg_enum ----

    @Test
    void testPgEnumContainsMood() throws SQLException {
        assertTrue(queryHasRows("SELECT enumlabel FROM pg_enum WHERE enumlabel = 'happy'"));
    }

    @Test
    void testPgEnumAllValues() throws SQLException {
        int count = queryInt("SELECT COUNT(*) FROM pg_enum");
        assertTrue(count >= 3); // happy, sad, neutral
    }

    @Test
    void testPgEnumSortOrder() throws SQLException {
        assertTrue(queryHasRows("SELECT enumsortorder FROM pg_enum WHERE enumlabel = 'happy'"));
    }

    // ---- pg_trigger ----

    @Test
    void testPgTriggerAccessible() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT tgname, tgrelid, tgenabled FROM pg_trigger LIMIT 1")) {
            assertNotNull(rs.getMetaData());
        }
    }

    // ---- pg_depend ----

    @Test
    void testPgDependAccessible() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT classid, objid, deptype FROM pg_depend LIMIT 1")) {
            assertNotNull(rs.getMetaData());
        }
    }

    // ---- pg_attrdef ----

    @Test
    void testPgAttrdefContainsDefaults() throws SQLException {
        // The 'active' column has DEFAULT true, SERIAL columns have nextval defaults
        assertTrue(queryHasRows("SELECT oid, adrelid, adsrc FROM pg_attrdef"));
    }

    @Test
    void testPgAttrdefAdnum() throws SQLException {
        int adnum = queryInt("SELECT adnum FROM pg_attrdef LIMIT 1");
        assertTrue(adnum >= 1);
    }

    // ---- pg_locks ----

    @Test
    void testPgLocksAccessible() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT locktype, relation, mode FROM pg_locks LIMIT 1")) {
            assertNotNull(rs.getMetaData());
        }
    }

    // ---- pg_stat_user_tables / pg_stat_all_tables ----

    @Test
    void testPgStatUserTablesHasRows() throws SQLException {
        assertTrue(queryHasRows("SELECT relname, n_live_tup FROM pg_stat_user_tables WHERE relname = 'users'"));
    }

    @Test
    void testPgStatUserTablesRowCount() throws SQLException {
        int count = queryInt("SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 'users'");
        assertEquals(2, count);
    }

    @Test
    void testPgStatUserTablesSchemaname() throws SQLException {
        String schema = query1("SELECT schemaname FROM pg_stat_user_tables WHERE relname = 'users'");
        assertEquals("public", schema);
    }

    @Test
    void testPgStatAllTablesAlias() throws SQLException {
        // pg_stat_all_tables should work as alias
        assertTrue(queryHasRows("SELECT relname FROM pg_stat_all_tables WHERE relname = 'users'"));
    }

    // ---- pg_stat_user_indexes ----

    @Test
    void testPgStatUserIndexesAccessible() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM pg_stat_user_indexes LIMIT 1")) {
            assertNotNull(rs.getMetaData());
        }
    }

    // ---- pg_prepared_xacts ----

    @Test
    void testPgPreparedXactsAccessible() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM pg_prepared_xacts LIMIT 1")) {
            assertNotNull(rs.getMetaData());
        }
    }

    // ---- pg_statio_user_tables ----

    @Test
    void testPgStatioUserTablesAccessible() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM pg_statio_user_tables LIMIT 1")) {
            assertNotNull(rs.getMetaData());
        }
    }

    // ==================== information_schema ====================

    // ---- information_schema.tables ----

    @Test
    void testIsTablesContainsUsers() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'users'"));
    }

    @Test
    void testIsTablesTableSchema() throws SQLException {
        String schema = query1(
                "SELECT table_schema FROM information_schema.tables WHERE table_name = 'users'");
        assertEquals("public", schema);
    }

    @Test
    void testIsTablesTableType() throws SQLException {
        String type = query1(
                "SELECT table_type FROM information_schema.tables WHERE table_name = 'users'");
        assertEquals("BASE TABLE", type);
    }

    @Test
    void testIsTablesMultipleTables() throws SQLException {
        int count = queryInt(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'");
        assertTrue(count >= 3); // users, orders, products
    }

    // ---- information_schema.columns ----

    @Test
    void testIsColumnsContainsUsersColumns() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'name'"));
    }

    @Test
    void testIsColumnsOrdinalPosition() throws SQLException {
        int pos = queryInt(
                "SELECT ordinal_position FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'id'");
        assertEquals(1, pos);
    }

    @Test
    void testIsColumnsDataType() throws SQLException {
        String dt = query1(
                "SELECT data_type FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'name'");
        assertNotNull(dt);
    }

    @Test
    void testIsColumnsIsNullable() throws SQLException {
        String nullable = query1(
                "SELECT is_nullable FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'name'");
        assertEquals("NO", nullable);
    }

    @Test
    void testIsColumnsColumnDefault() throws SQLException {
        String def = query1(
                "SELECT column_default FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'active'");
        assertNotNull(def);
    }

    @Test
    void testIsColumnsCharacterMaximumLength() throws SQLException {
        // Should have character_maximum_length column
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT character_maximum_length FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'name'")) {
            assertTrue(rs.next());
        }
    }

    @Test
    void testIsColumnsNumericPrecision() throws SQLException {
        // Verify the column exists in information_schema.columns
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT numeric_precision FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 'amount'")) {
            assertTrue(rs.next()); // row exists
        }
    }

    // ---- information_schema.schemata ----

    @Test
    void testIsSchemataPublic() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'public'"));
    }

    @Test
    void testIsSchemataCustom() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'myschema'"));
    }

    @Test
    void testIsSchemataOwner() throws SQLException {
        String owner = query1(
                "SELECT schema_owner FROM information_schema.schemata WHERE schema_name = 'public'");
        assertNotNull(owner);
    }

    // ---- information_schema.table_constraints ----

    @Test
    void testIsTableConstraintsPK() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT constraint_name FROM information_schema.table_constraints WHERE constraint_type = 'PRIMARY KEY' AND table_name = 'users'"));
    }

    @Test
    void testIsTableConstraintsFK() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT constraint_name FROM information_schema.table_constraints WHERE constraint_type = 'FOREIGN KEY' AND table_name = 'orders'"));
    }

    @Test
    void testIsTableConstraintsUnique() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT constraint_name FROM information_schema.table_constraints WHERE constraint_type = 'UNIQUE' AND table_name = 'users'"));
    }

    @Test
    void testIsTableConstraintsCheck() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT constraint_name FROM information_schema.table_constraints WHERE constraint_type = 'CHECK'"));
    }

    // ---- information_schema.key_column_usage ----

    @Test
    void testIsKeyColumnUsageExists() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT column_name FROM information_schema.key_column_usage WHERE table_name = 'users'"));
    }

    @Test
    void testIsKeyColumnUsageConstraintName() throws SQLException {
        String cn = query1(
                "SELECT constraint_name FROM information_schema.key_column_usage WHERE table_name = 'users' AND column_name = 'id'");
        assertNotNull(cn);
    }

    // ---- information_schema.referential_constraints ----

    @Test
    void testIsReferentialConstraints() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT constraint_name, unique_constraint_name FROM information_schema.referential_constraints"));
    }

    @Test
    void testIsReferentialConstraintsMatchOption() throws SQLException {
        String match = query1(
                "SELECT match_option FROM information_schema.referential_constraints LIMIT 1");
        assertEquals("NONE", match);
    }

    @Test
    void testIsReferentialConstraintsUpdateRule() throws SQLException {
        String rule = query1(
                "SELECT update_rule FROM information_schema.referential_constraints LIMIT 1");
        assertNotNull(rule);
    }

    // ---- information_schema.views ----

    @Test
    void testIsViewsContainsActiveUsers() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT table_name FROM information_schema.views WHERE table_name = 'active_users'"));
    }

    @Test
    void testIsViewsTableSchema() throws SQLException {
        String schema = query1(
                "SELECT table_schema FROM information_schema.views WHERE table_name = 'active_users'");
        assertEquals("public", schema);
    }

    @Test
    void testIsViewsCheckOption() throws SQLException {
        String co = query1(
                "SELECT check_option FROM information_schema.views WHERE table_name = 'active_users'");
        assertEquals("NONE", co);
    }

    @Test
    void testIsViewsIsUpdatable() throws SQLException {
        String upd = query1(
                "SELECT is_updatable FROM information_schema.views WHERE table_name = 'active_users'");
        assertEquals("YES", upd);
    }

    // ---- information_schema.routines ----

    @Test
    void testIsRoutinesContainsFunction() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT routine_name FROM information_schema.routines WHERE routine_name = 'add_nums'"));
    }

    @Test
    void testIsRoutinesRoutineType() throws SQLException {
        String type = query1(
                "SELECT routine_type FROM information_schema.routines WHERE routine_name = 'add_nums'");
        assertEquals("FUNCTION", type);
    }

    // ---- information_schema.sequences ----

    @Test
    void testIsSequencesContainsSeq() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT sequence_name FROM information_schema.sequences WHERE sequence_name = 'order_seq'"));
    }

    @Test
    void testIsSequencesDataType() throws SQLException {
        String dt = query1(
                "SELECT data_type FROM information_schema.sequences WHERE sequence_name = 'order_seq'");
        assertEquals("bigint", dt);
    }

    @Test
    void testIsSequencesStartValue() throws SQLException {
        String sv = query1(
                "SELECT start_value FROM information_schema.sequences WHERE sequence_name = 'order_seq'");
        assertEquals("100", sv);
    }

    // ---- information_schema.domains ----

    @Test
    void testIsDomainsContainsPositiveInt() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT domain_name FROM information_schema.domains WHERE domain_name = 'positive_int'"));
    }

    @Test
    void testIsDomainsDataType() throws SQLException {
        String dt = query1(
                "SELECT data_type FROM information_schema.domains WHERE domain_name = 'positive_int'");
        assertNotNull(dt);
    }

    // ---- information_schema.check_constraints ----

    @Test
    void testIsCheckConstraints() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT constraint_name, check_clause FROM information_schema.check_constraints"));
    }

    // ---- information_schema.constraint_column_usage ----

    @Test
    void testIsConstraintColumnUsage() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT table_name, column_name, constraint_name FROM information_schema.constraint_column_usage"));
    }

    // ---- information_schema.constraint_table_usage ----

    @Test
    void testIsConstraintTableUsage() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT table_name, constraint_name FROM information_schema.constraint_table_usage"));
    }

    // ---- information_schema.parameters ----

    @Test
    void testIsParametersAccessible() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM information_schema.parameters LIMIT 1")) {
            assertNotNull(rs.getMetaData());
        }
    }

    // ==================== Catalog Functions ====================

    // ---- current_setting / set_config ----

    @Test
    void testCurrentSettingServerVersion() throws SQLException {
        assertEquals("18.0", query1("SELECT current_setting('server_version')"));
    }

    @Test
    void testCurrentSettingServerVersionNum() throws SQLException {
        assertEquals("180000", query1("SELECT current_setting('server_version_num')"));
    }

    @Test
    void testCurrentSettingServerEncoding() throws SQLException {
        assertEquals("UTF8", query1("SELECT current_setting('server_encoding')"));
    }

    @Test
    void testCurrentSettingClientEncoding() throws SQLException {
        assertEquals("UTF8", query1("SELECT current_setting('client_encoding')"));
    }

    @Test
    void testCurrentSettingSearchPath() throws SQLException {
        String sp = query1("SELECT current_setting('search_path')");
        assertNotNull(sp);
        assertTrue(sp.contains("public"));
    }

    @Test
    void testCurrentSettingTimezone() throws SQLException {
        assertEquals("UTC", query1("SELECT current_setting('timezone')"));
    }

    @Test
    void testCurrentSettingTransactionIsolation() throws SQLException {
        assertEquals("read committed", query1("SELECT current_setting('transaction_isolation')"));
    }

    @Test
    void testCurrentSettingMissingOkTrue() throws SQLException {
        String result = query1("SELECT current_setting('nonexistent_xyz', true)");
        assertNull(result);
    }

    @Test
    void testCurrentSettingMissingOkFalse() throws SQLException {
        assertThrows(SQLException.class, () ->
                query1("SELECT current_setting('nonexistent_xyz', false)"));
    }

    @Test
    void testCurrentSettingMissingDefault() throws SQLException {
        assertThrows(SQLException.class, () ->
                query1("SELECT current_setting('nonexistent_xyz')"));
    }

    @Test
    void testSetConfigAndRetrieve() throws SQLException {
        query1("SELECT set_config('my.custom.param', 'hello', false)");
        assertEquals("hello", query1("SELECT current_setting('my.custom.param')"));
    }

    @Test
    void testSetConfigReturnsValue() throws SQLException {
        String result = query1("SELECT set_config('my.test', 'world', false)");
        assertEquals("world", result);
    }

    // ---- format_type ----

    @Test
    void testFormatTypeInteger() throws SQLException {
        String result = query1("SELECT format_type(23, -1)");
        assertEquals("integer", result);
    }

    @Test
    void testFormatTypeText() throws SQLException {
        String result = query1("SELECT format_type(25, -1)");
        assertEquals("text", result);
    }

    @Test
    void testFormatTypeBool() throws SQLException {
        String result = query1("SELECT format_type(16, -1)");
        assertEquals("boolean", result);
    }

    @Test
    void testFormatTypeNull() throws SQLException {
        String result = query1("SELECT format_type(NULL, NULL)");
        assertEquals("unknown", result);
    }

    // ---- pg_typeof ----

    @Test
    void testPgTypeofInteger() throws SQLException {
        String result = query1("SELECT pg_typeof(42)");
        assertNotNull(result);
    }

    @Test
    void testPgTypeofText() throws SQLException {
        String result = query1("SELECT pg_typeof('hello')");
        assertNotNull(result);
    }

    // ---- pg_get_viewdef ----

    @Test
    void testPgGetViewdef() throws SQLException {
        String result = query1("SELECT pg_get_viewdef('active_users')");
        assertNotNull(result);
    }

    // ---- pg_get_constraintdef ----

    @Test
    void testPgGetConstraintdefReturnsString() throws SQLException {
        // Just ensure it doesn't throw
        String result = query1("SELECT pg_get_constraintdef(0)");
        assertNotNull(result);
    }

    // ---- pg_get_indexdef ----

    @Test
    void testPgGetIndexdefReturnsString() throws SQLException {
        String result = query1("SELECT pg_get_indexdef(0)");
        assertNotNull(result);
    }

    // ---- pg_get_serial_sequence ----

    @Test
    void testPgGetSerialSequenceReturnsNull() throws SQLException {
        // No serial column with nextval default for 'name' column
        String result = query1("SELECT pg_get_serial_sequence('users', 'name')");
        assertNull(result);
    }

    // ---- pg_get_functiondef ----

    @Test
    void testPgGetFunctiondef() throws SQLException {
        String result = query1("SELECT pg_get_functiondef(0)");
        assertNotNull(result);
    }

    // ---- pg_table_is_visible ----

    @Test
    void testPgTableIsVisible() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT pg_table_is_visible(1)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // ---- pg_function_is_visible / pg_type_is_visible ----

    @Test
    void testPgFunctionIsVisible() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT pg_function_is_visible(1)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void testPgTypeIsVisible() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT pg_type_is_visible(1)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // ---- obj_description / col_description ----

    @Test
    void testObjDescriptionReturnsNull() throws SQLException {
        String result = query1("SELECT obj_description(1, 'pg_class')");
        assertNull(result);
    }

    @Test
    void testColDescriptionReturnsNull() throws SQLException {
        String result = query1("SELECT col_description(1, 1)");
        assertNull(result);
    }

    // ---- pg_get_userbyid ----

    @Test
    void testPgGetUserbyid() throws SQLException {
        assertEquals("memgres", query1("SELECT pg_get_userbyid(1)"));
    }

    // ---- pg_encoding_to_char ----

    @Test
    void testPgEncodingToChar() throws SQLException {
        assertEquals("UTF8", query1("SELECT pg_encoding_to_char(6)"));
    }

    // ---- has_*_privilege ----

    @Test
    void testHasSchemaPrivilege() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT has_schema_privilege('public', 'USAGE')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void testHasTablePrivilege() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT has_table_privilege('users', 'SELECT')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void testHasDatabasePrivilege() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT has_database_privilege('memgres', 'CONNECT')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // ---- pg_relation_size etc ----

    @Test
    void testPgRelationSize() throws SQLException {
        String result = query1("SELECT pg_relation_size(1)");
        assertEquals("8192", result);
    }

    @Test
    void testPgTotalRelationSize() throws SQLException {
        String result = query1("SELECT pg_total_relation_size(1)");
        assertEquals("8192", result);
    }

    @Test
    void testPgTableSize() throws SQLException {
        String result = query1("SELECT pg_table_size(1)");
        assertEquals("8192", result);
    }

    @Test
    void testPgDatabaseSize() throws SQLException {
        String result = query1("SELECT pg_database_size(1)");
        assertEquals("8192", result);
    }

    // ---- to_regclass ----

    @Test
    void testToRegclassExistingTable() throws SQLException {
        String result = query1("SELECT to_regclass('users')");
        assertEquals("users", result);
    }

    @Test
    void testToRegclassNonexistent() throws SQLException {
        String result = query1("SELECT to_regclass('nonexistent_table')");
        assertNull(result);
    }

    // ---- pg_get_function_arguments / pg_get_function_result ----

    @Test
    void testPgGetFunctionArguments() throws SQLException {
        String result = query1("SELECT pg_get_function_arguments(0)");
        assertNotNull(result);
    }

    @Test
    void testPgGetFunctionResult() throws SQLException {
        String result = query1("SELECT pg_get_function_result(0)");
        assertNotNull(result);
    }

    // ---- shobj_description ----

    @Test
    void testShobjDescription() throws SQLException {
        String result = query1("SELECT shobj_description(1, 'pg_database')");
        assertNull(result);
    }

    // ---- pg_get_expr ----

    @Test
    void testPgGetExpr() throws SQLException {
        String result = query1("SELECT pg_get_expr('hello', 0)");
        assertEquals("hello", result);
    }

    // ==================== SHOW / SET integration with pg_settings ====================

    @Test
    void testShowServerVersion() throws SQLException {
        String result = query1("SHOW server_version");
        assertEquals("18.0", result);
    }

    @Test
    void testShowSearchPath() throws SQLException {
        String result = query1("SHOW search_path");
        assertNotNull(result);
        assertTrue(result.contains("public"));
    }

    @Test
    void testShowTimezone() throws SQLException {
        String result = query1("SHOW timezone");
        assertEquals("UTC", result);
    }

    @Test
    void testShowTransactionIsolationLevel() throws SQLException {
        String result = query1("SHOW transaction_isolation");
        assertEquals("read committed", result);
    }

    @Test
    void testSetAndShowCustomParam() throws SQLException {
        // Use set_config to set a custom parameter, then retrieve with current_setting
        query1("SELECT set_config('my_app.debug', 'on', false)");
        assertEquals("on", query1("SELECT current_setting('my_app.debug')"));
    }

    @Test
    void testShowAll() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW ALL")) {
            int count = 0;
            while (rs.next()) count++;
            assertTrue(count > 10);
        }
    }

    // ==================== Cross-catalog queries ====================

    @Test
    void testJoinPgClassPgNamespace() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT c.relname, n.nspname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = 'users'"));
    }

    @Test
    void testJoinPgClassPgAttribute() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT c.relname, a.attname FROM pg_class c JOIN pg_attribute a ON c.oid = a.attrelid WHERE c.relname = 'users'"));
    }

    @Test
    void testJoinPgConstraintPgClass() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT con.conname, c.relname FROM pg_constraint con JOIN pg_class c ON con.conrelid = c.oid"));
    }

    @Test
    void testIsTablesColumnsJoin() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT t.table_name, c.column_name FROM information_schema.tables t " +
                        "JOIN information_schema.columns c ON t.table_name = c.table_name " +
                        "WHERE t.table_name = 'users' AND c.column_name = 'name'"));
    }

    @Test
    void testCountColumnsPerTable() throws SQLException {
        int count = queryInt(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'users'");
        assertEquals(4, count); // id, name, email, active
    }

    // ==================== Edge cases ====================

    @Test
    void testUnknownPgCatalogTableReturnsEmpty() throws SQLException {
        // PG errors on unknown catalog tables with "relation does not exist"
        assertThrows(SQLException.class,
                () -> { try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT * FROM pg_nonexistent_table LIMIT 1")) {} });
    }

    @Test
    void testPgCatalogSchemaPrefix() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'users'"));
    }

    @Test
    void testInformationSchemaPrefix() throws SQLException {
        assertTrue(queryHasRows(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'users'"));
    }
}
