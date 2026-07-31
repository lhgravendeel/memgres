package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage tests for items 118-128: No-op / parse-only DDL statements.
 * These statements should be accepted without error, even though Memgres
 * doesn't actually implement the underlying features.
 */
class NoopDdlCoverageTest {

    static Memgres memgres;
    static Connection conn;
    static Statement stmt;

    @BeforeAll
    static void setup() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        String url = "jdbc:postgresql://localhost:" + memgres.getPort() + "/test";
        conn = DriverManager.getConnection(url, "test", "test");
        stmt = conn.createStatement();
        // The objects the ALTER and IMPORT tests below name. PostgreSQL refuses every one of
        // those statements on a name that was never created, and the tests run in an order
        // nothing here fixes, so the names have to exist before any of them runs.
        stmt.execute("CREATE FOREIGN DATA WRAPPER myfdw");
        stmt.execute("CREATE SERVER myserver FOREIGN DATA WRAPPER myfdw");
        stmt.execute("CREATE FOREIGN TABLE ft1 (id int, name text) SERVER myserver");
        stmt.execute("CREATE PUBLICATION mypub FOR ALL TABLES");
        stmt.execute("CREATE SUBSCRIPTION mysub CONNECTION 'host=localhost' PUBLICATION mypub");
    }

    @AfterAll
    static void teardown() throws Exception {
        if (stmt != null) stmt.close();
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        stmt.execute(sql);
    }

    private String query1(String sql) throws SQLException {
        ResultSet rs = stmt.executeQuery(sql);
        assertTrue(rs.next());
        return rs.getString(1);
    }

    // ========================================================================
    // 118: Foreign Data Wrappers
    // ========================================================================

    // CREATE FOREIGN DATA WRAPPER
    @Test void testCreateForeignDataWrapper() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER myfdw");
    }

    @Test void testCreateForeignDataWrapperWithHandler() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER myfdw HANDLER myhandler");
    }

    @Test void testCreateForeignDataWrapperWithValidator() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER myfdw VALIDATOR myvalidator");
    }

    @Test void testCreateForeignDataWrapperWithOptions() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER myfdw OPTIONS (debug 'true')");
    }

    @Test void testCreateForeignDataWrapperNoHandlerNoValidator() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER myfdw HANDLER myhandler VALIDATOR myvalidator");
    }

    // ALTER FOREIGN DATA WRAPPER. Each of these used to name a wrapper no test had created and
    // passed because the ALTER was a silent no-op; PostgreSQL raises 42704 for every one of them,
    // so the wrapper has to exist for the statement to be the no-op it is meant to be.
    @Test void testAlterForeignDataWrapper() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER myfdw_h");
        exec("ALTER FOREIGN DATA WRAPPER myfdw_h HANDLER newhandler");
    }

    @Test void testAlterForeignDataWrapperOptions() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER myfdw_o");
        exec("ALTER FOREIGN DATA WRAPPER myfdw_o OPTIONS (SET debug 'false')");
    }

    @Test void testAlterForeignDataWrapperOwner() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER myfdw_w");
        exec("ALTER FOREIGN DATA WRAPPER myfdw_w OWNER TO newowner");
    }

    @Test void testAlterForeignDataWrapperRename() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER myfdw_r");
        exec("ALTER FOREIGN DATA WRAPPER myfdw_r RENAME TO newfdw");
    }

    @Test void testAlterForeignDataWrapperThatIsNotThere() {
        assertThrows(SQLException.class,
                () -> exec("ALTER FOREIGN DATA WRAPPER myfdw_nosuch OPTIONS (a 'b')"));
    }

    // DROP FOREIGN DATA WRAPPER
    @Test void testDropForeignDataWrapper() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER myfdw_d");
        exec("DROP FOREIGN DATA WRAPPER myfdw_d");
    }

    @Test void testDropForeignDataWrapperIfExists() throws SQLException {
        exec("DROP FOREIGN DATA WRAPPER IF EXISTS myfdw_none");
    }

    @Test void testDropForeignDataWrapperCascade() throws SQLException {
        exec("DROP FOREIGN DATA WRAPPER IF EXISTS myfdw_none CASCADE");
    }

    @Test void testDropForeignDataWrapperThatIsNotThere() {
        assertThrows(SQLException.class,
                () -> exec("DROP FOREIGN DATA WRAPPER myfdw_nosuch"));
    }

    // CREATE SERVER
    @Test void testCreateServer() throws SQLException {
        exec("CREATE SERVER myserver FOREIGN DATA WRAPPER myfdw");
    }

    @Test void testCreateServerWithType() throws SQLException {
        exec("CREATE SERVER myserver TYPE 'dbtype' FOREIGN DATA WRAPPER myfdw");
    }

    @Test void testCreateServerWithVersion() throws SQLException {
        exec("CREATE SERVER myserver VERSION '1.0' FOREIGN DATA WRAPPER myfdw");
    }

    @Test void testCreateServerWithOptions() throws SQLException {
        exec("CREATE SERVER myserver FOREIGN DATA WRAPPER myfdw OPTIONS (host 'localhost', port '5432')");
    }

    // ALTER SERVER, on a server that was created — the same correction as for the wrapper above
    @Test void testAlterServer() throws SQLException {
        exec("CREATE SERVER myserver_o FOREIGN DATA WRAPPER myfdw");
        exec("ALTER SERVER myserver_o OPTIONS (SET host 'newhost')");
    }

    @Test void testAlterServerOwner() throws SQLException {
        exec("CREATE SERVER myserver_w FOREIGN DATA WRAPPER myfdw");
        exec("ALTER SERVER myserver_w OWNER TO newowner");
    }

    @Test void testAlterServerRename() throws SQLException {
        exec("CREATE SERVER myserver_r FOREIGN DATA WRAPPER myfdw");
        exec("ALTER SERVER myserver_r RENAME TO newserver");
    }

    @Test void testAlterServerVersion() throws SQLException {
        exec("CREATE SERVER myserver_v FOREIGN DATA WRAPPER myfdw");
        exec("ALTER SERVER myserver_v VERSION '2.0'");
    }

    @Test void testAlterServerThatIsNotThere() {
        assertThrows(SQLException.class,
                () -> exec("ALTER SERVER myserver_nosuch OPTIONS (SET host 'h')"));
    }

    // DROP SERVER
    @Test void testDropServer() throws SQLException {
        exec("CREATE SERVER myserver_d FOREIGN DATA WRAPPER myfdw");
        exec("DROP SERVER myserver_d");
    }

    @Test void testDropServerIfExists() throws SQLException {
        exec("DROP SERVER IF EXISTS myserver_none");
    }

    @Test void testDropServerCascade() throws SQLException {
        exec("DROP SERVER IF EXISTS myserver_none CASCADE");
    }

    @Test void testDropServerThatIsNotThere() {
        assertThrows(SQLException.class, () -> exec("DROP SERVER myserver_nosuch"));
    }

    // CREATE USER MAPPING
    @Test void testCreateUserMapping() throws SQLException {
        exec("CREATE USER MAPPING FOR current_user SERVER myserver");
    }

    @Test void testCreateUserMappingWithOptions() throws SQLException {
        exec("CREATE USER MAPPING FOR current_user SERVER myserver OPTIONS (user 'dbuser', password 'secret')");
    }

    @Test void testCreateUserMappingForPublic() throws SQLException {
        exec("CREATE USER MAPPING FOR PUBLIC SERVER myserver");
    }

    // ALTER USER MAPPING
    @Test void testAlterUserMapping() throws SQLException {
        exec("ALTER USER MAPPING FOR current_user SERVER myserver OPTIONS (SET password 'newpass')");
    }

    // DROP USER MAPPING
    @Test void testDropUserMapping() throws SQLException {
        exec("DROP USER MAPPING FOR current_user SERVER myserver");
    }

    @Test void testDropUserMappingIfExists() throws SQLException {
        exec("DROP USER MAPPING IF EXISTS FOR current_user SERVER myserver");
    }

    // CREATE FOREIGN TABLE
    @Test void testCreateForeignTable() throws SQLException {
        exec("CREATE FOREIGN TABLE ft1 (id int, name text) SERVER myserver");
    }

    @Test void testCreateForeignTableWithOptions() throws SQLException {
        exec("CREATE FOREIGN TABLE ft1 (id int OPTIONS (column_name 'ID'), name text) SERVER myserver OPTIONS (table_name 'remote_t')");
    }

    @Test void testCreateForeignTableIfNotExists() throws SQLException {
        exec("CREATE FOREIGN TABLE IF NOT EXISTS ft1 (id int) SERVER myserver");
    }

    // ALTER FOREIGN TABLE
    @Test void testAlterForeignTable() throws SQLException {
        exec("ALTER FOREIGN TABLE ft1 ADD COLUMN extra text");
    }

    @Test void testAlterForeignTableOptions() throws SQLException {
        exec("ALTER FOREIGN TABLE ft1 OPTIONS (SET table_name 'other')");
    }

    // DROP FOREIGN TABLE, on one of its own so the shared ft1 survives for the ALTERs
    @Test void testDropForeignTable() throws SQLException {
        exec("CREATE FOREIGN TABLE ft_drop (id int) SERVER myserver");
        exec("DROP FOREIGN TABLE ft_drop");
    }

    @Test void testDropForeignTableIfExists() throws SQLException {
        exec("DROP FOREIGN TABLE IF EXISTS ft_none");
    }

    @Test void testDropForeignTableCascade() throws SQLException {
        exec("DROP FOREIGN TABLE IF EXISTS ft_none CASCADE");
    }

    @Test void testAlterForeignTableThatIsNotThere() {
        assertThrows(SQLException.class,
                () -> exec("ALTER FOREIGN TABLE ft_nosuch RENAME TO ft_other"));
    }

    // IMPORT FOREIGN SCHEMA — must error because FDW has no handler
    @Test void testImportForeignSchema() {
        assertThrows(SQLException.class,
                () -> exec("IMPORT FOREIGN SCHEMA remote_schema FROM SERVER myserver INTO public"),
                "Expected 55000: foreign-data wrapper has no handler");
    }

    @Test void testImportForeignSchemaLimitTo() {
        assertThrows(SQLException.class,
                () -> exec("IMPORT FOREIGN SCHEMA remote_schema LIMIT TO (t1, t2) FROM SERVER myserver INTO public"),
                "Expected 55000: foreign-data wrapper has no handler");
    }

    @Test void testImportForeignSchemaExcept() {
        assertThrows(SQLException.class,
                () -> exec("IMPORT FOREIGN SCHEMA remote_schema EXCEPT (t3) FROM SERVER myserver INTO public"),
                "Expected 55000: foreign-data wrapper has no handler");
    }

    // ========================================================================
    // 119: Publications & Subscriptions
    // ========================================================================

    // CREATE PUBLICATION
    @Test void testCreatePublication() throws SQLException {
        exec("CREATE PUBLICATION mypub FOR ALL TABLES");
    }

    @Test void testCreatePublicationForTable() throws SQLException {
        exec("CREATE PUBLICATION mypub FOR TABLE t1");
    }

    @Test void testCreatePublicationForMultipleTables() throws SQLException {
        exec("CREATE PUBLICATION mypub FOR TABLE t1, t2, t3");
    }

    @Test void testCreatePublicationWithParams() throws SQLException {
        exec("CREATE PUBLICATION mypub FOR ALL TABLES WITH (publish = 'insert, update')");
    }

    // ALTER PUBLICATION. Each of these used to name a publication no test had created, and
    // passed because the ALTER was a silent no-op; PostgreSQL raises 42704 for every one of
    // them, so the publication has to exist for the statement to be the no-op it is meant to be.
    @Test void testAlterPublication() throws SQLException {
        exec("CREATE PUBLICATION mypub_add FOR ALL TABLES");
        exec("ALTER PUBLICATION mypub_add ADD TABLE t4");
    }

    @Test void testAlterPublicationSetTable() throws SQLException {
        exec("CREATE PUBLICATION mypub_set FOR ALL TABLES");
        exec("ALTER PUBLICATION mypub_set SET TABLE t1, t2");
    }

    @Test void testAlterPublicationDropTable() throws SQLException {
        exec("CREATE PUBLICATION mypub_drop FOR TABLE t1");
        exec("ALTER PUBLICATION mypub_drop DROP TABLE t1");
    }

    @Test void testAlterPublicationOwner() throws SQLException {
        exec("CREATE PUBLICATION mypub_own FOR ALL TABLES");
        exec("ALTER PUBLICATION mypub_own OWNER TO newowner");
    }

    @Test void testAlterPublicationRename() throws SQLException {
        exec("CREATE PUBLICATION mypub_ren FOR ALL TABLES");
        exec("ALTER PUBLICATION mypub_ren RENAME TO mypub_ren2");
    }

    @Test void testAlterPublicationThatIsNotThere() {
        assertThrows(SQLException.class,
                () -> exec("ALTER PUBLICATION mypub_nosuch ADD TABLE t1"));
    }

    // DROP PUBLICATION, on one of its own so the shared mypub survives
    @Test void testDropPublication() throws SQLException {
        exec("CREATE PUBLICATION mypub_del FOR ALL TABLES");
        exec("DROP PUBLICATION mypub_del");
    }

    @Test void testDropPublicationIfExists() throws SQLException {
        exec("DROP PUBLICATION IF EXISTS mypub_none");
    }

    @Test void testDropPublicationThatIsNotThere() {
        assertThrows(SQLException.class, () -> exec("DROP PUBLICATION mypub_nosuch"));
    }

    // CREATE SUBSCRIPTION
    @Test void testCreateSubscription() throws SQLException {
        exec("CREATE SUBSCRIPTION mysub CONNECTION 'host=localhost' PUBLICATION mypub");
    }

    @Test void testCreateSubscriptionWithParams() throws SQLException {
        exec("CREATE SUBSCRIPTION mysub CONNECTION 'host=localhost' PUBLICATION mypub WITH (enabled = false)");
    }

    // ALTER SUBSCRIPTION
    @Test void testAlterSubscription() throws SQLException {
        exec("ALTER SUBSCRIPTION mysub SET PUBLICATION newpub");
    }

    @Test void testAlterSubscriptionConnection() throws SQLException {
        exec("ALTER SUBSCRIPTION mysub CONNECTION 'host=newhost'");
    }

    @Test void testAlterSubscriptionEnable() throws SQLException {
        exec("ALTER SUBSCRIPTION mysub ENABLE");
    }

    @Test void testAlterSubscriptionDisable() throws SQLException {
        exec("ALTER SUBSCRIPTION mysub DISABLE");
    }

    @Test void testAlterSubscriptionRefresh() throws SQLException {
        exec("ALTER SUBSCRIPTION mysub REFRESH PUBLICATION");
    }

    @Test void testAlterSubscriptionOwner() throws SQLException {
        exec("ALTER SUBSCRIPTION mysub OWNER TO newowner");
    }

    @Test void testAlterSubscriptionThatIsNotThere() {
        assertThrows(SQLException.class, () -> exec("ALTER SUBSCRIPTION mysub_nosuch ENABLE"));
    }

    // DROP SUBSCRIPTION, on one of its own so the shared mysub survives for the ALTERs
    @Test void testDropSubscription() throws SQLException {
        exec("CREATE SUBSCRIPTION mysub_del CONNECTION 'host=localhost' PUBLICATION mypub");
        exec("DROP SUBSCRIPTION mysub_del");
    }

    @Test void testDropSubscriptionIfExists() throws SQLException {
        exec("DROP SUBSCRIPTION IF EXISTS mysub_none");
    }

    @Test void testDropSubscriptionThatIsNotThere() {
        assertThrows(SQLException.class, () -> exec("DROP SUBSCRIPTION mysub_nosuch"));
    }

    // ========================================================================
    // 120: Database Management
    // ========================================================================

    @Test void testCreateDatabase() throws SQLException {
        exec("DROP DATABASE IF EXISTS noop_createdb");
        exec("CREATE DATABASE noop_createdb");
        exec("DROP DATABASE IF EXISTS noop_createdb");
    }

    @Test void testCreateDatabaseWithOptions() throws SQLException {
        exec("DROP DATABASE IF EXISTS noop_createoptdb");
        exec("CREATE DATABASE noop_createoptdb OWNER test ENCODING 'UTF8' LC_COLLATE 'en_US.UTF-8'");
        exec("DROP DATABASE IF EXISTS noop_createoptdb");
    }

    @Test void testCreateDatabaseTemplate() throws SQLException {
        exec("DROP DATABASE IF EXISTS noop_createtpldb");
        exec("CREATE DATABASE noop_createtpldb TEMPLATE template0");
        exec("DROP DATABASE IF EXISTS noop_createtpldb");
    }

    @Test void testCreateDatabaseConnectionLimit() throws SQLException {
        exec("DROP DATABASE IF EXISTS noop_createconlimdb");
        exec("CREATE DATABASE noop_createconlimdb CONNECTION LIMIT 100");
        exec("DROP DATABASE IF EXISTS noop_createconlimdb");
    }

    @Test void testAlterDatabase() throws SQLException {
        exec("DROP DATABASE IF EXISTS noop_alterdb");
        exec("CREATE DATABASE noop_alterdb");
        exec("ALTER DATABASE noop_alterdb SET timezone TO 'UTC'");
        exec("DROP DATABASE IF EXISTS noop_alterdb");
    }

    @Test void testAlterDatabaseOwner() throws SQLException {
        exec("DROP DATABASE IF EXISTS noop_alterownerdb");
        exec("CREATE DATABASE noop_alterownerdb");
        exec("ALTER DATABASE noop_alterownerdb OWNER TO newowner");
        exec("DROP DATABASE IF EXISTS noop_alterownerdb");
    }

    @Test void testAlterDatabaseRename() throws SQLException {
        exec("DROP DATABASE IF EXISTS noop_alterrenamedb");
        exec("DROP DATABASE IF EXISTS noop_alterrenamed2db");
        exec("CREATE DATABASE noop_alterrenamedb");
        exec("ALTER DATABASE noop_alterrenamedb RENAME TO noop_alterrenamed2db");
        exec("DROP DATABASE IF EXISTS noop_alterrenamed2db");
    }

    @Test void testAlterDatabaseConnectionLimit() throws SQLException {
        exec("DROP DATABASE IF EXISTS noop_alterconlimdb");
        exec("CREATE DATABASE noop_alterconlimdb");
        exec("ALTER DATABASE noop_alterconlimdb CONNECTION LIMIT 50");
        exec("DROP DATABASE IF EXISTS noop_alterconlimdb");
    }

    @Test void testDropDatabase() throws SQLException {
        exec("DROP DATABASE IF EXISTS noop_dropdb");
        exec("CREATE DATABASE noop_dropdb");
        exec("DROP DATABASE noop_dropdb");
    }

    @Test void testDropDatabaseIfExists() throws SQLException {
        exec("DROP DATABASE IF EXISTS noop_dropifexistsdb");
    }

    @Test void testDropDatabaseForce() throws SQLException {
        exec("DROP DATABASE IF EXISTS noop_dropforcedb WITH (FORCE)");
    }

    // ========================================================================
    // 121: Tablespace Management
    // ========================================================================

    @Test void testCreateTablespace() throws SQLException {
        exec("CREATE TABLESPACE myts LOCATION '/data/ts'");
    }

    @Test void testCreateTablespaceOwner() throws SQLException {
        exec("CREATE TABLESPACE myts OWNER test LOCATION '/data/ts'");
    }

    // Each ALTER creates its own tablespace: ALTER now refuses a name that was never created,
    // so these can no longer lean on whichever other test happened to run first.
    @Test void testAlterTablespace() throws SQLException {
        exec("CREATE TABLESPACE renamets LOCATION '/data/ts'");
        exec("ALTER TABLESPACE renamets RENAME TO newts");
    }

    @Test void testAlterTablespaceOwner() throws SQLException {
        exec("CREATE TABLESPACE ownerts LOCATION '/data/ts'");
        exec("ALTER TABLESPACE ownerts OWNER TO newowner");
    }

    @Test void testAlterTablespaceSet() throws SQLException {
        exec("CREATE TABLESPACE setts LOCATION '/data/ts'");
        exec("ALTER TABLESPACE setts SET (seq_page_cost = 1.5)");
    }

    @Test void testDropTablespace() throws SQLException {
        exec("DROP TABLESPACE myts");
    }

    @Test void testDropTablespaceIfExists() throws SQLException {
        exec("DROP TABLESPACE IF EXISTS myts");
    }

    // ========================================================================
    // 122: Language Management
    // ========================================================================

    @Test void testCreateLanguage() throws SQLException {
        exec("CREATE LANGUAGE plpgsql");
    }

    @Test void testCreateTrustedLanguage() throws SQLException {
        exec("CREATE TRUSTED LANGUAGE plperl HANDLER plperl_call_handler");
    }

    @Test void testCreateOrReplaceLanguage() throws SQLException {
        exec("CREATE OR REPLACE LANGUAGE plpgsql");
    }

    @Test void testCreateProceduralLanguage() throws SQLException {
        exec("CREATE PROCEDURAL LANGUAGE plpython3u HANDLER plpython3_call_handler");
    }

    @Test void testAlterLanguage() throws SQLException {
        exec("ALTER LANGUAGE plpgsql RENAME TO newlang");
    }

    @Test void testAlterLanguageOwner() throws SQLException {
        exec("ALTER LANGUAGE plpgsql OWNER TO newowner");
    }

    @Test void testDropLanguage() throws SQLException {
        exec("DROP LANGUAGE plpgsql");
    }

    @Test void testDropLanguageIfExists() throws SQLException {
        exec("DROP LANGUAGE IF EXISTS plpgsql");
    }

    @Test void testDropLanguageCascade() throws SQLException {
        exec("DROP LANGUAGE IF EXISTS plpgsql CASCADE");
    }

    // ========================================================================
    // 123: Event Triggers
    // ========================================================================

    /** PG only accepts a function returning event_trigger here, so each test declares one. */
    private void eventTriggerFunction(String name) throws SQLException {
        exec("CREATE OR REPLACE FUNCTION " + name + "() RETURNS event_trigger"
                + " LANGUAGE plpgsql AS $$ BEGIN END $$");
    }

    private void eventTrigger(String name) throws SQLException {
        eventTriggerFunction("my_func");
        exec("CREATE EVENT TRIGGER " + name + " ON ddl_command_start EXECUTE FUNCTION my_func()");
    }

    @Test void testCreateEventTrigger() throws SQLException {
        eventTrigger("et_create");
    }

    @Test void testCreateEventTriggerWithFilter() throws SQLException {
        eventTriggerFunction("my_func");
        exec("CREATE EVENT TRIGGER et_filter ON ddl_command_end WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION my_func()");
    }

    @Test void testCreateEventTriggerSqlDrop() throws SQLException {
        eventTriggerFunction("my_drop_func");
        exec("CREATE EVENT TRIGGER drop_trigger ON sql_drop EXECUTE FUNCTION my_drop_func()");
    }

    @Test void testCreateEventTriggerTableRewrite() throws SQLException {
        eventTriggerFunction("my_rewrite_func");
        exec("CREATE EVENT TRIGGER rewrite_trigger ON table_rewrite EXECUTE FUNCTION my_rewrite_func()");
    }

    @Test void testAlterEventTrigger() throws SQLException {
        eventTrigger("et_disable");
        exec("ALTER EVENT TRIGGER et_disable DISABLE");
    }

    @Test void testAlterEventTriggerEnable() throws SQLException {
        eventTrigger("et_enable");
        exec("ALTER EVENT TRIGGER et_enable ENABLE");
    }

    @Test void testAlterEventTriggerEnableAlways() throws SQLException {
        eventTrigger("et_always");
        exec("ALTER EVENT TRIGGER et_always ENABLE ALWAYS");
    }

    @Test void testAlterEventTriggerRename() throws SQLException {
        eventTrigger("et_rename");
        exec("ALTER EVENT TRIGGER et_rename RENAME TO newtrigger");
    }

    @Test void testAlterEventTriggerOwner() throws SQLException {
        eventTrigger("et_owner");
        exec("ALTER EVENT TRIGGER et_owner OWNER TO newowner");
    }

    @Test void testDropEventTrigger() throws SQLException {
        eventTrigger("et_drop");
        exec("DROP EVENT TRIGGER et_drop");
    }

    @Test void testDropEventTriggerIfExists() throws SQLException {
        exec("DROP EVENT TRIGGER IF EXISTS mytrigger");
    }

    @Test void testDropEventTriggerCascade() throws SQLException {
        exec("DROP EVENT TRIGGER IF EXISTS mytrigger CASCADE");
    }

    // Event trigger functions
    @Test void testPgEventTriggerTableRewriteOid() throws SQLException {
        // These only work inside event triggers, but we accept the call
        String result = query1("SELECT pg_event_trigger_table_rewrite_oid()");
        assertEquals("0", result);
    }

    @Test void testPgEventTriggerTableRewriteReason() throws SQLException {
        String result = query1("SELECT pg_event_trigger_table_rewrite_reason()");
        assertEquals("0", result);
    }

    // ========================================================================
    // 124: Large Objects
    // ========================================================================

    @Test void testLoCreat() throws SQLException {
        String result = query1("SELECT lo_creat(-1)");
        assertNotNull(result);
    }

    @Test void testLoCreate() throws SQLException {
        String result = query1("SELECT lo_create(0)");
        assertNotNull(result);
    }

    @Test void testLoFromBytea() throws SQLException {
        String result = query1("SELECT lo_from_bytea(0, '\\x48656c6c6f'::bytea)");
        assertNotNull(result);
    }

    @Test void testLoImport() throws SQLException {
        String result = query1("SELECT lo_import('/tmp/test.txt')");
        assertNotNull(result);
    }

    @Test void testLoExport() throws SQLException {
        String result = query1("SELECT lo_export(1, '/tmp/out.txt')");
        assertNotNull(result);
    }

    @Test void testLoUnlink() throws SQLException {
        String oid = query1("SELECT lo_from_bytea(0, '\\x'::bytea)");
        String result = query1("SELECT lo_unlink(" + oid + ")");
        assertNotNull(result);
    }

    @Test void testLoGet() throws SQLException {
        // lo_get returns bytea
        String oid = query1("SELECT lo_from_bytea(0, '\\x48656c6c6f'::bytea)");
        ResultSet rs = stmt.executeQuery("SELECT lo_get(" + oid + ")");
        assertTrue(rs.next());
        assertNotNull(rs.getObject(1));
    }

    @Test void testLoWrite() throws SQLException {
        String oid = query1("SELECT lo_from_bytea(0, '\\x'::bytea)");
        String fd = query1("SELECT lo_open(" + oid + ", 131072)"); // INV_WRITE = 0x20000
        String result = query1("SELECT lowrite(" + fd + ", '\\x48656c6c6f'::bytea)");
        assertNotNull(result);
        assertEquals("5", result); // 5 bytes written
        query1("SELECT lo_close(" + fd + ")");
    }

    @Test void testLoRead() throws SQLException {
        String oid = query1("SELECT lo_from_bytea(0, '\\x48656c6c6f'::bytea)");
        String fd = query1("SELECT lo_open(" + oid + ", 262144)");
        ResultSet rs = stmt.executeQuery("SELECT loread(" + fd + ", 10)");
        assertTrue(rs.next());
        assertNotNull(rs.getObject(1));
    }

    // PostgreSQL raises 42704 for an ALTER on a large object that was never created, so this
    // alters one it made rather than an OID nothing stands behind.
    @Test void testAlterLargeObject() throws SQLException {
        String oid = query1("SELECT lo_from_bytea(0, '\\x4869'::bytea)");
        exec("ALTER LARGE OBJECT " + oid + " OWNER TO test");
    }

    @Test void testAlterLargeObjectThatIsNotThere() {
        assertThrows(SQLException.class, () -> exec("ALTER LARGE OBJECT 987654 OWNER TO test"));
    }

    // ========================================================================
    // 125: Transforms
    // ========================================================================

    @Test void testCreateTransform() throws SQLException {
        exec("CREATE TRANSFORM FOR int LANGUAGE plpgsql (FROM SQL WITH FUNCTION my_from_func(internal), TO SQL WITH FUNCTION my_to_func(internal))");
    }

    @Test void testCreateOrReplaceTransform() throws SQLException {
        exec("CREATE OR REPLACE TRANSFORM FOR int LANGUAGE plpgsql (FROM SQL WITH FUNCTION my_from_func(internal), TO SQL WITH FUNCTION my_to_func(internal))");
    }

    @Test void testDropTransform() throws SQLException {
        exec("DROP TRANSFORM FOR int LANGUAGE plpgsql");
    }

    @Test void testDropTransformIfExists() throws SQLException {
        exec("DROP TRANSFORM IF EXISTS FOR int LANGUAGE plpgsql");
    }

    @Test void testDropTransformCascade() throws SQLException {
        exec("DROP TRANSFORM IF EXISTS FOR int LANGUAGE plpgsql CASCADE");
    }

    // ========================================================================
    // 126: Access Methods
    // ========================================================================

    @Test void testCreateAccessMethod() throws SQLException {
        exec("CREATE ACCESS METHOD myam TYPE INDEX HANDLER myhandler");
    }

    @Test void testCreateAccessMethodTable() throws SQLException {
        exec("CREATE ACCESS METHOD myam TYPE TABLE HANDLER myhandler");
    }

    @Test void testDropAccessMethod() throws SQLException {
        exec("DROP ACCESS METHOD myam");
    }

    @Test void testDropAccessMethodIfExists() throws SQLException {
        exec("DROP ACCESS METHOD IF EXISTS myam");
    }

    @Test void testDropAccessMethodCascade() throws SQLException {
        exec("DROP ACCESS METHOD IF EXISTS myam CASCADE");
    }

    // ========================================================================
    // 127: Statistics Objects
    // ========================================================================

    /** Statistics are defined over a real table's real columns, so each test needs one. */
    private void statTable() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS stat_test (a int, b int, c text)");
    }

    private void statistic(String name) throws SQLException {
        statTable();
        exec("CREATE STATISTICS " + name + " ON a, b FROM stat_test");
    }

    @Test void testCreateStatisticsBasic() throws SQLException {
        statistic("mystat");
    }

    @Test void testCreateStatisticsNdistinct() throws SQLException {
        statTable();
        exec("CREATE STATISTICS mystat_nd (ndistinct) ON a, b FROM stat_test");
    }

    @Test void testCreateStatisticsDependencies() throws SQLException {
        statTable();
        exec("CREATE STATISTICS mystat_dep (dependencies) ON a, b FROM stat_test");
    }

    @Test void testCreateStatisticsMcv() throws SQLException {
        statTable();
        exec("CREATE STATISTICS mystat_mcv (mcv) ON a, b FROM stat_test");
    }

    @Test void testCreateStatisticsMultipleKinds() throws SQLException {
        statTable();
        exec("CREATE STATISTICS mystat_multi (ndistinct, dependencies, mcv) ON a, b, c FROM stat_test");
    }

    @Test void testCreateStatisticsIfNotExists() throws SQLException {
        statistic("mystat_ine");
        exec("CREATE STATISTICS IF NOT EXISTS mystat_ine ON a, b FROM stat_test");
    }

    @Test void testAlterStatistics() throws SQLException {
        statistic("mystat_target");
        exec("ALTER STATISTICS mystat_target SET STATISTICS 1000");
    }

    @Test void testAlterStatisticsOwner() throws SQLException {
        statistic("mystat_owner");
        exec("ALTER STATISTICS mystat_owner OWNER TO newowner");
    }

    @Test void testAlterStatisticsRename() throws SQLException {
        statistic("mystat_rename");
        exec("ALTER STATISTICS mystat_rename RENAME TO newstat");
    }

    @Test void testAlterStatisticsSchema() throws SQLException {
        statistic("mystat_schema");
        exec("ALTER STATISTICS mystat_schema SET SCHEMA public");
    }

    @Test void testDropStatistics() throws SQLException {
        statistic("mystat_drop");
        exec("DROP STATISTICS mystat_drop");
    }

    @Test void testDropStatisticsIfExists() throws SQLException {
        exec("DROP STATISTICS IF EXISTS mystat");
    }

    @Test void testDropStatisticsCascade() throws SQLException {
        exec("DROP STATISTICS IF EXISTS mystat CASCADE");
    }

    // ========================================================================
    // 128: Groups (legacy alias for ROLE)
    // ========================================================================

    @Test void testCreateGroup() throws SQLException {
        exec("DROP ROLE IF EXISTS mygroup");
        exec("CREATE GROUP mygroup");
    }

    @Test void testCreateGroupWithUser() throws SQLException {
        exec("DROP ROLE IF EXISTS mygroup2");
        exec("CREATE GROUP mygroup2 WITH USER test");
    }

    @Test void testCreateGroupSuperuser() throws SQLException {
        exec("DROP ROLE IF EXISTS mygroup3");
        exec("CREATE GROUP mygroup3 SUPERUSER");
    }

    @Test void testAlterGroupAddUser() throws SQLException {
        exec("DROP ROLE IF EXISTS mygroup");
        exec("CREATE GROUP mygroup");
        exec("ALTER GROUP mygroup ADD USER test");
    }

    @Test void testAlterGroupDropUser() throws SQLException {
        exec("DROP ROLE IF EXISTS mygroup");
        exec("CREATE GROUP mygroup");
        exec("ALTER GROUP mygroup DROP USER test");
    }

    @Test void testAlterGroupRename() throws SQLException {
        exec("DROP ROLE IF EXISTS mygroup");
        exec("DROP ROLE IF EXISTS newgroup");
        exec("CREATE GROUP mygroup");
        exec("ALTER GROUP mygroup RENAME TO newgroup");
    }

    @Test void testDropGroup() throws SQLException {
        exec("DROP ROLE IF EXISTS mygroup");
        exec("CREATE GROUP mygroup");
        exec("DROP GROUP mygroup");
    }

    @Test void testDropGroupIfExists() throws SQLException {
        exec("DROP GROUP IF EXISTS mygroup");
    }

    // ========================================================================
    // Mixed / edge-case tests
    // ========================================================================

    @Test void testMultipleNoopStatementsInSequence() throws SQLException {
        // Verify multiple no-op statements work in sequence
        exec("CREATE FOREIGN DATA WRAPPER fdw1");
        exec("CREATE SERVER srv1 FOREIGN DATA WRAPPER fdw1");
        exec("CREATE USER MAPPING FOR current_user SERVER srv1");
        exec("CREATE FOREIGN TABLE ft_test (id int) SERVER srv1");
        exec("DROP FOREIGN TABLE IF EXISTS ft_test");
        exec("DROP USER MAPPING IF EXISTS FOR current_user SERVER srv1");
        exec("DROP SERVER IF EXISTS srv1");
        exec("DROP FOREIGN DATA WRAPPER IF EXISTS fdw1");
    }

    @Test void testCommentOnForeignObjects() throws SQLException {
        exec("COMMENT ON FOREIGN DATA WRAPPER myfdw IS 'My FDW'");
    }

    @Test void testCommentOnServer() throws SQLException {
        exec("COMMENT ON SERVER myserver IS 'My Server'");
    }

    @Test void testNoopsDontAffectRealTables() throws SQLException {
        // Ensure no-ops don't interfere with real table operations
        exec("CREATE TABLE noop_check (id serial PRIMARY KEY, val text)");
        exec("CREATE FOREIGN DATA WRAPPER test_fdw");
        exec("INSERT INTO noop_check (val) VALUES ('hello')");
        String result = query1("SELECT val FROM noop_check WHERE id = 1");
        assertEquals("hello", result);
        exec("DROP TABLE noop_check");
    }
}
