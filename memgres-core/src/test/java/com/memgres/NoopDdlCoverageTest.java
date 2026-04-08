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

    // ALTER FOREIGN DATA WRAPPER
    @Test void testAlterForeignDataWrapper() throws SQLException {
        exec("ALTER FOREIGN DATA WRAPPER myfdw HANDLER newhandler");
    }

    @Test void testAlterForeignDataWrapperOptions() throws SQLException {
        exec("ALTER FOREIGN DATA WRAPPER myfdw OPTIONS (SET debug 'false')");
    }

    @Test void testAlterForeignDataWrapperOwner() throws SQLException {
        exec("ALTER FOREIGN DATA WRAPPER myfdw OWNER TO newowner");
    }

    @Test void testAlterForeignDataWrapperRename() throws SQLException {
        exec("ALTER FOREIGN DATA WRAPPER myfdw RENAME TO newfdw");
    }

    // DROP FOREIGN DATA WRAPPER
    @Test void testDropForeignDataWrapper() throws SQLException {
        exec("DROP FOREIGN DATA WRAPPER myfdw");
    }

    @Test void testDropForeignDataWrapperIfExists() throws SQLException {
        exec("DROP FOREIGN DATA WRAPPER IF EXISTS myfdw");
    }

    @Test void testDropForeignDataWrapperCascade() throws SQLException {
        exec("DROP FOREIGN DATA WRAPPER IF EXISTS myfdw CASCADE");
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

    // ALTER SERVER
    @Test void testAlterServer() throws SQLException {
        exec("ALTER SERVER myserver OPTIONS (SET host 'newhost')");
    }

    @Test void testAlterServerOwner() throws SQLException {
        exec("ALTER SERVER myserver OWNER TO newowner");
    }

    @Test void testAlterServerRename() throws SQLException {
        exec("ALTER SERVER myserver RENAME TO newserver");
    }

    @Test void testAlterServerVersion() throws SQLException {
        exec("ALTER SERVER myserver VERSION '2.0'");
    }

    // DROP SERVER
    @Test void testDropServer() throws SQLException {
        exec("DROP SERVER myserver");
    }

    @Test void testDropServerIfExists() throws SQLException {
        exec("DROP SERVER IF EXISTS myserver");
    }

    @Test void testDropServerCascade() throws SQLException {
        exec("DROP SERVER IF EXISTS myserver CASCADE");
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

    // DROP FOREIGN TABLE
    @Test void testDropForeignTable() throws SQLException {
        exec("DROP FOREIGN TABLE ft1");
    }

    @Test void testDropForeignTableIfExists() throws SQLException {
        exec("DROP FOREIGN TABLE IF EXISTS ft1");
    }

    @Test void testDropForeignTableCascade() throws SQLException {
        exec("DROP FOREIGN TABLE IF EXISTS ft1 CASCADE");
    }

    // IMPORT FOREIGN SCHEMA
    @Test void testImportForeignSchema() throws SQLException {
        exec("IMPORT FOREIGN SCHEMA remote_schema FROM SERVER myserver INTO public");
    }

    @Test void testImportForeignSchemaLimitTo() throws SQLException {
        exec("IMPORT FOREIGN SCHEMA remote_schema LIMIT TO (t1, t2) FROM SERVER myserver INTO public");
    }

    @Test void testImportForeignSchemaExcept() throws SQLException {
        exec("IMPORT FOREIGN SCHEMA remote_schema EXCEPT (t3) FROM SERVER myserver INTO public");
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

    // ALTER PUBLICATION
    @Test void testAlterPublication() throws SQLException {
        exec("ALTER PUBLICATION mypub ADD TABLE t4");
    }

    @Test void testAlterPublicationSetTable() throws SQLException {
        exec("ALTER PUBLICATION mypub SET TABLE t1, t2");
    }

    @Test void testAlterPublicationDropTable() throws SQLException {
        exec("ALTER PUBLICATION mypub DROP TABLE t1");
    }

    @Test void testAlterPublicationOwner() throws SQLException {
        exec("ALTER PUBLICATION mypub OWNER TO newowner");
    }

    @Test void testAlterPublicationRename() throws SQLException {
        exec("ALTER PUBLICATION mypub RENAME TO newpub");
    }

    // DROP PUBLICATION
    @Test void testDropPublication() throws SQLException {
        exec("DROP PUBLICATION mypub");
    }

    @Test void testDropPublicationIfExists() throws SQLException {
        exec("DROP PUBLICATION IF EXISTS mypub");
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

    // DROP SUBSCRIPTION
    @Test void testDropSubscription() throws SQLException {
        exec("DROP SUBSCRIPTION mysub");
    }

    @Test void testDropSubscriptionIfExists() throws SQLException {
        exec("DROP SUBSCRIPTION IF EXISTS mysub");
    }

    // ========================================================================
    // 120: Database Management
    // ========================================================================

    @Test void testCreateDatabase() throws SQLException {
        exec("CREATE DATABASE IF NOT EXISTS testcreatedb");
        exec("DROP DATABASE IF EXISTS testcreatedb");
    }

    @Test void testCreateDatabaseWithOptions() throws SQLException {
        exec("CREATE DATABASE IF NOT EXISTS testcreateoptdb OWNER test ENCODING 'UTF8' LC_COLLATE 'en_US.UTF-8'");
        exec("DROP DATABASE IF EXISTS testcreateoptdb");
    }

    @Test void testCreateDatabaseTemplate() throws SQLException {
        exec("CREATE DATABASE IF NOT EXISTS testcreatetpldb TEMPLATE template0");
        exec("DROP DATABASE IF EXISTS testcreatetpldb");
    }

    @Test void testCreateDatabaseConnectionLimit() throws SQLException {
        exec("CREATE DATABASE IF NOT EXISTS testcreateconlimdb CONNECTION LIMIT 100");
        exec("DROP DATABASE IF EXISTS testcreateconlimdb");
    }

    @Test void testAlterDatabase() throws SQLException {
        exec("CREATE DATABASE IF NOT EXISTS testalterdb");
        exec("ALTER DATABASE testalterdb SET timezone TO 'UTC'");
        exec("DROP DATABASE IF EXISTS testalterdb");
    }

    @Test void testAlterDatabaseOwner() throws SQLException {
        exec("CREATE DATABASE IF NOT EXISTS testalterownerdb");
        exec("ALTER DATABASE testalterownerdb OWNER TO newowner");
        exec("DROP DATABASE IF EXISTS testalterownerdb");
    }

    @Test void testAlterDatabaseRename() throws SQLException {
        exec("CREATE DATABASE IF NOT EXISTS testalterrenamedb");
        exec("ALTER DATABASE testalterrenamedb RENAME TO testalterrenamed2db");
        exec("DROP DATABASE IF EXISTS testalterrenamed2db");
    }

    @Test void testAlterDatabaseConnectionLimit() throws SQLException {
        exec("CREATE DATABASE IF NOT EXISTS testalterconlimdb");
        exec("ALTER DATABASE testalterconlimdb CONNECTION LIMIT 50");
        exec("DROP DATABASE IF EXISTS testalterconlimdb");
    }

    @Test void testDropDatabase() throws SQLException {
        exec("CREATE DATABASE IF NOT EXISTS testdropdb");
        exec("DROP DATABASE testdropdb");
    }

    @Test void testDropDatabaseIfExists() throws SQLException {
        exec("DROP DATABASE IF EXISTS testdropifexistsdb");
    }

    @Test void testDropDatabaseForce() throws SQLException {
        exec("DROP DATABASE IF EXISTS testdropforcedb WITH (FORCE)");
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

    @Test void testAlterTablespace() throws SQLException {
        exec("ALTER TABLESPACE myts RENAME TO newts");
    }

    @Test void testAlterTablespaceOwner() throws SQLException {
        exec("ALTER TABLESPACE myts OWNER TO newowner");
    }

    @Test void testAlterTablespaceSet() throws SQLException {
        exec("ALTER TABLESPACE myts SET (seq_page_cost = 1.5)");
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

    @Test void testCreateEventTrigger() throws SQLException {
        exec("CREATE EVENT TRIGGER mytrigger ON ddl_command_start EXECUTE FUNCTION my_func()");
    }

    @Test void testCreateEventTriggerWithFilter() throws SQLException {
        exec("CREATE EVENT TRIGGER mytrigger ON ddl_command_end WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION my_func()");
    }

    @Test void testCreateEventTriggerSqlDrop() throws SQLException {
        exec("CREATE EVENT TRIGGER drop_trigger ON sql_drop EXECUTE FUNCTION my_drop_func()");
    }

    @Test void testCreateEventTriggerTableRewrite() throws SQLException {
        exec("CREATE EVENT TRIGGER rewrite_trigger ON table_rewrite EXECUTE FUNCTION my_rewrite_func()");
    }

    @Test void testAlterEventTrigger() throws SQLException {
        exec("ALTER EVENT TRIGGER mytrigger DISABLE");
    }

    @Test void testAlterEventTriggerEnable() throws SQLException {
        exec("ALTER EVENT TRIGGER mytrigger ENABLE");
    }

    @Test void testAlterEventTriggerEnableAlways() throws SQLException {
        exec("ALTER EVENT TRIGGER mytrigger ENABLE ALWAYS");
    }

    @Test void testAlterEventTriggerRename() throws SQLException {
        exec("ALTER EVENT TRIGGER mytrigger RENAME TO newtrigger");
    }

    @Test void testAlterEventTriggerOwner() throws SQLException {
        exec("ALTER EVENT TRIGGER mytrigger OWNER TO newowner");
    }

    @Test void testDropEventTrigger() throws SQLException {
        exec("DROP EVENT TRIGGER mytrigger");
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
        String result = query1("SELECT lo_unlink(1)");
        assertNotNull(result);
    }

    @Test void testLoGet() throws SQLException {
        // lo_get returns bytea
        ResultSet rs = stmt.executeQuery("SELECT lo_get(1)");
        assertTrue(rs.next());
        assertNotNull(rs.getObject(1));
    }

    @Test void testLoWrite() throws SQLException {
        String result = query1("SELECT lowrite(0, '\\x48656c6c6f'::bytea)");
        assertNotNull(result);
    }

    @Test void testLoRead() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT loread(0, 10)");
        assertTrue(rs.next());
        assertNotNull(rs.getObject(1));
    }

    @Test void testAlterLargeObject() throws SQLException {
        exec("ALTER LARGE OBJECT 12345 OWNER TO test");
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

    @Test void testCreateStatisticsBasic() throws SQLException {
        exec("CREATE TABLE stat_test (a int, b int, c text)");
        exec("CREATE STATISTICS mystat ON a, b FROM stat_test");
    }

    @Test void testCreateStatisticsNdistinct() throws SQLException {
        exec("CREATE STATISTICS mystat_nd (ndistinct) ON a, b FROM stat_test");
    }

    @Test void testCreateStatisticsDependencies() throws SQLException {
        exec("CREATE STATISTICS mystat_dep (dependencies) ON a, b FROM stat_test");
    }

    @Test void testCreateStatisticsMcv() throws SQLException {
        exec("CREATE STATISTICS mystat_mcv (mcv) ON a, b FROM stat_test");
    }

    @Test void testCreateStatisticsMultipleKinds() throws SQLException {
        exec("CREATE STATISTICS mystat_multi (ndistinct, dependencies, mcv) ON a, b, c FROM stat_test");
    }

    @Test void testCreateStatisticsIfNotExists() throws SQLException {
        exec("CREATE STATISTICS IF NOT EXISTS mystat ON a, b FROM stat_test");
    }

    @Test void testAlterStatistics() throws SQLException {
        exec("ALTER STATISTICS mystat SET STATISTICS 1000");
    }

    @Test void testAlterStatisticsOwner() throws SQLException {
        exec("ALTER STATISTICS mystat OWNER TO newowner");
    }

    @Test void testAlterStatisticsRename() throws SQLException {
        exec("ALTER STATISTICS mystat RENAME TO newstat");
    }

    @Test void testAlterStatisticsSchema() throws SQLException {
        exec("ALTER STATISTICS mystat SET SCHEMA public");
    }

    @Test void testDropStatistics() throws SQLException {
        exec("DROP STATISTICS mystat");
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
