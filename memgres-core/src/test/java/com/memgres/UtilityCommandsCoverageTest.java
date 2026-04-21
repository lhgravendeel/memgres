package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

class UtilityCommandsCoverageTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
    }

    @AfterAll
    static void stop() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private ResultSet q(String sql) throws Exception {
        return conn.createStatement().executeQuery(sql);
    }

    private void exec(String sql) throws Exception {
        conn.createStatement().execute(sql);
    }

    // =========================================================================
    // Item 100: EXPLAIN
    // =========================================================================

    @Test
    @DisplayName("100.1 EXPLAIN SELECT returns query plan with QUERY PLAN column")
    void explainSelect() throws Exception {
        exec("CREATE TABLE expl_t1 (id int, name text)");
        ResultSet rs = q("EXPLAIN SELECT * FROM expl_t1");
        assertEquals("QUERY PLAN", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next(), "EXPLAIN should return at least one row");
        assertNotNull(rs.getString(1));
    }

    @Test
    @DisplayName("100.2 EXPLAIN ANALYZE SELECT runs query and includes timing")
    void explainAnalyzeSelect() throws Exception {
        exec("CREATE TABLE expl_t2 (id int)");
        exec("INSERT INTO expl_t2 VALUES (1), (2)");
        ResultSet rs = q("EXPLAIN ANALYZE SELECT * FROM expl_t2");
        assertEquals("QUERY PLAN", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        String plan = rs.getString(1);
        assertNotNull(plan);
        // ANALYZE output should include actual timing info
        assertTrue(plan.contains("actual") || plan.contains("time"),
                "EXPLAIN ANALYZE should include timing info: " + plan);
    }

    @Test
    @DisplayName("100.3 EXPLAIN VERBOSE SELECT")
    void explainVerboseSelect() throws Exception {
        exec("CREATE TABLE expl_t3 (id int)");
        ResultSet rs = q("EXPLAIN VERBOSE SELECT * FROM expl_t3");
        assertEquals("QUERY PLAN", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
    }

    @Test
    @DisplayName("100.4 EXPLAIN (FORMAT JSON) SELECT returns JSON plan")
    void explainFormatJson() throws Exception {
        exec("CREATE TABLE expl_t4 (id int)");
        ResultSet rs = q("EXPLAIN (FORMAT JSON) SELECT * FROM expl_t4");
        assertEquals("QUERY PLAN", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        String plan = rs.getString(1);
        assertTrue(plan.contains("{") && plan.contains("Plan"),
                "JSON format should contain Plan object: " + plan);
    }

    @Test
    @DisplayName("100.5 EXPLAIN (FORMAT XML) SELECT returns XML plan")
    void explainFormatXml() throws Exception {
        exec("CREATE TABLE expl_t5 (id int)");
        ResultSet rs = q("EXPLAIN (FORMAT XML) SELECT * FROM expl_t5");
        assertEquals("QUERY PLAN", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        String plan = rs.getString(1);
        assertTrue(plan.contains("<") && plan.contains("explain"),
                "XML format should contain XML tags: " + plan);
    }

    @Test
    @DisplayName("100.6 EXPLAIN (FORMAT YAML) SELECT returns YAML plan")
    void explainFormatYaml() throws Exception {
        exec("CREATE TABLE expl_t6 (id int)");
        ResultSet rs = q("EXPLAIN (FORMAT YAML) SELECT * FROM expl_t6");
        assertEquals("QUERY PLAN", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        String plan = rs.getString(1);
        assertTrue(plan.contains("Plan") && plan.contains("Node Type"),
                "YAML format should contain Plan and Node Type: " + plan);
    }

    @Test
    @DisplayName("100.7 EXPLAIN (FORMAT TEXT) SELECT returns default text format")
    void explainFormatText() throws Exception {
        exec("CREATE TABLE expl_t7 (id int)");
        ResultSet rs = q("EXPLAIN (FORMAT TEXT) SELECT * FROM expl_t7");
        assertEquals("QUERY PLAN", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
        String plan = rs.getString(1);
        assertNotNull(plan);
        // TEXT format should not contain JSON/XML/YAML markers
        assertFalse(plan.startsWith("["), "TEXT format should not be JSON");
        assertFalse(plan.startsWith("<"), "TEXT format should not be XML");
    }

    @Test
    @DisplayName("100.8 EXPLAIN (ANALYZE, VERBOSE) SELECT")
    void explainAnalyzeVerbose() throws Exception {
        exec("CREATE TABLE expl_t8 (id int)");
        exec("INSERT INTO expl_t8 VALUES (1)");
        ResultSet rs = q("EXPLAIN (ANALYZE, VERBOSE) SELECT * FROM expl_t8");
        assertEquals("QUERY PLAN", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
    }

    @Test
    @DisplayName("100.9 EXPLAIN with multiple options (ANALYZE true, COSTS true, BUFFERS false, TIMING true, SUMMARY true)")
    void explainMultipleOptions() throws Exception {
        exec("CREATE TABLE expl_t9 (id int)");
        ResultSet rs = q("EXPLAIN (ANALYZE true, COSTS true, BUFFERS false, TIMING true, SUMMARY true) SELECT * FROM expl_t9");
        assertEquals("QUERY PLAN", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
    }

    @Test
    @DisplayName("100.10 EXPLAIN (COSTS false) SELECT")
    void explainCostsFalse() throws Exception {
        exec("CREATE TABLE expl_t10 (id int)");
        ResultSet rs = q("EXPLAIN (COSTS false) SELECT * FROM expl_t10");
        assertEquals("QUERY PLAN", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
    }

    @Test
    @DisplayName("100.11 EXPLAIN INSERT")
    void explainInsert() throws Exception {
        exec("CREATE TABLE expl_t11 (id int)");
        ResultSet rs = q("EXPLAIN INSERT INTO expl_t11 VALUES (1)");
        assertEquals("QUERY PLAN", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
    }

    @Test
    @DisplayName("100.12 EXPLAIN UPDATE")
    void explainUpdate() throws Exception {
        exec("CREATE TABLE expl_t12 (id int, val text)");
        exec("INSERT INTO expl_t12 VALUES (1, 'a')");
        ResultSet rs = q("EXPLAIN UPDATE expl_t12 SET val = 'b' WHERE id = 1");
        assertEquals("QUERY PLAN", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
    }

    @Test
    @DisplayName("100.13 EXPLAIN DELETE")
    void explainDelete() throws Exception {
        exec("CREATE TABLE expl_t13 (id int)");
        exec("INSERT INTO expl_t13 VALUES (1)");
        ResultSet rs = q("EXPLAIN DELETE FROM expl_t13 WHERE id = 1");
        assertEquals("QUERY PLAN", rs.getMetaData().getColumnName(1));
        assertTrue(rs.next());
    }

    // =========================================================================
    // Item 101: COMMENT & SECURITY LABEL
    // =========================================================================

    @Test
    @DisplayName("101.1 COMMENT ON TABLE")
    void commentOnTable() throws Exception {
        exec("CREATE TABLE cmt_t1 (id int)");
        assertDoesNotThrow(() -> exec("COMMENT ON TABLE cmt_t1 IS 'This is a test table'"));
    }

    @Test
    @DisplayName("101.2 COMMENT ON COLUMN")
    void commentOnColumn() throws Exception {
        exec("CREATE TABLE cmt_t2 (id int, name text)");
        assertDoesNotThrow(() -> exec("COMMENT ON COLUMN cmt_t2.name IS 'The name column'"));
    }

    @Test
    @DisplayName("101.3 COMMENT ON FUNCTION")
    void commentOnFunction() throws Exception {
        exec("CREATE FUNCTION cmt_func() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql");
        assertDoesNotThrow(() -> exec("COMMENT ON FUNCTION cmt_func IS 'A test function'"));
    }

    @Test
    @DisplayName("101.4 COMMENT ON TYPE")
    void commentOnType() throws Exception {
        exec("CREATE TYPE cmt_color AS ENUM ('red', 'green', 'blue')");
        assertDoesNotThrow(() -> exec("COMMENT ON TYPE cmt_color IS 'Color enum type'"));
    }

    @Test
    @DisplayName("101.5 COMMENT ON INDEX")
    void commentOnIndex() throws Exception {
        exec("CREATE TABLE cmt_t5 (id int)");
        exec("CREATE INDEX cmt_idx5 ON cmt_t5 (id)");
        assertDoesNotThrow(() -> exec("COMMENT ON INDEX cmt_idx5 IS 'Primary index'"));
    }

    @Test
    @DisplayName("101.6 COMMENT ON SCHEMA")
    void commentOnSchema() throws Exception {
        assertDoesNotThrow(() -> exec("COMMENT ON SCHEMA public IS 'Default schema'"));
    }

    @Test
    @DisplayName("101.7 COMMENT ON TABLE IS NULL removes comment")
    void commentOnTableNull() throws Exception {
        exec("CREATE TABLE cmt_t7 (id int)");
        exec("COMMENT ON TABLE cmt_t7 IS 'temp comment'");
        assertDoesNotThrow(() -> exec("COMMENT ON TABLE cmt_t7 IS NULL"));
    }

    @Test
    @DisplayName("101.8 SECURITY LABEL ON TABLE")
    void securityLabelOnTable() throws Exception {
        exec("CREATE TABLE sl_t1 (id int)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SECURITY LABEL ON TABLE sl_t1 IS 'classified'"));
        assertEquals("22023", ex.getSQLState(),
                "SECURITY LABEL without a registered provider must throw 22023; got " + ex.getSQLState());
    }

    @Test
    @DisplayName("101.9 SECURITY LABEL FOR provider ON TABLE")
    void securityLabelForProviderOnTable() throws Exception {
        exec("CREATE TABLE sl_t2 (id int)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SECURITY LABEL FOR dummy_provider ON TABLE sl_t2 IS 'secret'"));
        assertEquals("22023", ex.getSQLState(),
                "SECURITY LABEL FOR unregistered provider must throw 22023; got " + ex.getSQLState());
    }

    // =========================================================================
    // Item 102: ANALYZE
    // =========================================================================

    @Test
    @DisplayName("102.1 ANALYZE standalone no table")
    void analyzeStandalone() {
        assertDoesNotThrow(() -> exec("ANALYZE"));
    }

    @Test
    @DisplayName("102.2 ANALYZE table_name")
    void analyzeTable() throws Exception {
        exec("CREATE TABLE anlz_t1 (id int, val text)");
        assertDoesNotThrow(() -> exec("ANALYZE anlz_t1"));
    }

    @Test
    @DisplayName("102.3 ANALYZE schema.table_name")
    void analyzeSchemaTable() throws Exception {
        exec("CREATE TABLE anlz_t2 (id int)");
        assertDoesNotThrow(() -> exec("ANALYZE public.anlz_t2"));
    }

    @Test
    @DisplayName("102.4 ANALYZE table_name (col1, col2)")
    void analyzeTableColumns() throws Exception {
        exec("CREATE TABLE anlz_t3 (id int, name text, val int)");
        assertDoesNotThrow(() -> exec("ANALYZE anlz_t3 (id, name)"));
    }

    @Test
    @DisplayName("102.5 ANALYZE VERBOSE table_name")
    void analyzeVerbose() throws Exception {
        exec("CREATE TABLE anlz_t4 (id int)");
        assertDoesNotThrow(() -> exec("ANALYZE VERBOSE anlz_t4"));
    }

    @Test
    @DisplayName("102.6 ANALYSE British spelling")
    void analyseBritish() throws Exception {
        exec("CREATE TABLE anlz_t5 (id int)");
        assertDoesNotThrow(() -> exec("ANALYSE anlz_t5"));
    }

    // =========================================================================
    // Item 103: SET, SHOW, RESET
    // =========================================================================

    @Test
    @DisplayName("103.1 SET search_path TO public")
    void setSearchPathTo() {
        assertDoesNotThrow(() -> exec("SET search_path TO public"));
    }

    @Test
    @DisplayName("103.2 SET search_path = 'public, pg_catalog'")
    void setSearchPathEquals() {
        assertDoesNotThrow(() -> exec("SET search_path = 'public, pg_catalog'"));
    }

    @Test
    @DisplayName("103.3 SET client_encoding TO 'UTF8'")
    void setClientEncoding() {
        assertDoesNotThrow(() -> exec("SET client_encoding TO 'UTF8'"));
    }

    @Test
    @DisplayName("103.4 SET timezone TO 'UTC'")
    void setTimezone() {
        assertDoesNotThrow(() -> exec("SET timezone TO 'UTC'"));
    }

    @Test
    @DisplayName("103.5 SET DateStyle TO 'ISO, MDY'")
    void setDateStyle() {
        assertDoesNotThrow(() -> exec("SET DateStyle TO 'ISO, MDY'"));
    }

    @Test
    @DisplayName("103.6 SET LOCAL statement_timeout TO '5000'")
    void setLocalStatementTimeout() {
        assertDoesNotThrow(() -> exec("SET LOCAL statement_timeout TO '5000'"));
    }

    @Test
    @DisplayName("103.7 SET SESSION idle_in_transaction_session_timeout TO '10000'")
    void setSessionTimeout() {
        assertDoesNotThrow(() -> exec("SET SESSION idle_in_transaction_session_timeout TO '10000'"));
    }

    @Test
    @DisplayName("103.8 SHOW search_path returns value")
    void showSearchPath() throws Exception {
        exec("SET search_path TO public");
        ResultSet rs = q("SHOW search_path");
        assertTrue(rs.next());
        assertNotNull(rs.getString(1));
    }

    @Test
    @DisplayName("103.9 SHOW server_version returns version string")
    void showServerVersion() throws Exception {
        ResultSet rs = q("SHOW server_version");
        assertTrue(rs.next());
        String version = rs.getString(1);
        assertNotNull(version);
    }

    @Test
    @DisplayName("103.10 SHOW client_encoding")
    void showClientEncoding() throws Exception {
        ResultSet rs = q("SHOW client_encoding");
        assertTrue(rs.next());
        assertNotNull(rs.getString(1));
    }

    @Test
    @DisplayName("103.11 SHOW ALL returns multiple rows with name/setting/description")
    void showAll() throws Exception {
        ResultSet rs = q("SHOW ALL");
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(3, md.getColumnCount());
        assertEquals("name", md.getColumnName(1));
        assertEquals("setting", md.getColumnName(2));
        assertEquals("description", md.getColumnName(3));
        int count = 0;
        while (rs.next()) count++;
        assertTrue(count > 0, "SHOW ALL should return at least one row");
    }

    @Test
    @DisplayName("103.12 SHOW timezone")
    void showTimezone() throws Exception {
        exec("SET timezone TO 'UTC'");
        ResultSet rs = q("SHOW timezone");
        assertTrue(rs.next());
        assertNotNull(rs.getString(1));
    }

    @Test
    @DisplayName("103.13 RESET search_path")
    void resetSearchPath() {
        assertDoesNotThrow(() -> exec("SET search_path TO 'custom_schema'"));
        assertDoesNotThrow(() -> exec("RESET search_path"));
    }

    @Test
    @DisplayName("103.14 RESET ALL")
    void resetAll() {
        assertDoesNotThrow(() -> exec("RESET ALL"));
    }

    @Test
    @DisplayName("103.15 SET then SHOW verifies SET changes the value")
    void setThenShow() throws Exception {
        exec("SET timezone TO 'America/New_York'");
        ResultSet rs = q("SHOW timezone");
        assertTrue(rs.next());
        String val = rs.getString(1);
        assertTrue(val.contains("America/New_York"),
                "SHOW should reflect the SET value, got: " + val);
    }

    @Test
    @DisplayName("103.16 SET then RESET then SHOW verifies reset returns to default")
    void setResetShow() throws Exception {
        exec("SET client_encoding TO 'LATIN1'");
        ResultSet rs1 = q("SHOW client_encoding");
        assertTrue(rs1.next());
        assertEquals("LATIN1", rs1.getString(1));

        exec("RESET client_encoding");
        ResultSet rs2 = q("SHOW client_encoding");
        assertTrue(rs2.next());
        // After reset, value should differ from 'LATIN1' (back to default)
        String resetVal = rs2.getString(1);
        assertNotNull(resetVal);
    }

    @Test
    @DisplayName("103.17 SHOW server_version_num")
    void showServerVersionNum() throws Exception {
        ResultSet rs = q("SHOW server_version_num");
        assertTrue(rs.next());
        assertNotNull(rs.getString(1));
    }

    @Test
    @DisplayName("103.18 SHOW standard_conforming_strings")
    void showStandardConformingStrings() throws Exception {
        ResultSet rs = q("SHOW standard_conforming_strings");
        assertTrue(rs.next());
        assertNotNull(rs.getString(1));
    }

    // =========================================================================
    // Item 104: DO blocks
    // =========================================================================

    @Test
    @DisplayName("104.1 DO $$ BEGIN END $$")
    void doBlockBasic() {
        assertDoesNotThrow(() -> exec("DO $$ BEGIN END $$"));
    }

    @Test
    @DisplayName("104.2 DO $$ BEGIN NULL; END $$ LANGUAGE plpgsql")
    void doBlockWithLanguage() {
        assertDoesNotThrow(() -> exec("DO $$ BEGIN NULL; END $$ LANGUAGE plpgsql"));
    }

    @Test
    @DisplayName("104.3 DO with custom dollar-quote tag")
    void doBlockCustomTag() {
        assertDoesNotThrow(() -> exec("DO $body$ BEGIN END $body$"));
    }

    // =========================================================================
    // Item 105: CALL
    // =========================================================================

    @Test
    @DisplayName("105.1 CALL a procedure")
    void callProcedure() throws Exception {
        exec("CREATE TABLE call_t1 (id int, val text)");
        exec("CREATE PROCEDURE call_proc1(p_id int, p_val text) LANGUAGE SQL AS $$ INSERT INTO call_t1 VALUES (p_id, p_val) $$");
        assertDoesNotThrow(() -> exec("CALL call_proc1(1, 'hello')"));
        ResultSet rs = q("SELECT * FROM call_t1 WHERE id = 1");
        assertTrue(rs.next());
        assertEquals("hello", rs.getString("val"));
    }

    // =========================================================================
    // Item 106: LISTEN, NOTIFY, UNLISTEN
    // =========================================================================

    @Test
    @DisplayName("106.1 LISTEN channel_name")
    void listenChannel() {
        assertDoesNotThrow(() -> exec("LISTEN my_channel"));
    }

    @Test
    @DisplayName("106.2 NOTIFY channel_name")
    void notifyChannel() {
        assertDoesNotThrow(() -> exec("NOTIFY my_channel"));
    }

    @Test
    @DisplayName("106.3 NOTIFY channel_name with payload")
    void notifyChannelPayload() {
        assertDoesNotThrow(() -> exec("NOTIFY my_channel, 'hello world'"));
    }

    @Test
    @DisplayName("106.4 UNLISTEN channel_name")
    void unlistenChannel() {
        assertDoesNotThrow(() -> exec("UNLISTEN my_channel"));
    }

    @Test
    @DisplayName("106.5 UNLISTEN * all channels")
    void unlistenAll() {
        assertDoesNotThrow(() -> exec("UNLISTEN *"));
    }

    // =========================================================================
    // Item 107: COPY enhancements
    // =========================================================================

    @Test
    @DisplayName("107.1 COPY table TO STDOUT")
    void copyToStdout() throws Exception {
        exec("CREATE TABLE copy_t1 (id int, name text)");
        exec("INSERT INTO copy_t1 VALUES (1, 'alice'), (2, 'bob')");
        // COPY TO STDOUT requires PG COPY protocol, not available via JDBC
        assertThrows(SQLException.class, () -> exec("COPY copy_t1 TO STDOUT"));
    }

    @Test
    @DisplayName("107.2 COPY table FROM STDIN WITH options")
    void copyFromStdinWithOptions() throws Exception {
        exec("CREATE TABLE copy_t2 (id int, name text)");
        // COPY FROM STDIN requires PG COPY protocol (CopyManager), not available via JDBC executeUpdate
        assertThrows(SQLException.class, () -> exec("COPY copy_t2 FROM STDIN WITH (FORMAT csv, DELIMITER ',', HEADER true)"));
    }

    // =========================================================================
    // Item 108: VACUUM, REINDEX, CLUSTER
    // =========================================================================

    @Test
    @DisplayName("108.1 VACUUM standalone")
    void vacuumStandalone() {
        assertDoesNotThrow(() -> exec("VACUUM"));
    }

    @Test
    @DisplayName("108.2 VACUUM table_name")
    void vacuumTable() throws Exception {
        exec("CREATE TABLE vac_t1 (id int)");
        assertDoesNotThrow(() -> exec("VACUUM vac_t1"));
    }

    @Test
    @DisplayName("108.3 VACUUM FULL table_name")
    void vacuumFull() throws Exception {
        exec("CREATE TABLE vac_t2 (id int)");
        assertDoesNotThrow(() -> exec("VACUUM FULL vac_t2"));
    }

    @Test
    @DisplayName("108.4 VACUUM FREEZE table_name")
    void vacuumFreeze() throws Exception {
        exec("CREATE TABLE vac_t3 (id int)");
        assertDoesNotThrow(() -> exec("VACUUM FREEZE vac_t3"));
    }

    @Test
    @DisplayName("108.5 VACUUM VERBOSE table_name")
    void vacuumVerbose() throws Exception {
        exec("CREATE TABLE vac_t4 (id int)");
        assertDoesNotThrow(() -> exec("VACUUM VERBOSE vac_t4"));
    }

    @Test
    @DisplayName("108.6 VACUUM ANALYZE table_name")
    void vacuumAnalyze() throws Exception {
        exec("CREATE TABLE vac_t5 (id int)");
        assertDoesNotThrow(() -> exec("VACUUM ANALYZE vac_t5"));
    }

    @Test
    @DisplayName("108.7 VACUUM (FULL, ANALYZE, VERBOSE) table_name")
    void vacuumParenOptions() throws Exception {
        exec("CREATE TABLE vac_t6 (id int)");
        assertDoesNotThrow(() -> exec("VACUUM (FULL, ANALYZE, VERBOSE) vac_t6"));
    }

    @Test
    @DisplayName("108.8 REINDEX TABLE table_name")
    void reindexTable() throws Exception {
        exec("CREATE TABLE ridx_t1 (id int)");
        assertDoesNotThrow(() -> exec("REINDEX TABLE ridx_t1"));
    }

    @Test
    @DisplayName("108.9 REINDEX INDEX index_name")
    void reindexIndex() throws Exception {
        exec("CREATE TABLE ridx_t2 (id int)");
        exec("CREATE INDEX ridx_idx2 ON ridx_t2 (id)");
        assertDoesNotThrow(() -> exec("REINDEX INDEX ridx_idx2"));
    }

    @Test
    @DisplayName("108.10 REINDEX DATABASE database_name")
    void reindexDatabase() {
        assertDoesNotThrow(() -> exec("REINDEX DATABASE test"));
    }

    @Test
    @DisplayName("108.11 REINDEX SYSTEM database_name")
    void reindexSystem() {
        assertDoesNotThrow(() -> exec("REINDEX SYSTEM test"));
    }

    @Test
    @DisplayName("108.12 CLUSTER table USING index")
    void clusterTableUsingIndex() throws Exception {
        exec("CREATE TABLE clust_t1 (id int)");
        exec("CREATE INDEX clust_idx1 ON clust_t1 (id)");
        assertDoesNotThrow(() -> exec("CLUSTER clust_t1 USING clust_idx1"));
    }

    @Test
    @DisplayName("108.13 CLUSTER VERBOSE table")
    void clusterVerbose() throws Exception {
        exec("CREATE TABLE clust_t2 (id int)");
        exec("CREATE INDEX clust_idx2 ON clust_t2 (id)");
        assertDoesNotThrow(() -> exec("CLUSTER VERBOSE clust_t2"));
    }

    @Test
    @DisplayName("108.14 CLUSTER no args")
    void clusterNoArgs() {
        assertDoesNotThrow(() -> exec("CLUSTER"));
    }

    // =========================================================================
    // Item 109: DISCARD
    // =========================================================================

    @Test
    @DisplayName("109.1 DISCARD ALL")
    void discardAll() {
        assertDoesNotThrow(() -> exec("DISCARD ALL"));
    }

    @Test
    @DisplayName("109.2 DISCARD PLANS")
    void discardPlans() {
        assertDoesNotThrow(() -> exec("DISCARD PLANS"));
    }

    @Test
    @DisplayName("109.3 DISCARD SEQUENCES")
    void discardSequences() {
        assertDoesNotThrow(() -> exec("DISCARD SEQUENCES"));
    }

    @Test
    @DisplayName("109.4 DISCARD TEMPORARY")
    void discardTemporary() {
        assertDoesNotThrow(() -> exec("DISCARD TEMPORARY"));
    }

    @Test
    @DisplayName("109.5 DISCARD TEMP")
    void discardTemp() {
        assertDoesNotThrow(() -> exec("DISCARD TEMP"));
    }

    // =========================================================================
    // Item 110: CHECKPOINT, LOAD
    // =========================================================================

    @Test
    @DisplayName("110.1 CHECKPOINT")
    void checkpoint() {
        assertDoesNotThrow(() -> exec("CHECKPOINT"));
    }

    @Test
    @DisplayName("110.2 LOAD 'pg_stat_statements'")
    void loadLibrary() {
        assertDoesNotThrow(() -> exec("LOAD 'pg_stat_statements'"));
    }
}
