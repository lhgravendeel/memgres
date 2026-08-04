package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Whether a view accepts a write, and whether the catalog says the same thing the write does.
 *
 * <p>Two failures met here. A view containing a set-returning function was written through, so a
 * caller modified rows PostgreSQL would never have let it touch. And the catalog disagreed with
 * the executor about six other view shapes: {@code information_schema.views.is_updatable} said
 * YES for views whose DML memgres itself refused, {@code information_schema.columns.is_updatable}
 * was the constant YES for every column in the database, and the two functions a client actually
 * calls to check — {@code pg_relation_is_updatable} and {@code pg_column_is_updatable} — did not
 * exist. A tool that asks before writing was told the opposite of the truth.
 *
 * <p>The settings half is the same shape of problem one level down: {@code pg_settings} carried
 * no {@code extra_desc} at all, reported memgres's own environment as PostgreSQL's compiled-in
 * {@code boot_val}, was missing the planner knobs a test suite actually SETs, and let
 * {@code set_config} invent a parameter row no PostgreSQL server has.
 *
 * <p>Every expectation below was measured on a live PostgreSQL 18 server.
 */
class ViewUpdatabilityAndSettingsMetadataTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE vu_base (id int PRIMARY KEY, val text, n int)");
            s.execute("INSERT INTO vu_base VALUES (1,'a',1),(2,'b',2),(3,'c',3)");
            // Auto-updatable shapes.
            s.execute("CREATE VIEW vu_simple AS SELECT id, val, n FROM vu_base");
            s.execute("CREATE VIEW vu_where AS SELECT id, val, n FROM vu_base WHERE n > 0");
            s.execute("CREATE VIEW vu_order AS SELECT id, val, n FROM vu_base ORDER BY id");
            s.execute("CREATE VIEW vu_star AS SELECT * FROM vu_base");
            s.execute("CREATE VIEW vu_expr AS SELECT id, val, n * 2 AS n2 FROM vu_base");
            s.execute("CREATE VIEW vu_const AS SELECT id, val, 42 AS k FROM vu_base");
            // Shapes PostgreSQL refuses.
            s.execute("CREATE VIEW vu_distinct AS SELECT DISTINCT id, val, n FROM vu_base");
            s.execute("CREATE VIEW vu_group AS SELECT id, count(*) AS c FROM vu_base GROUP BY id");
            s.execute("CREATE VIEW vu_having AS SELECT id, count(*) AS c FROM vu_base"
                    + " GROUP BY id HAVING count(*) > 0");
            s.execute("CREATE VIEW vu_limit AS SELECT id, val, n FROM vu_base LIMIT 5");
            s.execute("CREATE VIEW vu_offset AS SELECT id, val, n FROM vu_base OFFSET 1");
            s.execute("CREATE VIEW vu_with AS WITH x AS (SELECT id, val, n FROM vu_base)"
                    + " SELECT id, val, n FROM x");
            s.execute("CREATE VIEW vu_window AS SELECT id, val, n, row_number() OVER () AS rn"
                    + " FROM vu_base");
            s.execute("CREATE VIEW vu_agg AS SELECT count(*) AS c FROM vu_base");
            s.execute("CREATE VIEW vu_srf AS SELECT id, val, generate_series(1,2) AS g FROM vu_base");
            s.execute("CREATE VIEW vu_union AS SELECT id, val, n FROM vu_base"
                    + " UNION ALL SELECT id, val, n FROM vu_base");
            s.execute("CREATE VIEW vu_nested AS SELECT id, val, n FROM vu_distinct");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    /** Run a statement expecting it to fail, and return its SQLSTATE plus first message line. */
    private static String refusal(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        } catch (SQLException e) {
            String message = e.getMessage();
            int newline = message.indexOf('\n');
            if (newline >= 0) message = message.substring(0, newline);
            return e.getSQLState() + " " + message.replace("ERROR: ", "");
        }
        return fail("expected " + sql + " to be refused");
    }

    // ---- the executor -------------------------------------------------------------------

    /**
     * The recorded failure: a view whose select list calls a set-returning function was written
     * through and the base table changed. PostgreSQL refuses all three writes with 55000.
     */
    @Test
    void setReturningFunctionInSelectListMakesAViewReadOnly() throws Exception {
        assertEquals("55000 cannot insert into view \"vu_srf\"",
                refusal("INSERT INTO vu_srf (id, val) VALUES (107,'ins')"));
        assertEquals("55000 cannot update view \"vu_srf\"",
                refusal("UPDATE vu_srf SET val = 'upd' WHERE id = 1"));
        assertEquals("55000 cannot delete from view \"vu_srf\"",
                refusal("DELETE FROM vu_srf WHERE id = 1"));
        assertEquals("3", one("SELECT count(*)::text FROM vu_base"));
        assertEquals("a", one("SELECT val FROM vu_base WHERE id = 1"));
    }

    /** The reason travels with the refusal: PostgreSQL names the clause that caused it. */
    @Test
    void theRefusalSaysWhyAndWhatWouldMakeTheWritePossible() {
        String message = "";
        try (Statement s = conn.createStatement()) {
            s.execute("DELETE FROM vu_distinct WHERE id = 1");
            fail("expected a refusal");
        } catch (SQLException e) {
            message = e.getMessage();
        }
        assertTrue(message.contains("Views containing DISTINCT are not automatically updatable."),
                "no Detail naming DISTINCT: " + message);
        assertTrue(message.contains("provide an INSTEAD OF DELETE trigger"),
                "no Hint naming the trigger: " + message);
    }

    /** Each shape PostgreSQL lists is refused, and the base table is left alone. */
    @Test
    void everyNonUpdatableShapeIsRefused() throws Exception {
        assertEquals("55000 cannot delete from view \"vu_distinct\"",
                refusal("DELETE FROM vu_distinct WHERE id = 1"));
        assertEquals("55000 cannot insert into view \"vu_group\"",
                refusal("INSERT INTO vu_group (id) VALUES (9)"));
        assertEquals("55000 cannot insert into view \"vu_having\"",
                refusal("INSERT INTO vu_having (id) VALUES (9)"));
        assertEquals("55000 cannot insert into view \"vu_limit\"",
                refusal("INSERT INTO vu_limit (id, val, n) VALUES (9,'x',1)"));
        assertEquals("55000 cannot insert into view \"vu_offset\"",
                refusal("INSERT INTO vu_offset (id, val, n) VALUES (9,'x',1)"));
        assertEquals("55000 cannot insert into view \"vu_with\"",
                refusal("INSERT INTO vu_with (id, val, n) VALUES (9,'x',1)"));
        assertEquals("55000 cannot insert into view \"vu_window\"",
                refusal("INSERT INTO vu_window (id, val, n) VALUES (9,'x',1)"));
        assertEquals("55000 cannot insert into view \"vu_agg\"",
                refusal("INSERT INTO vu_agg (c) VALUES (1)"));
        assertEquals("55000 cannot insert into view \"vu_union\"",
                refusal("INSERT INTO vu_union (id, val, n) VALUES (9,'x',1)"));
        // A view over a non-updatable view: PG blames the inner view, not the one written to.
        assertEquals("55000 cannot insert into view \"vu_distinct\"",
                refusal("INSERT INTO vu_nested (id, val, n) VALUES (9,'x',1)"));
        assertEquals("3", one("SELECT count(*)::text FROM vu_base"));
    }

    /** The converse: an auto-updatable view still takes the write. */
    @Test
    void anAutoUpdatableViewStillAcceptsDml() throws Exception {
        try (Statement s = conn.createStatement()) {
            assertEquals(1, s.executeUpdate("INSERT INTO vu_simple (id, val, n) VALUES (50,'ins',5)"));
            assertEquals(1, s.executeUpdate("UPDATE vu_where SET val = 'w' WHERE id = 50"));
            assertEquals(1, s.executeUpdate("UPDATE vu_order SET n = 6 WHERE id = 50"));
            assertEquals(1, s.executeUpdate("UPDATE vu_star SET n = 7 WHERE id = 50"));
            assertEquals(1, s.executeUpdate("UPDATE vu_expr SET val = 'e' WHERE id = 50"));
            assertEquals(1, s.executeUpdate("UPDATE vu_const SET val = 'k' WHERE id = 50"));
            assertEquals(1, s.executeUpdate("DELETE FROM vu_simple WHERE id = 50"));
        }
        assertEquals("3", one("SELECT count(*)::text FROM vu_base"));
    }

    /**
     * A column of an updatable view that is not a column of the base relation: the relation
     * takes the write, that one column does not, and PostgreSQL says so with 0A000 rather than
     * claiming the column is missing.
     */
    @Test
    void aComputedViewColumnIsRefusedByName() {
        assertEquals("0A000 cannot insert into column \"n2\" of view \"vu_expr\"",
                refusal("INSERT INTO vu_expr (id, val, n2) VALUES (108,'ins',4)"));
        assertEquals("0A000 cannot update column \"n2\" of view \"vu_expr\"",
                refusal("UPDATE vu_expr SET n2 = 4 WHERE id = 1"));
        assertEquals("0A000 cannot insert into column \"k\" of view \"vu_const\"",
                refusal("INSERT INTO vu_const (id, val, k) VALUES (109,'ins',1)"));
    }

    /** An INSTEAD OF trigger takes the write its own event names, and no other. */
    @Test
    void anInsteadOfTriggerOpensOnlyTheWriteItWasDeclaredFor() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE vu_log (what text)");
            s.execute("CREATE FUNCTION vu_trg() RETURNS trigger LANGUAGE plpgsql AS $$"
                    + " BEGIN INSERT INTO vu_log VALUES ('fired'); RETURN NEW; END $$");
            s.execute("CREATE VIEW vu_trigview AS SELECT DISTINCT id, val, n FROM vu_base");
            s.execute("CREATE TRIGGER vu_ti INSTEAD OF INSERT ON vu_trigview"
                    + " FOR EACH ROW EXECUTE FUNCTION vu_trg()");
        }
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO vu_trigview (id, val, n) VALUES (200,'t',1)");
        }
        assertEquals("1", one("SELECT count(*)::text FROM vu_log"));
        // No INSTEAD OF DELETE trigger: PostgreSQL still refuses the DELETE.
        assertEquals("55000 cannot delete from view \"vu_trigview\"",
                refusal("DELETE FROM vu_trigview WHERE id = 1"));
        // The catalog reports the trigger separately from auto-updatability.
        assertEquals("NO|NO|NO|NO|YES", one(
                "SELECT is_updatable || '|' || is_insertable_into || '|' || is_trigger_updatable"
                + " || '|' || is_trigger_deletable || '|' || is_trigger_insertable_into"
                + " FROM information_schema.views WHERE table_name = 'vu_trigview'"));
    }

    // ---- the catalog agreeing with it ---------------------------------------------------

    @Test
    void informationSchemaViewsReportsWhatTheExecutorDoes() throws Exception {
        assertEquals("YES|YES", isViews("vu_simple"));
        assertEquals("YES|YES", isViews("vu_where"));
        assertEquals("YES|YES", isViews("vu_order"));
        assertEquals("YES|YES", isViews("vu_star"));
        assertEquals("YES|YES", isViews("vu_expr"));
        assertEquals("YES|YES", isViews("vu_const"));
        assertEquals("NO|NO", isViews("vu_distinct"));
        assertEquals("NO|NO", isViews("vu_group"));
        assertEquals("NO|NO", isViews("vu_having"));
        assertEquals("NO|NO", isViews("vu_limit"));
        assertEquals("NO|NO", isViews("vu_offset"));
        assertEquals("NO|NO", isViews("vu_with"));
        assertEquals("NO|NO", isViews("vu_window"));
        assertEquals("NO|NO", isViews("vu_agg"));
        assertEquals("NO|NO", isViews("vu_srf"));
        assertEquals("NO|NO", isViews("vu_union"));
        assertEquals("NO|NO", isViews("vu_nested"));
    }

    private static String isViews(String view) throws SQLException {
        return one("SELECT is_updatable || '|' || is_insertable_into FROM information_schema.views"
                + " WHERE table_name = '" + view + "'");
    }

    @Test
    void informationSchemaTablesReportsTheSameInsertability() throws Exception {
        assertEquals("YES", one("SELECT is_insertable_into FROM information_schema.tables"
                + " WHERE table_name = 'vu_base'"));
        assertEquals("YES", one("SELECT is_insertable_into FROM information_schema.tables"
                + " WHERE table_name = 'vu_simple'"));
        assertEquals("NO", one("SELECT is_insertable_into FROM information_schema.tables"
                + " WHERE table_name = 'vu_agg'"));
        assertEquals("NO", one("SELECT is_insertable_into FROM information_schema.tables"
                + " WHERE table_name = 'vu_distinct'"));
    }

    /** Column-level updatability: the constant YES was hiding every one of these. */
    @Test
    void informationSchemaColumnsReportsPerColumnUpdatability() throws Exception {
        assertEquals("YES", isColumn("vu_base", "id"));
        assertEquals("YES", isColumn("vu_simple", "val"));
        assertEquals("YES", isColumn("vu_expr", "id"));
        assertEquals("YES", isColumn("vu_expr", "val"));
        assertEquals("NO", isColumn("vu_expr", "n2"));
        assertEquals("NO", isColumn("vu_const", "k"));
        assertEquals("NO", isColumn("vu_distinct", "id"));
        assertEquals("NO", isColumn("vu_agg", "c"));
        assertEquals("NO", isColumn("vu_window", "rn"));
        assertEquals("NO", isColumn("vu_union", "id"));
        assertEquals("NO", isColumn("vu_nested", "id"));
    }

    private static String isColumn(String table, String column) throws SQLException {
        return one("SELECT is_updatable FROM information_schema.columns WHERE table_name = '"
                + table + "' AND column_name = '" + column + "'");
    }

    /**
     * The two functions a client calls directly. PostgreSQL packs the events into a bitmask:
     * 4 UPDATE, 8 INSERT, 16 DELETE, so a table answers 28.
     */
    @Test
    void pgRelationIsUpdatableAnswersTheSameBitmaskPostgresDoes() throws Exception {
        assertEquals("28", one("SELECT pg_relation_is_updatable('vu_base'::regclass, false)::text"));
        assertEquals("28", one("SELECT pg_relation_is_updatable('vu_simple'::regclass, false)::text"));
        assertEquals("28", one("SELECT pg_relation_is_updatable('vu_expr'::regclass, false)::text"));
        assertEquals("28", one("SELECT pg_relation_is_updatable('vu_const'::regclass, false)::text"));
        assertEquals("28", one("SELECT pg_relation_is_updatable('vu_order'::regclass, false)::text"));
        assertEquals("0", one("SELECT pg_relation_is_updatable('vu_distinct'::regclass, false)::text"));
        assertEquals("0", one("SELECT pg_relation_is_updatable('vu_limit'::regclass, false)::text"));
        assertEquals("0", one("SELECT pg_relation_is_updatable('vu_with'::regclass, false)::text"));
        assertEquals("0", one("SELECT pg_relation_is_updatable('vu_window'::regclass, false)::text"));
        assertEquals("0", one("SELECT pg_relation_is_updatable('vu_agg'::regclass, false)::text"));
        assertEquals("0", one("SELECT pg_relation_is_updatable('vu_srf'::regclass, false)::text"));
        assertEquals("0", one("SELECT pg_relation_is_updatable('vu_nested'::regclass, false)::text"));
        // An oid that reaches no relation answers 0 rather than raising.
        assertEquals("0", one("SELECT pg_relation_is_updatable(0::oid, false)::text"));
    }

    @Test
    void pgColumnIsUpdatableAnswersPerColumn() throws Exception {
        assertEquals("true", one("SELECT pg_column_is_updatable('vu_simple'::regclass, 1::smallint, false)::text"));
        assertEquals("true", one("SELECT pg_column_is_updatable('vu_expr'::regclass, 2::smallint, false)::text"));
        assertEquals("false", one("SELECT pg_column_is_updatable('vu_expr'::regclass, 3::smallint, false)::text"));
        assertEquals("false", one("SELECT pg_column_is_updatable('vu_const'::regclass, 3::smallint, false)::text"));
        assertEquals("false", one("SELECT pg_column_is_updatable('vu_distinct'::regclass, 1::smallint, false)::text"));
        assertEquals("false", one("SELECT pg_column_is_updatable(0::oid, 1::smallint, false)::text"));
    }

    /** Both functions are declared where a client looks for them. */
    @Test
    void bothFunctionsAreListedInPgProc() throws Exception {
        assertEquals("2", one("SELECT count(*)::text FROM pg_proc"
                + " WHERE proname IN ('pg_relation_is_updatable','pg_column_is_updatable')"));
    }

    // ---- pg_settings --------------------------------------------------------------------

    private static String setting(String name, String column) throws SQLException {
        return one("SELECT " + column + "::text FROM pg_settings WHERE name = '" + name + "'");
    }

    /** Each setting carries its own metadata, not the metadata of settings in general. */
    @Test
    void namedSettingsCarryTheirOwnMetadata() throws Exception {
        assertEquals("bool", setting("array_nulls", "vartype"));
        assertEquals("user", setting("array_nulls", "context"));
        assertEquals("Version and Platform Compatibility / Previous PostgreSQL Versions",
                setting("array_nulls", "category"));
        assertEquals("integer", setting("work_mem", "vartype"));
        assertEquals("kB", setting("work_mem", "unit"));
        assertEquals("64", setting("work_mem", "min_val"));
        assertEquals("2147483647", setting("work_mem", "max_val"));
        assertEquals("enum", setting("constraint_exclusion", "vartype"));
        assertEquals("{partition,on,off}", setting("constraint_exclusion", "enumvals"));
        assertEquals("postmaster", setting("huge_pages", "context"));
        assertEquals("internal", setting("block_size", "context"));
        assertEquals("real", setting("bgwriter_lru_multiplier", "vartype"));
    }

    /**
     * extra_desc — the sentence that says what 0 or -1 means for a parameter that gives them a
     * special meaning. It was null for every setting memgres carried.
     */
    @Test
    void extraDescIsThePostgresSentence() throws Exception {
        assertEquals("When turned on, unquoted NULL in an array input value means a null value;"
                + " otherwise it is taken literally.", setting("array_nulls", "extra_desc"));
        assertEquals("This much memory can be used by each internal sort operation and hash table"
                + " before switching to temporary disk files.", setting("work_mem", "extra_desc"));
        assertEquals("0 disables the timeout.", setting("statement_timeout", "extra_desc"));
        assertEquals("0 disables the timeout.", setting("lock_timeout", "extra_desc"));
        assertEquals("This includes operations such as VACUUM and CREATE INDEX.",
                setting("maintenance_work_mem", "extra_desc"));
        assertEquals("Also controls interpretation of ambiguous date inputs.",
                setting("DateStyle", "extra_desc"));
        // A setting PostgreSQL leaves without one keeps it null.
        assertNull(setting("block_size", "extra_desc"));
        assertNull(setting("enable_bitmapscan", "extra_desc"));
    }

    /** boot_val is the compiled-in default, which does not vary with the machine. */
    @Test
    void bootValIsPostgresCompiledInDefault() throws Exception {
        assertEquals("SQL_ASCII", setting("client_encoding", "boot_val"));
        assertEquals("SQL_ASCII", setting("server_encoding", "boot_val"));
        assertEquals("GMT", setting("TimeZone", "boot_val"));
        assertEquals("", setting("application_name", "boot_val"));
        assertEquals("C", setting("lc_monetary", "boot_val"));
        assertEquals("C", setting("lc_numeric", "boot_val"));
        assertEquals("C", setting("lc_time", "boot_val"));
        assertEquals("0", setting("bgwriter_flush_after", "boot_val"));
        assertEquals("0", setting("checkpoint_flush_after", "boot_val"));
        assertEquals("100", setting("max_stack_depth", "boot_val"));
        assertEquals("-1", setting("wal_buffers", "boot_val"));
        // reset_val stays the value this server would actually go back to.
        assertEquals("UTC", setting("TimeZone", "reset_val"));
        assertEquals("UTF8", setting("client_encoding", "reset_val"));
        assertEquals("2048", setting("max_stack_depth", "reset_val"));
    }

    /** The planner knobs a test suite SETs are present, with PostgreSQL's own bounds. */
    @Test
    void thePlannerKnobsATestSuiteSetsExist() throws Exception {
        assertEquals("on", setting("enable_bitmapscan", "setting"));
        assertEquals("on", setting("enable_indexonlyscan", "setting"));
        assertEquals("on", setting("enable_sort", "setting"));
        assertEquals("on", setting("enable_material", "setting"));
        assertEquals("on", setting("enable_memoize", "setting"));
        assertEquals("on", setting("enable_tidscan", "setting"));
        assertEquals("on", setting("enable_partition_pruning", "setting"));
        assertEquals("off", setting("enable_partitionwise_aggregate", "setting"));
        assertEquals("100", setting("default_statistics_target", "setting"));
        assertEquals("1", setting("default_statistics_target", "min_val"));
        assertEquals("10000", setting("default_statistics_target", "max_val"));
        assertEquals("8", setting("from_collapse_limit", "setting"));
        assertEquals("8", setting("join_collapse_limit", "setting"));
        assertEquals("on", setting("geqo", "setting"));
        assertEquals("12", setting("geqo_threshold", "setting"));
        assertEquals("partition", setting("constraint_exclusion", "setting"));
        assertEquals("0.1", setting("cursor_tuple_fraction", "setting"));
        assertEquals("off", setting("transform_null_equals", "setting"));
        assertEquals("stderr", setting("log_destination", "setting"));
        assertEquals("base64", setting("xmlbinary", "setting"));
        assertEquals("5432", setting("port", "setting"));
        assertEquals("Default", setting("timezone_abbreviations", "setting"));
        assertEquals("$libdir", setting("dynamic_library_path", "setting"));
    }

    /** SET, SHOW, current_setting and the pg_settings row are one answer, not four. */
    @Test
    void setAndShowAndTheCatalogRowAgree() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("SET enable_sort = off");
        }
        assertEquals("off", one("SHOW enable_sort"));
        assertEquals("off", one("SELECT current_setting('enable_sort')"));
        assertEquals("off", setting("enable_sort", "setting"));
        assertEquals("session", setting("enable_sort", "source"));
        assertEquals("on", setting("enable_sort", "reset_val"));
        try (Statement s = conn.createStatement()) {
            s.execute("RESET enable_sort");
        }
        assertEquals("on", one("SHOW enable_sort"));
        assertEquals("default", setting("enable_sort", "source"));
    }

    /** Each setting's declared bounds are the bounds SET is judged against. */
    @Test
    void aSettingIsAssignedOnlyWhatItsOwnMetadataAllows() {
        assertEquals("22023 parameter \"enable_sort\" requires a Boolean value",
                refusal("SET enable_sort = 'maybe'"));
        assertEquals("22023 20000 is outside the valid range for parameter"
                + " \"default_statistics_target\" (1 .. 10000)",
                refusal("SET default_statistics_target = 20000"));
        assertEquals("22023 invalid value for parameter \"constraint_exclusion\": \"sometimes\"",
                refusal("SET constraint_exclusion = 'sometimes'"));
        assertEquals("55P02 parameter \"port\" cannot be changed without restarting the server",
                refusal("SET port = 6000"));
        assertEquals("55P02 parameter \"max_connections\""
                + " cannot be changed without restarting the server",
                refusal("SET max_connections = 500"));
    }

    /** A parameter that does not exist behaves the way PostgreSQL makes it behave. */
    @Test
    void anUnrecognizedParameterIsRefusedRatherThanInvented() throws Exception {
        assertEquals("42704 unrecognized configuration parameter \"totally_bogus_guc\"",
                refusal("SET totally_bogus_guc = 5"));
        assertEquals("42704 unrecognized configuration parameter \"totally_bogus_guc\"",
                refusal("SELECT set_config('totally_bogus_guc','7',false)"));
        assertEquals("42704 unrecognized configuration parameter \"totally_bogus_guc\"",
                refusal("SHOW totally_bogus_guc"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_settings"
                + " WHERE name = 'totally_bogus_guc'"));
    }

    /**
     * A qualified custom parameter is accepted and readable, but PostgreSQL keeps it out of
     * pg_settings until an extension declares it — so listing one would invent a row.
     */
    @Test
    void aSessionCustomParameterIsReadableButNotListed() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("SET myapp.zzz = 'q'");
        }
        assertEquals("q", one("SELECT current_setting('myapp.zzz')"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_settings WHERE name = 'myapp.zzz'"));
        assertEquals("5", one("SELECT set_config('myapp.qqq','5',false)"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_settings WHERE name = 'myapp.qqq'"));
    }

    /** A transaction's own settings are imposed by the transaction, and pg_settings says so. */
    @Test
    void aTransactionScopedSettingReportsSourceOverride() throws Exception {
        assertEquals("override", setting("transaction_isolation", "source"));
    }

    // ---- the catalogs still answer when joined ------------------------------------------

    /** The queries a schema browser makes over these catalogs still return rows. */
    @Test
    void catalogsStillJoinAfterTheChange() throws Exception {
        assertEquals("3", one(
                "SELECT count(*)::text FROM information_schema.columns c"
                + " JOIN information_schema.tables t"
                + "   ON t.table_name = c.table_name AND t.table_schema = c.table_schema"
                + " WHERE c.table_name = 'vu_simple' AND t.is_insertable_into = 'YES'"));
        assertEquals("YES", one(
                "SELECT v.is_updatable FROM information_schema.views v"
                + " JOIN pg_class c ON c.relname = v.table_name"
                + " WHERE v.table_name = 'vu_simple'"));
        assertTrue(Integer.parseInt(one("SELECT count(*)::text FROM pg_settings s"
                + " WHERE s.vartype = 'bool'")) > 30);
    }
}
