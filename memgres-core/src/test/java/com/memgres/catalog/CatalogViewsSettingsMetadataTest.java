package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The rest of what a tool reads before it does anything: which relations the catalog names, which
 * settings the server admits to having, and whether a view's columns may be null. A relation the
 * server can read from but does not list is one nothing will think to ask for, and a view column
 * wrongly marked NOT NULL makes a schema validator reject a mapping it should accept.
 * Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>D4 information_schema views, D5 catalog relations, D6 relkind, D7 pg_settings,
 * D9 view column nullability.
 */
class CatalogViewsSettingsMetadataTest {

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
            s.execute("CREATE TABLE cvs_t (id int PRIMARY KEY, name text NOT NULL, note text)");
            s.execute("CREATE VIEW cvs_v AS SELECT id, name FROM cvs_t");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static int count(String sql) throws SQLException {
        return Integer.parseInt(one(sql));
    }

    // ---- D5: the catalog names the relations the server can read ----

    @Test
    void theApplicationRelevantCatalogViewsAreListed() throws Exception {
        String[] views = {"pg_indexes", "pg_policies", "pg_rules", "pg_stats", "pg_user",
                          "pg_shadow", "pg_group", "pg_timezone_abbrevs", "pg_user_mappings",
                          "pg_publication_tables", "pg_stat_io", "pg_stat_archiver"};
        for (String v : views) {
            assertEquals(1, count(
                    "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace"
                  + " WHERE n.nspname='pg_catalog' AND c.relname='" + v + "'"),
                    v + " should be listed in pg_class");
        }
    }

    /** A relation the catalog names has to be one the server will actually read from. */
    @Test
    void aListedCatalogViewCanBeQueried() throws Exception {
        for (String v : new String[]{"pg_indexes", "pg_policies", "pg_rules", "pg_stats",
                                     "pg_user", "pg_stat_io"}) {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT * FROM " + v + " LIMIT 1")) {
                rs.next(); // reaching here at all is the point: the relation resolves
            }
        }
    }

    // ---- D6: an index is not a table ----

    /**
     * A handful of PG views are themselves named "..._index", so the question is about the
     * indexes rather than about the spelling.
     */
    @Test
    void catalogIndexesAreReportedAsIndexes() throws Exception {
        for (String idx : new String[]{"pg_class_oid_index", "pg_type_oid_index",
                                       "pg_proc_oid_index", "pg_namespace_oid_index"}) {
            assertEquals("i", one("SELECT relkind::text FROM pg_class WHERE relname='" + idx + "'"),
                    idx + " is an index, not a table");
        }
    }

    @Test
    void realCatalogTablesAreStillTables() throws Exception {
        assertEquals("r", one("SELECT relkind::text FROM pg_class WHERE relname='pg_class'"));
    }

    // ---- D4: information_schema describes itself ----

    @Test
    void informationSchemaListsItsOwnViews() throws Exception {
        assertTrue(count("SELECT count(*) FROM information_schema.tables"
                + " WHERE table_schema='information_schema'") >= 60);
    }

    @Test
    void theStandardViewsAreAmongThem() throws Exception {
        for (String v : new String[]{"tables", "columns", "views", "routines", "schemata",
                                     "table_constraints", "key_column_usage", "sql_features"}) {
            assertEquals(1, count("SELECT count(*) FROM information_schema.tables"
                    + " WHERE table_schema='information_schema' AND table_name='" + v + "'"),
                    v + " should be listed");
        }
    }

    @Test
    void theUsersOwnTablesAreStillListed() throws Exception {
        assertEquals(1, count("SELECT count(*) FROM information_schema.tables"
                + " WHERE table_schema='public' AND table_name='cvs_t'"));
    }

    // ---- D7: pg_settings admits the settings a client reads at startup ----

    @Test
    void theSettingsAClientReadsArePresent() throws Exception {
        for (String name : new String[]{"array_nulls", "backslash_quote", "block_size",
                                        "autovacuum", "archive_mode", "bgwriter_delay",
                                        "checkpoint_timeout", "wal_level", "max_wal_size"}) {
            assertEquals(1, count("SELECT count(*) FROM pg_settings WHERE name='" + name + "'"),
                    name + " should be a known setting");
        }
    }

    @Test
    void suchASettingCanBeReadWithCurrentSetting() throws Exception {
        assertEquals("on", one("SELECT current_setting('array_nulls')"));
        assertEquals("8192", one("SELECT current_setting('block_size')"));
    }

    @Test
    void anUnknownSettingIsStillUnknown() throws Exception {
        assertEquals(0, count("SELECT count(*) FROM pg_settings WHERE name='no_such_setting'"));
    }

    // ---- D9: a view column is nullable ----

    @Test
    void viewColumnsAreReportedNullable() throws Exception {
        assertEquals("false", one(
                "SELECT a.attnotnull::text FROM pg_attribute a"
              + " WHERE a.attrelid='cvs_v'::regclass AND a.attname='name'"));
    }

    @Test
    void baseTableColumnsKeepTheirConstraint() throws Exception {
        assertEquals("true", one(
                "SELECT a.attnotnull::text FROM pg_attribute a"
              + " WHERE a.attrelid='cvs_t'::regclass AND a.attname='name'"));
        assertEquals("false", one(
                "SELECT a.attnotnull::text FROM pg_attribute a"
              + " WHERE a.attrelid='cvs_t'::regclass AND a.attname='note'"));
    }

    @Test
    void theDriverReportsAViewColumnAsNullable() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, "public", "cvs_v", "name")) {
            assertTrue(rs.next(), "view column should be reported");
            assertEquals(DatabaseMetaData.columnNullable, rs.getInt("NULLABLE"));
            assertEquals("YES", rs.getString("IS_NULLABLE"));
        }
    }

    @Test
    void theDriverStillReportsTheBaseColumnAsNotNull() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, "public", "cvs_t", "name")) {
            assertTrue(rs.next(), "base column should be reported");
            assertEquals(DatabaseMetaData.columnNoNulls, rs.getInt("NULLABLE"));
            assertEquals("NO", rs.getString("IS_NULLABLE"));
        }
    }
}
