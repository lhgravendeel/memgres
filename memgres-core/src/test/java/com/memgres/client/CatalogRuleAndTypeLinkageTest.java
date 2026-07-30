package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The catalogs described less than the engine held, in two places.
 *
 * <p>A rule written with CREATE RULE worked, but nothing in the catalogs said so: pg_rewrite
 * carried only the implicit _RETURN rules of views, pg_class.relhasrules stayed false, and
 * pg_get_ruledef had no row to be reached through. A tool asking what rules a relation carries was
 * told there were none.
 *
 * <p>And every composite type reported typarray = 0. In PostgreSQL a composite always has its
 * matching {@code _name} array type — a client following typarray to describe an array of a row
 * type found nothing at the far end.
 */
class CatalogRuleAndTypeLinkageTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    private static void freshTable() throws SQLException {
        exec("DROP TABLE IF EXISTS crl_r CASCADE");
        exec("CREATE TABLE crl_r (i int PRIMARY KEY, j int)");
    }

    @Test
    void aRuleIsARowInPgRewriteWithItsEventAndItsInsteadFlag() throws Exception {
        freshTable();
        exec("CREATE RULE crl_del AS ON DELETE TO crl_r DO INSTEAD NOTHING");
        exec("CREATE RULE crl_upd AS ON UPDATE TO crl_r DO INSTEAD NOTHING");

        assertEquals(List.of("crl_del|4|O|true", "crl_upd|2|O|true"),
                rows("SELECT rulename, ev_type, ev_enabled, is_instead::text FROM pg_rewrite "
                        + "WHERE ev_class = 'crl_r'::regclass ORDER BY rulename"),
                "DELETE is event 4 and UPDATE is 2, and both of these replace the statement");
    }

    @Test
    void relhasrulesSaysTheRelationCarriesOneAndKeepsSayingSo() throws Exception {
        freshTable();
        assertEquals(List.of("false"),
                rows("SELECT relhasrules::text FROM pg_class WHERE relname = 'crl_r'"));

        exec("CREATE RULE crl_del AS ON DELETE TO crl_r DO INSTEAD NOTHING");
        assertEquals(List.of("true"),
                rows("SELECT relhasrules::text FROM pg_class WHERE relname = 'crl_r'"));

        // PostgreSQL documents the flag as "has (or once had) rules" and only clears it at VACUUM.
        exec("DROP RULE crl_del ON crl_r");
        assertEquals(List.of("true"),
                rows("SELECT relhasrules::text FROM pg_class WHERE relname = 'crl_r'"),
                "the flag stands once a rule has existed");
        assertEquals(List.of("0"),
                rows("SELECT count(*) FROM pg_rewrite WHERE ev_class = 'crl_r'::regclass"),
                "but the rule itself is gone from pg_rewrite");
    }

    @Test
    void theDefinitionIsWrittenTheWayPostgresqlWritesIt() throws Exception {
        freshTable();
        exec("CREATE RULE crl_del AS ON DELETE TO crl_r DO INSTEAD NOTHING");

        String expected = "CREATE RULE crl_del AS\n    ON DELETE TO public.crl_r DO INSTEAD NOTHING;";
        assertEquals(List.of(expected),
                rows("SELECT definition FROM pg_rules WHERE tablename = 'crl_r'"),
                "header on its own line, event indented under it, relation schema-qualified");
        assertEquals(List.of(expected),
                rows("SELECT pg_get_ruledef(oid) FROM pg_rewrite "
                        + "WHERE ev_class = 'crl_r'::regclass"),
                "and pg_get_ruledef is reachable through the pg_rewrite row");
    }

    @Test
    void aViewsImplicitReturnRuleIsStillThere() throws Exception {
        exec("DROP VIEW IF EXISTS crl_v CASCADE");
        freshTable();
        exec("CREATE VIEW crl_v AS SELECT i FROM crl_r");

        assertEquals(List.of("_RETURN|1|true"),
                rows("SELECT rulename, ev_type, is_instead::text FROM pg_rewrite "
                        + "WHERE ev_class = 'crl_v'::regclass"),
                "adding user rules must not disturb the rule a view is made of");
        exec("DROP VIEW IF EXISTS crl_v CASCADE");
    }

    @Test
    void noCompositeTypeIsLeftWithoutItsArrayType() throws Exception {
        assertEquals(List.of("0"),
                rows("SELECT count(*) FROM pg_type WHERE typtype = 'c' AND typarray = 0"));
    }

    @Test
    void aCatalogRelationsRowTypeHasOneToo() throws Exception {
        assertEquals(List.of("_pg_class", "_pg_index", "_pg_proc", "_pg_type"),
                rows("SELECT typname FROM pg_type WHERE typname IN "
                        + "('_pg_class','_pg_proc','_pg_type','_pg_index') ORDER BY typname"));
        assertEquals(List.of("_pg_class|pg_class"),
                rows("SELECT t.typname, e.typname FROM pg_type t "
                        + "JOIN pg_type e ON e.oid = t.typelem WHERE t.typname = '_pg_class'"),
                "and typelem leads back to the composite");
    }

    @Test
    void aTableRowTypeAndAUserCompositeGetOneTheSameWay() throws Exception {
        exec("DROP TABLE IF EXISTS crl_t CASCADE");
        exec("CREATE TABLE crl_t (a int, b text)");
        assertEquals(List.of("true"),
                rows("SELECT (typarray <> 0)::text FROM pg_type WHERE typname = 'crl_t'"));
        assertEquals(List.of("_crl_t|b"),
                rows("SELECT typname, typtype FROM pg_type WHERE typname = '_crl_t'"),
                "an array type is a base type, category A");
        exec("DROP TABLE IF EXISTS crl_t CASCADE");

        exec("DROP TYPE IF EXISTS crl_ct CASCADE");
        exec("CREATE TYPE crl_ct AS (x int, y text)");
        assertEquals(List.of("_crl_ct|crl_ct"),
                rows("SELECT t.typname, e.typname FROM pg_type t "
                        + "JOIN pg_type e ON e.oid = t.typelem WHERE t.typname = '_crl_ct'"));
        exec("DROP TYPE IF EXISTS crl_ct CASCADE");
    }
}
