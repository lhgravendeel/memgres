package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What pg_catalog reports about itself and about what it holds.
 *
 * <p>The catalog did not describe itself: no pg_catalog relation had a single row in
 * pg_attribute, so anything that reads a relation's shape from the catalog rather than querying
 * it — a driver, an ORM's schema reflection, psql's {@code \d} — got nothing back. The relations
 * that did have rows carried references that led nowhere: pg_operator listed thirty operators
 * with no operand or result type, pg_cast named types and functions that were in no other
 * catalog, and a hundred and eighty pg_proc rows had no return type at all.
 *
 * <p>The assertions here are shape and reference invariants rather than counts, because the row
 * counts of a live server drift with its version and its installed extensions. Every one of them
 * was checked against a live PostgreSQL 18 first.
 */
class PgCatalogCompletenessTest {

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
            s.execute("CREATE TABLE pcc_t (id int PRIMARY KEY, name text NOT NULL, amt numeric(10,2))");
            s.execute("CREATE INDEX pcc_x ON pcc_t (lower(name))");
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

    private static int count(String sql) throws SQLException {
        return Integer.parseInt(one(sql));
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) sb.append(",");
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    // ---- the catalog describes itself ----

    @Test
    void everyCatalogTableHasItsAttributes() throws SQLException {
        assertEquals(0, count(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'pg_catalog' AND c.relkind = 'r'"
                + " AND NOT EXISTS (SELECT 1 FROM pg_attribute a"
                + "                 WHERE a.attrelid = c.oid AND a.attnum > 0)"));
    }

    @Test
    void relnattsAgreesWithTheAttributeRows() throws SQLException {
        assertEquals(0, count(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'pg_catalog' AND c.relkind IN ('r','v')"
                + " AND c.relnatts <> (SELECT count(*) FROM pg_attribute a"
                + "                    WHERE a.attrelid = c.oid AND a.attnum > 0)"));
    }

    @Test
    void pgClassDescribesItself() throws SQLException {
        assertEquals(
                "oid oid|relacl aclitem[]|relallfrozen integer|relallvisible integer|relam oid"
                + "|relchecks smallint|relfilenode oid|relforcerowsecurity boolean"
                + "|relfrozenxid xid|relhasindex boolean|relhasrules boolean"
                + "|relhassubclass boolean|relhastriggers boolean|relispartition boolean"
                + "|relispopulated boolean|relisshared boolean|relkind \"char\"|relminmxid xid"
                + "|relname name|relnamespace oid|relnatts smallint|reloftype oid"
                + "|reloptions text[]|relowner oid|relpages integer|relpartbound pg_node_tree"
                + "|relpersistence \"char\"|relreplident \"char\"|relrewrite oid"
                + "|relrowsecurity boolean|reltablespace oid|reltoastrelid oid|reltuples real"
                + "|reltype oid",
                String.join("|", rows(
                        "SELECT a.attname || ' ' || format_type(a.atttypid, a.atttypmod)"
                        + " FROM pg_attribute a WHERE a.attrelid = 'pg_class'::regclass"
                        + " AND a.attnum > 0 ORDER BY a.attname")));
    }

    @Test
    void aCatalogAttributeNamesATypeThatExists() throws SQLException {
        assertEquals(0, count(
                "SELECT count(*) FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'pg_catalog' AND a.attnum > 0"
                + " AND NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = a.atttypid)"));
    }

    @Test
    void catalogViewsAreNotReportedAsTables() throws SQLException {
        assertEquals("v", one("SELECT relkind FROM pg_class WHERE relname = 'pg_tables'"));
        assertEquals("v", one("SELECT relkind FROM pg_class WHERE relname = 'pg_stats'"));
        assertEquals("v", one("SELECT relkind FROM pg_class WHERE relname = 'pg_settings'"));
        assertEquals("r", one("SELECT relkind FROM pg_class WHERE relname = 'pg_class'"));
        assertEquals("r", one("SELECT relkind FROM pg_class WHERE relname = 'pg_proc'"));
    }

    @Test
    void everyRelationPgClassNamesCanBeSelectedFrom() throws SQLException {
        List<String> unqueryable = new ArrayList<>();
        for (String rel : rows("SELECT c.relname FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'pg_catalog' AND c.relkind IN ('r','v') ORDER BY 1")) {
            try (Statement s = conn.createStatement()) {
                s.executeQuery("SELECT * FROM pg_catalog." + rel + " WHERE false").close();
            } catch (SQLException e) {
                unqueryable.add(rel);
            }
        }
        assertEquals(new ArrayList<String>(), unqueryable,
                "pg_class names a relation the server cannot answer for");
    }

    // ---- the columns individual relations were missing ----

    @Test
    void theColumnsThatToolsReadAreThere() throws SQLException {
        String[][] cols = {
                {"pg_attribute", "attbyval"}, {"pg_index", "indcheckxmin"},
                {"pg_tables", "rowsecurity"}, {"pg_user", "userepl"},
                {"pg_user", "usebypassrls"}, {"pg_user", "useconfig"},
                {"pg_aggregate", "aggfinalmodify"}, {"pg_aggregate", "aggmfinalmodify"},
                {"pg_collation", "collicurules"}, {"pg_database", "dathasloginevt"},
                {"pg_amop", "amoppurpose"}, {"pg_statistic_ext", "stxexprs"},
                {"pg_publication_tables", "attnames"}, {"pg_publication_tables", "rowfilter"},
                {"pg_trigger", "tgconstrindid"}, {"pg_trigger", "tgnargs"},
                {"pg_trigger", "tgqual"}, {"pg_statistic", "stakind1"},
                {"pg_statistic", "stavalues5"}, {"pg_stats", "range_empty_frac"},
                {"pg_stats_ext", "n_distinct"}, {"pg_stats_ext", "dependencies"},
                {"pg_stat_all_tables", "total_vacuum_time"}, {"pg_stat_io", "read_bytes"},
                {"pg_stat_progress_vacuum", "indexes_total"},
                {"pg_stat_subscription_stats", "confl_update_missing"},
                {"pg_replication_slots", "failover"},
        };
        for (String[] c : cols) {
            try (Statement s = conn.createStatement()) {
                s.executeQuery("SELECT " + c[1] + " FROM " + c[0] + " WHERE false").close();
            } catch (SQLException e) {
                throw new AssertionError(c[0] + "." + c[1] + " does not resolve: " + e.getMessage());
            }
        }
    }

    // ---- pg_operator identifies its operators ----

    @Test
    void noOperatorIsMissingItsTypes() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_operator"
                + " WHERE oprname = '' OR oprresult = 0"));
        assertEquals(0, count("SELECT count(*) FROM pg_operator"
                + " WHERE oprkind = 'b' AND (oprleft = 0 OR oprright = 0)"));
        assertEquals(0, count("SELECT count(*) FROM pg_operator"
                + " WHERE oprkind = 'l' AND (oprleft <> 0 OR oprright = 0)"));
    }

    @Test
    void theOperatorCatalogHasNoPhantoms() throws SQLException {
        // IS is syntax, not an operator; PG spells <> as <> and treats != as a parser alias
        assertEquals(0, count("SELECT count(*) FROM pg_operator WHERE oprname = 'IS'"));
        assertEquals(0, count("SELECT count(*) FROM pg_operator WHERE oprname = '!='"));
    }

    @Test
    void anOperatorNamesTypesAndAFunctionThatExist() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_operator o"
                + " WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.oprresult)"));
        assertEquals(0, count("SELECT count(*) FROM pg_operator o"
                + " WHERE o.oprleft <> 0"
                + " AND NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.oprleft)"));
        assertEquals(0, count("SELECT count(*) FROM pg_operator o"
                + " LEFT JOIN pg_proc p ON p.oid = o.oprcode"
                + " WHERE o.oprcode <> 0 AND p.oid IS NULL"));
    }

    @Test
    void theArithmeticOperatorsCarryTheirSignature() throws SQLException {
        assertEquals(
                "*,int4,int4,int4|+,int4,int4,int4|-,int4,int4,int4|/,int4,int4,int4"
                + "|<,int4,int4,bool|<>,int4,int4,bool|=,int4,int4,bool|>,int4,int4,bool",
                String.join("|", rows(
                        "SELECT o.oprname, lt.typname, rt.typname, rs.typname FROM pg_operator o"
                        + " JOIN pg_type lt ON lt.oid = o.oprleft"
                        + " JOIN pg_type rt ON rt.oid = o.oprright"
                        + " JOIN pg_type rs ON rs.oid = o.oprresult"
                        + " WHERE o.oprname IN ('+','-','*','/','<','=','>','<>')"
                        + " AND lt.typname = 'int4' AND rt.typname = 'int4'"
                        + " ORDER BY o.oprname")));
    }

    // ---- pg_opclass, pg_opfamily, pg_collation ----

    @Test
    void anOperatorClassNamesThingsThatExist() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_opclass c"
                + " WHERE NOT EXISTS (SELECT 1 FROM pg_am a WHERE a.oid = c.opcmethod)"
                + " OR NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = c.opcintype)"
                + " OR NOT EXISTS (SELECT 1 FROM pg_opfamily f WHERE f.oid = c.opcfamily)"
                + " OR NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE n.oid = c.opcnamespace)"));
        assertEquals(0, count("SELECT count(*) FROM pg_amop o"
                + " WHERE NOT EXISTS (SELECT 1 FROM pg_opfamily f WHERE f.oid = o.amopfamily)"
                + " OR NOT EXISTS (SELECT 1 FROM pg_operator x WHERE x.oid = o.amopopr)"));
    }

    @Test
    void varcharOpsIsNotTheDefaultBtreeClass() throws SQLException {
        assertEquals("f", one("SELECT c.opcdefault FROM pg_opclass c"
                + " JOIN pg_am a ON a.oid = c.opcmethod"
                + " WHERE c.opcname = 'varchar_ops' AND a.amname = 'btree'"));
        assertEquals("t", one("SELECT c.opcdefault FROM pg_opclass c"
                + " JOIN pg_am a ON a.oid = c.opcmethod"
                + " WHERE c.opcname = 'text_ops' AND a.amname = 'btree'"));
    }

    @Test
    void theCollationCatalogOffersNothingPgWouldReject() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_collation WHERE collname IN"
                + " ('C.UTF-8', 'C.utf8', 'en_US.UTF-8', 'en_US.utf8')"));
        assertEquals("b,true,6", one("SELECT collprovider || ',' || collisdeterministic || ','"
                + " || collencoding FROM pg_collation WHERE collname = 'ucs_basic'"));
        assertEquals("en-US", one("SELECT colllocale FROM pg_collation"
                + " WHERE collname = 'en-US-x-icu'"));
    }

    @Test
    void gistDoesNotClaimAnInetOperatorFamily() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_opfamily f JOIN pg_am a ON a.oid = f.opfmethod"
                + " WHERE f.opfname = 'inet_ops' AND a.amname = 'gist'"));
    }

    // ---- pg_cast ----

    @Test
    void everyCastNamesTypesAndAFunctionThatExist() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_cast c"
                + " WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = c.castsource)"
                + " OR NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = c.casttarget)"));
        assertEquals(0, count("SELECT count(*) FROM pg_cast c"
                + " LEFT JOIN pg_proc p ON p.oid = c.castfunc"
                + " WHERE c.castfunc <> 0 AND p.oid IS NULL"));
    }

    @Test
    void castContextAndMethodAreWhatPgReports() throws SQLException {
        assertEquals(
                "bit->int4 ef|bool->text af|bpchar->text if|date->timestamp if|int4->float8 if"
                + "|int4->int8 if|int4->numeric if|int8->int4 af|text->varchar ib|varchar->text ib",
                String.join("|", rows(
                        "SELECT s.typname || '->' || t.typname || ' '"
                        + " || c.castcontext::text || c.castmethod::text FROM pg_cast c"
                        + " JOIN pg_type s ON s.oid = c.castsource"
                        + " JOIN pg_type t ON t.oid = c.casttarget"
                        + " WHERE (s.typname, t.typname) IN"
                        + " (('bpchar','text'),('bool','text'),('bit','int4'),('int4','int8'),"
                        + "  ('int4','numeric'),('text','varchar'),('varchar','text'),"
                        + "  ('int4','float8'),('date','timestamp'),('int8','int4'))"
                        + " ORDER BY 1")));
    }

    // ---- pg_proc ----

    @Test
    void everyFunctionHasAReturnTypeThatExists() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_proc WHERE prorettype = 0"));
        assertEquals(0, count("SELECT count(*) FROM pg_proc p"
                + " WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = p.prorettype)"));
    }

    @Test
    void arityAgreesWithTheArgumentList() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_proc WHERE prokind = 'f'"
                + " AND pronargs <> coalesce("
                + "     array_length(string_to_array(trim(proargtypes::text), ' '), 1), 0)"));
    }

    @Test
    void anAggregateReportsThePerOverloadReturnType() throws SQLException {
        assertEquals(
                "avg(20) -> numeric|avg(21) -> numeric|avg(23) -> numeric|avg(700) -> float8"
                + "|avg(701) -> float8|avg(1700) -> numeric|max(20) -> int8|max(21) -> int2"
                + "|max(23) -> int4|max(25) -> text|max(700) -> float4|max(701) -> float8"
                + "|max(1082) -> date|max(1700) -> numeric|min(20) -> int8|min(21) -> int2"
                + "|min(23) -> int4|min(25) -> text|min(700) -> float4|min(701) -> float8"
                + "|min(1082) -> date|min(1700) -> numeric|sum(20) -> numeric|sum(21) -> int8"
                + "|sum(23) -> int8|sum(700) -> float4|sum(701) -> float8|sum(1700) -> numeric",
                String.join("|", rows(
                        "SELECT p.proname || '(' || p.proargtypes::text || ') -> ' || t.typname"
                        + " FROM pg_proc p JOIN pg_type t ON t.oid = p.prorettype"
                        + " WHERE p.proname IN ('min','max','sum','avg') AND p.prokind = 'a'"
                        + " AND p.proargtypes::text IN"
                        + "     ('20','21','23','25','700','701','1082','1700')"
                        + " ORDER BY p.proname, p.proargtypes::text::int")));
    }

    @Test
    void noAggregateIsRegisteredAsReturningAnyelement() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_proc"
                + " WHERE prokind = 'a' AND prorettype = 2276"));
    }

    @Test
    void windowFunctionsAreMarkedAsSuch() throws SQLException {
        assertEquals("cume_dist float8|dense_rank int8|percent_rank float8|rank int8"
                        + "|row_number int8",
                String.join("|", rows(
                        "SELECT p.proname || ' ' || t.typname FROM pg_proc p"
                        + " JOIN pg_type t ON t.oid = p.prorettype"
                        + " WHERE p.prokind = 'w' AND p.pronargs = 0 ORDER BY p.proname")));
    }

    // ---- pg_index ----

    @Test
    void indexExpressionColumnsAreParseTreesNotText() throws SQLException {
        assertEquals("indcheckxmin boolean|indexprs pg_node_tree|indpred pg_node_tree",
                String.join("|", rows(
                        "SELECT a.attname || ' ' || format_type(a.atttypid, a.atttypmod)"
                        + " FROM pg_attribute a WHERE a.attrelid = 'pg_index'::regclass"
                        + " AND a.attname IN ('indexprs','indpred','indcheckxmin')"
                        + " ORDER BY a.attname")));
        assertEquals("pg_node_tree,-1,b,Z", one(
                "SELECT typname || ',' || typlen || ',' || typtype || ',' || typcategory"
                + " FROM pg_type WHERE typname = 'pg_node_tree'"));
    }

    @Test
    void anIndexOverAnExpressionSaysSo() throws SQLException {
        assertEquals("true", one("SELECT (i.indexprs IS NOT NULL)::text FROM pg_index i"
                + " JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'pcc_x'"));
    }

    // ---- the relations themselves ----

    @Test
    void anAttributeBelongsToARelationThatExists() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_attribute a"
                + " WHERE NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = a.attrelid)"));
        assertEquals(0, count("SELECT count(*) FROM pg_type t WHERE t.typrelid <> 0"
                + " AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = t.typrelid)"));
    }

    @Test
    void aRangeTypeNamesASubtypeThatExists() throws SQLException {
        assertEquals("daterange,date|int4range,int4|int8range,int8|numrange,numeric"
                        + "|tsrange,timestamp|tstzrange,timestamptz",
                String.join("|", new TreeSet<>(rows(
                        "SELECT t.typname, st.typname FROM pg_range r"
                        + " JOIN pg_type t ON t.oid = r.rngtypid"
                        + " JOIN pg_type st ON st.oid = r.rngsubtype"))));
    }
}
