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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What pg_cast, pg_operator, pg_opclass, pg_opfamily, pg_amop, pg_amproc, pg_collation and pg_am
 * say, checked against the answers a live PostgreSQL 18 server gives for the same named rows.
 *
 * <p>Nothing here counts a catalogue. A count is the one thing that cannot be compared — the
 * reference server carries contrib extensions and locale-imported collations that no memgres
 * instance has — so every check names the row it is about and the column value it expects.
 *
 * <p>The defects these cover, measured before the fix: pg_opclass gave int4_ops the OID 403,
 * which is btree's own pg_am OID, while index creation writes 1978 into pg_index.indclass for
 * every integer column, so every integer index and every integer primary key named a pg_opclass
 * row that did not exist; the gist, spgist and brin operator classes were absent altogether, and
 * a gin index on jsonb_path_ops dangled the same way; pg_amop carried ten rows for two families,
 * leaving seventy-two of memgres's own families claiming no operators; pg_operator numbered all
 * 799 built-in operators above 16384, the range PostgreSQL reserves for objects somebody
 * created, and reported oprcanmerge and oprcanhash false for {@code =(int4,int4)}; pg_collation
 * advertised an en_US row PostgreSQL has nowhere in that shape and was missing pg_unicode_fast.
 */
class CatalogCastOperatorOpclassContentTest {

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
            s.execute("CREATE TABLE coc_t (i int, s text, d date, u uuid, j jsonb, PRIMARY KEY (i))");
            s.execute("CREATE INDEX coc_bi ON coc_t (i)");
            s.execute("CREATE INDEX coc_bs ON coc_t (s)");
            s.execute("CREATE INDEX coc_bd ON coc_t (d)");
            s.execute("CREATE INDEX coc_bu ON coc_t (u)");
            s.execute("CREATE INDEX coc_hi ON coc_t USING hash (i)");
            s.execute("CREATE INDEX coc_gj ON coc_t USING gin (j jsonb_path_ops)");
            s.execute("CREATE INDEX coc_pat ON coc_t (s text_pattern_ops)");
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

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append('|');
                    String v = rs.getString(i);
                    sb.append(v == null ? "" : v);
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ pg_opclass

    /**
     * int4_ops must sit at the OID index creation writes. This is the whole reason the number
     * matters: CatalogConstraintBuilder puts 1978 in pg_index.indclass for an integer column and
     * for any type it has no class for, and the class has to be findable there.
     */
    @Test
    void int4OpsKeepsPostgresOwnOid() throws Exception {
        assertEquals(List.of("1978|int4_ops|23|0|t|403"),
                rows("SELECT oid, opcname, opcintype, opckeytype, opcdefault, opcmethod"
                        + " FROM pg_opclass WHERE opcname = 'int4_ops' AND opcmethod = 403"));
    }

    /** Named classes from each access method, with the family and input type PostgreSQL gives them. */
    @Test
    void namedOperatorClassesMatchPostgres() throws Exception {
        assertEquals(List.of(
                        "brin|box_inclusion_ops|box|t|box_inclusion_ops",
                        "brin|int4_minmax_ops|integer|t|integer_minmax_ops",
                        "btree|text_pattern_ops|text|f|text_pattern_ops",
                        "gin|jsonb_path_ops|jsonb|f|jsonb_path_ops",
                        "hash|text_pattern_ops|text|f|text_pattern_ops",
                        "spgist|quad_point_ops|point|t|quad_point_ops"),
                rows("SELECT a.amname, o.opcname, format_type(o.opcintype, NULL), o.opcdefault,"
                        + " f.opfname FROM pg_opclass o JOIN pg_am a ON a.oid = o.opcmethod"
                        + " JOIN pg_opfamily f ON f.oid = o.opcfamily WHERE o.opcname IN"
                        + " ('text_pattern_ops', 'jsonb_path_ops', 'quad_point_ops',"
                        + " 'int4_minmax_ops', 'box_inclusion_ops') ORDER BY 1, 2"));
    }

    /** The three brin classes over integer. psql's \dAc brin int4 returns exactly these. */
    @Test
    void brinClassesOverIntegerAreListed() throws Exception {
        assertEquals(List.of("int4_bloom_ops", "int4_minmax_multi_ops", "int4_minmax_ops"),
                rows("SELECT o.opcname FROM pg_opclass o JOIN pg_am a ON a.oid = o.opcmethod"
                        + " WHERE a.amname = 'brin' AND o.opcintype = 23 ORDER BY 1"));
    }

    /** A gin class with a storage type says which type it stores. */
    @Test
    void ginClassesCarryTheirStorageType() throws Exception {
        assertEquals(List.of("array_ops|anyelement", "jsonb_ops|text"),
                rows("SELECT o.opcname, o.opckeytype::regtype::text FROM pg_opclass o"
                        + " JOIN pg_am a ON a.oid = o.opcmethod WHERE a.amname = 'gin'"
                        + " AND o.opcname IN ('jsonb_ops', 'array_ops') ORDER BY 1"));
    }

    /** The seven spgist classes PostgreSQL ships, by name. */
    @Test
    void spgistClassesArePresent() throws Exception {
        assertEquals("7", one("SELECT count(*) FROM pg_opclass o JOIN pg_am a"
                + " ON a.oid = o.opcmethod WHERE a.amname = 'spgist' AND o.opcname IN"
                + " ('quad_point_ops', 'kd_point_ops', 'text_ops', 'poly_ops', 'box_ops',"
                + " 'range_ops', 'inet_ops')"));
    }

    /** PostgreSQL's own rule: an access method has at most one default class per input type. */
    @Test
    void atMostOneDefaultClassPerAccessMethodAndType() throws Exception {
        assertEquals("0", one("SELECT count(*) FROM (SELECT opcmethod, opcintype FROM pg_opclass"
                + " WHERE opcdefault GROUP BY opcmethod, opcintype HAVING count(*) > 1) d"));
    }

    // ------------------------------------------------------------------ pg_amop

    /** The pattern family's strategies are the pattern operators, not the ordinary ones. */
    @Test
    void textPatternOpsListsItsStrategies() throws Exception {
        assertEquals(List.of("1|~<~", "2|~<=~", "3|=", "4|~>=~", "5|~>~"),
                rows("SELECT p.amopstrategy, op.oprname FROM pg_amop p"
                        + " JOIN pg_opfamily f ON f.oid = p.amopfamily"
                        + " JOIN pg_am a ON a.oid = p.amopmethod"
                        + " JOIN pg_operator op ON op.oid = p.amopopr"
                        + " WHERE f.opfname = 'text_pattern_ops' AND a.amname = 'btree'"
                        + " AND p.amoplefttype = 25 ORDER BY 1"));
    }

    /** btree's five search strategies over integer. */
    @Test
    void integerBtreeStrategiesMatchPostgres() throws Exception {
        assertEquals(List.of("1|<|s", "2|<=|s", "3|=|s", "4|>=|s", "5|>|s"),
                rows("SELECT p.amopstrategy, op.oprname, p.amoppurpose FROM pg_amop p"
                        + " JOIN pg_opfamily f ON f.oid = p.amopfamily"
                        + " JOIN pg_am a ON a.oid = p.amopmethod"
                        + " JOIN pg_operator op ON op.oid = p.amopopr"
                        + " WHERE f.opfname = 'integer_ops' AND a.amname = 'btree'"
                        + " AND p.amoplefttype = 23 AND p.amoprighttype = 23 ORDER BY 1"));
    }

    /** gin over jsonb answers containment and the key-exists family, not comparison. */
    @Test
    void jsonbGinStrategiesMatchPostgres() throws Exception {
        assertEquals(List.of("7|@>", "9|?", "10|?|", "11|?&", "15|@?", "16|@@"),
                rows("SELECT p.amopstrategy, op.oprname FROM pg_amop p"
                        + " JOIN pg_opfamily f ON f.oid = p.amopfamily"
                        + " JOIN pg_am a ON a.oid = p.amopmethod"
                        + " JOIN pg_operator op ON op.oid = p.amopopr"
                        + " WHERE f.opfname = 'jsonb_ops' AND a.amname = 'gin' ORDER BY 1"));
    }

    /**
     * An ordering operator carries purpose 'o' and names the family its distances sort in.
     * A knn-gist query is planned off exactly this row.
     */
    @Test
    void gistPointOrderingOperatorNamesItsSortFamily() throws Exception {
        assertEquals(List.of("point_ops|gist|15|<->|float_ops"),
                rows("SELECT f.opfname, a.amname, p.amopstrategy, op.oprname, sf.opfname"
                        + " FROM pg_amop p JOIN pg_opfamily f ON f.oid = p.amopfamily"
                        + " JOIN pg_am a ON a.oid = p.amopmethod"
                        + " JOIN pg_operator op ON op.oid = p.amopopr"
                        + " JOIN pg_opfamily sf ON sf.oid = p.amopsortfamily"
                        + " WHERE p.amoppurpose = 'o' AND f.opfname = 'point_ops'"
                        + " AND a.amname = 'gist'"));
    }

    /** Named families that must have operators behind them; PostgreSQL has none without. */
    @Test
    void namedFamiliesAllCarryOperators() throws Exception {
        assertEquals("0", one("SELECT count(*) FROM pg_opfamily f JOIN pg_am a"
                + " ON a.oid = f.opfmethod WHERE f.opfname IN ('integer_ops', 'text_ops',"
                + " 'datetime_ops', 'float_ops', 'numeric_ops', 'uuid_ops', 'bool_ops',"
                + " 'network_ops', 'jsonb_ops', 'array_ops', 'text_pattern_ops',"
                + " 'integer_minmax_ops', 'quad_point_ops', 'box_inclusion_ops')"
                + " AND NOT EXISTS (SELECT 1 FROM pg_amop x WHERE x.amopfamily = f.oid)"));
    }

    // ------------------------------------------------------------------ pg_operator

    /** Built-in operators carry PostgreSQL's own OIDs, below the user-object boundary. */
    @Test
    void builtinOperatorsCarryPostgresOids() throws Exception {
        assertEquals(List.of("<|97|f|f", "=|98|t|t", "+|551|f|f", "|||654|f|f", "=|2972|t|t"),
                rows("SELECT oprname, oid, oprcanmerge, oprcanhash FROM pg_operator"
                        + " WHERE (oprname = '<' AND oprleft = 23 AND oprright = 23)"
                        + " OR (oprname = '=' AND oprleft = 25 AND oprright = 25)"
                        + " OR (oprname = '+' AND oprleft = 23 AND oprright = 23)"
                        + " OR (oprname = '||' AND oprleft = 25 AND oprright = 25)"
                        + " OR (oprname = '=' AND oprleft = 2950 AND oprright = 2950)"
                        + " ORDER BY 2"));
    }

    /** Equality over integer admits a merge join and a hash join, and is int4eq. */
    @Test
    void integerEqualityIsMergeableAndHashable() throws Exception {
        assertEquals(List.of("96|t|t|int4eq"),
                rows("SELECT oid, oprcanmerge, oprcanhash, oprcode::text FROM pg_operator"
                        + " WHERE oprname = '=' AND oprleft = 23 AND oprright = 23"));
    }

    /** int4_ops is a shipped object, so its OID is below FirstNormalObjectId. */
    @Test
    void shippedObjectsAreNumberedBelowTheUserRange() throws Exception {
        assertEquals("0", one("SELECT count(*) FROM pg_opclass WHERE opcname = 'int4_ops'"
                + " AND opcmethod = 403 AND oid >= 16384"));
        assertEquals("0", one("SELECT count(*) FROM pg_operator WHERE oprname = '='"
                + " AND oprleft = 23 AND oprright = 23 AND oid >= 16384"));
    }

    // ------------------------------------------------------------------ pg_cast

    /** A cast is referred to by OID, so the OID has to be the one PostgreSQL uses. */
    @Test
    void castsCarryPostgresOids() throws Exception {
        assertEquals(List.of("10035|e|f"), rows("SELECT oid, castcontext, castmethod"
                + " FROM pg_cast WHERE castsource = 16 AND casttarget = 23"));
        assertEquals(List.of("i|b|10125"), rows("SELECT castcontext, castmethod, oid"
                + " FROM pg_cast WHERE castsource = 25 AND casttarget = 1042"));
    }

    // ------------------------------------------------------------------ pg_collation, pg_am

    /** The seven collations PG 18 pins, at the OIDs it pins them at. */
    @Test
    void pinnedCollationsMatchPostgres() throws Exception {
        assertEquals(List.of(
                        "C|c|t|-1|",
                        "POSIX|c|t|-1|",
                        "default|d|t|-1|",
                        "pg_c_utf8|b|t|6|C.UTF-8",
                        "pg_unicode_fast|b|t|6|PG_UNICODE_FAST",
                        "ucs_basic|b|t|6|C",
                        "unicode|i|t|-1|und"),
                rows("SELECT collname, collprovider, collisdeterministic, collencoding, colllocale"
                        + " FROM pg_collation WHERE collname IN ('pg_unicode_fast', 'pg_c_utf8',"
                        + " 'ucs_basic', 'unicode', 'C', 'POSIX', 'default') ORDER BY collname"));
        assertEquals(List.of("C|950", "POSIX|951", "default|100", "pg_c_utf8|811",
                        "pg_unicode_fast|6411", "ucs_basic|962", "unicode|963"),
                rows("SELECT collname, oid FROM pg_collation WHERE collname IN ('default', 'C',"
                        + " 'POSIX', 'ucs_basic', 'pg_c_utf8', 'unicode', 'pg_unicode_fast')"
                        + " ORDER BY 1"));
    }

    /**
     * There is no en_US row. PostgreSQL imports one only on a host whose locale list has it, and
     * then as a libc collation at that locale's encoding — never as ICU at UTF8.
     */
    @Test
    void noPhantomEnUsCollation() throws Exception {
        assertEquals("0", one("SELECT count(*) FROM pg_collation WHERE collname = 'en_US'"
                + " AND collencoding = 6 AND collprovider = 'i'"));
    }

    /** An ICU collation is encoding-independent, which PostgreSQL records as collencoding -1. */
    @Test
    void createCollationWithIcuProviderIsEncodingIndependent() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE COLLATION coc_coll (provider = icu, locale = 'de-DE')");
        }
        assertEquals(List.of("coc_coll|i|t|-1"),
                rows("SELECT collname, collprovider, collisdeterministic, collencoding"
                        + " FROM pg_collation WHERE collname = 'coc_coll'"));
    }

    /** The seven access methods, in PostgreSQL's OID order and with its own type letters. */
    @Test
    void accessMethodsMatchPostgres() throws Exception {
        assertEquals(List.of("heap|t", "btree|i", "hash|i", "gist|i", "gin|i", "brin|i", "spgist|i"),
                rows("SELECT amname, amtype FROM pg_am ORDER BY oid"));
    }

    // ------------------------------------------------------------------ referential integrity

    /**
     * The walk this branch owes: every OID-valued column of these catalogues must name a row that
     * exists in the same engine. PostgreSQL answers 0 to each of these, and so must memgres — a
     * reference a reader cannot follow is worse than a row that was never offered.
     */
    @Test
    void noReferenceDangles() throws Exception {
        String[][] checks = {
                {"pg_opclass.opcfamily", "SELECT count(*) FROM pg_opclass o WHERE NOT EXISTS (SELECT 1 FROM pg_opfamily f WHERE f.oid = o.opcfamily)"},
                {"pg_opclass.opcintype", "SELECT count(*) FROM pg_opclass o WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.opcintype)"},
                {"pg_opclass.opckeytype", "SELECT count(*) FROM pg_opclass o WHERE o.opckeytype <> 0 AND NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.opckeytype)"},
                {"pg_opclass.opcmethod", "SELECT count(*) FROM pg_opclass o WHERE NOT EXISTS (SELECT 1 FROM pg_am a WHERE a.oid = o.opcmethod)"},
                {"pg_opfamily.opfmethod", "SELECT count(*) FROM pg_opfamily f WHERE NOT EXISTS (SELECT 1 FROM pg_am a WHERE a.oid = f.opfmethod)"},
                {"pg_amop.amopfamily", "SELECT count(*) FROM pg_amop p WHERE NOT EXISTS (SELECT 1 FROM pg_opfamily f WHERE f.oid = p.amopfamily)"},
                {"pg_amop.amopopr", "SELECT count(*) FROM pg_amop p WHERE NOT EXISTS (SELECT 1 FROM pg_operator o WHERE o.oid = p.amopopr)"},
                {"pg_amop.amoplefttype", "SELECT count(*) FROM pg_amop p WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = p.amoplefttype)"},
                {"pg_amop.amoprighttype", "SELECT count(*) FROM pg_amop p WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = p.amoprighttype)"},
                {"pg_amop.amopmethod", "SELECT count(*) FROM pg_amop p WHERE NOT EXISTS (SELECT 1 FROM pg_am a WHERE a.oid = p.amopmethod)"},
                {"pg_amop.amopsortfamily", "SELECT count(*) FROM pg_amop p WHERE p.amopsortfamily <> 0 AND NOT EXISTS (SELECT 1 FROM pg_opfamily f WHERE f.oid = p.amopsortfamily)"},
                {"pg_amproc.amproc", "SELECT count(*) FROM pg_amproc p WHERE NOT EXISTS (SELECT 1 FROM pg_proc r WHERE r.oid = p.amproc)"},
                {"pg_cast.castsource", "SELECT count(*) FROM pg_cast c WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = c.castsource)"},
                {"pg_cast.casttarget", "SELECT count(*) FROM pg_cast c WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = c.casttarget)"},
                {"pg_cast.castfunc", "SELECT count(*) FROM pg_cast c WHERE c.castfunc <> 0 AND NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid = c.castfunc)"},
                {"pg_operator.oprleft", "SELECT count(*) FROM pg_operator o WHERE o.oprleft <> 0 AND NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.oprleft)"},
                {"pg_operator.oprright", "SELECT count(*) FROM pg_operator o WHERE o.oprright <> 0 AND NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.oprright)"},
                {"pg_operator.oprresult", "SELECT count(*) FROM pg_operator o WHERE NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.oprresult)"},
                {"pg_operator.oprcode", "SELECT count(*) FROM pg_operator o WHERE NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid = o.oprcode)"},
                {"pg_operator.oprcom", "SELECT count(*) FROM pg_operator o WHERE o.oprcom <> 0 AND NOT EXISTS (SELECT 1 FROM pg_operator x WHERE x.oid = o.oprcom)"},
                {"pg_operator.oprnegate", "SELECT count(*) FROM pg_operator o WHERE o.oprnegate <> 0 AND NOT EXISTS (SELECT 1 FROM pg_operator x WHERE x.oid = o.oprnegate)"},
                {"pg_am.amhandler", "SELECT count(*) FROM pg_am a WHERE NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid = a.amhandler)"},
                {"pg_index.indclass", "SELECT count(*) FROM pg_index i WHERE NOT EXISTS (SELECT 1 FROM pg_opclass o WHERE o.oid = i.indclass[0])"},
        };
        for (String[] check : checks) {
            assertEquals("0", one(check[1]), check[0] + " has dangling references");
        }
    }

    /** No catalogue may hand out the same OID twice: every join over these reads by OID. */
    @Test
    void noDuplicateOids() throws Exception {
        for (String rel : new String[]{"pg_opclass", "pg_opfamily", "pg_amop", "pg_operator",
                "pg_cast", "pg_collation", "pg_am"}) {
            assertEquals("0", one("SELECT count(*) FROM (SELECT oid FROM " + rel
                    + " GROUP BY oid HAVING count(*) > 1) d"), rel + " has duplicate OIDs");
        }
    }

    /**
     * Every index this test built names an operator class that is there. Before the fix an
     * integer primary key, a hash index on integer and a gin index on jsonb_path_ops all pointed
     * pg_index at no row at all.
     */
    @Test
    void everyIndexNamesAnOperatorClassThatExists() throws Exception {
        assertEquals(List.of(
                        "coc_bd|date_ops",
                        "coc_bi|int4_ops",
                        "coc_bs|text_ops",
                        "coc_bu|uuid_ops",
                        "coc_gj|jsonb_path_ops",
                        "coc_hi|int4_ops",
                        "coc_pat|text_pattern_ops",
                        "coc_t_pkey|int4_ops"),
                rows("SELECT c.relname, coalesce(o.opcname, '<DANGLING>') FROM pg_index i"
                        + " JOIN pg_class c ON c.oid = i.indexrelid"
                        + " LEFT JOIN pg_opclass o ON o.oid = i.indclass[0]"
                        + " WHERE c.relname LIKE 'coc_%' ORDER BY 1"));
    }
}
