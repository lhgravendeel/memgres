package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * pg_get_constraintdef / pg_get_indexdef / check_clause must be byte-identical to
 * PostgreSQL 18. Every expectation below was captured from a live PG 18.0 server, so
 * these are ground truth, not a restatement of what memgres happens to print.
 *
 * <p>PostgreSQL deparses the post-parse-analysis tree, which carries the implicit casts
 * the analyzer inserted — hence {@code price >= 0} on numeric rendering as
 * {@code price >= (0)::numeric} but on integer rendering unchanged.
 */
class ExpressionDeparseFidelityTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
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

    private static Map<String, String> constraintDefs(String table) throws SQLException {
        Map<String, String> out = new TreeMap<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT conname, pg_get_constraintdef(c.oid) AS def FROM pg_constraint c "
                             + "JOIN pg_class r ON r.oid = c.conrelid "
                             + "WHERE r.relname = '" + table + "' AND c.contype = 'c'")) {
            while (rs.next()) out.put(rs.getString("conname"), rs.getString("def"));
        }
        return out;
    }

    private static Map<String, String> checkClauses() throws SQLException {
        Map<String, String> out = new TreeMap<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT constraint_name, check_clause FROM information_schema.check_constraints")) {
            while (rs.next()) out.put(rs.getString(1), rs.getString(2));
        }
        return out;
    }

    private static void assertAll(Map<String, String> expected, Map<String, String> actual) {
        StringBuilder diff = new StringBuilder();
        for (Map.Entry<String, String> e : expected.entrySet()) {
            String got = actual.get(e.getKey());
            if (!e.getValue().equals(got)) {
                diff.append("\n  ").append(e.getKey())
                        .append("\n    PG 18   : ").append(e.getValue())
                        .append("\n    memgres : ").append(got);
            }
        }
        if (diff.length() > 0) {
            throw new AssertionError("Deparsed text differs from PostgreSQL 18:" + diff);
        }
    }

    // ------------------------------------------------------------------
    // Constant typing, operator resolution and parenthesisation
    // ------------------------------------------------------------------

    @Test
    void constraintDefMatchesPg() throws Exception {
        exec("CREATE TABLE gd_t (id int, price numeric, qty int, name text, vc varchar(20), "
                + "d date, b bool, f float8, s smallint, bi bigint, "
                + "CONSTRAINT c_num CHECK (price >= 0), "
                + "CONSTRAINT c_int CHECK (qty > 0), "
                + "CONSTRAINT c_txt CHECK (name <> ''), "
                + "CONSTRAINT c_vc CHECK (vc <> 'x'), "
                + "CONSTRAINT c_and CHECK (qty > 0 AND price < 100), "
                + "CONSTRAINT c_or CHECK (qty > 0 OR price < 100), "
                + "CONSTRAINT c_and3 CHECK (qty = 1 AND qty = 2 AND qty = 3), "
                + "CONSTRAINT c_notandor CHECK (NOT (qty > 0 AND price < 100)), "
                + "CONSTRAINT c_arith CHECK (qty + 1 > 2), "
                + "CONSTRAINT c_arith2 CHECK (price * 2 >= 0), "
                + "CONSTRAINT c_fn CHECK (lower(name) <> 'zz'), "
                + "CONSTRAINT c_fnvc CHECK (lower(vc) <> 'zz'), "
                + "CONSTRAINT c_len CHECK (length(name) > 1), "
                + "CONSTRAINT c_lenvc CHECK (length(vc) > 0), "
                + "CONSTRAINT c_between CHECK (qty BETWEEN 1 AND 10), "
                + "CONSTRAINT c_null CHECK (name IS NOT NULL), "
                + "CONSTRAINT c_like CHECK (name LIKE 'a%'), "
                + "CONSTRAINT c_notlike CHECK (name NOT LIKE 'a%'), "
                + "CONSTRAINT c_regex CHECK (name ~ '^a'), "
                + "CONSTRAINT c_bool CHECK (b), "
                + "CONSTRAINT c_notbool CHECK (NOT b), "
                + "CONSTRAINT c_istrue CHECK (b IS TRUE), "
                + "CONSTRAINT c_float CHECK (f > 0), "
                + "CONSTRAINT c_floatlit CHECK (f > 1.5), "
                + "CONSTRAINT c_small CHECK (s > 0), "
                + "CONSTRAINT c_big CHECK (bi > 0), "
                + "CONSTRAINT c_date CHECK (d > '2000-01-01'), "
                + "CONSTRAINT c_neg CHECK (qty > -1), "
                + "CONSTRAINT c_numlit CHECK (price >= 0.5), "
                + "CONSTRAINT c_numint CHECK (price >= 100), "
                + "CONSTRAINT c_concat CHECK (name || vc <> 'q'), "
                + "CONSTRAINT c_mod CHECK (qty % 2 = 0), "
                + "CONSTRAINT c_div CHECK (price / 2 > 0), "
                + "CONSTRAINT c_pow CHECK (qty ^ 2 > 0), "
                + "CONSTRAINT c_abs CHECK (abs(price) > 0), "
                + "CONSTRAINT c_round CHECK (round(price, 2) > 0), "
                + "CONSTRAINT c_coalesce CHECK (coalesce(name, vc) <> ''), "
                + "CONSTRAINT c_nullif CHECK (nullif(name, '') IS NOT NULL), "
                + "CONSTRAINT c_greatest CHECK (greatest(qty, bi) > 0), "
                + "CONSTRAINT c_least CHECK (least(price, 1) > 0), "
                + "CONSTRAINT c_cast CHECK (qty::text = name), "
                + "CONSTRAINT c_castnum CHECK (qty::numeric > 0), "
                + "CONSTRAINT c_bigcast CHECK (qty > 0::bigint), "
                + "CONSTRAINT c_biglit CHECK (qty > 2147483648), "
                + "CONSTRAINT c_nulor CHECK (qty IS NULL OR qty > 0), "
                + "CONSTRAINT c_in CHECK (qty IN (1,2,3)), "
                + "CONSTRAINT c_notin CHECK (qty NOT IN (1,2)), "
                + "CONSTRAINT c_case CHECK (CASE WHEN qty > 0 THEN true ELSE false END)"
                + ")");

        Map<String, String> pg = new LinkedHashMap<>();
        pg.put("c_num", "CHECK ((price >= (0)::numeric))");
        pg.put("c_int", "CHECK ((qty > 0))");
        pg.put("c_txt", "CHECK ((name <> ''::text))");
        pg.put("c_vc", "CHECK (((vc)::text <> 'x'::text))");
        pg.put("c_and", "CHECK (((qty > 0) AND (price < (100)::numeric)))");
        pg.put("c_or", "CHECK (((qty > 0) OR (price < (100)::numeric)))");
        pg.put("c_and3", "CHECK (((qty = 1) AND (qty = 2) AND (qty = 3)))");
        pg.put("c_notandor", "CHECK ((NOT ((qty > 0) AND (price < (100)::numeric))))");
        pg.put("c_arith", "CHECK (((qty + 1) > 2))");
        pg.put("c_arith2", "CHECK (((price * (2)::numeric) >= (0)::numeric))");
        pg.put("c_fn", "CHECK ((lower(name) <> 'zz'::text))");
        pg.put("c_fnvc", "CHECK ((lower((vc)::text) <> 'zz'::text))");
        pg.put("c_len", "CHECK ((length(name) > 1))");
        pg.put("c_lenvc", "CHECK ((length((vc)::text) > 0))");
        pg.put("c_between", "CHECK (((qty >= 1) AND (qty <= 10)))");
        pg.put("c_null", "CHECK ((name IS NOT NULL))");
        pg.put("c_like", "CHECK ((name ~~ 'a%'::text))");
        pg.put("c_notlike", "CHECK ((name !~~ 'a%'::text))");
        pg.put("c_regex", "CHECK ((name ~ '^a'::text))");
        pg.put("c_bool", "CHECK (b)");
        pg.put("c_notbool", "CHECK ((NOT b))");
        pg.put("c_istrue", "CHECK ((b IS TRUE))");
        pg.put("c_float", "CHECK ((f > (0)::double precision))");
        pg.put("c_floatlit", "CHECK ((f > (1.5)::double precision))");
        pg.put("c_small", "CHECK ((s > 0))");
        pg.put("c_big", "CHECK ((bi > 0))");
        pg.put("c_date", "CHECK ((d > '2000-01-01'::date))");
        pg.put("c_neg", "CHECK ((qty > '-1'::integer))");
        pg.put("c_numlit", "CHECK ((price >= 0.5))");
        pg.put("c_numint", "CHECK ((price >= (100)::numeric))");
        pg.put("c_concat", "CHECK (((name || (vc)::text) <> 'q'::text))");
        pg.put("c_mod", "CHECK (((qty % 2) = 0))");
        pg.put("c_div", "CHECK (((price / (2)::numeric) > (0)::numeric))");
        pg.put("c_pow", "CHECK ((((qty)::double precision ^ (2)::double precision) > (0)::double precision))");
        pg.put("c_abs", "CHECK ((abs(price) > (0)::numeric))");
        pg.put("c_round", "CHECK ((round(price, 2) > (0)::numeric))");
        pg.put("c_coalesce", "CHECK ((COALESCE(name, (vc)::text) <> ''::text))");
        pg.put("c_nullif", "CHECK ((NULLIF(name, ''::text) IS NOT NULL))");
        pg.put("c_greatest", "CHECK ((GREATEST((qty)::bigint, bi) > 0))");
        pg.put("c_least", "CHECK ((LEAST(price, (1)::numeric) > (0)::numeric))");
        pg.put("c_cast", "CHECK (((qty)::text = name))");
        pg.put("c_castnum", "CHECK (((qty)::numeric > (0)::numeric))");
        pg.put("c_bigcast", "CHECK ((qty > (0)::bigint))");
        pg.put("c_biglit", "CHECK ((qty > '2147483648'::bigint))");
        pg.put("c_nulor", "CHECK (((qty IS NULL) OR (qty > 0)))");
        pg.put("c_in", "CHECK ((qty = ANY (ARRAY[1, 2, 3])))");
        pg.put("c_notin", "CHECK ((qty <> ALL (ARRAY[1, 2])))");
        pg.put("c_case", "CHECK (\nCASE\n    WHEN (qty > 0) THEN true\n    ELSE false\nEND)");

        assertAll(pg, constraintDefs("gd_t"));
    }

    /** check_clause is pg_get_constraintdef with the CHECK (...) wrapper removed. */
    @Test
    void checkClauseMatchesPg() throws Exception {
        exec("CREATE TABLE gd_cc (price numeric, qty int, name text, vc varchar(10), "
                + "CONSTRAINT cc_num CHECK (price >= 0), "
                + "CONSTRAINT cc_txt CHECK (name <> ''), "
                + "CONSTRAINT cc_vc CHECK (lower(vc) <> 'zz'), "
                + "CONSTRAINT cc_and CHECK (qty > 0 AND price < 100))");

        Map<String, String> pg = new LinkedHashMap<>();
        pg.put("cc_num", "(price >= (0)::numeric)");
        pg.put("cc_txt", "(name <> ''::text)");
        pg.put("cc_vc", "(lower((vc)::text) <> 'zz'::text)");
        pg.put("cc_and", "((qty > 0) AND (price < (100)::numeric))");

        assertAll(pg, checkClauses());
    }

    // ------------------------------------------------------------------
    // Numeric-category operator resolution
    // ------------------------------------------------------------------

    @Test
    void numericPromotionMatchesPg() throws Exception {
        exec("CREATE TABLE gd_x (i2 smallint, i4 int, i8 bigint, n numeric, f4 real, f8 double precision, "
                + "CONSTRAINT k01 CHECK (i2 + i4 > 0), "
                + "CONSTRAINT k02 CHECK (i4 + i8 > 0), "
                + "CONSTRAINT k03 CHECK (i4 + n > 0), "
                + "CONSTRAINT k04 CHECK (i4 + f4 > 0), "
                + "CONSTRAINT k05 CHECK (i4 + f8 > 0), "
                + "CONSTRAINT k06 CHECK (n + f4 > 0), "
                + "CONSTRAINT k07 CHECK (n + f8 > 0), "
                + "CONSTRAINT k08 CHECK (f4 + f8 > 0), "
                + "CONSTRAINT k09 CHECK (i8 + n > 0))");

        Map<String, String> pg = new LinkedHashMap<>();
        pg.put("k01", "CHECK (((i2 + i4) > 0))");
        pg.put("k02", "CHECK (((i4 + i8) > 0))");
        pg.put("k03", "CHECK ((((i4)::numeric + n) > (0)::numeric))");
        pg.put("k04", "CHECK ((((i4)::double precision + f4) > (0)::double precision))");
        pg.put("k05", "CHECK ((((i4)::double precision + f8) > (0)::double precision))");
        pg.put("k06", "CHECK ((((n)::double precision + f4) > (0)::double precision))");
        pg.put("k07", "CHECK ((((n)::double precision + f8) > (0)::double precision))");
        pg.put("k08", "CHECK (((f4 + f8) > (0)::double precision))");
        pg.put("k09", "CHECK ((((i8)::numeric + n) > (0)::numeric))");

        assertAll(pg, constraintDefs("gd_x"));
    }

    @Test
    void stringPromotionMatchesPg() throws Exception {
        exec("CREATE TABLE gd_s (t text, vc varchar(10), vc2 varchar(5), bc char(5), "
                + "CONSTRAINT s01 CHECK (t = vc), "
                + "CONSTRAINT s02 CHECK (vc = bc), "
                + "CONSTRAINT s03 CHECK (vc = vc2), "
                + "CONSTRAINT s04 CHECK (bc = t), "
                + "CONSTRAINT s05 CHECK (bc <> ''), "
                + "CONSTRAINT s06 CHECK (t || 'x' <> ''), "
                + "CONSTRAINT s07 CHECK (vc || 'x' <> ''), "
                + "CONSTRAINT s08 CHECK (upper(t) = lower(vc)))");

        Map<String, String> pg = new LinkedHashMap<>();
        pg.put("s01", "CHECK ((t = (vc)::text))");
        pg.put("s02", "CHECK (((vc)::bpchar = bc))");
        pg.put("s03", "CHECK (((vc)::text = (vc2)::text))");
        pg.put("s04", "CHECK (((bc)::text = t))");
        pg.put("s05", "CHECK ((bc <> ''::bpchar))");
        pg.put("s06", "CHECK (((t || 'x'::text) <> ''::text))");
        pg.put("s07", "CHECK ((((vc)::text || 'x'::text) <> ''::text))");
        pg.put("s08", "CHECK ((upper(t) = lower((vc)::text)))");

        assertAll(pg, constraintDefs("gd_s"));
    }

    /** PG's get_const_expr: only bool, non-negative int4 and float-looking numerics print bare. */
    @Test
    void constantLabellingMatchesPg() throws Exception {
        exec("CREATE TABLE gd_c (i4 int, n numeric, f8 double precision, "
                + "CONSTRAINT p01 CHECK (i4 > -1), "
                + "CONSTRAINT p03 CHECK (n > -0.5), "
                + "CONSTRAINT p04 CHECK (f8 > -1.5), "
                + "CONSTRAINT p06 CHECK (n > 1e-5), "
                + "CONSTRAINT p07 CHECK (f8 > 1e20), "
                + "CONSTRAINT p10 CHECK (n > 0.0), "
                + "CONSTRAINT p13 CHECK (f8 > 0.1), "
                + "CONSTRAINT p15 CHECK (i4 > 1000000), "
                + "CONSTRAINT p17 CHECK (i4 > -2147483648), "
                + "CONSTRAINT p18 CHECK (n > .5))");

        Map<String, String> pg = new LinkedHashMap<>();
        pg.put("p01", "CHECK ((i4 > '-1'::integer))");
        pg.put("p03", "CHECK ((n > '-0.5'::numeric))");
        pg.put("p04", "CHECK ((f8 > ('-1.5'::numeric)::double precision))");
        pg.put("p06", "CHECK ((n > 0.00001))");
        pg.put("p07", "CHECK ((f8 > ('100000000000000000000'::numeric)::double precision))");
        pg.put("p10", "CHECK ((n > 0.0))");
        pg.put("p13", "CHECK ((f8 > (0.1)::double precision))");
        pg.put("p15", "CHECK ((i4 > 1000000))");
        pg.put("p17", "CHECK ((i4 > '-2147483648'::integer))");
        pg.put("p18", "CHECK ((n > 0.5))");

        assertAll(pg, constraintDefs("gd_c"));
    }

    // ------------------------------------------------------------------
    // Domains
    // ------------------------------------------------------------------

    @Test
    void domainCheckMatchesPg() throws Exception {
        exec("CREATE DOMAIN gd_dom AS numeric CHECK (VALUE > 0)");
        exec("CREATE DOMAIN gd_dom2 AS text CHECK (VALUE <> '' AND length(VALUE) < 10)");

        Map<String, String> defs = new TreeMap<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conname LIKE 'gd_dom%'")) {
            while (rs.next()) defs.put(rs.getString(1), rs.getString(2));
        }

        Map<String, String> pg = new LinkedHashMap<>();
        pg.put("gd_dom_check", "CHECK ((VALUE > (0)::numeric))");
        pg.put("gd_dom2_check", "CHECK (((VALUE <> ''::text) AND (length(VALUE) < 10)))");
        assertAll(pg, defs);
    }

    // ------------------------------------------------------------------
    // Index definitions
    // ------------------------------------------------------------------

    @Test
    void indexDefMatchesPg() throws Exception {
        exec("CREATE TABLE gd_i (id int primary key, name text, vc varchar(20), price numeric, "
                + "qty int, c char(5))");
        exec("CREATE INDEX gi1 ON gd_i (lower(name))");
        exec("CREATE INDEX gi2 ON gd_i (lower(vc))");
        exec("CREATE INDEX gi3 ON gd_i ((qty + 1))");
        exec("CREATE INDEX gi4 ON gd_i ((price * 2))");
        exec("CREATE INDEX gi5 ON gd_i (name) WHERE qty > 0");
        exec("CREATE INDEX gi6 ON gd_i (name) WHERE price > 0");
        exec("CREATE INDEX gi7 ON gd_i (name, qty)");
        exec("CREATE INDEX gi8 ON gd_i ((name || vc))");
        exec("CREATE INDEX gi9 ON gd_i (upper(c))");
        exec("CREATE INDEX gi10 ON gd_i ((qty::text))");
        exec("CREATE INDEX gi11 ON gd_i (name text_pattern_ops)");
        exec("CREATE INDEX gi12 ON gd_i ((coalesce(name,'')))");
        exec("CREATE INDEX gi13 ON gd_i (name COLLATE \"C\")");

        Map<String, String> defs = new TreeMap<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'gd_i'")) {
            while (rs.next()) defs.put(rs.getString(1), rs.getString(2));
        }

        Map<String, String> pg = new LinkedHashMap<>();
        pg.put("gi1", "CREATE INDEX gi1 ON public.gd_i USING btree (lower(name))");
        pg.put("gi2", "CREATE INDEX gi2 ON public.gd_i USING btree (lower((vc)::text))");
        pg.put("gi3", "CREATE INDEX gi3 ON public.gd_i USING btree (((qty + 1)))");
        pg.put("gi4", "CREATE INDEX gi4 ON public.gd_i USING btree (((price * (2)::numeric)))");
        pg.put("gi5", "CREATE INDEX gi5 ON public.gd_i USING btree (name) WHERE (qty > 0)");
        pg.put("gi6", "CREATE INDEX gi6 ON public.gd_i USING btree (name) WHERE (price > (0)::numeric)");
        pg.put("gi7", "CREATE INDEX gi7 ON public.gd_i USING btree (name, qty)");
        pg.put("gi8", "CREATE INDEX gi8 ON public.gd_i USING btree (((name || (vc)::text)))");
        pg.put("gi9", "CREATE INDEX gi9 ON public.gd_i USING btree (upper((c)::text))");
        pg.put("gi10", "CREATE INDEX gi10 ON public.gd_i USING btree (((qty)::text))");
        pg.put("gi11", "CREATE INDEX gi11 ON public.gd_i USING btree (name text_pattern_ops)");
        pg.put("gi12", "CREATE INDEX gi12 ON public.gd_i USING btree (COALESCE(name, ''::text))");
        pg.put("gi13", "CREATE INDEX gi13 ON public.gd_i USING btree (name COLLATE \"C\")");

        assertAll(pg, defs);
    }

    @Test
    void pgGetIndexdefMatchesPgIndexes() throws Exception {
        exec("CREATE TABLE gd_g (id int, name varchar(10), qty int)");
        exec("CREATE INDEX gg1 ON gd_g (lower(name))");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT pg_get_indexdef(c.oid) FROM pg_class c WHERE c.relname = 'gg1'")) {
            assertTrue(rs.next());
            assertEquals("CREATE INDEX gg1 ON public.gd_g USING btree (lower((name)::text))", rs.getString(1));
        }
    }

    /** Identifiers that are not safe bare words must be double-quoted, as quote_identifier does. */
    @Test
    void identifierQuotingMatchesPg() throws Exception {
        exec("CREATE TABLE gd_q (\"Mixed\" int, \"select\" int, plain int, "
                + "CONSTRAINT q01 CHECK (\"Mixed\" > 0), "
                + "CONSTRAINT q02 CHECK (\"select\" > 0), "
                + "CONSTRAINT q03 CHECK (plain > 0))");

        Map<String, String> pg = new LinkedHashMap<>();
        pg.put("q01", "CHECK ((\"Mixed\" > 0))");
        pg.put("q02", "CHECK ((\"select\" > 0))");
        pg.put("q03", "CHECK ((plain > 0))");
        assertAll(pg, constraintDefs("gd_q"));
    }
}
