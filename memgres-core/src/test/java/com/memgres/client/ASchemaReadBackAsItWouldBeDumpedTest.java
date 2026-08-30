package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a schema reads back as, for a reader assembling the statements that would rebuild it.
 *
 * <p>A default is stored as text and read back by parsing that text, so the grouping the writer
 * wrote has to survive being written down: flattened, a default of {@code (2 + 3) * 4} was stored
 * as {@code 2 + 3 * 4}, read back as a different expression, and reported as one — while still
 * computing the right value, so nothing but a dump would ever notice.
 *
 * <p>A domain is stored the way its base type is stored — same width, same alignment, same
 * storage, written out by the base type's own output function — and only reading a value in
 * belongs to the domain, because that is where its constraints are checked. Written from a fixed
 * template, every domain described a variable-width extended-storage type that does not exist.
 * An array's modifier is its element's, so numeric(6,2)[] is an array of numeric(6,2) and not of
 * numeric. A routine's SET clauses are part of its definition, and a SECURITY DEFINER function
 * that loses its pinned search_path runs under whatever the caller left behind.
 *
 * <p>And a relation is described by what it is: a partitioned table holds no rows so it has no
 * access method, a materialized view holds rows so it has one and can be indexed, an index on a
 * partition is a partition of the parent's index, and a relation that can hold an oversized value
 * has somewhere to put it.
 */
class ASchemaReadBackAsItWouldBeDumpedTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String one(String sql) throws SQLException {
        List<String> rows = rows(sql);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int c = 1; c <= rs.getMetaData().getColumnCount(); c++) {
                    if (c > 1) row.append('/');
                    row.append(rs.getString(c));
                }
                out.add(row.toString());
            }
            return out;
        }
    }

    /** A default keeps the grouping it was written with, through being stored and read back. */
    @Test
    void aDefaultKeepsItsGroupingThroughTheCatalogue() throws SQLException {
        exec("CREATE TABLE zsd_d (id int, a int DEFAULT (2 + 3) * 4, b int DEFAULT 2 * (3 + 4),"
                + " c int DEFAULT 2 + 3 * 4, d int DEFAULT (10 - 4) - 3)");
        try {
            assertEquals(List.of("a/((2 + 3) * 4)", "b/(2 * (3 + 4))", "c/(2 + (3 * 4))",
                            "d/((10 - 4) - 3)"),
                    rows("SELECT a.attname, pg_get_expr(d.adbin, d.adrelid)"
                            + " FROM pg_attrdef d JOIN pg_attribute a"
                            + " ON a.attrelid = d.adrelid AND a.attnum = d.adnum"
                            + " WHERE d.adrelid = 'zsd_d'::regclass ORDER BY a.attnum"));
            // The value the default computes is the one the grouping asks for, either way.
            exec("INSERT INTO zsd_d (id) VALUES (1)");
            assertEquals("20/14/14/3", one("SELECT a, b, c, d FROM zsd_d"));
        } finally {
            exec("DROP TABLE zsd_d");
        }
    }

    /** A domain is stored the way its base type is stored, and says so. */
    @Test
    void aDomainIsStoredTheWayItsBaseTypeIs() throws SQLException {
        exec("CREATE DOMAIN zsd_dv AS varchar(20)");
        exec("CREATE DOMAIN zsd_dn AS integer NOT NULL DEFAULT 1 CHECK (VALUE > 0)");
        try {
            assertEquals("4/t/i/p/int4out/int4send",
                    one("SELECT typlen, typbyval, typalign, typstorage, typoutput, typsend"
                            + " FROM pg_type WHERE typname = 'zsd_dn'"));
            // Only reading a value in belongs to the domain: that is where it is checked.
            assertEquals("domain_in/domain_recv",
                    one("SELECT typinput, typreceive FROM pg_type WHERE typname = 'zsd_dn'"));
            // The modifier the base type was written with is the domain's modifier.
            assertEquals("character varying(20)",
                    one("SELECT format_type(typbasetype, typtypmod) FROM pg_type"
                            + " WHERE typname = 'zsd_dv'"));
            // A default is kept in both the forms the catalogue keeps an expression in.
            assertEquals("1/1", one("SELECT typdefault, pg_get_expr(typdefaultbin, 0)"
                    + " FROM pg_type WHERE typname = 'zsd_dn'"));
        } finally {
            exec("DROP DOMAIN zsd_dv, zsd_dn");
        }
    }

    /** An array column's modifier is the one its element type was declared with. */
    @Test
    void anArrayColumnKeepsItsElementsModifier() throws SQLException {
        exec("CREATE TABLE zsd_a (n numeric(6,2)[], v varchar(9)[], t text[])");
        try {
            assertEquals(List.of("n/numeric(6,2)[]", "v/character varying(9)[]", "t/text[]"),
                    rows("SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute"
                            + " WHERE attrelid = 'zsd_a'::regclass AND attnum > 0"
                            + " ORDER BY attnum"));
        } finally {
            exec("DROP TABLE zsd_a");
        }
    }

    /** A routine's own settings are part of its definition. */
    @Test
    void aRoutineKeepsTheSettingsItPinned() throws SQLException {
        exec("CREATE FUNCTION zsd_f() RETURNS integer LANGUAGE sql SECURITY DEFINER"
                + " SET search_path TO public, pg_temp SET work_mem TO '4MB'"
                + " AS $$ SELECT 1 $$");
        try {
            assertEquals("{\"search_path=public, pg_temp\",work_mem=4MB}",
                    one("SELECT proconfig::text FROM pg_proc WHERE proname = 'zsd_f'"));
            String def = one("SELECT pg_get_functiondef('zsd_f()'::regprocedure)");
            assertTrue(def.contains(" SET search_path TO 'public', 'pg_temp'\n"), def);
            assertTrue(def.contains(" SET work_mem TO '4MB'\n"), def);
        } finally {
            exec("DROP FUNCTION zsd_f()");
        }
    }

    /** SET ... FROM CURRENT takes the value the session holds when the routine is defined. */
    @Test
    void aSettingTakenFromCurrentIsResolvedWhenItIsWritten() throws SQLException {
        exec("SET work_mem = '7MB'");
        exec("CREATE FUNCTION zsd_g() RETURNS integer LANGUAGE sql"
                + " SET work_mem FROM CURRENT AS $$ SELECT 1 $$");
        try {
            assertEquals("{work_mem=7MB}",
                    one("SELECT proconfig::text FROM pg_proc WHERE proname = 'zsd_g'"));
            // Changing the session's value afterwards leaves the routine's own alone.
            exec("SET work_mem = '9MB'");
            assertEquals("{work_mem=7MB}",
                    one("SELECT proconfig::text FROM pg_proc WHERE proname = 'zsd_g'"));
        } finally {
            exec("DROP FUNCTION zsd_g()");
            exec("RESET work_mem");
        }
    }

    /** A relation's access method is the one it stores its rows with, or none at all. */
    @Test
    void aRelationReportsTheAccessMethodItStoresWith() throws SQLException {
        exec("CREATE TABLE zsd_t (a int)");
        exec("CREATE VIEW zsd_v AS SELECT a FROM zsd_t");
        exec("CREATE MATERIALIZED VIEW zsd_m AS SELECT a FROM zsd_t");
        exec("CREATE TABLE zsd_p (id int, ts date) PARTITION BY RANGE (ts)");
        try {
            assertEquals(List.of("zsd_m/m/2", "zsd_p/p/0", "zsd_t/r/2", "zsd_v/v/0"),
                    rows("SELECT relname, relkind, relam FROM pg_class"
                            + " WHERE relname IN ('zsd_t','zsd_v','zsd_m','zsd_p')"
                            + " ORDER BY relname"));
        } finally {
            exec("DROP TABLE zsd_p");
            exec("DROP MATERIALIZED VIEW zsd_m");
            exec("DROP VIEW zsd_v");
            exec("DROP TABLE zsd_t");
        }
    }

    /** A materialized view holds rows, so it can be indexed, and the index is a relation. */
    @Test
    void anIndexOnAMaterializedViewIsARelationLikeAnyOther() throws SQLException {
        exec("CREATE TABLE zsd_b (a int, b int)");
        exec("CREATE MATERIALIZED VIEW zsd_mv AS SELECT a, b FROM zsd_b");
        exec("CREATE UNIQUE INDEX zsd_mi ON zsd_mv (a)");
        try {
            assertEquals("i", one("SELECT relkind FROM pg_class WHERE relname = 'zsd_mi'"));
            assertEquals("t", one("SELECT relhasindex FROM pg_class WHERE relname = 'zsd_mv'"));
        } finally {
            exec("DROP MATERIALIZED VIEW zsd_mv CASCADE");
            exec("DROP TABLE zsd_b");
        }
    }

    /** An index on a partition is a partition of the index on the parent. */
    @Test
    void anIndexOnAPartitionSaysItIsOne() throws SQLException {
        exec("CREATE TABLE zsd_pt (id int NOT NULL, ts date NOT NULL) PARTITION BY RANGE (ts)");
        exec("CREATE TABLE zsd_pta PARTITION OF zsd_pt"
                + " FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')");
        exec("CREATE INDEX zsd_pti ON zsd_pt (id)");
        try {
            assertEquals("zsd_pta_id_idx/t",
                    one("SELECT i.relname, i.relispartition FROM pg_index x"
                            + " JOIN pg_class i ON i.oid = x.indexrelid"
                            + " JOIN pg_class t ON t.oid = x.indrelid"
                            + " WHERE t.relname = 'zsd_pta'"));
            assertEquals("f", one("SELECT relispartition FROM pg_class"
                    + " WHERE relname = 'zsd_pti'"));
        } finally {
            exec("DROP TABLE zsd_pt CASCADE");
        }
    }

    /** A relation that can hold an oversized value has somewhere to put it. */
    @Test
    void aRelationThatCanHoldALongValueHasSomewhereToPutIt() throws SQLException {
        exec("CREATE TABLE zsd_x (a int, b date, c boolean)");
        exec("CREATE TABLE zsd_y (a varchar(10))");
        exec("CREATE TABLE zsd_z (a varchar(2000))");
        exec("CREATE TABLE zsd_w (a int, b int, c text)");
        try {
            // A narrow declaration fits in the page it is written on; a wide one may not, and a
            // text column has no declared width at all.
            assertEquals(List.of("zsd_w/true", "zsd_x/false", "zsd_y/false", "zsd_z/true"),
                    rows("SELECT relname, (reltoastrelid <> 0)::text FROM pg_class"
                            + " WHERE relname IN ('zsd_x','zsd_y','zsd_z','zsd_w')"
                            + " ORDER BY relname"));
            // What it points at is a relation, in the schema PostgreSQL keeps them in.
            assertEquals("t/pg_toast", one("SELECT c.relkind, n.nspname FROM pg_class c"
                    + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                    + " WHERE c.oid = (SELECT reltoastrelid FROM pg_class"
                    + " WHERE relname = 'zsd_w')"));
        } finally {
            exec("DROP TABLE zsd_x, zsd_y, zsd_z, zsd_w");
        }
    }
}
