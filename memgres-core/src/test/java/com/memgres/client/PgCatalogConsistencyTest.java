package com.memgres.client;

import com.memgres.core.Memgres;
import com.memgres.engine.CatalogHelper;
import com.memgres.engine.DataType;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the catalog says about itself has to agree with what it answers, and be reachable.
 *
 * <p>Describing pg_catalog was only half the job. The listing in information_schema.columns
 * stopped at the first column of a type the type-name mapping did not know, and the caller
 * turned that into missing rows instead of an error, so pg_attribute and information_schema
 * flatly contradicted each other about the same relation. {@code SELECT *} returned one column
 * more than pg_attribute declared. Attributes were numbered in memgres's own order rather than
 * PostgreSQL's, and declared with memgres's own types — oid as integer, name as text, a parse
 * tree as text. References that had just been made to resolve pointed at rows that were never
 * added: fifteen typarrays, a hundred and fifteen aggregates. And a correlated NOT EXISTS over
 * a catalog, which is the shape of every integrity check a tool runs, scanned the whole relation
 * once per outer row.
 *
 * <p>Every assertion here was checked against a live PostgreSQL 18 first. They are shape and
 * reference invariants rather than counts: a live server's totals move with its version and its
 * installed extensions.
 */
class PgCatalogConsistencyTest {

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
            s.execute("CREATE TYPE pcx_mood AS ENUM ('sad','ok','happy')");
            s.execute("CREATE TABLE pcx_t (id integer PRIMARY KEY,"
                    + " code varchar(10) NOT NULL UNIQUE, descr text,"
                    + " amount numeric(10,2) DEFAULT 0.00,"
                    + " created timestamp without time zone, flag boolean DEFAULT false,"
                    + " m pcx_mood, arr integer[])");
            s.execute("CREATE TABLE pcx_c (cid bigserial PRIMARY KEY, pid integer)");
            s.execute("INSERT INTO pcx_t (id, code) VALUES (1,'a'), (2,'b'), (3,'c')");
            s.execute("INSERT INTO pcx_c (pid) VALUES (1), (3), (null)");
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

    /** The column names {@code SELECT *} answers with, in order. */
    private static List<String> starColumns(String relation) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM pg_catalog." + relation + " WHERE false")) {
            ResultSetMetaData md = rs.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) out.add(md.getColumnName(i));
        }
        return out;
    }

    // ---- the type-name mapping answers for every type there is ----

    @Test
    void everyDataTypeHasAnInformationSchemaName() {
        List<String> unnamed = new ArrayList<>();
        for (DataType dt : DataType.values()) {
            try {
                String name = CatalogHelper.pgTypeName(dt);
                if (name == null || name.isEmpty()) unnamed.add(dt.name());
            } catch (RuntimeException e) {
                unnamed.add(dt.name() + " (" + e.getMessage() + ")");
            }
        }
        assertEquals(new ArrayList<String>(), unnamed,
                "a type with no name here truncates every listing that reaches a column of it");
    }

    // ---- information_schema and pg_attribute agree ----

    @Test
    void informationSchemaListsEveryColumnOfEveryCatalogRelation() throws SQLException {
        assertEquals(new ArrayList<String>(), rows(
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'pg_catalog' AND c.relkind IN ('r','v')"
                + " AND c.relnatts <> (SELECT count(*) FROM information_schema.columns i"
                + "                    WHERE i.table_schema = 'pg_catalog'"
                + "                      AND i.table_name = c.relname)"
                + " ORDER BY 1"),
                "information_schema.columns and pg_class disagree about a relation's width");
    }

    @Test
    void pgTypeIsListedInFull() throws SQLException {
        assertEquals(
                "oid|typname|typnamespace|typowner|typlen|typbyval|typtype|typcategory"
                + "|typispreferred|typisdefined|typdelim|typrelid|typsubscript|typelem|typarray"
                + "|typinput|typoutput|typreceive|typsend|typmodin|typmodout|typanalyze|typalign"
                + "|typstorage|typnotnull|typbasetype|typtypmod|typndims|typcollation"
                + "|typdefaultbin|typdefault|typacl",
                String.join("|", rows(
                        "SELECT column_name FROM information_schema.columns"
                        + " WHERE table_schema = 'pg_catalog' AND table_name = 'pg_type'"
                        + " ORDER BY ordinal_position")));
    }

    @Test
    void informationSchemaListsTheCatalogRelationsThemselves() throws SQLException {
        assertEquals("BASE TABLE", one("SELECT table_type FROM information_schema.tables"
                + " WHERE table_schema = 'pg_catalog' AND table_name = 'pg_class'"));
        assertEquals("VIEW", one("SELECT table_type FROM information_schema.tables"
                + " WHERE table_schema = 'pg_catalog' AND table_name = 'pg_tables'"));
        assertEquals(0, count(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'pg_catalog' AND c.relkind IN ('r','v')"
                + " AND NOT EXISTS (SELECT 1 FROM information_schema.tables t"
                + "                 WHERE t.table_schema = 'pg_catalog'"
                + "                   AND t.table_name = c.relname)"));
    }

    // ---- selecting from a relation returns what the catalog says it holds ----

    @Test
    void selectStarReturnsExactlyWhatPgAttributeDeclares() throws SQLException {
        List<String> disagreeing = new ArrayList<>();
        for (String rel : rows("SELECT c.relname FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'pg_catalog' AND c.relkind IN ('r','v') ORDER BY 1")) {
            List<String> declared = rows(
                    "SELECT a.attname FROM pg_attribute a"
                    + " WHERE a.attrelid = '" + rel + "'::regclass AND a.attnum > 0"
                    + " ORDER BY a.attnum");
            if (!declared.equals(starColumns(rel))) disagreeing.add(rel);
        }
        assertEquals(new ArrayList<String>(), disagreeing,
                "a relation answers with columns pg_attribute does not declare");
    }

    // ---- attributes are numbered and typed the way PostgreSQL numbers and types them ----

    @Test
    void pgAttributeIsNumberedThePostgresWay() throws SQLException {
        assertEquals("attrelid|attname|atttypid|attlen|attnum|atttypmod|attndims|attbyval"
                        + "|attalign|attstorage|attcompression|attnotnull|atthasdef|atthasmissing"
                        + "|attidentity|attgenerated|attisdropped|attislocal|attinhcount"
                        + "|attcollation|attstattarget|attacl|attoptions|attfdwoptions"
                        + "|attmissingval",
                String.join("|", rows(
                        "SELECT a.attname FROM pg_attribute a"
                        + " WHERE a.attrelid = 'pg_attribute'::regclass AND a.attnum > 0"
                        + " ORDER BY a.attnum")));
    }

    @Test
    void pgConstraintIsNumberedAndTypedThePostgresWay() throws SQLException {
        assertEquals("oid oid|conname name|connamespace oid|contype \"char\""
                        + "|condeferrable boolean|condeferred boolean|conenforced boolean"
                        + "|convalidated boolean|conrelid oid|contypid oid|conindid oid"
                        + "|conparentid oid|confrelid oid|confupdtype \"char\""
                        + "|confdeltype \"char\"|confmatchtype \"char\"|conislocal boolean"
                        + "|coninhcount smallint|connoinherit boolean|conperiod boolean"
                        + "|conkey smallint[]|confkey smallint[]|conpfeqop oid[]|conppeqop oid[]"
                        + "|conffeqop oid[]|confdelsetcols smallint[]|conexclop oid[]"
                        + "|conbin pg_node_tree",
                String.join("|", rows(
                        "SELECT a.attname || ' ' || format_type(a.atttypid, a.atttypmod)"
                        + " FROM pg_attribute a WHERE a.attrelid = 'pg_constraint'::regclass"
                        + " AND a.attnum > 0 ORDER BY a.attnum")));
    }

    @Test
    void pgIndexAndPgTriggerAreNumberedThePostgresWay() throws SQLException {
        assertEquals("indexrelid|indrelid|indnatts|indnkeyatts|indisunique|indnullsnotdistinct"
                        + "|indisprimary|indisexclusion|indimmediate|indisclustered|indisvalid"
                        + "|indcheckxmin|indisready|indislive|indisreplident|indkey|indcollation"
                        + "|indclass|indoption|indexprs|indpred",
                String.join("|", rows(
                        "SELECT a.attname FROM pg_attribute a"
                        + " WHERE a.attrelid = 'pg_index'::regclass AND a.attnum > 0"
                        + " ORDER BY a.attnum")));
        assertEquals("oid|tgrelid|tgparentid|tgname|tgfoid|tgtype|tgenabled|tgisinternal"
                        + "|tgconstrrelid|tgconstrindid|tgconstraint|tgdeferrable|tginitdeferred"
                        + "|tgnargs|tgattr|tgargs|tgqual|tgoldtable|tgnewtable",
                String.join("|", rows(
                        "SELECT a.attname FROM pg_attribute a"
                        + " WHERE a.attrelid = 'pg_trigger'::regclass AND a.attnum > 0"
                        + " ORDER BY a.attnum")));
    }

    @Test
    void aParseTreeColumnIsTypedAsOne() throws SQLException {
        assertEquals("ev_action pg_node_tree|ev_qual pg_node_tree", String.join("|", rows(
                "SELECT a.attname || ' ' || format_type(a.atttypid, a.atttypmod)"
                + " FROM pg_attribute a WHERE a.attrelid = 'pg_rewrite'::regclass"
                + " AND a.attname IN ('ev_qual','ev_action') ORDER BY a.attname")));
        assertEquals("polqual pg_node_tree|polwithcheck pg_node_tree", String.join("|", rows(
                "SELECT a.attname || ' ' || format_type(a.atttypid, a.atttypmod)"
                + " FROM pg_attribute a WHERE a.attrelid = 'pg_policy'::regclass"
                + " AND a.attname IN ('polqual','polwithcheck') ORDER BY a.attname")));
        assertEquals("nspname name", one(
                "SELECT a.attname || ' ' || format_type(a.atttypid, a.atttypmod)"
                + " FROM pg_attribute a WHERE a.attrelid = 'pg_namespace'::regclass"
                + " AND a.attname = 'nspname'"));
        assertEquals("enumsortorder real|enumlabel name", String.join("|", rows(
                "SELECT a.attname || ' ' || format_type(a.atttypid, a.atttypmod)"
                + " FROM pg_attribute a WHERE a.attrelid = 'pg_enum'::regclass"
                + " AND a.attname IN ('enumsortorder','enumlabel') ORDER BY a.attnum")));
    }

    // ---- a user column's storage properties are its type's ----

    @Test
    void aColumnReportsItsTypesStorageProperties() throws SQLException {
        assertEquals("id,4,t,i,p|code,-1,f,i,x|descr,-1,f,i,x|amount,-1,f,i,m|created,8,t,d,p"
                        + "|flag,1,t,c,p|m,4,t,i,p|arr,-1,f,i,x",
                String.join("|", rows(
                        "SELECT a.attname, a.attlen, a.attbyval, a.attalign, a.attstorage"
                        + " FROM pg_attribute a WHERE a.attrelid = 'pcx_t'::regclass"
                        + " AND a.attnum > 0 ORDER BY a.attnum")));
        assertEquals("t,d,p", String.join("|", rows(
                "SELECT attbyval, attalign, attstorage FROM pg_attribute"
                + " WHERE attrelid = 'pcx_c'::regclass AND attname = 'cid'")));
    }

    @Test
    void aFixedWidthTypeSaysSo() throws SQLException {
        assertEquals("date,D,4,t|interval,T,16,f|oid,N,4,t|time,D,8,t|timestamp,D,8,t"
                        + "|timestamptz,D,8,t|uuid,U,16,f",
                String.join("|", rows(
                        "SELECT typname, typcategory, typlen, typbyval FROM pg_type"
                        + " WHERE typname IN ('date','time','timestamp','timestamptz','uuid',"
                        + "                   'oid','interval')"
                        + " ORDER BY typname")));
    }

    @Test
    void anArrayColumnReportsOneDimension() throws SQLException {
        assertEquals("1", one("SELECT attndims::text FROM pg_attribute"
                + " WHERE attrelid = 'pcx_t'::regclass AND attname = 'arr'"));
        assertEquals("0", one("SELECT attndims::text FROM pg_attribute"
                + " WHERE attrelid = 'pcx_t'::regclass AND attname = 'descr'"));
    }

    // ---- references reach something ----

    @Test
    void aTypeArrayReferenceResolves() throws SQLException {
        assertEquals(new ArrayList<String>(), rows(
                "SELECT t.typname FROM pg_type t WHERE t.typarray <> 0"
                + " AND NOT EXISTS (SELECT 1 FROM pg_type x WHERE x.oid = t.typarray)"
                + " ORDER BY t.typname"),
                "a typarray naming a type that was never added is a reference to nothing");
    }

    @Test
    void aTypeNamesIoFunctionsThatExist() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_type t WHERE t.typinput <> 0"
                + " AND NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid = t.typinput)"));
        assertEquals(0, count("SELECT count(*) FROM pg_type t WHERE t.typoutput <> 0"
                + " AND NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid = t.typoutput)"));
    }

    @Test
    void everyAggregateHasItsPgAggregateRow() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_proc WHERE prokind = 'a'"
                + " AND NOT EXISTS (SELECT 1 FROM pg_aggregate a WHERE a.aggfnoid = pg_proc.oid)"));
        // ... and the join every aggregate-introspection query makes reaches them
        assertTrue(count("SELECT count(*) FROM pg_aggregate a JOIN pg_proc p ON p.oid = a.aggfnoid"
                + " WHERE p.proname IN ('min','max','sum','count','avg')") >= 28,
                "the per-overload aggregate rows have to be reachable through aggfnoid");
    }

    @Test
    void aRelationsRowTypeExists() throws SQLException {
        assertEquals("pg_attribute,pg_attribute|pg_class,pg_class|pg_type,pg_type",
                String.join("|", rows(
                        "SELECT c.relname, t.typname FROM pg_class c"
                        + " JOIN pg_type t ON t.oid = c.reltype"
                        + " WHERE c.relname IN ('pg_class','pg_attribute','pg_type')"
                        + " ORDER BY c.relname")));
        assertEquals("pcx_t", one("SELECT t.typname FROM pg_class c"
                + " JOIN pg_type t ON t.oid = c.reltype WHERE c.relname = 'pcx_t'"));
    }

    // ---- an OID read out of a catalog column reads back as a name ----

    @Test
    void regtypeNamesTheTypeRatherThanEchoingTheOid() throws SQLException {
        assertEquals("name", one("SELECT 19::regtype::text"));
        assertEquals("\"char\"", one("SELECT 18::regtype::text"));
        assertEquals("pg_node_tree", one("SELECT 194::regtype::text"));
        assertEquals("jsonb[]", one("SELECT 3807::regtype::text"));
        assertEquals(0, count("SELECT count(*) FROM pg_type t"
                + " WHERE t.oid::regtype::text ~ '^[0-9]+$'"));
    }

    @Test
    void regprocNamesTheFunctionRatherThanEchoingTheOid() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_proc p"
                + " WHERE p.oid::regproc::text ~ '^[0-9]+$'"));
        assertEquals("=,text,name,boolean,texteqname", one(
                "SELECT oprname || ',' || oprleft::regtype::text || ','"
                + " || oprright::regtype::text || ',' || oprresult::regtype::text || ','"
                + " || oprcode::text FROM pg_operator"
                + " WHERE oprname = '=' AND oprleft = 'text'::regtype"
                + " AND oprright = 'name'::regtype"));
    }

    // ---- the rest of what the audit named ----

    @Test
    void aSuperuserIsReportedAsBypassingRowLevelSecurity() throws SQLException {
        assertEquals("t,t", String.join("|", rows("SELECT rolsuper, rolbypassrls FROM pg_roles"
                + " WHERE rolname = current_user")));
        assertEquals("t,t", String.join("|", rows("SELECT usesuper, usebypassrls FROM pg_user"
                + " WHERE usename = current_user")));
    }

    @Test
    void aBuiltinFunctionReportsItsStrictnessAndParallelSafety() throws SQLException {
        assertEquals("t,s,s", String.join("|", rows(
                "SELECT proisstrict, proparallel, provolatile FROM pg_proc WHERE proname = 'now'")));
        assertEquals("t,s,i", String.join("|", rows(
                "SELECT DISTINCT proisstrict, proparallel, provolatile FROM pg_proc"
                + " WHERE proname = 'upper'")));
        assertEquals("t,r,v", String.join("|", rows(
                "SELECT DISTINCT proisstrict, proparallel, provolatile FROM pg_proc"
                + " WHERE proname = 'random'")));
    }

    /**
     * The locale is spelt the way PostgreSQL spells it — en-US, with a hyphen — and the row that
     * carries that spelling is the ICU one.
     *
     * <p>This used to read the spelling off a row named {@code en_US}, and memgres carried one:
     * an ICU collation at encoding 6. Measured against PostgreSQL 18, that row does not exist in
     * that shape anywhere. What PostgreSQL has, and only on a host whose locale list offers it,
     * is a *libc* en_US at that locale's own encoding — 24 (WIN1252) on the reference server,
     * never 6 — so the row memgres offered was one no PostgreSQL server would match, and
     * COLLATE "en_US" failed on both engines anyway. The name has been dropped from
     * pg_collation; the spelling it was there to demonstrate is asserted here on en-US-x-icu,
     * which PostgreSQL does register wherever ICU is available.
     */
    @Test
    void anIcuLocaleIsSpeltTheWayPostgresSpellsIt() throws SQLException {
        // collprovider is a "char", which PostgreSQL will not concatenate without being told
        // which of its two text concatenations was meant.
        assertEquals("i,-1,en-US", one("SELECT collprovider::text || ',' || collencoding || ',' || colllocale"
                + " FROM pg_collation WHERE collname = 'en-US-x-icu'"));
        assertEquals("0", one("SELECT count(*) FROM pg_collation"
                + " WHERE collname = 'en_US' AND collencoding = 6"));
    }

    @Test
    void varcharOpsIsKeyedOnText() throws SQLException {
        assertEquals("btree,f,text|hash,f,text", String.join("|", rows(
                "SELECT am.amname, o.opcdefault, o.opcintype::regtype::text"
                + " FROM pg_opclass o JOIN pg_am am ON am.oid = o.opcmethod"
                + " WHERE o.opcname = 'varchar_ops' ORDER BY am.amname")));
    }

    // ---- a correlated EXISTS over a catalog is answered without rescanning it ----

    @Test
    void aCatalogIntegrityCheckAnswersPromptly() throws SQLException {
        // The shape every tool's integrity check has. Run as a scan per outer row it took
        // sixteen seconds against pg_operator and pg_proc; the timeout is what the audit's
        // harness used, so a return to that behaviour fails here rather than merely being slow.
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(10);
            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*) FROM pg_operator o WHERE o.oprcode <> 0"
                    + " AND NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid = o.oprcode)")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    void aCorrelatedExistsStillAnswersWhatItDidBefore() throws SQLException {
        // The key-set path has to agree with the scan it replaces, including about NULL: a row
        // whose key is NULL matches nothing, so EXISTS is false and NOT EXISTS is true.
        assertEquals("1,2", String.join(",", rows(
                "SELECT c.cid::text FROM pcx_c c"
                + " WHERE EXISTS (SELECT 1 FROM pcx_t t WHERE t.id = c.pid) ORDER BY c.cid")));
        assertEquals("3", String.join(",", rows(
                "SELECT c.cid::text FROM pcx_c c"
                + " WHERE NOT EXISTS (SELECT 1 FROM pcx_t t WHERE t.id = c.pid) ORDER BY c.cid")));
        assertEquals(3, count("SELECT count(*) FROM pcx_c c"
                + " WHERE NOT EXISTS (SELECT 1 FROM pcx_t t WHERE t.id = c.pid + 100)"));
        // A subquery that is not one relation tested on one key is still run as written
        assertEquals(2, count("SELECT count(*) FROM pcx_c c"
                + " WHERE EXISTS (SELECT 1 FROM pcx_t t WHERE t.id = c.pid AND t.code <> 'b')"));
    }
}
