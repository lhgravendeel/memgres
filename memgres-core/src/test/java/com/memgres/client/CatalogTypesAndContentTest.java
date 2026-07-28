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
 * What a tool that introspects the server can do with what the catalog says.
 *
 * <p>The catalogs were queryable but not truthful: object-identifier columns were text, so a
 * query that compares one to 0 or takes its length would not run; pg_cast listed conversions
 * PostgreSQL deliberately does not have; fifty information_schema views were listed with no
 * columns behind them; pg_proc rows carried a name and no signature; and array types were named
 * after an OID rather than after what they hold. Expectations captured from a live
 * PostgreSQL 18 server.
 */
class CatalogTypesAndContentTest {

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
            s.execute("CREATE TABLE ctc_t (id int PRIMARY KEY, name text)");
            s.execute("CREATE FUNCTION ctc_add(a int, b int) RETURNS bigint LANGUAGE sql"
                    + " AS $$ SELECT (a + b)::bigint $$");
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

    private static List<String> column(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }

    // ---- PG's own catalog-consistency checks have to be runnable ----

    @Test
    void typinputAndTypoutputCompareAgainstZero() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_type WHERE typinput = 0 OR typoutput = 0"));
    }

    @Test
    void proargtypesIsSearchableWithAny() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_proc WHERE 0 = ANY (proargtypes)"));
    }

    @Test
    void proargtypesHasAnArrayLength() throws SQLException {
        assertEquals(1, count("SELECT count(*) FROM pg_proc WHERE proname = 'ctc_add'"
                + " AND array_length(proargtypes, 1) = 2"));
    }

    // ---- the declared types of the object-identifier columns ----

    @Test
    void objectIdentifierColumnsAreOid() throws SQLException {
        assertEquals("oid", one("SELECT pg_typeof(oid)::text FROM pg_type WHERE typname = 'int4'"));
        assertEquals("oid", one("SELECT pg_typeof(typnamespace)::text FROM pg_type WHERE typname = 'int4'"));
        assertEquals("oid", one("SELECT pg_typeof(typelem)::text FROM pg_type WHERE typname = '_int4'"));
        assertEquals("oid", one("SELECT pg_typeof(typarray)::text FROM pg_type WHERE typname = 'int4'"));
        assertEquals("oid", one("SELECT pg_typeof(oid)::text FROM pg_class WHERE relname = 'ctc_t'"));
        assertEquals("oid", one("SELECT pg_typeof(relnamespace)::text FROM pg_class WHERE relname = 'ctc_t'"));
        assertEquals("oid", one("SELECT pg_typeof(atttypid)::text FROM pg_attribute"
                + " WHERE attrelid = 'ctc_t'::regclass AND attname = 'id'"));
        assertEquals("oid", one("SELECT pg_typeof(oid)::text FROM pg_namespace WHERE nspname = 'public'"));
        assertEquals("oid", one("SELECT pg_typeof(prorettype)::text FROM pg_proc WHERE proname = 'ctc_add'"));
        assertEquals("oid", one("SELECT pg_typeof(castsource)::text FROM pg_cast"
                + " WHERE castsource = 23 AND casttarget = 20"));
    }

    @Test
    void functionReferenceColumnsAreRegproc() throws SQLException {
        assertEquals("regproc", one("SELECT pg_typeof(typinput)::text FROM pg_type WHERE typname = 'int4'"));
        assertEquals("regproc", one("SELECT pg_typeof(typoutput)::text FROM pg_type WHERE typname = 'int4'"));
    }

    @Test
    void argumentListColumnIsOidvector() throws SQLException {
        assertEquals("oidvector",
                one("SELECT pg_typeof(proargtypes)::text FROM pg_proc WHERE proname = 'ctc_add'"));
    }

    @Test
    void regprocStillPrintsTheFunctionName() throws SQLException {
        assertEquals("int4in", one("SELECT typinput::text FROM pg_type WHERE typname = 'int4'"));
        assertEquals("int4out", one("SELECT typoutput::text FROM pg_type WHERE typname = 'int4'"));
        // PG names the I/O functions after the implementation, not always after the type
        assertEquals("numeric_in", one("SELECT typinput::text FROM pg_type WHERE typname = 'numeric'"));
        assertEquals("cash_in", one("SELECT typinput::text FROM pg_type WHERE typname = 'money'"));
        assertEquals("poly_in", one("SELECT typinput::text FROM pg_type WHERE typname = 'polygon'"));
        assertEquals("range_in", one("SELECT typinput::text FROM pg_type WHERE typname = 'int4range'"));
        assertEquals("multirange_in",
                one("SELECT typinput::text FROM pg_type WHERE typname = 'int4multirange'"));
        assertEquals("array_in", one("SELECT typinput::text FROM pg_type WHERE typname = '_text'"));
    }

    @Test
    void absentRegprocPrintsAsInvalidOid() throws SQLException {
        assertEquals("-", one("SELECT typmodin::text FROM pg_type WHERE typname = 'int4'"));
        assertEquals("-", one("SELECT typsubscript::text FROM pg_type WHERE typname = 'int4'"));
        assertEquals("numerictypmodin",
                one("SELECT typmodin::text FROM pg_type WHERE typname = 'numeric'"));
        assertEquals("array_subscript_handler",
                one("SELECT typsubscript::text FROM pg_type WHERE typname = '_int4'"));
        assertEquals("range_typanalyze",
                one("SELECT typanalyze::text FROM pg_type WHERE typname = 'daterange'"));
        assertEquals("array_typanalyze",
                one("SELECT typanalyze::text FROM pg_type WHERE typname = '_text'"));
    }

    // ---- pg_cast lists what PG lists, and nothing else ----

    @Test
    void noCastFromNumericTypesToText() throws SQLException {
        // Its absence is what makes '5'::text = 5 an error in PG rather than a comparison
        assertEquals(0, count("SELECT count(*) FROM pg_cast"
                + " WHERE castsource IN (20, 21, 23, 700, 701, 1700) AND casttarget = 25"));
    }

    @Test
    void noCastBetweenBooleanAndSmallint() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_cast"
                + " WHERE (castsource = 16 AND casttarget = 21) OR (castsource = 21 AND casttarget = 16)"));
    }

    @Test
    void noCastIntoJsonbExceptFromJson() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_cast WHERE casttarget = 3802"
                + " AND castsource IN (16, 20, 21, 23, 25, 700, 701, 1700)"));
    }

    @Test
    void noCastFromMoneyOrNameToInteger() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_cast"
                + " WHERE castsource IN (790, 19) AND casttarget IN (23, 20)"));
    }

    @Test
    void noCastFromPathToPoint() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_cast WHERE castsource = 602 AND casttarget = 600"));
    }

    @Test
    void onlyTypmodCoercionsCastATypeToItself() throws SQLException {
        // PG's ten same-type entries are length/precision coercions, not identity casts
        assertEquals(10, count("SELECT count(*) FROM pg_cast WHERE castsource = casttarget"));
        assertEquals(0, count("SELECT count(*) FROM pg_cast WHERE castsource = casttarget"
                + " AND castsource IN (23, 20, 21, 16, 25, 701, 700)"));
    }

    @Test
    void noCastBetweenTextAndTheTypesPgSpellsOut() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_cast WHERE castsource = 25"
                + " AND casttarget IN (23, 17, 114, 1114, 3802, 3614, 600, 829)"));
        assertEquals(0, count("SELECT count(*) FROM pg_cast WHERE casttarget = 25"
                + " AND castsource IN (603, 718, 1082, 1186, 114, 3802, 17, 3614, 600)"));
    }

    @Test
    void castContextAndMethodMatchPg() throws SQLException {
        assertEquals("i", one("SELECT castcontext::text FROM pg_cast"
                + " WHERE castsource = 1042 AND casttarget = 25"));  // bpchar -> text
        assertEquals("e", one("SELECT castcontext::text FROM pg_cast"
                + " WHERE castsource = 1560 AND casttarget = 23"));  // bit -> int4
        assertEquals("a", one("SELECT castcontext::text FROM pg_cast"
                + " WHERE castsource = 16 AND casttarget = 25"));    // bool -> text
        assertEquals("f", one("SELECT castmethod::text FROM pg_cast"
                + " WHERE castsource = 21 AND casttarget = 23"));
        assertEquals("f", one("SELECT castmethod::text FROM pg_cast"
                + " WHERE castsource = 1700 AND casttarget = 23"));
        assertEquals("i", one("SELECT castmethod::text FROM pg_cast"
                + " WHERE castsource = 114 AND casttarget = 3802"));
    }

    @Test
    void castFunctionIsNamedWhenThereIsOne() throws SQLException {
        assertEquals("1740", one("SELECT castfunc::text FROM pg_cast"
                + " WHERE castsource = 23 AND casttarget = 1700"));
        assertEquals("0", one("SELECT castfunc::text FROM pg_cast"
                + " WHERE castsource = 23 AND casttarget = 26"));
    }

    @Test
    void theCastsPgDoesHaveAreStillThere() throws SQLException {
        assertEquals(1, count("SELECT count(*) FROM pg_cast"
                + " WHERE castsource = 23 AND casttarget = 20 AND castcontext = 'i'"));
        assertEquals(1, count("SELECT count(*) FROM pg_cast"
                + " WHERE castsource = 114 AND casttarget = 3802"));
        assertEquals(1, count("SELECT count(*) FROM pg_cast"
                + " WHERE castsource = 650 AND casttarget = 869"));
    }

    // ---- an information_schema view that is listed answers for its columns ----

    @Test
    void listedInformationSchemaViewsHaveTheirColumns() throws SQLException {
        assertEquals(0, count("SELECT count(grantee) FROM information_schema.column_privileges"
                + " WHERE 1 = 0"));
        assertEquals(0, count("SELECT count(*) FROM (SELECT grantor, grantee, table_catalog,"
                + " table_schema, table_name, privilege_type, is_grantable, with_hierarchy"
                + " FROM information_schema.table_privileges WHERE 1 = 0) s"));
        assertEquals(0, count("SELECT count(*) FROM (SELECT object_catalog, object_schema,"
                + " object_name, object_type, dtd_identifier"
                + " FROM information_schema.data_type_privileges WHERE 1 = 0) s"));
        assertEquals(0, count("SELECT count(*) FROM (SELECT udt_name, attribute_name,"
                + " ordinal_position, data_type FROM information_schema.attributes WHERE 1 = 0) s"));
        assertEquals(0, count("SELECT count(*) FROM (SELECT feature_id, feature_name, is_supported"
                + " FROM information_schema.sql_features WHERE 1 = 0) s"));
        assertEquals(0, count("SELECT count(*) FROM (SELECT view_name, table_name, column_name"
                + " FROM information_schema.view_column_usage WHERE 1 = 0) s"));
        assertEquals(0, count("SELECT count(*) FROM (SELECT foreign_server_name,"
                + " foreign_data_wrapper_name FROM information_schema.foreign_servers"
                + " WHERE 1 = 0) s"));
    }

    @Test
    void oneRowInformationSchemaViewsCarryTheirRow() throws SQLException {
        assertEquals(1, count("SELECT count(*) FROM information_schema.information_schema_catalog_name"));
        assertEquals("UTF8", one("SELECT character_set_name FROM information_schema.character_sets"));
        assertEquals("UCS", one("SELECT character_repertoire FROM information_schema.character_sets"));
    }

    @Test
    void informationSchemaDescribesItsOwnViewColumns() throws SQLException {
        assertEquals(
                List.of("grantor", "grantee", "table_catalog", "table_schema", "table_name",
                        "column_name", "privilege_type", "is_grantable"),
                column("SELECT column_name FROM information_schema.columns"
                        + " WHERE table_schema = 'information_schema'"
                        + " AND table_name = 'column_privileges' ORDER BY ordinal_position"));
        assertEquals(7, count("SELECT count(*) FROM information_schema.columns"
                + " WHERE table_schema = 'information_schema' AND table_name = 'sql_features'"));
    }

    // ---- pg_proc rows carry a signature ----

    @Test
    void builtinsCarryTheirReturnType() throws SQLException {
        assertEquals(1, count("SELECT count(*) FROM pg_proc WHERE proname = 'upper' AND prorettype = 25"));
        assertEquals(1, count("SELECT count(*) FROM pg_proc WHERE proname = 'lower' AND prorettype = 25"));
        assertEquals(1, count("SELECT count(*) FROM pg_proc WHERE proname = 'now' AND prorettype = 1184"));
        assertEquals(1, count("SELECT count(*) FROM pg_proc WHERE proname = 'sqrt' AND prorettype = 701"));
        assertEquals(1, count("SELECT count(*) FROM pg_proc"
                + " WHERE proname = 'abs' AND prorettype = 23 AND pronargs = 1"));
    }

    @Test
    void builtinsCarryEveryOverloadPgDeclares() throws SQLException {
        assertEquals(6, count("SELECT count(*) FROM pg_proc WHERE proname = 'abs'"));
        assertEquals(8, count("SELECT count(*) FROM pg_proc WHERE proname = 'length'"));
        assertEquals(3, count("SELECT count(*) FROM pg_proc WHERE proname = 'upper'"));
    }

    @Test
    void builtinsCarryTheirArgumentTypes() throws SQLException {
        assertEquals(List.of("25", "3831", "4537"),
                column("SELECT proargtypes::text FROM pg_proc WHERE proname = 'upper'"));
        assertEquals(8, count("SELECT count(*) FROM pg_proc WHERE proname = 'to_char'"));
    }

    @Test
    void userFunctionCarriesItsSignature() throws SQLException {
        assertEquals("2", one("SELECT pronargs::text FROM pg_proc WHERE proname = 'ctc_add'"));
        assertEquals("20", one("SELECT prorettype::text FROM pg_proc WHERE proname = 'ctc_add'"));
        assertEquals("23 23", one("SELECT proargtypes::text FROM pg_proc WHERE proname = 'ctc_add'"));
        assertEquals(List.of("23", "23"),
                column("SELECT unnest(proargtypes)::text FROM pg_proc WHERE proname = 'ctc_add'"));
    }

    // ---- array types are named for what they hold, and linked back from it ----

    @Test
    void noArrayTypeIsNamedAfterAnOid() throws SQLException {
        assertEquals(0, count("SELECT count(*) FROM pg_type WHERE typname ~ '^_[0-9]+$'"));
    }

    @Test
    void arrayTypesOfUnmodelledElementsKeepTheirPgNames() throws SQLException {
        assertEquals("_char", one("SELECT typname FROM pg_type WHERE oid = 1002"));
        assertEquals("_regproc", one("SELECT typname FROM pg_type WHERE oid = 1008"));
        assertEquals("_regclass", one("SELECT typname FROM pg_type WHERE oid = 2210"));
        assertEquals("_regtype", one("SELECT typname FROM pg_type WHERE oid = 2211"));
        assertEquals("_oid", one("SELECT typname FROM pg_type WHERE oid = 1028"));
    }

    @Test
    void everyElementTypePointsAtItsArrayType() throws SQLException {
        // How pgjdbc's TypeInfoCache discovers array support for a type it does not hardcode
        assertEquals(0, count("SELECT count(*) FROM pg_type t1"
                + " WHERE t1.typname IN ('int2','int4','int8','float4','float8','numeric','text',"
                + " 'varchar','bpchar','name','bool','date','time','timetz','timestamp',"
                + " 'timestamptz','interval','uuid','bytea','json','jsonb','inet','cidr','macaddr',"
                + " 'macaddr8','oid','money','bit','varbit','xml','point','lseg','path','box',"
                + " 'polygon','circle','line','tsvector','tsquery','int4range','int8range',"
                + " 'numrange','daterange','tsrange','tstzrange','regproc','regclass','regtype',"
                + " 'oidvector','int2vector','xid')"
                + " AND NOT EXISTS (SELECT 1 FROM pg_type t2 WHERE t2.typname = '_' || t1.typname"
                + " AND t2.typelem = t1.oid AND t1.typarray = t2.oid)"));
    }

    @Test
    void typarrayResolvesToTheArrayTypesName() throws SQLException {
        assertEquals("1005", one("SELECT typarray::text FROM pg_type WHERE typname = 'int2'"));
        assertEquals("1231", one("SELECT typarray::text FROM pg_type WHERE typname = 'numeric'"));
        assertEquals("1028", one("SELECT typarray::text FROM pg_type WHERE typname = 'oid'"));
        assertEquals("_int2", one("SELECT typname FROM pg_type"
                + " WHERE oid = (SELECT typarray FROM pg_type WHERE typname = 'int2')"));
        assertEquals("_numeric", one("SELECT typname FROM pg_type"
                + " WHERE oid = (SELECT typarray FROM pg_type WHERE typname = 'numeric')"));
    }

    @Test
    void objectIdentifierTypesAreRegistered() throws SQLException {
        assertEquals(5, count("SELECT count(*) FROM pg_type"
                + " WHERE typname IN ('regproc','regclass','regtype','oidvector','int2vector')"));
        assertEquals("N", one("SELECT typcategory::text FROM pg_type WHERE typname = 'regproc'"));
        assertEquals("4", one("SELECT typlen::text FROM pg_type WHERE typname = 'regproc'"));
        assertEquals("A", one("SELECT typcategory::text FROM pg_type WHERE typname = 'oidvector'"));
        assertEquals("26", one("SELECT typelem::text FROM pg_type WHERE typname = 'oidvector'"));
        assertEquals("21", one("SELECT typelem::text FROM pg_type WHERE typname = 'int2vector'"));
    }

    // ---- neighbouring catalog behaviour that must keep working ----

    @Test
    void catalogJoinsOnObjectIdentifiersStillResolve() throws SQLException {
        assertEquals("pg_catalog", one("SELECT n.nspname FROM pg_proc p"
                + " JOIN pg_namespace n ON n.oid = p.pronamespace WHERE p.proname = 'upper'"));
        assertEquals("int4", one("SELECT e.typname FROM pg_type a JOIN pg_type e"
                + " ON e.oid = a.typelem WHERE a.typname = '_int4'"));
        assertEquals("ctc_t", one("SELECT c.relname FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'public' AND c.relname = 'ctc_t'"));
        assertEquals("int4", one("SELECT t.typname FROM pg_attribute a JOIN pg_type t"
                + " ON t.oid = a.atttypid WHERE a.attrelid = 'ctc_t'::regclass"
                + " AND a.attname = 'id'"));
    }

    @Test
    void typeClassificationIsUnchanged() throws SQLException {
        assertEquals("r", one("SELECT typtype::text FROM pg_type WHERE typname = 'int4range'"));
        assertEquals("m", one("SELECT typtype::text FROM pg_type WHERE typname = 'int4multirange'"));
        assertEquals("b", one("SELECT typtype::text FROM pg_type WHERE typname = 'int4'"));
        assertEquals("c", one("SELECT typtype::text FROM pg_type WHERE typname = 'ctc_t'"));
        assertEquals("A", one("SELECT typcategory::text FROM pg_type WHERE typname = '_int4'"));
    }

    @Test
    void nameLookupsThroughTheCatalogStillWork() throws SQLException {
        assertEquals("pg_backend_pid", one("SELECT 'pg_backend_pid'::regproc::text"));
        assertEquals("integer", one("SELECT 'int4'::regtype::text"));
        assertTrue(count("SELECT count(*) FROM pg_proc WHERE proname = 'ctc_add'") >= 1);
    }
}
