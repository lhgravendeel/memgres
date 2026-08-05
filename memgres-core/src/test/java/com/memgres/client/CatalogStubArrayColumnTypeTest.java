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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The pg_catalog relations built by CatalogStubBuilder said a column was a scalar where
 * PostgreSQL says it is an array.
 *
 * <p>Every foreign-data catalog carries a {@code text[]} of {@code key=value} options, an event
 * trigger carries a {@code text[]} of command tags, a subscription a {@code text[]} of
 * publications, and a partitioned table three vectors — {@code int2vector} of key columns and two
 * {@code oidvector}s beside them. memgres wrote exactly the right value into each and then
 * declared the column text, so {@code unnest(fdwoptions)} worked while pg_attribute,
 * format_type, pg_typeof and the wire type all reported a string. That is the same defect a
 * {@code domain[]} column has — the data is there and the advertised type makes it unreadable to
 * a client that reads by type.
 *
 * <p>Two more things the same relations claimed and could not back: pg_am.amhandler is a regproc
 * and printed its raw OID instead of {@code bthandler}, and pg_ts_config_map listed two of the
 * nineteen token types the shipped configurations actually lexize, so the catalog said memgres
 * would drop an email address or a URL out of a tsvector it in fact indexes.
 *
 * <p>Every expectation here was measured on PostgreSQL 18. Values that vary with the host — the
 * OIDs initdb hands out, the contents of pg_hba.conf, the extensions installed — are deliberately
 * not asserted.
 */
class CatalogStubArrayColumnTypeTest {

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

    private static String one(String sql) throws SQLException {
        List<String> r = rows(sql);
        return r.isEmpty() ? null : r.get(0);
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

    /** What format_type says a pg_catalog relation's column is. */
    private static String columnType(String relname, String attname) throws SQLException {
        return one("SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a"
                + " JOIN pg_class c ON c.oid = a.attrelid"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE n.nspname = 'pg_catalog' AND c.relname = '" + relname + "'"
                + " AND a.attname = '" + attname + "'");
    }

    // ---------------------------------------------------------------
    //  The option arrays of the foreign-data catalogs
    // ---------------------------------------------------------------

    @Test
    void foreignDataCatalogOptionColumnsAreTextArrays() throws Exception {
        assertEquals("text[]", columnType("pg_foreign_data_wrapper", "fdwoptions"));
        assertEquals("text[]", columnType("pg_foreign_server", "srvoptions"));
        assertEquals("text[]", columnType("pg_user_mapping", "umoptions"));
        assertEquals("text[]", columnType("pg_user_mappings", "umoptions"));
        assertEquals("text[]", columnType("pg_foreign_table", "ftoptions"));
    }

    @Test
    void aWrittenOptionListReadsBackAsAnArray() throws Exception {
        exec("DROP FOREIGN DATA WRAPPER IF EXISTS csa_fdw CASCADE");
        exec("CREATE FOREIGN DATA WRAPPER csa_fdw OPTIONS (k1 'v1', k2 'v2')");
        exec("CREATE SERVER csa_srv FOREIGN DATA WRAPPER csa_fdw OPTIONS (host 'h', port '5')");
        try {
            assertEquals("{k1=v1,k2=v2}", one("SELECT fdwoptions::text"
                    + " FROM pg_foreign_data_wrapper WHERE fdwname = 'csa_fdw'"));
            // The value already answered to these; what changed is that the column now says so.
            assertEquals(java.util.Arrays.asList("k1=v1", "k2=v2"),
                    rows("SELECT unnest(fdwoptions) FROM pg_foreign_data_wrapper"
                            + " WHERE fdwname = 'csa_fdw'"));
            assertEquals("host=h", one("SELECT srvoptions[1] FROM pg_foreign_server"
                    + " WHERE srvname = 'csa_srv'"));
            assertEquals("2", one("SELECT array_length(srvoptions, 1) FROM pg_foreign_server"
                    + " WHERE srvname = 'csa_srv'"));
            assertEquals("text[]", one("SELECT pg_typeof(srvoptions)::text FROM pg_foreign_server"
                    + " WHERE srvname = 'csa_srv'"));
        } finally {
            exec("DROP FOREIGN DATA WRAPPER IF EXISTS csa_fdw CASCADE");
        }
    }

    /**
     * PostgreSQL orders pg_foreign_server srvacl then srvoptions. memgres had them swapped, so a
     * reader taking the row positionally took the ACL out of the options column.
     */
    @Test
    void foreignServerColumnsAreInPostgresOrder() throws Exception {
        assertEquals(java.util.Arrays.asList(
                        "oid", "srvname", "srvowner", "srvfdw", "srvtype", "srvversion",
                        "srvacl", "srvoptions"),
                rows("SELECT a.attname FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE n.nspname = 'pg_catalog' AND c.relname = 'pg_foreign_server'"
                        + " AND a.attnum > 0 AND a.attname <> 'xmin' ORDER BY a.attnum"));
    }

    // ---------------------------------------------------------------
    //  Event-trigger tags, subscription publications, hba lists
    // ---------------------------------------------------------------

    @Test
    void eventTriggerTagsAreATextArray() throws Exception {
        assertEquals("text[]", columnType("pg_event_trigger", "evttags"));
        exec("DROP EVENT TRIGGER IF EXISTS csa_et");
        exec("DROP FUNCTION IF EXISTS csa_etf()");
        exec("CREATE FUNCTION csa_etf() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END $$");
        exec("CREATE EVENT TRIGGER csa_et ON ddl_command_start"
                + " WHEN TAG IN ('CREATE TABLE','DROP TABLE') EXECUTE FUNCTION csa_etf()");
        try {
            assertEquals("{\"CREATE TABLE\",\"DROP TABLE\"}",
                    one("SELECT evttags::text FROM pg_event_trigger WHERE evtname = 'csa_et'"));
            assertEquals("2", one("SELECT array_length(evttags, 1) FROM pg_event_trigger"
                    + " WHERE evtname = 'csa_et'"));
            assertEquals("text[]", one("SELECT pg_typeof(evttags)::text FROM pg_event_trigger"
                    + " WHERE evtname = 'csa_et'"));
        } finally {
            exec("DROP EVENT TRIGGER IF EXISTS csa_et");
            exec("DROP FUNCTION IF EXISTS csa_etf()");
        }
    }

    @Test
    void subscriptionAndHbaListColumnsAreTextArrays() throws Exception {
        assertEquals("text[]", columnType("pg_subscription", "subpublications"));
        assertEquals("text[]", columnType("pg_hba_file_rules", "database"));
        assertEquals("text[]", columnType("pg_hba_file_rules", "user_name"));
        assertEquals("text[]", columnType("pg_hba_file_rules", "options"));
        assertEquals("text[]", columnType("pg_tablespace", "spcoptions"));
        // name[], not text[]: requires names extensions, and an extension name is an identifier.
        assertEquals("name[]", columnType("pg_available_extension_versions", "requires"));
    }

    /**
     * An hba line names a list of databases and a list of roles. Flattened to the bare word
     * {@code all} it could not be told from one database literally called "all,other".
     */
    @Test
    void hbaRuleListsAreArrayValues() throws Exception {
        assertEquals("{all}", one("SELECT database::text FROM pg_hba_file_rules"
                + " ORDER BY rule_number LIMIT 1"));
        assertEquals("all", one("SELECT user_name[1] FROM pg_hba_file_rules"
                + " ORDER BY rule_number LIMIT 1"));
    }

    // ---------------------------------------------------------------
    //  The vectors of a partitioned table and of extended statistics
    // ---------------------------------------------------------------

    @Test
    void partitionKeyVectorsAreVectorsAndHaveOneEntryPerKeyColumn() throws Exception {
        assertEquals("int2vector", columnType("pg_partitioned_table", "partattrs"));
        assertEquals("oidvector", columnType("pg_partitioned_table", "partclass"));
        assertEquals("oidvector", columnType("pg_partitioned_table", "partcollation"));
        assertEquals("pg_node_tree", columnType("pg_partitioned_table", "partexprs"));

        exec("DROP TABLE IF EXISTS csa_p CASCADE");
        exec("CREATE TABLE csa_p (a int, b int) PARTITION BY RANGE (a, b)");
        try {
            // PostgreSQL: 2 | 1 2 | <two opclass oids> | 0 0. memgres does not record which
            // operator class a key was resolved through, so both oidvectors read 0 — but they
            // are now as long as partnatts says, where a two-column key used to describe one.
            assertEquals("2|1 2|0 0|0 0", one(
                    "SELECT partnatts, partattrs::text, partclass::text, partcollation::text"
                            + " FROM pg_partitioned_table WHERE partrelid = 'csa_p'::regclass"));
        } finally {
            exec("DROP TABLE IF EXISTS csa_p CASCADE");
        }
    }

    @Test
    void extendedStatisticsKeysAreAnInt2Vector() throws Exception {
        assertEquals("int2vector", columnType("pg_statistic_ext", "stxkeys"));
        // PostgreSQL orders stxstattarget before stxkind.
        assertEquals(java.util.Arrays.asList(
                        "oid", "stxrelid", "stxname", "stxnamespace", "stxowner", "stxkeys",
                        "stxstattarget", "stxkind", "stxexprs"),
                rows("SELECT a.attname FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE n.nspname = 'pg_catalog' AND c.relname = 'pg_statistic_ext'"
                        + " AND a.attnum > 0 ORDER BY a.attnum"));

        exec("DROP TABLE IF EXISTS csa_s CASCADE");
        exec("CREATE TABLE csa_s (a int, b int)");
        exec("CREATE STATISTICS csa_st (ndistinct) ON a, b FROM csa_s");
        try {
            assertEquals("1 2|{d}", one("SELECT stxkeys::text, stxkind::text"
                    + " FROM pg_statistic_ext WHERE stxname = 'csa_st'"));
        } finally {
            exec("DROP TABLE IF EXISTS csa_s CASCADE");
        }
    }

    @Test
    void publicationColumnListIsAnInt2Vector() throws Exception {
        assertEquals("int2vector", columnType("pg_publication_rel", "prattrs"));
        assertEquals("pg_node_tree", columnType("pg_publication_rel", "prqual"));
    }

    // ---------------------------------------------------------------
    //  Claims the catalog made and could not back
    // ---------------------------------------------------------------

    /** amhandler is a regproc; PostgreSQL prints the handler's name, memgres printed its OID. */
    @Test
    void accessMethodHandlerPrintsItsName() throws Exception {
        assertEquals(java.util.Arrays.asList(
                        "brin|brinhandler", "btree|bthandler", "gin|ginhandler",
                        "gist|gisthandler", "hash|hashhandler", "heap|heap_tableam_handler",
                        "spgist|spghandler"),
                rows("SELECT amname, amhandler::text FROM pg_am ORDER BY amname"));
        assertEquals("regproc", columnType("pg_am", "amhandler"));
        // And the name it prints is a row pg_proc really has.
        assertEquals(7L, (long) rows("SELECT a.amname FROM pg_am a"
                + " JOIN pg_proc p ON p.oid = a.amhandler").size());
    }

    /**
     * PostgreSQL maps nineteen of the parser's twenty-three token types in both shipped
     * configurations — everything except blank, tag, protocol and entity, which carry no lexeme.
     */
    @Test
    void shippedTextSearchConfigurationsMapEveryTokenTypeTheyLexize() throws Exception {
        assertEquals("19", one("SELECT count(*) FROM pg_ts_config_map m"
                + " JOIN pg_ts_config c ON c.oid = m.mapcfg WHERE c.cfgname = 'simple'"));
        assertEquals("19", one("SELECT count(*) FROM pg_ts_config_map m"
                + " JOIN pg_ts_config c ON c.oid = m.mapcfg WHERE c.cfgname = 'english'"));
        String expected = "asciihword,asciiword,email,file,float,host,hword,hword_asciipart,"
                + "hword_numpart,hword_part,int,numhword,numword,sfloat,uint,url,url_path,"
                + "version,word";
        for (String cfg : new String[]{"simple", "english"}) {
            assertEquals(expected, one(
                    "SELECT string_agg(DISTINCT t.alias, ',' ORDER BY t.alias)"
                            + " FROM pg_ts_config_map m JOIN pg_ts_config c ON c.oid = m.mapcfg"
                            + " JOIN ts_token_type(c.cfgparser) t ON t.tokid = m.maptokentype"
                            + " WHERE c.cfgname = '" + cfg + "'"),
                    "mapping of " + cfg);
        }
        // Every mapping points at a dictionary that is there, and every config at a real parser.
        assertEquals("38", one("SELECT count(*) FROM pg_ts_config_map m"
                + " JOIN pg_ts_dict d ON d.oid = m.mapdict"));
        assertEquals("2", one("SELECT count(*) FROM pg_ts_config c"
                + " JOIN pg_ts_parser p ON p.oid = c.cfgparser"));
        assertEquals("2", one("SELECT count(*) FROM pg_ts_dict d"
                + " JOIN pg_ts_template t ON t.oid = d.dicttemplate"));
    }

    /**
     * The mapping is only worth anything if the server really lexizes those token types. It does:
     * an email address, a URL, a file path and a float all reach the tsvector.
     */
    @Test
    void theMappedTokenTypesAreOnesTheConfigurationsActuallyLexize() throws Exception {
        assertEquals("'a@b.com':1", one(
                "SELECT to_tsvector('simple', 'a@b.com')::text"));
        assertEquals("'3.5':1", one("SELECT to_tsvector('english', '3.5')::text"));
        assertEquals("'42':1", one("SELECT to_tsvector('english', '42')::text"));
    }

    /**
     * Which dictionary each token type goes through, which is the whole point of the mapping.
     *
     * <p>A snowball stemmer is for words. PostgreSQL's {@code english} configuration sends the six
     * word-shaped token types through {@code english_stem} and everything else — an email address,
     * a URL, a host name, a version string, a number — through {@code simple}, because stemming
     * those would change a value that has to come back out as it went in. Naming english_stem for
     * all nineteen said the server would drop the tail off an address it in fact indexes whole,
     * and contradicted what {@code to_tsvector} here actually does.
     */
    @Test
    void theEnglishConfigurationStemsWordsAndLeavesEverythingElseAlone() throws Exception {
        assertEquals("asciihword,asciiword,hword,hword_asciipart,hword_part,word", one(
                "SELECT string_agg(DISTINCT t.alias, ',' ORDER BY t.alias)"
                        + " FROM pg_ts_config_map m JOIN pg_ts_config c ON c.oid = m.mapcfg"
                        + " JOIN pg_ts_dict d ON d.oid = m.mapdict"
                        + " JOIN ts_token_type(c.cfgparser) t ON t.tokid = m.maptokentype"
                        + " WHERE c.cfgname = 'english' AND d.dictname = 'english_stem'"));
        assertEquals("email,file,float,host,hword_numpart,int,numhword,numword,sfloat,uint,"
                + "url,url_path,version", one(
                "SELECT string_agg(DISTINCT t.alias, ',' ORDER BY t.alias)"
                        + " FROM pg_ts_config_map m JOIN pg_ts_config c ON c.oid = m.mapcfg"
                        + " JOIN pg_ts_dict d ON d.oid = m.mapdict"
                        + " JOIN ts_token_type(c.cfgparser) t ON t.tokid = m.maptokentype"
                        + " WHERE c.cfgname = 'english' AND d.dictname = 'simple'"));
        // 'simple' stems nothing, so every one of its nineteen goes through the simple dictionary.
        assertEquals("19", one("SELECT count(*) FROM pg_ts_config_map m"
                + " JOIN pg_ts_config c ON c.oid = m.mapcfg JOIN pg_ts_dict d ON d.oid = m.mapdict"
                + " WHERE c.cfgname = 'simple' AND d.dictname = 'simple'"));
        // And the engine agrees with the catalog: the word is stemmed, the address is not.
        assertEquals("'run':1", one("SELECT to_tsvector('english', 'running')::text"));
        assertEquals("'user@example.com':1", one(
                "SELECT to_tsvector('english', 'user@example.com')::text"));
        assertEquals("'12345':1", one("SELECT to_tsvector('english', '12345')::text"));
    }

    /**
     * pg_ts_template's snowball row used to carry OID 3726, which PostgreSQL hands to the
     * function dsimple_lexize — a number that resolves to something else entirely on a real
     * server. The three pinned template OIDs are the ones PostgreSQL writes in its .dat file.
     */
    @Test
    void textSearchTemplateOidsDoNotCollideWithPostgresPinnedOids() throws Exception {
        assertEquals("3727", one("SELECT oid::text FROM pg_ts_template WHERE tmplname = 'simple'"));
        assertEquals("3730", one("SELECT oid::text FROM pg_ts_template WHERE tmplname = 'synonym'"));
        assertEquals("3765", one("SELECT oid::text FROM pg_ts_dict WHERE dictname = 'simple'"));
        assertEquals("3722", one("SELECT oid::text FROM pg_ts_parser WHERE prsname = 'default'"));
        assertEquals("3748", one("SELECT oid::text FROM pg_ts_config WHERE cfgname = 'simple'"));
        assertEquals("0", one("SELECT count(*) FROM pg_ts_template WHERE oid = 3726"));
    }

    // ---------------------------------------------------------------
    //  What a driver sees on the wire
    // ---------------------------------------------------------------

    /**
     * The point of the declared type: pgjdbc classifies a column from the OID in the row
     * description, so an option list advertised as text came back as a String and getArray on it
     * threw. It now comes back as a java.sql.Array.
     */
    @Test
    void optionArraysReadBackThroughTheDriverAsArrays() throws Exception {
        exec("DROP FOREIGN DATA WRAPPER IF EXISTS csa_fdw2 CASCADE");
        exec("CREATE FOREIGN DATA WRAPPER csa_fdw2 OPTIONS (a '1', b '2')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT fdwoptions FROM pg_foreign_data_wrapper"
                     + " WHERE fdwname = 'csa_fdw2'")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("_text", md.getColumnTypeName(1));
            assertEquals(java.sql.Types.ARRAY, md.getColumnType(1));
            assertNotNull(rs.next() ? rs.getArray(1) : null);
        } finally {
            exec("DROP FOREIGN DATA WRAPPER IF EXISTS csa_fdw2 CASCADE");
        }
    }
}
