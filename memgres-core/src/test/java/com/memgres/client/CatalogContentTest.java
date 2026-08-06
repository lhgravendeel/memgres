package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What the catalogs say a server holds, where they used to answer for only part of it.
 *
 * <p>{@code pg_settings} carried 226 of PostgreSQL's parameters, so a tool that enumerates the
 * configuration saw a server missing 171 of them — and not one with a {@code backend} context at
 * all, which is a whole class of parameter rather than a few names.
 *
 * <p>{@code hstore} had no array type: {@code typarray} was 0, {@code hstore[]} did not resolve,
 * and a column declared with it stored something that did not read back. Every other base type
 * memgres carries has one, so the gap showed up as an inconsistency rather than as a missing
 * feature — {@code jsonb[]} worked and {@code hstore[]} did not.
 */
class CatalogContentTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS hstore");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    // ---------------------------------------------------------------- pg_settings

    /**
     * The parameter list is a whole one. The count is not pinned to PostgreSQL's exactly, because
     * that depends on how the server was built — the same reason {@code io_method} lists
     * {@code io_uring} on some builds and not others.
     */
    @Test
    void everyParameterIsCarried() throws Exception {
        assertTrue(Integer.parseInt(scalar("SELECT count(*)::text FROM pg_settings")) >= 390,
                "pg_settings should describe the whole configuration");
        // Every context PostgreSQL uses is represented, backend included.
        assertEquals("backend,internal,postmaster,sighup,superuser,superuser-backend,user",
                scalar("SELECT string_agg(DISTINCT context, ',' ORDER BY context) FROM pg_settings"));
        assertTrue(Integer.parseInt(scalar("SELECT count(*)::text FROM pg_settings"
                + " WHERE context = 'backend'")) > 0);
    }

    /** A parameter carries the metadata a client reads to decide what it may do with it. */
    @Test
    void aParameterCarriesItsMetadata() throws Exception {
        assertEquals("enum|user|Client Connection Defaults / Statement Behavior|escape,hex",
                scalar("SELECT vartype || '|' || context || '|' || category || '|'"
                        + " || array_to_string(enumvals, ',') FROM pg_settings WHERE name = 'bytea_output'"));
        assertEquals("integer|postmaster|Resource Usage / Memory|8kB|16|1073741823",
                scalar("SELECT vartype || '|' || context || '|' || category || '|' || unit || '|'"
                        + " || min_val || '|' || max_val FROM pg_settings WHERE name = 'shared_buffers'"));
        // A newly carried one, with the sentence PostgreSQL puts under the short description.
        assertEquals("0 disables the timeout.",
                scalar("SELECT extra_desc FROM pg_settings WHERE name = 'transaction_timeout'"));
        assertEquals("Sets the maximum number of concurrent connections.",
                scalar("SELECT short_desc FROM pg_settings WHERE name = 'max_connections'"));
    }

    /**
     * The File Locations parameters name files a running server was started with, so nothing was
     * compiled in for them. memgres has no such files and says so — but it may not claim the empty
     * string was the built-in default, because that is what {@code boot_val} means.
     */
    @Test
    void aParameterWithNoCompiledDefaultSaysSo() throws Exception {
        for (String name : new String[]{"config_file", "data_directory", "hba_file", "ident_file"}) {
            assertEquals("", scalar("SELECT setting FROM pg_settings WHERE name = '" + name + "'"), name);
            assertNull(scalar("SELECT boot_val FROM pg_settings WHERE name = '" + name + "'"), name);
        }
        assertNull(scalar("SELECT boot_val FROM pg_settings WHERE name = 'timezone_abbreviations'"));
    }

    /** A parameter that is carried can be read the three ways a client reads one. */
    @Test
    void aNewlyCarriedParameterAnswersEveryWay() throws Exception {
        assertEquals("on", scalar("SELECT current_setting('allow_alter_system')"));
        assertEquals("on", scalar("SHOW allow_alter_system"));
        assertEquals("on", scalar("SELECT setting FROM pg_settings WHERE name = 'allow_alter_system'"));
    }

    // ---------------------------------------------------------------- hstore[]

    /** hstore has an array type, and following typarray from it reaches one. */
    @Test
    void hstoreHasAnArrayType() throws Exception {
        assertEquals("_hstore", scalar("SELECT (SELECT a.typname FROM pg_type a"
                + " WHERE a.oid = t.typarray) FROM pg_type t WHERE t.typname = 'hstore'"));
        assertEquals("hstore", scalar("SELECT typelem::regtype::text FROM pg_type WHERE typname = '_hstore'"));
        assertEquals("A", scalar("SELECT typcategory FROM pg_type WHERE typname = '_hstore'"));
    }

    /** And it is a type a table may be built on, which is what having one is for. */
    @Test
    void aColumnMayBeAnArrayOfHstore() throws Exception {
        exec("DROP TABLE IF EXISTS cc_h");
        exec("CREATE TABLE cc_h (h hstore[])");
        assertEquals("hstore[]", scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute"
                + " WHERE attrelid = 'cc_h'::regclass AND attname = 'h'"));
        assertEquals("ARRAY/_hstore", scalar("SELECT data_type || '/' || udt_name"
                + " FROM information_schema.columns WHERE table_name = 'cc_h' AND column_name = 'h'"));
        exec("INSERT INTO cc_h VALUES (ARRAY['a=>1'::hstore, 'b=>2'::hstore])");
        // The value that goes in is the value that comes back out.
        assertEquals("\"a\"=>\"1\"", scalar("SELECT h[1]::text FROM cc_h"));
        assertEquals("\"b\"=>\"2\"", scalar("SELECT h[2]::text FROM cc_h"));
        assertEquals("2", scalar("SELECT array_length(h, 1)::text FROM cc_h"));
        exec("DROP TABLE cc_h");
    }

    // ---------------------------------------------------------------- array literals

    /**
     * An element is written into an array literal the way the value itself is written, and quoted
     * where its own text would otherwise run into the punctuation around it.
     *
     * <p>Anything memgres does not hold as a string went in through {@code toString} and unquoted:
     * a timestamp arrived as Java's {@code 2020-01-01T10:00}, and an hstore, an interval and a
     * tsvector arrived bare, so the literal could not be read back.
     */
    @Test
    void anArrayQuotesTheElementsThatNeedIt() throws Exception {
        assertEquals("{\"2020-01-01 10:00:00\"}",
                scalar("SELECT (ARRAY[timestamp '2020-01-01 10:00:00'])::text"));
        assertEquals("{\"1 day 02:00:00\"}", scalar("SELECT (ARRAY[interval '1 day 2 hours'])::text"));
        assertEquals("{\"'a' 'b'\"}", scalar("SELECT (ARRAY['a b'::tsvector])::text"));
        assertEquals("{\"\\\"a\\\"=>\\\"1\\\"\"}", scalar("SELECT (ARRAY['a=>1'::hstore])::text"));
        // What did not need quoting still does not get it.
        assertEquals("{1,2}", scalar("SELECT (ARRAY[1,2])::text"));
        assertEquals("{2020-01-01}", scalar("SELECT (ARRAY[date '2020-01-01'])::text"));
        assertEquals("{1.50}", scalar("SELECT (ARRAY[1.50::numeric])::text"));
    }
}
