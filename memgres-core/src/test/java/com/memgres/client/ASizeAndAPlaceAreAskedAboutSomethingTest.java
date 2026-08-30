package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A size and a place are asked about something, and the something has to be there.
 *
 * <p>The size functions evaluated their argument for its own errors and then answered a constant,
 * so a mistyped relation name reported a relation holding eight kilobytes and a mistyped database
 * name reported a database that does not exist. pg_tablespace_location answered the empty string
 * for any OID at all, including OIDs no tablespace has — and the two tablespaces every cluster
 * has were numbered as though they had been created here rather than with the numbers PostgreSQL
 * fixes for them, so a reader following pg_tablespace.oid asked about a tablespace by a number
 * that names nothing.
 *
 * <p>TRUNCATE looked for a bare name in the default schema and public and nowhere else, so it
 * could not find a temporary table that CREATE, INSERT, SELECT and DROP all resolve. And a DO
 * block's error named the function it was raised in as "()", where PostgreSQL calls an anonymous
 * block inline_code_block.
 */
class ASizeAndAPlaceAreAskedAboutSomethingTest {

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

    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** A relation's size is asked about a relation, and a name has to name one. */
    @Test
    void aRelationsSizeIsAskedAboutARelation() throws SQLException {
        exec("CREATE TABLE zsp_t (a int)");
        try {
            assertEquals("42P01", stateOf("SELECT pg_relation_size('zsp_absent')"));
            assertEquals("42P01", stateOf("SELECT pg_total_relation_size('zsp_absent')"));
            assertEquals("42P01", stateOf("SELECT pg_table_size('zsp_absent')"));
            assertEquals("42P01", stateOf("SELECT pg_indexes_size('zsp_absent')"));
            // An OID is not a name, so one that names nothing is no size rather than an error.
            assertEquals("null", String.valueOf(one("SELECT pg_relation_size(999999::oid)")));
            assertNull(stateOf("SELECT pg_relation_size('zsp_t')"));
        } finally {
            exec("DROP TABLE zsp_t");
        }
    }

    /** A database's size is asked about a database. */
    @Test
    void aDatabasesSizeIsAskedAboutADatabase() throws SQLException {
        assertEquals("3D000", stateOf("SELECT pg_database_size('zsp_nodb')"));
        assertEquals("42704", stateOf("SELECT pg_database_size(999999::oid)"));
        assertNull(stateOf("SELECT pg_database_size(current_database())"));
    }

    /** A tablespace has the number PostgreSQL fixes for it, and a place only if it is one. */
    @Test
    void aTablespaceIsNumberedTheWayPostgresqlNumbersIt() throws SQLException {
        assertEquals(List.of("1663/pg_default", "1664/pg_global"),
                rows("SELECT oid::text, spcname FROM pg_tablespace ORDER BY oid"));
        assertEquals("", one("SELECT pg_tablespace_location(1663::oid)"));
        assertEquals("58P01", stateOf("SELECT pg_tablespace_location(999999::oid)"));
    }

    /** TRUNCATE finds a bare name where every other statement finds one. */
    @Test
    void truncateFindsATemporaryTable() throws SQLException {
        exec("CREATE TEMP TABLE zsp_tmp (a int)");
        exec("INSERT INTO zsp_tmp VALUES (1),(2)");
        try {
            assertEquals("2", one("SELECT count(*)::int FROM zsp_tmp"));
            exec("TRUNCATE zsp_tmp");
            assertEquals("0", one("SELECT count(*)::int FROM zsp_tmp"));
        } finally {
            exec("DROP TABLE zsp_tmp");
        }
    }

    /** An anonymous block is named the way PostgreSQL names one. */
    @Test
    void anAnonymousBlockIsNamedInlineCodeBlock() {
        try (Statement st = conn.createStatement()) {
            st.execute("DO $$ BEGIN RAISE EXCEPTION 'zsp boom'; END $$");
            fail("the block raises");
        } catch (SQLException e) {
            org.postgresql.util.ServerErrorMessage m =
                    ((org.postgresql.util.PSQLException) e).getServerErrorMessage();
            assertEquals("P0001", m.getSQLState());
            assertEquals("PL/pgSQL function inline_code_block line 1 at RAISE", m.getWhere());
        }
    }
}
