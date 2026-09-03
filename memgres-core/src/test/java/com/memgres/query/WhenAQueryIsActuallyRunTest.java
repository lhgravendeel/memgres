package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * When the statement behind a definition is actually run.
 *
 * <p>DECLARE opens a portal and runs nothing: PostgreSQL plans the query then and executes it when
 * the cursor is first read, so a sequence the query draws from is not drawn from at the DECLARE --
 * and a cursor declared and rolled back leaves the sequence where it was.
 *
 * <p>A plain view keeps no rows either: its query is planned when the view is created and run when
 * the view is read. Running it to find the columns out moved every sequence its select list names.
 *
 * <p>And a query written to keep no row works nothing out: {@code SELECT nextval('s') LIMIT 0}
 * calls nothing.
 */
class WhenAQueryIsActuallyRunTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** A view's query is not run when the view is created. */
    @Test
    void whatCreatingAViewRuns() throws SQLException {
        exec("CREATE SEQUENCE zwq_s");
        exec("CREATE VIEW zwq_v AS SELECT nextval('zwq_s') AS n");
        assertEquals("1", one("SELECT nextval('zwq_s')::text"));
        // Reading the view is what runs it.
        assertEquals("2", one("SELECT n::text FROM zwq_v"));
        exec("DROP VIEW zwq_v");
        exec("DROP SEQUENCE zwq_s");
    }

    /** A cursor's query is not run when the cursor is declared. */
    @Test
    void whatDeclaringACursorRuns() throws SQLException {
        exec("CREATE SEQUENCE zwq_cs");
        exec("BEGIN");
        exec("DECLARE zwq_c CURSOR FOR SELECT nextval('zwq_cs')");
        exec("COMMIT");
        assertEquals("1", one("SELECT nextval('zwq_cs')::text"));
        // A name the query gets wrong is still wrong at the DECLARE.
        exec("BEGIN");
        assertEquals("42P01", stateOf("DECLARE zwq_c2 CURSOR FOR SELECT * FROM zwq_nosuch"));
        exec("ROLLBACK");
        exec("DROP SEQUENCE zwq_cs");
    }

    /** A query that keeps no row works nothing out. */
    @Test
    void whatAQueryKeepingNoRowWorksOut() throws SQLException {
        exec("CREATE SEQUENCE zwq_ls");
        exec("SELECT nextval('zwq_ls') LIMIT 0");
        assertEquals("55000", stateOf("SELECT currval('zwq_ls')"));
        // An OFFSET is not the same thing: the row is produced and then stepped over.
        exec("SELECT nextval('zwq_ls') OFFSET 1");
        assertEquals("1", one("SELECT currval('zwq_ls')::text"));
        // The shape is still the shape it would have answered with.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT nextval('zwq_ls') AS n LIMIT 0")) {
            assertEquals("n", rs.getMetaData().getColumnName(1));
            assertEquals("int8", rs.getMetaData().getColumnTypeName(1));
            assertFalse(rs.next());
        }
        // A set-returning call is as many rows as it returns, and an offset steps over those.
        assertEquals("2", one("SELECT count(*)::text FROM"
                + " (SELECT generate_series(1,10) g ORDER BY g LIMIT 2 OFFSET 8) s"));
        assertEquals("2", one("SELECT nextval('zwq_ls')::text"));
        exec("DROP SEQUENCE zwq_ls");
    }
}
