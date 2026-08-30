package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The case a name folds to is the server's business, not the machine's.
 *
 * <p>An unquoted identifier and a keyword are folded to lower case before anything looks at them,
 * and folding them through the JVM's default locale made that answer depend on where the machine
 * thought it was: under a Turkish locale a capital I folds to a dotless ı, so the type "int" in
 * "CREATE TABLE t (ID int)" was read as "ınt" and the definition was refused — as was every
 * reference to a relation whose name held an I. A server whose behaviour turns on the operating
 * system's locale is a server that behaves differently in CI than on the machine that wrote the
 * test, which is the one thing a test database must not do.
 */
class TheServerFoldsCaseItsOwnWayTest {

    static Locale original;
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        original = Locale.getDefault();
        // The locale whose case folding differs from every other one's for the letter I.
        Locale.setDefault(new Locale("tr", "TR"));
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        try {
            if (conn != null) conn.close();
            if (memgres != null) memgres.close();
        } finally {
            Locale.setDefault(original);
        }
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

    /** A definition naming types and columns with a capital I is read the same way anywhere. */
    @Test
    void aDefinitionIsReadTheSameWhereverTheServerRuns() throws SQLException {
        exec("CREATE TABLE ztc_INDEX (ID int, TITLE text, INFO varchar(10))");
        try {
            exec("CREATE INDEX ztc_i ON ztc_INDEX (ID)");
            exec("INSERT INTO ztc_index (id, title, info) VALUES (1, 'x', 'y')");
            assertEquals("1/x/y", one("SELECT ID, TITLE, INFO FROM ztc_INDEX"));
            // The names the catalogue holds are the folded ones, folded the server's way.
            assertEquals(List.of("id", "title", "info"),
                    rows("SELECT attname FROM pg_attribute"
                            + " WHERE attrelid = 'ztc_index'::regclass AND attnum > 0"
                            + " ORDER BY attnum"));
            assertEquals("integer/text/character varying(10)",
                    one("SELECT format_type(a.atttypid, a.atttypmod) || '/'"
                            + " || format_type(b.atttypid, b.atttypmod) || '/'"
                            + " || format_type(c.atttypid, c.atttypmod)"
                            + " FROM pg_attribute a, pg_attribute b, pg_attribute c"
                            + " WHERE a.attrelid = 'ztc_index'::regclass AND a.attname = 'id'"
                            + " AND b.attrelid = a.attrelid AND b.attname = 'title'"
                            + " AND c.attrelid = a.attrelid AND c.attname = 'info'"));
        } finally {
            exec("DROP TABLE ztc_INDEX");
        }
    }

    /** A view over such a table resolves its names the same way. */
    @Test
    void aViewResolvesItsNamesTheSameWay() throws SQLException {
        exec("CREATE TABLE ztc_LIST (ID int)");
        exec("INSERT INTO ztc_list VALUES (1), (2)");
        try {
            exec("CREATE VIEW ztc_v AS SELECT ID FROM ztc_LIST WHERE ID > 1");
            assertEquals("1", one("SELECT count(*)::int FROM ztc_v"));
        } finally {
            exec("DROP VIEW IF EXISTS ztc_v");
            exec("DROP TABLE ztc_LIST");
        }
    }

    /** Folding a value's case is the collation's business, and the default one is not Turkish. */
    @Test
    void foldingAValueDoesNotFollowTheMachinesLocale() throws SQLException {
        assertEquals("ISTANBUL/istanbul",
                one("SELECT upper('istanbul'), lower('ISTANBUL')"));
    }
}
