package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A foreign key over a period, which PostgreSQL 18 spells {@code FOREIGN KEY (id, PERIOD v)}.
 *
 * <p>An ordinary key names one referenced row: the child holds the parent's value, and the parent
 * must hold it too. A temporal key names a <em>span</em>, and is satisfied when the rows sharing
 * the child's other key columns cover that span between them. Two referenced rows meeting end to
 * end cover a period that crosses the join, which is the whole point of the feature — the period
 * does not have to sit inside any one referenced row.
 *
 * <p>The referenced side has to be keyed the matching way, with {@code WITHOUT OVERLAPS}: it is
 * that constraint which makes the periods a partition of time per key rather than an arbitrary
 * collection of spans.
 */
class TemporalForeignKeyTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeEach
    void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        exec("CREATE TABLE tfk_p (id int, v daterange, PRIMARY KEY (id, v WITHOUT OVERLAPS))");
        exec("INSERT INTO tfk_p VALUES (1, daterange('2020-01-01','2020-06-01'))");
        exec("INSERT INTO tfk_p VALUES (1, daterange('2020-06-01','2021-01-01'))");
        exec("INSERT INTO tfk_p VALUES (2, daterange('2020-01-01','2020-02-01'))");
        exec("CREATE TABLE tfk_c (id int, v daterange,"
                + " FOREIGN KEY (id, PERIOD v) REFERENCES tfk_p (id, PERIOD v))");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static SQLException refusalOf(String sql) {
        return assertThrows(SQLException.class, () -> exec(sql), sql);
    }

    /** A period the referenced rows cover, whether by one row or by several in a row. */
    @Test
    void aCoveredPeriodIsAccepted() throws Exception {
        exec("INSERT INTO tfk_c VALUES (1, daterange('2020-02-01','2020-03-01'))");
        // Across the join between two referenced rows that meet exactly.
        exec("INSERT INTO tfk_c VALUES (1, daterange('2020-05-01','2020-08-01'))");
        // The whole of what is covered.
        exec("INSERT INTO tfk_c VALUES (1, daterange('2020-01-01','2021-01-01'))");
        assertEquals("3", scalar("SELECT count(*)::text FROM tfk_c"));
    }

    /** A period the referenced rows do not cover, in each of the ways they can fail to. */
    @Test
    void anUncoveredPeriodIsRefused() {
        // Running past the end of what is covered.
        assertEquals("23503",
                refusalOf("INSERT INTO tfk_c VALUES (1, daterange('2020-05-01','2021-06-01'))").getSQLState());
        // A key with no referenced rows at all.
        assertEquals("23503",
                refusalOf("INSERT INTO tfk_c VALUES (9, daterange('2020-05-01','2020-06-01'))").getSQLState());
        // Covered in part, which is not covered.
        assertEquals("23503",
                refusalOf("INSERT INTO tfk_c VALUES (2, daterange('2020-01-01','2020-03-01'))").getSQLState());
        // An empty period is covered by nothing rather than by everything.
        assertEquals("23503",
                refusalOf("INSERT INTO tfk_c VALUES (3, daterange('2020-01-01','2020-01-01'))").getSQLState());
    }

    /** A key with a null in it references nothing, and is asked nothing. */
    @Test
    void aNullKeyIsNotChecked() throws Exception {
        exec("INSERT INTO tfk_c VALUES (NULL, daterange('2020-01-01','2020-02-01'))");
        exec("INSERT INTO tfk_c VALUES (1, NULL)");
        assertEquals("2", scalar("SELECT count(*)::text FROM tfk_c"));
    }

    /**
     * Taking away what a child depends on. A referenced row may go while the periods left still
     * cover the child's, and may not once they do not.
     */
    @Test
    void aReferencedRowMayNotLeaveAChildUncovered() throws Exception {
        exec("INSERT INTO tfk_c VALUES (1, daterange('2020-05-01','2020-08-01'))");
        // This row is half of what covers the child's period.
        SQLException e = refusalOf("DELETE FROM tfk_p WHERE v = daterange('2020-01-01','2020-06-01')");
        assertEquals("23503", e.getSQLState());
        assertTrue(e.getMessage().contains("update or delete on table"), e.getMessage());
        // Moving it out from under the child is the same thing said another way.
        assertEquals("23503", refusalOf(
                "UPDATE tfk_p SET id = 5 WHERE id = 1 AND v = daterange('2020-06-01','2021-01-01')")
                .getSQLState());
        // A row no child needs may go.
        exec("DELETE FROM tfk_p WHERE id = 2");
        assertEquals("2", scalar("SELECT count(*)::text FROM tfk_p"));
    }

    /** The catalog records it as the temporal key it is, and spells it back the same way. */
    @Test
    void theCatalogRecordsThePeriod() throws Exception {
        assertEquals("true", scalar("SELECT conperiod::text FROM pg_constraint"
                + " WHERE conrelid = 'tfk_c'::regclass AND contype = 'f'"));
        assertEquals("FOREIGN KEY (id, PERIOD v) REFERENCES tfk_p(id, PERIOD v)",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conrelid = 'tfk_c'::regclass AND contype = 'f'"));
    }

    /** A key that could not enforce anything is refused rather than stored. */
    @Test
    void aKeyThatCannotBeEnforcedIsRefused() {
        // The referenced side is keyed, but not per period: an ordinary primary key says nothing
        // about which spans exist.
        assertEquals("42830", refusalOf("CREATE TABLE tfk_plain (id int primary key, v daterange)"
                + "; CREATE TABLE tfk_bad (id int, v daterange,"
                + " FOREIGN KEY (id, PERIOD v) REFERENCES tfk_plain (id, PERIOD v))").getSQLState());
        // PERIOD on one side only says two different things about the same key.
        assertEquals("42830", refusalOf("CREATE TABLE tfk_bad2 (id int, v daterange,"
                + " FOREIGN KEY (id, PERIOD v) REFERENCES tfk_p (id, v))").getSQLState());
        // A period has to be a span; a number cannot be covered.
        assertEquals("42804", refusalOf("CREATE TABLE tfk_bad3 (id int, v int,"
                + " FOREIGN KEY (id, PERIOD v) REFERENCES tfk_p (id, PERIOD v))").getSQLState());
    }

    /** PERIOD is not a reserved word, so a column may still be called it. */
    @Test
    void aColumnMayStillBeNamedPeriod() throws Exception {
        exec("CREATE TABLE tfk_named (period int PRIMARY KEY)");
        exec("CREATE TABLE tfk_ref (period int, FOREIGN KEY (period) REFERENCES tfk_named (period))");
        exec("INSERT INTO tfk_named VALUES (1)");
        exec("INSERT INTO tfk_ref VALUES (1)");
        assertEquals("23503", refusalOf("INSERT INTO tfk_ref VALUES (2)").getSQLState());
    }
}
