package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a catalogue column reads off the state, rather than what a constant says.
 *
 * <p>A relation has six system columns, numbered downwards from -1, and PostgreSQL records them
 * in pg_attribute beside the ones the writer declared: projected from the live column list alone,
 * a query looking for ctid or xmin found nothing. A column of a collatable type always has a
 * collation — the database's default where none was written — and zero there means the type is
 * not collatable at all, which is what every text column claimed. A foreign key written without a
 * column list references the referenced relation's primary key, which PostgreSQL resolves when it
 * stores the constraint, so what is read back names those columns whether or not the writer did.
 *
 * <p>And two functions that write a size for a reader to read had thresholds of their own:
 * PostgreSQL stays in bytes until ten kilobytes rather than switching at one, and reads a number
 * with its own scanner, so an exponent is a number like any other.
 */
class WhatACatalogueColumnReadsOffTheStateTest {

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

    /** Every relation carries its six system columns, numbered downwards from -1. */
    @Test
    void aRelationsSystemColumnsAreInTheAttributeCatalogue() throws SQLException {
        exec("CREATE TABLE zwc_t (a int, b text)");
        try {
            assertEquals(List.of("tableoid/-6", "cmax/-5", "xmax/-4", "cmin/-3", "xmin/-2",
                            "ctid/-1", "a/1", "b/2"),
                    rows("SELECT attname, attnum FROM pg_attribute"
                            + " WHERE attrelid='zwc_t'::regclass ORDER BY attnum"));
            // A column the writer declared is still told apart from one the system carries.
            assertEquals("2", one("SELECT count(*)::int FROM pg_attribute"
                    + " WHERE attrelid='zwc_t'::regclass AND attnum > 0"));
        } finally {
            exec("DROP TABLE zwc_t");
        }
    }

    /** A column of a collatable type points at a collation, written or default. */
    @Test
    void aCollatableColumnPointsAtACollation() throws SQLException {
        exec("CREATE TABLE zwc_c (s text COLLATE \"C\", t text, i int, v varchar(10))");
        try {
            assertEquals(List.of("s/true", "t/true", "i/false", "v/true"),
                    rows("SELECT attname, (attcollation <> 0)::text FROM pg_attribute"
                            + " WHERE attrelid='zwc_c'::regclass AND attnum>0 ORDER BY attnum"));
            // A dropped column keeps the collation it had, which says its type had one.
            exec("ALTER TABLE zwc_c DROP COLUMN t");
            assertEquals("true", one("SELECT (attcollation <> 0)::text FROM pg_attribute"
                    + " WHERE attrelid='zwc_c'::regclass AND attisdropped"));
        } finally {
            exec("DROP TABLE zwc_c");
        }
    }

    /** A foreign key written without a column list references the primary key, and says so. */
    @Test
    void aForeignKeyNamesTheColumnsItReferences() throws SQLException {
        exec("CREATE TABLE zwc_k (a int, b int, PRIMARY KEY (a,b))");
        exec("CREATE TABLE zwc_f (x int, y int, FOREIGN KEY (x,y) REFERENCES zwc_k)");
        try {
            assertEquals("{1,2}/{1,2}", one("SELECT conkey::text, confkey::text"
                    + " FROM pg_constraint WHERE conrelid='zwc_f'::regclass"));
            assertEquals("FOREIGN KEY (x, y) REFERENCES zwc_k(a, b)",
                    one("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                            + " WHERE conrelid='zwc_f'::regclass"));
        } finally {
            exec("DROP TABLE zwc_f, zwc_k CASCADE");
        }
    }

    /** A size stays in its unit until ten of the next one fit. */
    @Test
    void aSizeChangesUnitAtTenOfTheNextOne() throws SQLException {
        assertEquals("10 bytes", one("SELECT pg_size_pretty(10::bigint)"));
        assertEquals("1023 bytes", one("SELECT pg_size_pretty(1023::bigint)"));
        assertEquals("1024 bytes", one("SELECT pg_size_pretty(1024::bigint)"));
        assertEquals("10239 bytes", one("SELECT pg_size_pretty(10239::bigint)"));
        assertEquals("10 kB", one("SELECT pg_size_pretty(10240::bigint)"));
        assertEquals("1024 kB", one("SELECT pg_size_pretty(1048576::bigint)"));
        assertEquals("10 MB", one("SELECT pg_size_pretty(10485760::bigint)"));
        assertEquals("1024 GB", one("SELECT pg_size_pretty(1099511627776::bigint)"));
        assertEquals("10 PB", one("SELECT pg_size_pretty(11258999068426240::bigint)"));
        // A negative size reads the same way, and a numeric keeps its fraction while in bytes.
        assertEquals("-1536 bytes", one("SELECT pg_size_pretty(-1536::bigint)"));
        assertEquals("-10 kB", one("SELECT pg_size_pretty(-10240::bigint)"));
        assertEquals("1.5 bytes", one("SELECT pg_size_pretty(1.5::numeric)"));
    }

    /** A size written as text is read with PostgreSQL's own number scanner. */
    @Test
    void aWrittenSizeIsReadTheWayPostgresqlReadsANumber() throws SQLException {
        assertEquals("1", one("SELECT pg_size_bytes('1')"));
        assertEquals("1024", one("SELECT pg_size_bytes('1 kB')"));
        assertEquals("1024", one("SELECT pg_size_bytes('1kB')"));
        assertEquals("1572864", one("SELECT pg_size_bytes('1.5 MB')"));
        assertEquals("3298534883328", one("SELECT pg_size_bytes('3 TB')"));
        assertEquals("1", one("SELECT pg_size_bytes('1 bytes')"));
        assertEquals("10", one("SELECT pg_size_bytes(' 10 ')"));
        // An exponent is part of the number, not a unit nobody knows.
        assertEquals("1000", one("SELECT pg_size_bytes('1e3')"));
        assertEquals("-2048", one("SELECT pg_size_bytes('-2 kB')"));
        try (Statement st = conn.createStatement()) {
            st.execute("SELECT pg_size_bytes('1 XB')");
            fail("a unit PostgreSQL does not read is refused");
        } catch (SQLException e) {
            org.postgresql.util.ServerErrorMessage m =
                    ((org.postgresql.util.PSQLException) e).getServerErrorMessage();
            assertEquals("22023", m.getSQLState());
            assertEquals("Invalid size unit: \"XB\".", m.getDetail());
            assertTrue(m.getHint().contains("Valid units are"), m.getHint());
        }
    }

    /** A whole number is the narrowest of the integer types that holds it. */
    @Test
    void anIntegerConstantIsAsWideAsItNeedsToBe() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT 2147483647 AS a, 2147483648 AS b,"
                     + " 11258999068426240 AS c, 9223372036854775808 AS d")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("int4", md.getColumnTypeName(1));
            assertEquals("int8", md.getColumnTypeName(2));
            assertEquals("int8", md.getColumnTypeName(3));
            assertEquals("numeric", md.getColumnTypeName(4));
        }
    }

    /** An untyped literal beside a number is read as that number's type. */
    @Test
    void anUntypedLiteralIsReadAsTheTypeBesideIt() throws SQLException {
        assertEquals("3", one("SELECT 1::int + '2'"));
        assertEquals("t", one("SELECT 1::int = '1'"));
        assertEquals("f", one("SELECT 1::int > '2'"));
        // What goes wrong is the value, not the operator: it is a bad integer, not a bad double.
        try (Statement st = conn.createStatement()) {
            st.execute("SELECT 1::int + 'x'");
            fail("a literal that is not a number is refused");
        } catch (SQLException e) {
            assertEquals("22P02", e.getSQLState());
            assertTrue(e.getMessage().contains("invalid input syntax for type integer: \"x\""),
                    e.getMessage());
        }
    }
}
