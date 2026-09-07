package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a partition may say about its own columns, and which collation a name sorts by.
 *
 * <p>A partition takes its shape from the table it belongs to, but between the parent's name and
 * the bound it may restate a column to say something more about it -- a default of its own, a NOT
 * NULL. Left unread, the opening parenthesis ended the statement as far as the reader was
 * concerned: the bound was never read at all, so the partition held no slot of the parent's key
 * and every row routed to it was refused as belonging to no partition.
 *
 * <p>What the partition says is the partition's and not the parent's, so the column is a copy: a
 * default written on one partition must not appear on the parent or on a sibling. And it is used
 * only where the partition is written to directly -- a row routed from the partitioned table takes
 * that table's default, which is none.
 *
 * <p>The {@code name} type sorts by C rather than by the database's collation, because a name has
 * to order the same way whatever locale the database was created in.
 */
class WhatAPartitionSaysAboutItselfTest {

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

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int width = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 1; i <= width; i++) {
                    if (i > 1) line.append('|');
                    line.append(rs.getString(i));
                }
                out.add(line.toString());
            }
        }
        return out;
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** A partition that restates a column still reads its bound. */
    @Test
    void whatAPartitionThatRestatesAColumnHolds() throws SQLException {
        exec("CREATE TABLE zwa_p (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zwa_a PARTITION OF zwa_p FOR VALUES FROM (1) TO (10)");
        exec("CREATE TABLE zwa_b PARTITION OF zwa_p (s DEFAULT 'c') FOR VALUES FROM (10) TO (20)");
        assertEquals("FOR VALUES FROM (10) TO (20)",
                one("SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE relname='zwa_b'"));
        assertEquals("'c'::text",
                one("SELECT column_default FROM information_schema.columns"
                        + " WHERE table_name='zwa_b' AND column_name='s'"));
        // The default belongs to that partition alone.
        assertEquals("<null>",
                one("SELECT coalesce(column_default,'<null>') FROM information_schema.columns"
                        + " WHERE table_name='zwa_a' AND column_name='s'"));
        assertEquals("<null>",
                one("SELECT coalesce(column_default,'<null>') FROM information_schema.columns"
                        + " WHERE table_name='zwa_p' AND column_name='s'"));
        // A row routed from the partitioned table takes that table's default, which is none.
        exec("INSERT INTO zwa_p VALUES (15, DEFAULT)");
        exec("INSERT INTO zwa_b (i) VALUES (16)");
        exec("INSERT INTO zwa_p VALUES (5, 'x')");
        // The sort key is named apart from the select list: ORDER BY i would otherwise reach the
        // text the list projects under that name, and sort 15 before 5.
        assertEquals(java.util.Arrays.asList("5|x", "15|<null>", "16|c"),
                rows("SELECT i::text AS shown, coalesce(s,'<null>') FROM zwa_p ORDER BY i"));
        exec("DROP TABLE zwa_p");
    }

    /** A name sorts by C; the other text types take the database's collation. */
    @Test
    void whichCollationEachColumnPointsAt() throws SQLException {
        exec("CREATE TABLE zwa_c (i int, s text, v varchar(10), n name, na name[], c \"char\")");
        assertEquals(java.util.Arrays.asList("i|0", "s|100", "v|100", "n|950", "na|950", "c|0"),
                rows("SELECT attname, attcollation::text FROM pg_attribute"
                        + " WHERE attrelid='zwa_c'::regclass AND attnum>0 ORDER BY attnum"));
        exec("DROP TABLE zwa_c");
    }
}
