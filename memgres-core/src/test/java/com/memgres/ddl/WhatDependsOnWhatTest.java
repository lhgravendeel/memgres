package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What one object needs another for, and what a name reaches while it is looking.
 *
 * <p>An identity column does not merely default to a sequence: the sequence is part of what the
 * column is, so it goes when the column goes and not before -- CASCADE included, which offers to
 * drop what depends on the sequence and cannot offer to drop half a column.
 *
 * <p>A foreign key over a partitioned table holds every partition of it, so a partition cannot go
 * while the key is there any more than the whole can. And the relation a key names is looked for
 * along the search path and nowhere else: reaching into every schema let a key point at a relation
 * the statement could not have named.
 */
class WhatDependsOnWhatTest {

    private static Memgres memgres;
    private static Connection conn;

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

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** The sequence an identity column is made of does not go without the column. */
    @Test
    void theSequenceAnIdentityColumnIsMadeOf() throws SQLException {
        exec("CREATE TABLE zwd_i (a int GENERATED ALWAYS AS IDENTITY, b text)");
        assertEquals("2BP01", stateOf("DROP SEQUENCE zwd_i_a_seq"));
        assertEquals("2BP01", stateOf("DROP SEQUENCE zwd_i_a_seq CASCADE"));
        assertTrue(messageOf("DROP SEQUENCE zwd_i_a_seq CASCADE")
                .contains("cannot drop sequence zwd_i_a_seq because column a of table zwd_i"
                        + " requires it"));
        // The column still draws from it.
        assertEquals("1", one("INSERT INTO zwd_i (b) VALUES ('x') RETURNING a::text"));
        exec("DROP TABLE zwd_i");
        // A serial's sequence is only a default, and CASCADE takes the default away.
        exec("CREATE TABLE zwd_s (a serial, b text)");
        assertEquals("2BP01", stateOf("DROP SEQUENCE zwd_s_a_seq"));
        assertNull(stateOf("DROP SEQUENCE zwd_s_a_seq CASCADE"));
        exec("DROP TABLE zwd_s");
    }

    /** A key over the whole holds every part of it. */
    @Test
    void aKeyOverAPartitionedTable() throws SQLException {
        exec("CREATE TABLE zwd_p (i int PRIMARY KEY) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zwd_p1 PARTITION OF zwd_p FOR VALUES FROM (1) TO (10)");
        exec("CREATE TABLE zwd_d (j int REFERENCES zwd_p(i))");
        assertEquals("2BP01", stateOf("DROP TABLE zwd_p1"));
        assertTrue(messageOf("DROP TABLE zwd_p1")
                .contains("cannot drop table zwd_p1 because other objects depend on it"));
        assertNull(stateOf("DROP TABLE zwd_p1 CASCADE"));
        exec("DROP TABLE zwd_d");
        exec("DROP TABLE zwd_p");
    }

    /** The relation a key names is looked for along the search path. */
    @Test
    void whereAKeyLooksForWhatItNames() throws SQLException {
        exec("CREATE SCHEMA zwd_s2");
        exec("CREATE TABLE zwd_s2.zwd_tgt (a int PRIMARY KEY)");
        assertEquals("42P01", stateOf("CREATE TABLE zwd_ch (b int REFERENCES zwd_tgt(a))"));
        assertTrue(messageOf("CREATE TABLE zwd_ch (b int REFERENCES zwd_tgt(a))")
                .contains("relation \"zwd_tgt\" does not exist"));
        // Written with its schema it is found.
        assertNull(stateOf("CREATE TABLE zwd_ch2 (b int REFERENCES zwd_s2.zwd_tgt(a))"));
        exec("DROP TABLE zwd_ch2");
        exec("DROP SCHEMA zwd_s2 CASCADE");
    }
}
