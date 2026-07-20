package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for L13: Partition-child NOT NULL constraint naming.
 */
class PartitionNotNullTest {

    static Memgres memgres;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (memgres != null) memgres.close();
    }

    private Connection conn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    @Test
    void partitionChildHasConparentid() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE l13_parent(id int NOT NULL, val text NOT NULL) PARTITION BY RANGE(id)");
            st.execute("CREATE TABLE l13_child PARTITION OF l13_parent FOR VALUES FROM (1) TO (100)");
            // Child's NOT NULL constraint should have conparentid pointing to parent's constraint
            try (ResultSet rs = st.executeQuery(
                    "SELECT child.conname, child.conparentid, parent.conname AS parent_conname " +
                    "FROM pg_constraint child " +
                    "JOIN pg_class cc ON cc.oid = child.conrelid " +
                    "LEFT JOIN pg_constraint parent ON parent.oid = child.conparentid " +
                    "WHERE cc.relname = 'l13_child' AND child.contype = 'n' " +
                    "ORDER BY child.conname")) {
                // id NOT NULL
                assertTrue(rs.next());
                String childConname = rs.getString("conname");
                assertTrue(childConname.contains("l13_child"), "Child conname should use child table name: " + childConname);
                int conparentid = rs.getInt("conparentid");
                assertTrue(conparentid > 0, "conparentid should be > 0 for inherited constraint: " + conparentid);
                String parentConname = rs.getString("parent_conname");
                assertNotNull(parentConname, "Should resolve parent constraint name");
                assertTrue(parentConname.contains("l13_parent"), "Parent conname should use parent table name: " + parentConname);

                // val NOT NULL
                assertTrue(rs.next());
                assertTrue(rs.getInt("conparentid") > 0, "val NOT NULL should also have conparentid");
            } finally {
                st.execute("DROP TABLE l13_parent CASCADE");
            }
        }
    }

    @Test
    void partitionChildConinhcount() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE l13_inh(id int NOT NULL) PARTITION BY LIST(id)");
            st.execute("CREATE TABLE l13_inh_p1 PARTITION OF l13_inh FOR VALUES IN (1, 2, 3)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT coninhcount FROM pg_constraint c " +
                    "JOIN pg_class cc ON cc.oid = c.conrelid " +
                    "WHERE cc.relname = 'l13_inh_p1' AND c.contype = 'n'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("coninhcount"), "Inherited NOT NULL should have coninhcount=1");
            } finally {
                st.execute("DROP TABLE l13_inh CASCADE");
            }
        }
    }

    @Test
    void nonPartitionHasZeroConparentid() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE l13_reg(id int NOT NULL)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT conparentid, coninhcount FROM pg_constraint c " +
                    "JOIN pg_class cc ON cc.oid = c.conrelid " +
                    "WHERE cc.relname = 'l13_reg' AND c.contype = 'n'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt("conparentid"), "Non-partition NOT NULL should have conparentid=0");
                assertEquals(0, rs.getInt("coninhcount"), "Non-partition should have coninhcount=0");
            } finally {
                st.execute("DROP TABLE l13_reg");
            }
        }
    }
}
