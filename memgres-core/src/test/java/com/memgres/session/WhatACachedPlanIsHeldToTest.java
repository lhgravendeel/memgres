package com.memgres.session;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a cached plan is held to.
 *
 * <p>PostgreSQL keeps a prepared statement's analysed plan and revalidates it before every
 * execution: a relation it reads that has gained a column, or whose column has changed type,
 * changes the shape of the answer, and a client that was told one shape must not be handed
 * another. Re-analysed from scratch each time, {@code EXECUTE} quietly returned the new shape.
 */
class WhatACachedPlanIsHeldToTest {

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

    /** A column added under a SELECT * changes the shape, and the plan is refused. */
    @Test
    void whatAWiderRelationDoesToThePlan() throws SQLException {
        exec("CREATE TABLE zxc_t (a int)");
        exec("INSERT INTO zxc_t VALUES (1)");
        exec("PREPARE zxc_p AS SELECT * FROM zxc_t");
        assertEquals("1", one("EXECUTE zxc_p"));
        exec("ALTER TABLE zxc_t ADD COLUMN b int");
        assertEquals("0A000", stateOf("EXECUTE zxc_p"));
        exec("DEALLOCATE zxc_p");
        exec("DROP TABLE zxc_t");
    }

    /** So does a column that changed type. */
    @Test
    void whatARetypedColumnDoesToThePlan() throws SQLException {
        exec("CREATE TABLE zxc_u (a int, b int)");
        exec("INSERT INTO zxc_u VALUES (1, 2)");
        exec("PREPARE zxc_q AS SELECT * FROM zxc_u");
        assertEquals("1", one("EXECUTE zxc_q"));
        exec("ALTER TABLE zxc_u ALTER COLUMN a TYPE text");
        assertEquals("0A000", stateOf("EXECUTE zxc_q"));
        exec("DEALLOCATE zxc_q");
        exec("DROP TABLE zxc_u");
    }

    /** A change the plan does not read leaves it alone. */
    @Test
    void whatAnUnreadColumnDoesNotDo() throws SQLException {
        exec("CREATE TABLE zxc_v (a int)");
        exec("INSERT INTO zxc_v VALUES (1)");
        exec("PREPARE zxc_r AS SELECT a FROM zxc_v");
        assertEquals("1", one("EXECUTE zxc_r"));
        exec("ALTER TABLE zxc_v ADD COLUMN c int");
        assertEquals("1", one("EXECUTE zxc_r"));
        exec("DEALLOCATE zxc_r");
        exec("DROP TABLE zxc_v");
    }
}
