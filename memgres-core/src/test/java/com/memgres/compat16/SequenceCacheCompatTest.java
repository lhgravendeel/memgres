package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests sequence CACHE semantics differences between Memgres and PostgreSQL 18.
 *
 * PG 18: When CACHE n > 1, each session pre-allocates n values from the sequence.
 * If a session disconnects without using all cached values, those values are "lost"
 * (gaps appear). Two concurrent sessions calling nextval() will get interleaved but
 * non-overlapping blocks of values.
 *
 * Memgres: CACHE is parsed and stored but nextval() always hits the AtomicLong
 * counter directly. No session-level caching occurs, so sequences are strictly
 * sequential with no gaps (even with CACHE > 1).
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres.
 */
class SequenceCacheCompatTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() {
        if (memgres != null) memgres.close();
    }

    Connection connect() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    // -------------------------------------------------------------------------
    // CACHE should cause gaps when sessions disconnect
    // -------------------------------------------------------------------------

    @Test
    void cache_shouldCauseGapsOnDisconnect() throws SQLException {
        try (Connection setup = connect()) {
            setup.createStatement().execute("DROP SEQUENCE IF EXISTS seq_cache_test");
            setup.createStatement().execute(
                    "CREATE SEQUENCE seq_cache_test START 1 CACHE 10");
        }

        // Session 1: get one value (caches 1-10 internally in PG)
        long val1;
        try (Connection c1 = connect()) {
            try (ResultSet rs = c1.createStatement().executeQuery(
                    "SELECT nextval('seq_cache_test')")) {
                rs.next();
                val1 = rs.getLong(1);
            }
        } // Session 1 disconnects - cached values 2-10 are lost in PG

        // Session 2: get next value
        long val2;
        try (Connection c2 = connect()) {
            try (ResultSet rs = c2.createStatement().executeQuery(
                    "SELECT nextval('seq_cache_test')")) {
                rs.next();
                val2 = rs.getLong(1);
            }
        }

        // PG: val1 = 1, val2 = 11 (because values 2-10 were cached by session 1 and lost)
        // Memgres: val1 = 1, val2 = 2 (no caching, strictly sequential)
        assertTrue(val2 > val1 + 1,
                "With CACHE 10, disconnecting session should cause gap. "
                        + "val1=" + val1 + ", val2=" + val2
                        + ". Expected val2 >= " + (val1 + 10) + " but got " + val2
                        + ". Suggests no session-level sequence caching.");
    }

    // -------------------------------------------------------------------------
    // Concurrent sessions should get interleaved cache blocks
    // -------------------------------------------------------------------------

    @Test
    void cache_concurrentSessions_shouldGetInterleavedBlocks() throws SQLException {
        try (Connection setup = connect()) {
            setup.createStatement().execute("DROP SEQUENCE IF EXISTS seq_interleave");
            setup.createStatement().execute(
                    "CREATE SEQUENCE seq_interleave START 1 CACHE 5");
        }

        Connection c1 = connect();
        Connection c2 = connect();

        try {
            // Session 1 gets first value (caches 1-5 in PG)
            long s1v1;
            try (ResultSet rs = c1.createStatement().executeQuery(
                    "SELECT nextval('seq_interleave')")) {
                rs.next();
                s1v1 = rs.getLong(1);
            }

            // Session 2 gets first value (caches 6-10 in PG)
            long s2v1;
            try (ResultSet rs = c2.createStatement().executeQuery(
                    "SELECT nextval('seq_interleave')")) {
                rs.next();
                s2v1 = rs.getLong(1);
            }

            // Session 1 gets second value
            long s1v2;
            try (ResultSet rs = c1.createStatement().executeQuery(
                    "SELECT nextval('seq_interleave')")) {
                rs.next();
                s1v2 = rs.getLong(1);
            }

            // In PG with CACHE 5: s1v1=1, s2v1=6, s1v2=2
            // In Memgres (no cache): s1v1=1, s2v1=2, s1v2=3
            // The key difference: s2v1 should NOT be s1v1+1 (should jump by cache size)
            assertTrue(s2v1 > s1v1 + 1,
                    "With CACHE 5, second session's first value should skip ahead. "
                            + "s1v1=" + s1v1 + ", s2v1=" + s2v1
                            + ". Expected s2v1 >= " + (s1v1 + 5) + " with caching.");
        } finally {
            c1.close();
            c2.close();
        }
    }

    // -------------------------------------------------------------------------
    // CACHE 1 should behave identically (no gaps)
    // -------------------------------------------------------------------------

    @Test
    void cache1_shouldHaveNoGaps() throws SQLException {
        try (Connection setup = connect()) {
            setup.createStatement().execute("DROP SEQUENCE IF EXISTS seq_cache1");
            setup.createStatement().execute(
                    "CREATE SEQUENCE seq_cache1 START 1 CACHE 1");
        }

        // With CACHE 1, each session gets one value at a time - no gaps
        long val1, val2, val3;
        try (Connection c = connect()) {
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT nextval('seq_cache1')")) {
                rs.next();
                val1 = rs.getLong(1);
            }
        }
        try (Connection c = connect()) {
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT nextval('seq_cache1')")) {
                rs.next();
                val2 = rs.getLong(1);
            }
        }
        try (Connection c = connect()) {
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT nextval('seq_cache1')")) {
                rs.next();
                val3 = rs.getLong(1);
            }
        }

        assertEquals(1, val1);
        assertEquals(2, val2);
        assertEquals(3, val3);
    }
}
