package com.memgres.session;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for three session-level fixes:
 * 1. PREPARE parameter-count inference must find $N in ALL statement positions
 *    (JOIN ON conditions, CTE bodies incl. recursive branches, HAVING, ORDER BY,
 *    LIMIT/OFFSET, VALUES rows, set-operation branches, window expressions,
 *    ON CONFLICT clauses, ...), not just SELECT list / WHERE / plain FROM subqueries.
 * 2. NOTIFY deduplicates identical channel+payload notifications within one
 *    transaction (delivered once on COMMIT), but not across autocommit statements.
 * 3. FETCH/MOVE ABSOLUTE past either end of a cursor repositions the cursor
 *    (after-last / before-first) even though no row is returned, and
 *    FETCH FIRST/LAST on an empty cursor behaves sanely.
 */
class PrepareNotifyFetchTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = connect();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static Connection connect() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private List<Integer> queryInts(String sql) throws SQLException {
        List<Integer> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) out.add(rs.getInt(1));
        }
        return out;
    }

    // =====================================================================
    // 1. PREPARE parameter inference in all statement positions
    // =====================================================================

    @Nested
    class PrepareParamInference {

        @BeforeEach
        void setUpTables() throws SQLException {
            exec("DEALLOCATE ALL");
            exec("DROP TABLE IF EXISTS prep_a CASCADE");
            exec("DROP TABLE IF EXISTS prep_b CASCADE");
            exec("CREATE TABLE prep_a (id int PRIMARY KEY, v text)");
            exec("CREATE TABLE prep_b (id int PRIMARY KEY, a_id int)");
            exec("INSERT INTO prep_a VALUES (1, 'one'), (2, 'two'), (3, 'three')");
            exec("INSERT INTO prep_b VALUES (10, 1), (20, 2), (30, 3)");
        }

        @AfterEach
        void dropTables() throws SQLException {
            exec("DEALLOCATE ALL");
            exec("DROP TABLE IF EXISTS prep_a CASCADE");
            exec("DROP TABLE IF EXISTS prep_b CASCADE");
        }

        @Test
        void param_in_join_on_condition() throws SQLException {
            exec("PREPARE pj AS SELECT a.id FROM prep_a a JOIN prep_b b ON b.id = $1 AND b.a_id = a.id");
            assertEquals(List.of(2), queryInts("EXECUTE pj(20)"));
        }

        @Test
        void param_in_cte_body() throws SQLException {
            exec("PREPARE pc AS WITH w AS (SELECT $1::int AS a) SELECT a FROM w");
            assertEquals(List.of(42), queryInts("EXECUTE pc(42)"));
        }

        @Test
        void param_in_recursive_cte_both_branches() throws SQLException {
            exec("PREPARE pr AS WITH RECURSIVE r(n) AS ("
                    + "SELECT $1::int UNION ALL SELECT n + 1 FROM r WHERE n < $2::int"
                    + ") SELECT n FROM r ORDER BY n");
            assertEquals(List.of(2, 3, 4), queryInts("EXECUTE pr(2, 4)"));
        }

        @Test
        void param_in_having() throws SQLException {
            exec("INSERT INTO prep_b VALUES (40, 3)");
            exec("PREPARE ph AS SELECT a_id FROM prep_b GROUP BY a_id HAVING count(*) > $1 ORDER BY a_id");
            assertEquals(List.of(3), queryInts("EXECUTE ph(1)"));
        }

        @Test
        void param_in_order_by() throws SQLException {
            exec("PREPARE po AS SELECT id FROM prep_a ORDER BY id * $1::int");
            assertEquals(List.of(3, 2, 1), queryInts("EXECUTE po(-1)"));
        }

        @Test
        void param_in_limit_and_offset() throws SQLException {
            exec("PREPARE pl AS SELECT id FROM prep_a ORDER BY id LIMIT $1 OFFSET $2");
            assertEquals(List.of(2), queryInts("EXECUTE pl(1, 1)"));
        }

        @Test
        void param_in_values_in_from() throws SQLException {
            exec("PREPARE pv AS SELECT x FROM (VALUES ($1::int), ($2::int)) AS v(x) ORDER BY x");
            assertEquals(List.of(5, 9), queryInts("EXECUTE pv(9, 5)"));
        }

        @Test
        void param_in_union_branches() throws SQLException {
            exec("PREPARE pu AS SELECT $1::int AS x UNION ALL SELECT $2::int ORDER BY x");
            assertEquals(List.of(1, 7), queryInts("EXECUTE pu(7, 1)"));
        }

        @Test
        void param_in_window_expression() throws SQLException {
            exec("PREPARE pw AS SELECT sum(id + $1::int) OVER () FROM prep_a LIMIT 1");
            assertEquals(List.of(36), queryInts("EXECUTE pw(10)"));
        }

        @Test
        void param_in_on_conflict_set_and_where() throws SQLException {
            exec("PREPARE pcu AS INSERT INTO prep_a VALUES (1, 'ignored') "
                    + "ON CONFLICT (id) DO UPDATE SET v = $1 WHERE prep_a.id = $2");
            exec("EXECUTE pcu('updated', 1)");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT v FROM prep_a WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("updated", rs.getString(1));
            }
        }

        @Test
        void param_in_subquery_inside_join_on() throws SQLException {
            exec("PREPARE ps AS SELECT a.id FROM prep_a a JOIN prep_b b "
                    + "ON b.a_id = a.id AND b.id IN (SELECT $1::int) ORDER BY a.id");
            assertEquals(List.of(3), queryInts("EXECUTE ps(30)"));
        }

        @Test
        void wrong_parameter_count_still_errors() throws SQLException {
            exec("PREPARE pe AS SELECT a.id FROM prep_a a JOIN prep_b b ON b.id = $1");
            SQLException tooFew = assertThrows(SQLException.class, () -> exec("EXECUTE pe"));
            assertTrue(tooFew.getMessage().contains("wrong number of parameters"),
                    "unexpected message: " + tooFew.getMessage());
            SQLException tooMany = assertThrows(SQLException.class, () -> exec("EXECUTE pe(1, 2)"));
            assertTrue(tooMany.getMessage().contains("wrong number of parameters"),
                    "unexpected message: " + tooMany.getMessage());
        }

        @Test
        void declared_types_still_enforced() throws SQLException {
            // Explicit type list continues to work alongside body inference
            exec("PREPARE pt (int) AS SELECT a.id FROM prep_a a JOIN prep_b b ON b.id = $1 AND b.a_id = a.id");
            assertEquals(List.of(1), queryInts("EXECUTE pt(10)"));
            assertThrows(SQLException.class, () -> exec("EXECUTE pt"));
        }
    }

    // =====================================================================
    // 2. NOTIFY dedup within a transaction
    // =====================================================================

    @Nested
    class NotifyDedup {

        private PGNotification[] poll(Connection c) throws SQLException {
            try (Statement s = c.createStatement()) { s.execute("SELECT 1"); }
            return c.unwrap(PGConnection.class).getNotifications();
        }

        private int count(PGNotification[] notes) {
            return notes == null ? 0 : notes.length;
        }

        @Test
        void identical_notifies_in_transaction_delivered_once() throws Exception {
            try (Connection listener = connect(); Connection notifier = connect()) {
                try (Statement s = listener.createStatement()) { s.execute("LISTEN dedup_ch"); }

                notifier.setAutoCommit(false);
                try (Statement s = notifier.createStatement()) {
                    s.execute("NOTIFY dedup_ch, 'x'");
                    s.execute("NOTIFY dedup_ch, 'x'");
                    s.execute("NOTIFY dedup_ch, 'x'");
                }
                notifier.commit();
                notifier.setAutoCommit(true);

                PGNotification[] notes = poll(listener);
                assertEquals(1, count(notes), "identical NOTIFYs in one txn must deliver once");
                assertEquals("x", notes[0].getParameter());
            }
        }

        @Test
        void distinct_payloads_in_transaction_both_delivered() throws Exception {
            try (Connection listener = connect(); Connection notifier = connect()) {
                try (Statement s = listener.createStatement()) { s.execute("LISTEN dedup_ch2"); }

                notifier.setAutoCommit(false);
                try (Statement s = notifier.createStatement()) {
                    s.execute("NOTIFY dedup_ch2, 'x'");
                    s.execute("NOTIFY dedup_ch2, 'y'");
                }
                notifier.commit();
                notifier.setAutoCommit(true);

                PGNotification[] notes = poll(listener);
                assertEquals(2, count(notes));
                assertEquals("x", notes[0].getParameter());
                assertEquals("y", notes[1].getParameter());
            }
        }

        @Test
        void same_payload_different_channels_both_delivered() throws Exception {
            try (Connection listener = connect(); Connection notifier = connect()) {
                try (Statement s = listener.createStatement()) {
                    s.execute("LISTEN dedup_cha");
                    s.execute("LISTEN dedup_chb");
                }

                notifier.setAutoCommit(false);
                try (Statement s = notifier.createStatement()) {
                    s.execute("NOTIFY dedup_cha, 'same'");
                    s.execute("NOTIFY dedup_chb, 'same'");
                }
                notifier.commit();
                notifier.setAutoCommit(true);

                assertEquals(2, count(poll(listener)));
            }
        }

        @Test
        void autocommit_duplicates_each_delivered() throws Exception {
            try (Connection listener = connect(); Connection notifier = connect()) {
                try (Statement s = listener.createStatement()) { s.execute("LISTEN dedup_auto"); }

                // autocommit: each NOTIFY is its own transaction, no dedup across them
                try (Statement s = notifier.createStatement()) {
                    s.execute("NOTIFY dedup_auto, 'x'");
                    s.execute("NOTIFY dedup_auto, 'x'");
                }

                assertEquals(2, count(poll(listener)),
                        "autocommit NOTIFYs are separate transactions and must each deliver");
            }
        }

        @Test
        void rollback_discards_deduped_notification() throws Exception {
            try (Connection listener = connect(); Connection notifier = connect()) {
                try (Statement s = listener.createStatement()) { s.execute("LISTEN dedup_rb"); }

                notifier.setAutoCommit(false);
                try (Statement s = notifier.createStatement()) {
                    s.execute("NOTIFY dedup_rb, 'x'");
                    s.execute("NOTIFY dedup_rb, 'x'");
                }
                notifier.rollback();
                notifier.setAutoCommit(true);

                assertEquals(0, count(poll(listener)));
            }
        }

        @Test
        void renotify_after_savepoint_rollback_is_requeued() throws Exception {
            try (Connection listener = connect(); Connection notifier = connect()) {
                try (Statement s = listener.createStatement()) { s.execute("LISTEN dedup_sp"); }

                notifier.setAutoCommit(false);
                try (Statement s = notifier.createStatement()) {
                    s.execute("SAVEPOINT sp1");
                    s.execute("NOTIFY dedup_sp, 'x'");
                    s.execute("ROLLBACK TO SAVEPOINT sp1"); // discards the pending entry
                    s.execute("NOTIFY dedup_sp, 'x'");      // must be re-queued, not deduped away
                }
                notifier.commit();
                notifier.setAutoCommit(true);

                PGNotification[] notes = poll(listener);
                assertEquals(1, count(notes));
                assertEquals("x", notes[0].getParameter());
            }
        }
    }

    // =====================================================================
    // 3. FETCH/MOVE ABSOLUTE past-the-end repositioning
    // =====================================================================

    @Nested
    class FetchAbsolutePositioning {

        @BeforeEach
        void setUpTable() throws SQLException {
            exec("DROP TABLE IF EXISTS fetch_t CASCADE");
            exec("CREATE TABLE fetch_t (id int PRIMARY KEY)");
            exec("INSERT INTO fetch_t VALUES (1), (2), (3)");
        }

        @AfterEach
        void dropTable() throws SQLException {
            conn.setAutoCommit(true);
            exec("DROP TABLE IF EXISTS fetch_t CASCADE");
        }

        private void begin() throws SQLException { conn.setAutoCommit(false); }

        private void end() throws SQLException {
            conn.rollback();
            conn.setAutoCommit(true);
        }

        @Test
        void absolute_past_end_then_prior_returns_last_row() throws SQLException {
            begin();
            try {
                exec("DECLARE fc SCROLL CURSOR FOR SELECT id FROM fetch_t ORDER BY id");
                assertEquals(List.of(), queryInts("FETCH ABSOLUTE 100 FROM fc"));
                assertEquals(List.of(3), queryInts("FETCH PRIOR FROM fc"),
                        "after past-end ABSOLUTE, cursor must sit after the last row");
            } finally {
                end();
            }
        }

        @Test
        void absolute_negative_past_start_then_next_returns_first_row() throws SQLException {
            begin();
            try {
                exec("DECLARE fc SCROLL CURSOR FOR SELECT id FROM fetch_t ORDER BY id");
                exec("MOVE ABSOLUTE 2 IN fc"); // park in the middle first
                assertEquals(List.of(), queryInts("FETCH ABSOLUTE -100 FROM fc"));
                assertEquals(List.of(1), queryInts("FETCH NEXT FROM fc"),
                        "after negative-past-start ABSOLUTE, cursor must sit before the first row");
            } finally {
                end();
            }
        }

        @Test
        void move_absolute_past_end_then_prior_returns_last_row() throws SQLException {
            begin();
            try {
                exec("DECLARE fc SCROLL CURSOR FOR SELECT id FROM fetch_t ORDER BY id");
                try (Statement st = conn.createStatement()) {
                    // MOVE returns no rows; command tag is MOVE 0
                    assertFalse(st.execute("MOVE ABSOLUTE 100 IN fc"),
                            "MOVE must not return a result set");
                    assertEquals(0, st.getUpdateCount(), "MOVE past end reports MOVE 0");
                }
                assertEquals(List.of(3), queryInts("FETCH PRIOR FROM fc"));
            } finally {
                end();
            }
        }

        @Test
        void move_absolute_negative_past_start_then_next_returns_first_row() throws SQLException {
            begin();
            try {
                exec("DECLARE fc SCROLL CURSOR FOR SELECT id FROM fetch_t ORDER BY id");
                exec("MOVE LAST IN fc");
                exec("MOVE ABSOLUTE -100 IN fc");
                assertEquals(List.of(1), queryInts("FETCH NEXT FROM fc"));
            } finally {
                end();
            }
        }

        @Test
        void absolute_in_range_still_works() throws SQLException {
            begin();
            try {
                exec("DECLARE fc SCROLL CURSOR FOR SELECT id FROM fetch_t ORDER BY id");
                assertEquals(List.of(2), queryInts("FETCH ABSOLUTE 2 FROM fc"));
                assertEquals(List.of(2), queryInts("FETCH ABSOLUTE -2 FROM fc"));
                assertEquals(List.of(3), queryInts("FETCH NEXT FROM fc"));
            } finally {
                end();
            }
        }

        @Test
        void fetch_last_and_first_on_empty_cursor_no_crash() throws SQLException {
            begin();
            try {
                exec("DECLARE fce SCROLL CURSOR FOR SELECT id FROM fetch_t WHERE id > 999");
                assertEquals(List.of(), queryInts("FETCH LAST FROM fce"));
                assertEquals(List.of(), queryInts("FETCH NEXT FROM fce"));
                assertEquals(List.of(), queryInts("FETCH FIRST FROM fce"));
                assertEquals(List.of(), queryInts("FETCH PRIOR FROM fce"));
                assertEquals(List.of(), queryInts("FETCH ABSOLUTE 5 FROM fce"));
                assertEquals(List.of(), queryInts("FETCH ABSOLUTE -5 FROM fce"));
            } finally {
                end();
            }
        }
    }
}
