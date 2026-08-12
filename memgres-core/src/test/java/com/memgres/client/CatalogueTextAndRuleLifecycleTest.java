package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the catalogue prints, what a drop says it would break, what the wire says a transaction
 * did, and what a composite type is.
 *
 * <p>All four were answers a client could not rely on. A stored definition came back as the text
 * it was typed as rather than as the tree the server analysed it into, so a view lost its WITH
 * clause and printed a window call as a Java record dump; a blocked DROP listed its dependents in
 * hash-map order and a cascading one named none of them; a rule was stored without resolving the
 * columns it named, and survived the relation it was written on; ROLLBACK outside a block said
 * nothing at all and START TRANSACTION answered under BEGIN's tag; and a composite type was
 * reachable past the search path, stored as text when a column declared it, and changed under a
 * relation that depended on it.
 *
 * <p>The same four answers went on being unreliable further in. A definition echoed the text a
 * constant was typed as rather than the value its type read, gave a relation it read twice one
 * name, wrote CURRENT_DATE as a call, kept a star it could not expand, and showed neither the
 * entry an operator spelling resolved to nor the conversion that entry needs. A rule was filed
 * under a relation's bare name, so two schemas holding a relation of that name shared one set of
 * them. A DROP that named several objects dropped only the first, and counted an object the same
 * statement was dropping as a reason to refuse. A cast of NULL reached a type the search path
 * does not reach, and a composite was not held to the shape it declares.
 *
 * <p>And a drop of a schema reached every rule whose action named a relation of a name the schema
 * held, wherever that relation really was. A rule was taken off a relation in a schema still
 * standing, and the relation it had sat on could not be written to at all.
 *
 * <p>What the drop of a schema itself answered was unreliable in the same way. A schema still
 * holding something was refused without saying which SQLSTATE, what stood in the way or what to
 * do about it; CASCADE said nothing of what it took and did not reach past the schema to a view,
 * a policy or a column default elsewhere that depended on one of the schema's relations; and a
 * drop rolled back did not put back what it had taken. A rule's action was read again every time
 * it fired rather than settled when the rule was written, and the stored definition was printed
 * without regard to the reading session's search path.
 *
 * <p>Every expectation here was read off PostgreSQL 18 before it was written down.
 */
class CatalogueTextAndRuleLifecycleTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ------------------------------------------------------------ helpers

    /** The first column of the first row, as text, or "(no rows)" when the query answers none. */
    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    /** The one value the query returns, read as the number it is. */
    private static long num(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getLong(1);
        }
    }

    /** Every value of the first column, in order, joined with a comma. */
    private static String column(String sql) throws SQLException {
        List<String> got = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) got.add(rs.getString(1));
        }
        return String.join(",", got);
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** The SQLSTATE a statement raises, or "OK" when it does not raise at all. */
    private static String stateOf(String sql) {
        try {
            exec(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The fields of the error a statement raises, as a client reads them off the wire. */
    private static org.postgresql.util.ServerErrorMessage fieldsOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> exec(sql),
                "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    /**
     * The primary message of the error a statement raises. PostgreSQL sends severity in its own
     * field, so the message on the wire never carries an "ERROR: " prefix.
     */
    private static String messageOf(String sql) {
        return fieldsOf(sql).getMessage();
    }

    private static String detailOf(String sql) {
        return fieldsOf(sql).getDetail();
    }

    /** The hint the error carries, which PostgreSQL sends in a field of its own. */
    private static String hintOf(String sql) {
        return fieldsOf(sql).getHint();
    }

    /** The message and the DETAIL of the first notice a statement raised, in that order. */
    private static String[] noticeOf(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            SQLWarning w = s.getWarnings();
            if (w == null) return new String[]{null, null};
            String detail = null;
            if (w instanceof org.postgresql.util.PSQLWarning) {
                org.postgresql.util.ServerErrorMessage m =
                        ((org.postgresql.util.PSQLWarning) w).getServerErrorMessage();
                if (m != null) detail = m.getDetail();
            }
            return new String[]{w.getMessage(), detail};
        }
    }

    /** Everything the server said, up to and including its ReadyForQuery. */
    private static List<String> readToReady(RawWireClient wire) throws IOException {
        List<String> seen = new ArrayList<>();
        while (true) {
            RawWireClient.Msg m = wire.read();
            if (m == null) break;
            seen.add(m.toString());
            if (m.type == 'Z') break;
        }
        return seen;
    }

    /** One simple query per entry, each answered as the frames the server sent for it. */
    private static List<String> rawSimpleQueries(List<String> sqls) throws IOException {
        List<String> out = new ArrayList<>();
        try (RawWireClient wire = new RawWireClient(memgres.getPort())) {
            wire.startup("memgres", "memgres");
            for (String sql : sqls) {
                wire.write(RawWireClient.query(sql));
                out.add(readToReady(wire).toString());
            }
        }
        return out;
    }

    /**
     * Parse/Bind/Execute/Sync of one statement, on a connection of its own, as frames. Any
     * statements named in {@code before} are run first as simple queries and their answers thrown
     * away, so a case can be put inside an open transaction block.
     */
    private static String rawExtended(String sql, String... before) throws IOException {
        try (RawWireClient wire = new RawWireClient(memgres.getPort())) {
            wire.startup("memgres", "memgres");
            for (String q : before) {
                wire.write(RawWireClient.query(q));
                readToReady(wire);
            }
            wire.write(RawWireClient.parse(sql));
            wire.write(RawWireClient.bind());
            wire.write(RawWireClient.execute());
            wire.write(RawWireClient.sync());
            return readToReady(wire).toString();
        }
    }

    /** Parse the statement, then Bind it with exactly {@code values} text parameters. */
    private static String rawBind(String sql, int values) throws IOException {
        try (RawWireClient wire = new RawWireClient(memgres.getPort())) {
            wire.startup("memgres", "memgres");
            wire.write(RawWireClient.parse(sql));
            byte[] body = RawWireClient.concat(RawWireClient.cstring(""), RawWireClient.cstring(""),
                    RawWireClient.int16(0), RawWireClient.int16(values));
            for (int i = 0; i < values; i++) {
                body = RawWireClient.concat(body, RawWireClient.int32(1), new byte[]{'1'});
            }
            body = RawWireClient.concat(body, RawWireClient.int16(0));
            wire.write(RawWireClient.frame('B', body));
            wire.write(RawWireClient.execute());
            wire.write(RawWireClient.sync());
            return readToReady(wire).toString();
        }
    }

    // ------------------------------------------------------------ a view prints as PostgreSQL prints it

    @Test
    void aViewDefinitionKeepsItsWithClause() throws Exception {
        exec("CREATE TABLE ctl_r1 (id int, name text, amt numeric)");
        exec("CREATE VIEW ctl_cte AS WITH q AS (SELECT id FROM ctl_r1) SELECT id FROM q");
        String expected = " WITH q AS (\n"
                + "         SELECT ctl_r1.id\n"
                + "           FROM ctl_r1\n"
                + "        )\n"
                + " SELECT id\n"
                + "   FROM q;";
        assertEquals(expected, scalar("SELECT pg_get_viewdef('ctl_cte'::regclass, true)"));
        // The definition a client reads is a definition it could replay: it names q, so it must
        // carry the clause that defines q, in both forms and through both catalogue views.
        assertEquals(expected, scalar("SELECT pg_get_viewdef('ctl_cte'::regclass, false)"));
        assertEquals(expected, scalar("SELECT definition FROM pg_views WHERE viewname = 'ctl_cte'"));
        assertEquals(expected, scalar("SELECT view_definition FROM information_schema.views"
                + " WHERE table_name = 'ctl_cte'"));
    }

    @Test
    void aWindowCallIsDeparsedAsSql() throws Exception {
        exec("CREATE TABLE ctl_w1 (id int, name text, amt numeric)");
        exec("CREATE VIEW ctl_win AS SELECT id,"
                + " row_number() OVER (PARTITION BY name ORDER BY id) AS rn FROM ctl_w1");
        assertEquals(" SELECT id,\n"
                        + "    row_number() OVER (PARTITION BY name ORDER BY id) AS rn\n"
                        + "   FROM ctl_w1;",
                scalar("SELECT pg_get_viewdef('ctl_win'::regclass, true)"));

        exec("CREATE VIEW ctl_win2 AS SELECT sum(amt) OVER (PARTITION BY name ORDER BY id"
                + " ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM ctl_w1");
        assertEquals(" SELECT sum(amt) OVER (PARTITION BY name ORDER BY id"
                        + " ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s\n"
                        + "   FROM ctl_w1;",
                scalar("SELECT pg_get_viewdef('ctl_win2'::regclass, true)"));

        exec("CREATE VIEW ctl_win3 AS SELECT count(*) OVER () AS c FROM ctl_w1");
        assertEquals(" SELECT count(*) OVER () AS c\n   FROM ctl_w1;",
                scalar("SELECT pg_get_viewdef('ctl_win3'::regclass, true)"));

        // A named window is written back as a WINDOW clause, and DESC already means NULLS FIRST,
        // so PostgreSQL does not write the clause it was given.
        exec("CREATE VIEW ctl_win4 AS SELECT rank() OVER w AS r FROM ctl_w1"
                + " WINDOW w AS (ORDER BY id DESC NULLS FIRST)");
        assertEquals(" SELECT rank() OVER w AS r\n"
                        + "   FROM ctl_w1\n"
                        + "  WINDOW w AS (ORDER BY id DESC);",
                scalar("SELECT pg_get_viewdef('ctl_win4'::regclass, true)"));
    }

    @Test
    void aColumnIsQualifiedOnlyWhereItsQueryIsNested() throws Exception {
        exec("CREATE TABLE ctl_q1 (id int, name text, amt numeric)");
        exec("CREATE TABLE ctl_q2 (id int, tag text)");
        // A single FROM item of any kind leaves the outer columns bare; the sub-select inside it
        // is a nested query, so its own columns take the relation's name.
        exec("CREATE VIEW ctl_sub AS SELECT s.id FROM (SELECT id FROM ctl_q1) s");
        assertEquals(" SELECT id\n"
                        + "   FROM ( SELECT ctl_q1.id\n"
                        + "           FROM ctl_q1) s;",
                scalar("SELECT pg_get_viewdef('ctl_sub'::regclass, true)"));

        exec("CREATE VIEW ctl_sub2 AS SELECT x.id FROM (SELECT y.id"
                + " FROM (SELECT id FROM ctl_q1) y) x");
        assertEquals(" SELECT id\n"
                        + "   FROM ( SELECT y.id\n"
                        + "           FROM ( SELECT ctl_q1.id\n"
                        + "                   FROM ctl_q1) y) x;",
                scalar("SELECT pg_get_viewdef('ctl_sub2'::regclass, true)"));

        // A join makes the rangetable longer than one entry, so even the outer column qualifies.
        exec("CREATE VIEW ctl_using AS SELECT id FROM ctl_q1 JOIN ctl_q2 USING (id)");
        assertEquals(" SELECT ctl_q1.id\n"
                        + "   FROM ctl_q1\n"
                        + "     JOIN ctl_q2 USING (id);",
                scalar("SELECT pg_get_viewdef('ctl_using'::regclass, true)"));
    }

    @Test
    void aJoinTreeBreaksBeforeEveryJoin() throws Exception {
        exec("CREATE TABLE ctl_j1 (id int, name text, amt numeric)");
        exec("CREATE TABLE ctl_j2 (id int, tag text)");
        exec("CREATE TABLE ctl_j3 (id int, note text)");
        exec("CREATE VIEW ctl_join AS SELECT a.id, b.tag"
                + " FROM ctl_j1 a LEFT JOIN ctl_j2 b ON a.id = b.id");
        assertEquals(" SELECT a.id,\n"
                        + "    b.tag\n"
                        + "   FROM ctl_j1 a\n"
                        + "     LEFT JOIN ctl_j2 b ON a.id = b.id;",
                scalar("SELECT pg_get_viewdef('ctl_join'::regclass, true)"));
        // The non-pretty form brackets the whole tree once and doubles the ON predicate.
        assertEquals(" SELECT a.id,\n"
                        + "    b.tag\n"
                        + "   FROM (ctl_j1 a\n"
                        + "     LEFT JOIN ctl_j2 b ON ((a.id = b.id)));",
                scalar("SELECT pg_get_viewdef('ctl_join'::regclass, false)"));

        exec("CREATE VIEW ctl_join3 AS SELECT a.id FROM ctl_j1 a"
                + " JOIN ctl_j2 b ON a.id = b.id JOIN ctl_j3 c ON b.id = c.id");
        assertEquals(" SELECT a.id\n"
                        + "   FROM ctl_j1 a\n"
                        + "     JOIN ctl_j2 b ON a.id = b.id\n"
                        + "     JOIN ctl_j3 c ON b.id = c.id;",
                scalar("SELECT pg_get_viewdef('ctl_join3'::regclass, true)"));
        assertEquals(" SELECT a.id\n"
                        + "   FROM ((ctl_j1 a\n"
                        + "     JOIN ctl_j2 b ON ((a.id = b.id)))\n"
                        + "     JOIN ctl_j3 c ON ((b.id = c.id)));",
                scalar("SELECT pg_get_viewdef('ctl_join3'::regclass, false)"));

        exec("CREATE VIEW ctl_cross AS SELECT a.id, b.tag FROM ctl_j1 a CROSS JOIN ctl_j2 b");
        assertEquals(" SELECT a.id,\n"
                        + "    b.tag\n"
                        + "   FROM ctl_j1 a\n"
                        + "     CROSS JOIN ctl_j2 b;",
                scalar("SELECT pg_get_viewdef('ctl_cross'::regclass, true)"));
    }

    @Test
    void aSetOperationPutsItsOperatorOnALineOfItsOwn() throws Exception {
        exec("CREATE TABLE ctl_s1 (id int, name text)");
        exec("CREATE TABLE ctl_s2 (id int, tag text)");
        exec("CREATE TABLE ctl_s3 (id int, note text)");
        exec("CREATE VIEW ctl_un AS SELECT id FROM ctl_s1 UNION ALL SELECT id FROM ctl_s2");
        assertEquals(" SELECT ctl_s1.id\n"
                        + "   FROM ctl_s1\n"
                        + "UNION ALL\n"
                        + " SELECT ctl_s2.id\n"
                        + "   FROM ctl_s2;",
                scalar("SELECT pg_get_viewdef('ctl_un'::regclass, true)"));

        exec("CREATE VIEW ctl_un3 AS SELECT id FROM ctl_s1 UNION SELECT id FROM ctl_s2"
                + " UNION SELECT id FROM ctl_s3");
        assertEquals(" SELECT ctl_s1.id\n"
                        + "   FROM ctl_s1\n"
                        + "UNION\n"
                        + " SELECT ctl_s2.id\n"
                        + "   FROM ctl_s2\n"
                        + "UNION\n"
                        + " SELECT ctl_s3.id\n"
                        + "   FROM ctl_s3;",
                scalar("SELECT pg_get_viewdef('ctl_un3'::regclass, true)"));
    }

    @Test
    void aCaseIsLaidOutOverLinesOfItsOwn() throws Exception {
        exec("CREATE TABLE ctl_c1 (id int, name text, amt numeric)");
        exec("CREATE VIEW ctl_case AS SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END AS k"
                + " FROM ctl_c1");
        assertEquals(" SELECT\n"
                        + "        CASE\n"
                        + "            WHEN id > 1 THEN 'big'::text\n"
                        + "            ELSE 'small'::text\n"
                        + "        END AS k\n"
                        + "   FROM ctl_c1;",
                scalar("SELECT pg_get_viewdef('ctl_case'::regclass, true)"));
        // The two forms differ only in the brackets round the WHEN condition.
        assertEquals(" SELECT\n"
                        + "        CASE\n"
                        + "            WHEN (id > 1) THEN 'big'::text\n"
                        + "            ELSE 'small'::text\n"
                        + "        END AS k\n"
                        + "   FROM ctl_c1;",
                scalar("SELECT pg_get_viewdef('ctl_case'::regclass, false)"));

        // A CASE with no ELSE is written with the one it means, of the type its arms settled on.
        exec("CREATE VIEW ctl_case2 AS SELECT CASE id WHEN 1 THEN 'a' WHEN 2 THEN 'b' END AS k"
                + " FROM ctl_c1");
        assertEquals(" SELECT\n"
                        + "        CASE id\n"
                        + "            WHEN 1 THEN 'a'::text\n"
                        + "            WHEN 2 THEN 'b'::text\n"
                        + "            ELSE NULL::text\n"
                        + "        END AS k\n"
                        + "   FROM ctl_c1;",
                scalar("SELECT pg_get_viewdef('ctl_case2'::regclass, true)"));

        // Only the CASE item goes onto lines of its own; the items round it keep the usual layout.
        exec("CREATE VIEW ctl_case3 AS SELECT id, CASE WHEN id > 1 THEN 1 ELSE 2 END AS k, name"
                + " FROM ctl_c1");
        assertEquals(" SELECT id,\n"
                        + "        CASE\n"
                        + "            WHEN id > 1 THEN 1\n"
                        + "            ELSE 2\n"
                        + "        END AS k,\n"
                        + "    name\n"
                        + "   FROM ctl_c1;",
                scalar("SELECT pg_get_viewdef('ctl_case3'::regclass, true)"));
    }

    @Test
    void aSubSelectIsLaidOutRelativeToItsParenthesis() throws Exception {
        exec("CREATE TABLE ctl_b1 (id int, name text)");
        exec("CREATE TABLE ctl_b2 (id int, tag text)");
        // An unlabelled aggregate inside a sub-select is given the name it will be known by.
        exec("CREATE VIEW ctl_scal AS SELECT id, (SELECT count(*) FROM ctl_b2) AS n FROM ctl_b1");
        assertEquals(" SELECT id,\n"
                        + "    ( SELECT count(*) AS count\n"
                        + "           FROM ctl_b2) AS n\n"
                        + "   FROM ctl_b1;",
                scalar("SELECT pg_get_viewdef('ctl_scal'::regclass, true)"));

        exec("CREATE VIEW ctl_ex AS SELECT id FROM ctl_b1"
                + " WHERE EXISTS (SELECT 1 FROM ctl_b2 WHERE ctl_b2.id = ctl_b1.id)");
        assertEquals(" SELECT id\n"
                        + "   FROM ctl_b1\n"
                        + "  WHERE (EXISTS ( SELECT 1\n"
                        + "           FROM ctl_b2\n"
                        + "          WHERE ctl_b2.id = ctl_b1.id));",
                scalar("SELECT pg_get_viewdef('ctl_ex'::regclass, true)"));

        exec("CREATE VIEW ctl_insub AS SELECT id FROM ctl_b1"
                + " WHERE id IN (SELECT id FROM ctl_b2)");
        assertEquals(" SELECT id\n"
                        + "   FROM ctl_b1\n"
                        + "  WHERE (id IN ( SELECT ctl_b2.id\n"
                        + "           FROM ctl_b2));",
                scalar("SELECT pg_get_viewdef('ctl_insub'::regclass, true)"));

        exec("CREATE VIEW ctl_notin AS SELECT id FROM ctl_b1"
                + " WHERE id NOT IN (SELECT id FROM ctl_b2)");
        assertEquals(" SELECT id\n"
                        + "   FROM ctl_b1\n"
                        + "  WHERE NOT (id IN ( SELECT ctl_b2.id\n"
                        + "           FROM ctl_b2));",
                scalar("SELECT pg_get_viewdef('ctl_notin'::regclass, true)"));
    }

    @Test
    void castsCoalesceBetweenAndInAreSpeltAsPostgresSpellsThem() throws Exception {
        exec("CREATE TABLE ctl_e1 (id int, name text, amt numeric)");
        exec("CREATE VIEW ctl_cast AS SELECT id::text AS t, amt::int AS n, upper(name) AS u"
                + " FROM ctl_e1");
        assertEquals(" SELECT id::text AS t,\n"
                        + "    amt::integer AS n,\n"
                        + "    upper(name) AS u\n"
                        + "   FROM ctl_e1;",
                scalar("SELECT pg_get_viewdef('ctl_cast'::regclass, true)"));
        assertEquals(" SELECT (id)::text AS t,\n"
                        + "    (amt)::integer AS n,\n"
                        + "    upper(name) AS u\n"
                        + "   FROM ctl_e1;",
                scalar("SELECT pg_get_viewdef('ctl_cast'::regclass, false)"));

        exec("CREATE VIEW ctl_expr AS SELECT coalesce(name, 'n') AS c,"
                + " id BETWEEN 1 AND 5 AS b, name IN ('a','b') AS i FROM ctl_e1");
        assertEquals(" SELECT COALESCE(name, 'n'::text) AS c,\n"
                        + "    id >= 1 AND id <= 5 AS b,\n"
                        + "    name = ANY (ARRAY['a'::text, 'b'::text]) AS i\n"
                        + "   FROM ctl_e1;",
                scalar("SELECT pg_get_viewdef('ctl_expr'::regclass, true)"));

        exec("CREATE VIEW ctl_sp AS SELECT name::varchar(4) AS v, nullif(name,'a') AS n,"
                + " greatest(id,1) AS g, least(id,1) AS l FROM ctl_e1");
        assertEquals(" SELECT name::character varying(4) AS v,\n"
                        + "    NULLIF(name, 'a'::text) AS n,\n"
                        + "    GREATEST(id, 1) AS g,\n"
                        + "    LEAST(id, 1) AS l\n"
                        + "   FROM ctl_e1;",
                scalar("SELECT pg_get_viewdef('ctl_sp'::regclass, true)"));

        exec("CREATE VIEW ctl_lk AS SELECT id FROM ctl_e1"
                + " WHERE name LIKE 'a%' OR name NOT LIKE 'b%' OR name ILIKE 'c%'");
        assertEquals(" SELECT id\n"
                        + "   FROM ctl_e1\n"
                        + "  WHERE name ~~ 'a%'::text OR name !~~ 'b%'::text"
                        + " OR name ~~* 'c%'::text;",
                scalar("SELECT pg_get_viewdef('ctl_lk'::regclass, true)"));

        exec("CREATE VIEW ctl_nbw AS SELECT id FROM ctl_e1 WHERE name NOT BETWEEN 'a' AND 'b'");
        assertEquals(" SELECT id\n"
                        + "   FROM ctl_e1\n"
                        + "  WHERE name < 'a'::text OR name > 'b'::text;",
                scalar("SELECT pg_get_viewdef('ctl_nbw'::regclass, true)"));
    }

    @Test
    void aConstantCarriesTheTypeItWasReadAs() throws Exception {
        exec("CREATE TABLE ctl_l1 (id int, name text, amt numeric)");
        exec("CREATE VIEW ctl_lit AS SELECT id, name FROM ctl_l1 WHERE name = 'x' AND amt > 1");
        assertEquals(" SELECT id,\n"
                        + "    name\n"
                        + "   FROM ctl_l1\n"
                        + "  WHERE name = 'x'::text AND amt > 1::numeric;",
                scalar("SELECT pg_get_viewdef('ctl_lit'::regclass, true)"));
        assertEquals(" SELECT id,\n"
                        + "    name\n"
                        + "   FROM ctl_l1\n"
                        + "  WHERE ((name = 'x'::text) AND (amt > (1)::numeric));",
                scalar("SELECT pg_get_viewdef('ctl_lit'::regclass, false)"));

        exec("CREATE TABLE ctl_l2 (bi bigint, si smallint, re real, nu numeric(10,2), da date,"
                + " ch char(3), vc varchar(5))");
        exec("CREATE VIEW ctl_lits AS SELECT bi, si FROM ctl_l2 WHERE bi = 1 AND si = 2"
                + " AND re = 1.5 AND nu = 3 AND da = '2020-01-01' AND ch = 'a'");
        assertEquals(" SELECT bi,\n"
                        + "    si\n"
                        + "   FROM ctl_l2\n"
                        + "  WHERE bi = 1 AND si = 2 AND re = 1.5::double precision"
                        + " AND nu = 3::numeric AND da = '2020-01-01'::date"
                        + " AND ch = 'a'::bpchar;",
                scalar("SELECT pg_get_viewdef('ctl_lits'::regclass, true)"));

        // An integer column against a string constant reads the constant back as the number.
        exec("CREATE VIEW ctl_ints AS SELECT id FROM ctl_l1 WHERE id = '4'");
        assertEquals(" SELECT id\n   FROM ctl_l1\n  WHERE id = 4;",
                scalar("SELECT pg_get_viewdef('ctl_ints'::regclass, true)"));

        // A constant written with a cast carries one label and no brackets, and the cast is
        // dropped when the constant already reads as that type.
        exec("CREATE VIEW ctl_writ AS SELECT 'v'::text AS a, NULL::text AS b, 1::numeric AS c,"
                + " 1.9::numeric AS d, 1::int AS e FROM ctl_l1");
        assertEquals(" SELECT 'v'::text AS a,\n"
                        + "    NULL::text AS b,\n"
                        + "    1::numeric AS c,\n"
                        + "    1.9 AS d,\n"
                        + "    1 AS e\n"
                        + "   FROM ctl_l1;",
                scalar("SELECT pg_get_viewdef('ctl_writ'::regclass, true)"));
    }

    @Test
    void everyClauseStartsItsOwnLineAndTheWrapColumnDecidesTheBreak() throws Exception {
        exec("CREATE TABLE ctl_o1 (id int, name text, amt numeric)");
        exec("CREATE VIEW ctl_agg AS SELECT name, count(*) AS c FROM ctl_o1"
                + " GROUP BY name HAVING count(*) > 1 ORDER BY name");
        assertEquals(" SELECT name,\n"
                        + "    count(*) AS c\n"
                        + "   FROM ctl_o1\n"
                        + "  GROUP BY name\n"
                        + " HAVING (count(*) > 1)\n"
                        + "  ORDER BY name;",
                scalar("SELECT pg_get_viewdef('ctl_agg'::regclass, false)"));

        // OFFSET is written before LIMIT whichever order they were written in.
        exec("CREATE VIEW ctl_lo AS SELECT id FROM ctl_o1 ORDER BY id DESC LIMIT 5 OFFSET 2");
        assertEquals(" SELECT id\n"
                        + "   FROM ctl_o1\n"
                        + "  ORDER BY id DESC\n"
                        + " OFFSET 2\n"
                        + " LIMIT 5;",
                scalar("SELECT pg_get_viewdef('ctl_lo'::regclass, true)"));

        exec("CREATE VIEW ctl_wrap AS SELECT id, name, amt FROM ctl_o1 WHERE id > 0");
        assertEquals(" SELECT id, name, amt\n   FROM ctl_o1\n  WHERE id > 0;",
                scalar("SELECT pg_get_viewdef('ctl_wrap'::regclass, 80)"));
        assertEquals(" SELECT id, name,\n    amt\n   FROM ctl_o1\n  WHERE id > 0;",
                scalar("SELECT pg_get_viewdef('ctl_wrap'::regclass, 20)"));
    }

    @Test
    void aParenthesisedBodyKeepsTheClausesWrittenAfterIt() throws Exception {
        exec("CREATE TABLE ctl_p1 (id int)");
        exec("CREATE TABLE ctl_p2 (id int)");
        exec("INSERT INTO ctl_p1 VALUES (1),(2),(3),(4),(5)");
        exec("INSERT INTO ctl_p2 VALUES (6),(7)");
        exec("CREATE VIEW ctl_lim AS (SELECT id FROM ctl_p1 UNION SELECT id FROM ctl_p2)"
                + " ORDER BY 1 LIMIT 3");
        // The LIMIT belongs to the view, not to the bracketed arm: three rows, not seven.
        assertEquals(3, num("SELECT count(*) FROM ctl_lim"));
        assertEquals(" SELECT ctl_p1.id\n"
                        + "   FROM ctl_p1\n"
                        + "UNION\n"
                        + " SELECT ctl_p2.id\n"
                        + "   FROM ctl_p2\n"
                        + "  ORDER BY 1\n"
                        + " LIMIT 3;",
                scalar("SELECT pg_get_viewdef('ctl_lim'::regclass, true)"));

        exec("CREATE VIEW ctl_lim2 AS (SELECT id FROM ctl_p1) ORDER BY id LIMIT 2");
        assertEquals(2, num("SELECT count(*) FROM ctl_lim2"));
        assertEquals(" SELECT id\n   FROM ctl_p1\n  ORDER BY id\n LIMIT 2;",
                scalar("SELECT pg_get_viewdef('ctl_lim2'::regclass, true)"));
    }

    // ------------------------------------------------------------ a stored expression prints as its tree

    @Test
    void aStoredDefaultPrintsAsTheTreeItWasAnalysedInto() throws Exception {
        exec("CREATE TABLE ctl_def (a bigint DEFAULT 1::int, b int DEFAULT 1.9::numeric,"
                + " c int DEFAULT (2+3), d int DEFAULT '7', e text DEFAULT 'x',"
                + " f int DEFAULT '-1'::int, g numeric DEFAULT .5, h boolean DEFAULT 'true',"
                + " i text DEFAULT upper('q'), j text DEFAULT 'a' || 'b',"
                + " k numeric(10,2) DEFAULT 0.00, l date DEFAULT '2020-01-01'::date)");
        String expected = "a=1 b=1.9 c=(2 + 3) d=7 e='x'::text f='-1'::integer g=0.5 h=true"
                + " i=upper('q'::text) j=('a'::text || 'b'::text) k=0.00 l='2020-01-01'::date";
        assertEquals(expected, scalar("SELECT string_agg(a.attname || '='"
                + " || pg_get_expr(d.adbin, d.adrelid), ' ' ORDER BY d.adnum)"
                + " FROM pg_attrdef d JOIN pg_class c ON c.oid = d.adrelid"
                + " JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum"
                + " WHERE c.relname = 'ctl_def'"));
        // information_schema reads the same catalogue column and must answer identically.
        assertEquals(expected, scalar("SELECT string_agg(column_name || '=' || column_default,"
                + " ' ' ORDER BY ordinal_position) FROM information_schema.columns"
                + " WHERE table_name = 'ctl_def'"));

        // The folded text still behaves as it was written.
        exec("INSERT INTO ctl_def DEFAULT VALUES");
        assertEquals("1 2 5 7 x -1 0.5 true Q ab 0.00 2020-01-01",
                scalar("SELECT a || ' ' || b || ' ' || c || ' ' || d || ' ' || e || ' ' || f"
                        + " || ' ' || g || ' ' || h || ' ' || i || ' ' || j || ' ' || k"
                        + " || ' ' || l FROM ctl_def"));
    }

    @Test
    void aCastTheConstantDoesNotAlreadyReadAsIsKeptWithItsOperandBracketed() throws Exception {
        exec("CREATE TABLE ctl_keep (l int DEFAULT 1::bigint, m int DEFAULT 1::smallint,"
                + " n bigint DEFAULT 1::text::int)");
        assertEquals("l=(1)::bigint m=(1)::smallint n=((1)::text)::integer",
                scalar("SELECT string_agg(a.attname || '='"
                        + " || pg_get_expr(d.adbin, d.adrelid), ' ' ORDER BY d.adnum)"
                        + " FROM pg_attrdef d JOIN pg_class c ON c.oid = d.adrelid"
                        + " JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum"
                        + " WHERE c.relname = 'ctl_keep'"));
    }

    /**
     * The defaults that are not constants must survive the folding untouched: a call keeps its
     * parentheses, a SQL keyword function keeps its keyword spelling, and a serial column's
     * sequence call keeps the ::regclass the server put there.
     */
    @Test
    void aDefaultThatIsNotAFoldableConstantIsPrintedAsItStands() throws Exception {
        exec("CREATE TABLE ctl_live (b timestamp DEFAULT CURRENT_TIMESTAMP, c serial,"
                + " d numeric DEFAULT 1500, e timestamptz DEFAULT now())");
        assertEquals("b=CURRENT_TIMESTAMP c=nextval('ctl_live_c_seq'::regclass) d=1500 e=now()",
                scalar("SELECT string_agg(a.attname || '='"
                        + " || pg_get_expr(d.adbin, d.adrelid), ' ' ORDER BY d.adnum)"
                        + " FROM pg_attrdef d JOIN pg_class c ON c.oid = d.adrelid"
                        + " JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum"
                        + " WHERE c.relname = 'ctl_live'"));
    }

    /**
     * A constraint's text comes from its own deparser, and reads exactly as PostgreSQL writes it.
     * Asserted here because the default and the view definition are printed by machinery a
     * constraint shares parts of: a change to either must not reach this.
     */
    @Test
    void aConstraintPrintsTheWayPostgresPrintsIt() throws Exception {
        exec("CREATE TABLE ctl_ck (i int CHECK (i > 0), j text UNIQUE, k int PRIMARY KEY)");
        exec("CREATE TABLE ctl_fk (a int REFERENCES ctl_ck(k) ON DELETE CASCADE)");
        assertEquals("ctl_ck_i_check=CHECK ((i > 0)) ctl_ck_j_key=UNIQUE (j)"
                        + " ctl_ck_k_not_null=NOT NULL k ctl_ck_pkey=PRIMARY KEY (k)",
                scalar("SELECT string_agg(conname || '=' || pg_get_constraintdef(oid),"
                        + " ' ' ORDER BY conname) FROM pg_constraint"
                        + " WHERE conrelid = 'ctl_ck'::regclass"));
        assertEquals("FOREIGN KEY (a) REFERENCES ctl_ck(k) ON DELETE CASCADE",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conrelid = 'ctl_fk'::regclass"));
    }

    @Test
    void anIndexOverAPartitionedRelationIsPrintedWithOnOnly() throws Exception {
        exec("CREATE TABLE ctl_pg (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE INDEX ctl_pg_idx ON ctl_pg (s)");
        exec("CREATE TABLE ctl_pg_0 PARTITION OF ctl_pg FOR VALUES FROM (0) TO (100)"
                + " PARTITION BY RANGE (i)");
        exec("CREATE TABLE ctl_pg_0_0 PARTITION OF ctl_pg_0 FOR VALUES FROM (0) TO (10)");
        assertEquals("CREATE INDEX ctl_pg_idx ON ONLY public.ctl_pg USING btree (s)",
                scalar("SELECT pg_get_indexdef('ctl_pg_idx'::regclass)"));
        assertEquals("CREATE INDEX ctl_pg_0_s_idx ON ONLY public.ctl_pg_0 USING btree (s)",
                scalar("SELECT pg_get_indexdef('ctl_pg_0_s_idx'::regclass)"));
        // A leaf partition is an ordinary table, so it gets no ONLY.
        assertEquals("CREATE INDEX ctl_pg_0_0_s_idx ON public.ctl_pg_0_0 USING btree (s)",
                scalar("SELECT pg_get_indexdef('ctl_pg_0_0_s_idx'::regclass)"));
        // The pretty form drops the schema and keeps the ONLY.
        assertEquals("CREATE INDEX ctl_pg_idx ON ONLY ctl_pg USING btree (s)",
                scalar("SELECT pg_get_indexdef('ctl_pg_idx'::regclass, 0, true)"));
        assertEquals("CREATE INDEX ctl_pg_idx ON ONLY public.ctl_pg USING btree (s)"
                        + " | CREATE INDEX ctl_pg_0_s_idx ON ONLY public.ctl_pg_0 USING btree (s)"
                        + " | CREATE INDEX ctl_pg_0_0_s_idx ON public.ctl_pg_0_0 USING btree (s)",
                scalar("SELECT string_agg(indexdef, ' | ' ORDER BY tablename, indexname)"
                        + " FROM pg_indexes WHERE tablename LIKE 'ctl!_pg%' ESCAPE '!'"));

        // A constraint-derived index over a partitioned table says ON ONLY too.
        exec("CREATE TABLE ctl_kp (i int PRIMARY KEY, j text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE ctl_kp_0 PARTITION OF ctl_kp FOR VALUES FROM (0) TO (10)");
        assertEquals("CREATE UNIQUE INDEX ctl_kp_pkey ON ONLY public.ctl_kp USING btree (i)"
                        + " | CREATE UNIQUE INDEX ctl_kp_0_pkey ON public.ctl_kp_0"
                        + " USING btree (i)",
                scalar("SELECT string_agg(indexdef, ' | ' ORDER BY tablename)"
                        + " FROM pg_indexes WHERE tablename LIKE 'ctl!_kp%' ESCAPE '!'"));
        exec("CREATE UNIQUE INDEX ctl_kp_u ON ctl_kp (i, j)");
        assertEquals("CREATE UNIQUE INDEX ctl_kp_u ON ONLY public.ctl_kp USING btree (i, j)",
                scalar("SELECT pg_get_indexdef('ctl_kp_u'::regclass)"));
    }

    @Test
    void anIndexPredicateKeepsItsOuterParentheses() throws Exception {
        exec("CREATE TABLE ctl_pe (i int, s text)");
        exec("CREATE INDEX ctl_pe_idx ON ctl_pe (s) WHERE i > 5");
        exec("CREATE INDEX ctl_pe_ix2 ON ctl_pe (s) WHERE i > 5 AND s IS NOT NULL");
        exec("CREATE INDEX ctl_pe_ix3 ON ctl_pe (lower(s))");
        assertEquals("(i > 5)", scalar("SELECT pg_get_expr(i.indpred, i.indrelid)"
                + " FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid"
                + " WHERE ic.relname = 'ctl_pe_idx'"));
        assertEquals("((i > 5) AND (s IS NOT NULL))",
                scalar("SELECT pg_get_expr(i.indpred, i.indrelid)"
                        + " FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid"
                        + " WHERE ic.relname = 'ctl_pe_ix2'"));
        // An index with no predicate has a null indpred, and an expression index keeps indexprs.
        assertNull(scalar("SELECT pg_get_expr(i.indpred, i.indrelid)"
                + " FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid"
                + " WHERE ic.relname = 'ctl_pe_ix3'"));
        assertEquals("lower(s)", scalar("SELECT pg_get_expr(i.indexprs, i.indrelid)"
                + " FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid"
                + " WHERE ic.relname = 'ctl_pe_ix3'"));
    }

    // ------------------------------------------------------------ a drop says what it would break

    @Test
    void aBlockedDropListsItsDependentsInCreationOrder() throws Exception {
        exec("CREATE TABLE ctl_ord (id int)");
        exec("CREATE VIEW ctl_zview AS SELECT id FROM ctl_ord");
        exec("CREATE VIEW ctl_mview AS SELECT id FROM ctl_ord");
        exec("CREATE VIEW ctl_aview AS SELECT id FROM ctl_ord");
        assertEquals("cannot drop table ctl_ord because other objects depend on it",
                messageOf("DROP TABLE ctl_ord"));
        assertEquals("2BP01", stateOf("DROP TABLE ctl_ord"));
        // Creation order, not name order: the whole string, because the order is the point.
        assertEquals("view ctl_zview depends on table ctl_ord\n"
                        + "view ctl_mview depends on table ctl_ord\n"
                        + "view ctl_aview depends on table ctl_ord",
                detailOf("DROP TABLE ctl_ord"));
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.",
                hintOf("DROP TABLE ctl_ord"));
        // The refusal dropped nothing.
        assertEquals(4, num("SELECT count(*) FROM pg_class WHERE relname IN"
                + " ('ctl_ord','ctl_zview','ctl_mview','ctl_aview')"));
    }

    @Test
    void theDependencyWalkFollowsEachDependentsOwnDependentsFirst() throws Exception {
        exec("CREATE TABLE ctl_dfs (id int)");
        exec("CREATE VIEW ctl_v1 AS SELECT id FROM ctl_dfs");
        exec("CREATE VIEW ctl_v2 AS SELECT id FROM ctl_dfs");
        exec("CREATE VIEW ctl_v1a AS SELECT id FROM ctl_v1");
        // ctl_v1a was created last and is reported second: the walk is depth first.
        assertEquals("view ctl_v1 depends on table ctl_dfs\n"
                        + "view ctl_v1a depends on view ctl_v1\n"
                        + "view ctl_v2 depends on table ctl_dfs",
                detailOf("DROP TABLE ctl_dfs"));
    }

    @Test
    void aBlockedDropSequenceNamesEveryColumnDefaultThatCallsIt() throws Exception {
        exec("CREATE SEQUENCE ctl_sq1");
        exec("CREATE TABLE ctl_d1 (a int DEFAULT nextval('ctl_sq1'),"
                + " b int DEFAULT nextval('ctl_sq1'), c int DEFAULT nextval('ctl_sq1'))");
        exec("CREATE TABLE ctl_d2 (a int DEFAULT nextval('ctl_sq1'))");
        assertEquals("cannot drop sequence ctl_sq1 because other objects depend on it",
                messageOf("DROP SEQUENCE ctl_sq1"));
        // Tables in creation order, columns in declaration order within a table.
        assertEquals("default value for column a of table ctl_d1 depends on sequence ctl_sq1\n"
                        + "default value for column b of table ctl_d1 depends on sequence ctl_sq1\n"
                        + "default value for column c of table ctl_d1 depends on sequence ctl_sq1\n"
                        + "default value for column a of table ctl_d2 depends on sequence ctl_sq1",
                detailOf("DROP SEQUENCE ctl_sq1"));
    }

    @Test
    void dropCascadeSaysWhatItDropped() throws Exception {
        exec("CREATE TABLE ctl_cp (id int PRIMARY KEY)");
        exec("CREATE VIEW ctl_cv1 AS SELECT id FROM ctl_cp");
        exec("CREATE VIEW ctl_cv2 AS SELECT id FROM ctl_cp");
        exec("CREATE VIEW ctl_cv3 AS SELECT id FROM ctl_cp");
        String[] notice = noticeOf("DROP TABLE ctl_cp CASCADE");
        assertEquals("drop cascades to 3 other objects", notice[0]);
        assertEquals("drop cascades to view ctl_cv1\n"
                + "drop cascades to view ctl_cv2\n"
                + "drop cascades to view ctl_cv3", notice[1]);

        // Two dependents already take the counted form.
        exec("CREATE TABLE ctl_two (id int)");
        exec("CREATE VIEW ctl_tv1 AS SELECT id FROM ctl_two");
        exec("CREATE VIEW ctl_tv2 AS SELECT id FROM ctl_tv1");
        String[] two = noticeOf("DROP TABLE ctl_two CASCADE");
        assertEquals("drop cascades to 2 other objects", two[0]);
        assertEquals("drop cascades to view ctl_tv1\ndrop cascades to view ctl_tv2", two[1]);
    }

    @Test
    void oneDependentIsNamedInTheMessageAndCarriesNoDetail() throws Exception {
        exec("CREATE TABLE ctl_one (id int)");
        exec("CREATE VIEW ctl_ov1 AS SELECT id FROM ctl_one");
        String[] notice = noticeOf("DROP TABLE ctl_one CASCADE");
        assertEquals("drop cascades to view ctl_ov1", notice[0]);
        assertNull(notice[1], "a single dependent is named in the message, so no DETAIL is sent");
    }

    @Test
    void pastAHundredDependentsTheListIsCutOff() throws Exception {
        exec("CREATE TABLE ctl_big (id int)");
        for (int i = 1; i <= 101; i++) {
            exec(String.format("CREATE VIEW ctl_bv%03d AS SELECT id FROM ctl_big", i));
        }
        String[] blocking = detailOf("DROP TABLE ctl_big").split("\n", -1);
        assertEquals(101, blocking.length);
        assertEquals("view ctl_bv001 depends on table ctl_big", blocking[0]);
        assertEquals("view ctl_bv100 depends on table ctl_big", blocking[99]);
        assertEquals("and 1 other object (see server log for list)", blocking[100]);

        String[] notice = noticeOf("DROP TABLE ctl_big CASCADE");
        assertEquals("drop cascades to 101 other objects", notice[0]);
        String[] lines = notice[1].split("\n", -1);
        assertEquals(101, lines.length);
        assertEquals("drop cascades to view ctl_bv001", lines[0]);
        assertEquals("drop cascades to view ctl_bv100", lines[99]);
        assertEquals("and 1 other object (see server log for list)", lines[100]);
    }

    // ------------------------------------------------------------ a rule is checked when it is written

    @Test
    void everyColumnARuleNamesIsResolvedWhenTheRuleIsWritten() throws Exception {
        exec("CREATE TABLE ctl_ra (i int, j int)");
        exec("CREATE TABLE ctl_rb (i int, k int)");
        String[] refused = {
                // an inner SELECT's target list
                "CREATE RULE ctl_r1 AS ON INSERT TO ctl_ra DO ALSO"
                        + " INSERT INTO ctl_rb SELECT nosuchcol FROM ctl_rb",
                // its WHERE
                "CREATE RULE ctl_r2 AS ON INSERT TO ctl_ra DO ALSO"
                        + " INSERT INTO ctl_rb SELECT i FROM ctl_rb WHERE nosuchcol > 1",
                // its ORDER BY
                "CREATE RULE ctl_r3 AS ON INSERT TO ctl_ra DO ALSO"
                        + " INSERT INTO ctl_rb SELECT i FROM ctl_rb ORDER BY nosuchcol",
                // a DELETE's WHERE
                "CREATE RULE ctl_r4 AS ON DELETE TO ctl_ra DO ALSO"
                        + " DELETE FROM ctl_rb WHERE nosuchcol = 1",
                // the right of an UPDATE SET
                "CREATE RULE ctl_r5 AS ON UPDATE TO ctl_ra DO ALSO"
                        + " UPDATE ctl_rb SET k = nosuchcol",
                // an UPDATE's WHERE
                "CREATE RULE ctl_r6 AS ON UPDATE TO ctl_ra DO ALSO"
                        + " UPDATE ctl_rb SET k = 1 WHERE nosuchcol = 2",
                // the rule's own qualification
                "CREATE RULE ctl_r7 AS ON INSERT TO ctl_ra WHERE nosuchcol > 1 DO ALSO"
                        + " INSERT INTO ctl_rb VALUES (1, 2)",
                // a scalar subquery inside VALUES
                "CREATE RULE ctl_r8 AS ON INSERT TO ctl_ra DO ALSO"
                        + " INSERT INTO ctl_rb VALUES ((SELECT nosuchcol FROM ctl_rb), 2)",
                // a CTE inside the action
                "CREATE RULE ctl_r9 AS ON INSERT TO ctl_ra DO ALSO"
                        + " WITH c AS (SELECT nosuchcol FROM ctl_rb)"
                        + " INSERT INTO ctl_rb SELECT 1, 2",
                // a DO INSTEAD action
                "CREATE RULE ctl_r11 AS ON INSERT TO ctl_ra DO INSTEAD"
                        + " INSERT INTO ctl_rb SELECT nosuchcol FROM ctl_rb",
                // the second action of a parenthesised action list
                "CREATE RULE ctl_r13 AS ON INSERT TO ctl_ra DO ALSO"
                        + " (INSERT INTO ctl_rb VALUES (1, 2);"
                        + " INSERT INTO ctl_rb SELECT nosuchcol FROM ctl_rb;)"};
        for (String sql : refused) {
            assertEquals("42703", stateOf(sql), sql);
            org.postgresql.util.ServerErrorMessage m = fieldsOf(sql);
            assertEquals("column \"nosuchcol\" does not exist", m.getMessage(), sql);
            assertNull(m.getDetail(), sql);
            assertNull(m.getHint(), sql);
        }
        // A qualified miss is named by the alias it was written with, and is not quoted.
        assertEquals("column x.nosuchcol does not exist",
                messageOf("CREATE RULE ctl_r10 AS ON INSERT TO ctl_ra DO ALSO"
                        + " INSERT INTO ctl_rb SELECT x.i, 1 FROM ctl_rb x"
                        + " JOIN ctl_rb y ON x.nosuchcol = y.i"));

        // Not one of them was stored, so the relation is still writable.
        assertEquals(0, num("SELECT count(*) FROM pg_rules WHERE tablename = 'ctl_ra'"));
        exec("INSERT INTO ctl_ra VALUES (1, 2)");
        assertEquals(1, num("SELECT count(*) FROM ctl_ra"));
    }

    @Test
    void theRuledRelationsColumnDoesNotReachInsideAnAction() throws Exception {
        exec("CREATE TABLE ctl_ja (i int, j int)");
        exec("CREATE TABLE ctl_jb (i int, k int)");
        String sql = "CREATE RULE ctl_r12 AS ON INSERT TO ctl_ja DO ALSO"
                + " INSERT INTO ctl_jb VALUES (j, 2)";
        org.postgresql.util.ServerErrorMessage m = fieldsOf(sql);
        assertEquals("column \"j\" does not exist", m.getMessage());
        assertEquals("There are columns named \"j\", but they are in tables that cannot be"
                + " referenced from this part of the query.", m.getDetail());
        assertEquals("Try using a table-qualified name.", m.getHint());

        // The rule's own qualification does reach those columns unqualified.
        exec("CREATE RULE ctl_ok1 AS ON INSERT TO ctl_ja WHERE j > 1 DO ALSO"
                + " INSERT INTO ctl_jb VALUES (1, 2)");
        assertEquals(1, num("SELECT count(*) FROM pg_rules WHERE tablename = 'ctl_ja'"));
    }

    @Test
    void aNearMissCarriesTheSuggestionPostgresMakes() throws Exception {
        exec("CREATE TABLE ctl_nc (x int, y int)");
        exec("CREATE TABLE ctl_nb (i int, k int)");
        assertEquals("Perhaps you meant to reference the column \"old.x\" or the column"
                        + " \"new.x\".",
                hintOf("CREATE RULE ctl_h2 AS ON INSERT TO ctl_nc WHERE xx > 1 DO ALSO"
                        + " INSERT INTO ctl_nb VALUES (1, 2)"));
        assertEquals("Perhaps you meant to reference the column \"new.x\".",
                hintOf("CREATE RULE ctl_h3 AS ON INSERT TO ctl_nc DO ALSO"
                        + " INSERT INTO ctl_nb VALUES (new.xx, 2)"));
        assertEquals("column new.xx does not exist",
                messageOf("CREATE RULE ctl_h3 AS ON INSERT TO ctl_nc DO ALSO"
                        + " INSERT INTO ctl_nb VALUES (new.xx, 2)"));
        assertEquals("Perhaps you meant to reference the column \"t.i\".",
                hintOf("CREATE RULE ctl_p4 AS ON INSERT TO ctl_nc DO ALSO"
                        + " INSERT INTO ctl_nb SELECT t.ii, 1 FROM ctl_nb t"));
        assertEquals(0, num("SELECT count(*) FROM pg_rules WHERE tablename = 'ctl_nc'"));
    }

    @Test
    void aRuleWhoseNamesAllResolveIsStillAccepted() throws Exception {
        exec("CREATE TABLE ctl_qa (i int, j int)");
        exec("CREATE TABLE ctl_qb (i int, k int)");
        exec("CREATE RULE ctl_q2 AS ON INSERT TO ctl_qa DO ALSO"
                + " WITH c AS (SELECT i AS z FROM ctl_qb) INSERT INTO ctl_qb SELECT z, 1 FROM c");
        exec("CREATE RULE ctl_q3 AS ON INSERT TO ctl_qa DO ALSO"
                + " INSERT INTO ctl_qb SELECT s.v, 1 FROM (SELECT 7 AS v) s");
        exec("CREATE RULE ctl_q4 AS ON INSERT TO ctl_qa DO ALSO"
                + " INSERT INTO ctl_qb SELECT g, 1 FROM generate_series(1, 3) AS g");
        exec("CREATE RULE ctl_q5 AS ON INSERT TO ctl_qa DO ALSO"
                + " INSERT INTO ctl_qb SELECT i AS zz, 1 FROM ctl_qb ORDER BY zz");
        exec("CREATE RULE ctl_q6 AS ON INSERT TO ctl_qa DO ALSO"
                + " INSERT INTO ctl_qb SELECT t.i, 1 FROM ctl_qb t WHERE t.k > 0");
        exec("CREATE RULE ctl_q7 AS ON INSERT TO ctl_qa WHERE new.i > 1 DO ALSO"
                + " INSERT INTO ctl_qb VALUES (new.i, 2)");
        assertEquals("ctl_q2,ctl_q3,ctl_q4,ctl_q5,ctl_q6,ctl_q7",
                column("SELECT rulename FROM pg_rules WHERE tablename = 'ctl_qa'"
                        + " ORDER BY rulename"));
        exec("INSERT INTO ctl_qa VALUES (5, 6)");
        assertEquals(17, num("SELECT count(*) FROM ctl_qb"));
    }

    @Test
    void aRuleCanBeWrittenOnASchemaQualifiedRelation() throws Exception {
        exec("CREATE SCHEMA ctl_sq");
        exec("CREATE TABLE ctl_sq.t (i int)");
        exec("CREATE TABLE ctl_sq.l (m text)");
        exec("CREATE RULE ctl_rsq AS ON INSERT TO ctl_sq.t DO ALSO"
                + " INSERT INTO ctl_sq.l VALUES ('q')");
        exec("INSERT INTO ctl_sq.t VALUES (1)");
        assertEquals(1, num("SELECT count(*) FROM ctl_sq.l"));
        assertEquals("CREATE RULE ctl_rsq AS\n"
                        + "    ON INSERT TO ctl_sq.t DO  INSERT INTO ctl_sq.l (m)\n"
                        + "  VALUES ('q'::text);",
                scalar("SELECT definition FROM pg_rules WHERE rulename = 'ctl_rsq'"));
        // The action is analysed against the schema it was written with.
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("CREATE RULE ctl_rsq2 AS ON INSERT TO ctl_sq.t DO ALSO"
                        + " INSERT INTO ctl_sq.l SELECT nosuchcol FROM ctl_sq.l"));
        // A qualifier naming no schema is a missing schema, not a missing relation.
        assertEquals("3F000", stateOf("CREATE RULE ctl_rsq3 AS ON INSERT TO"
                + " ctl_nosuchschema.t DO INSTEAD NOTHING"));
        assertEquals("schema \"ctl_nosuchschema\" does not exist",
                messageOf("CREATE RULE ctl_rsq3 AS ON INSERT TO ctl_nosuchschema.t"
                        + " DO INSTEAD NOTHING"));
    }

    @Test
    void droppingAViewDropsItsRule() throws Exception {
        exec("CREATE TABLE ctl_db (i int)");
        exec("CREATE VIEW ctl_dv AS SELECT i FROM ctl_db");
        exec("CREATE RULE ctl_rdv AS ON INSERT TO ctl_dv DO INSTEAD"
                + " INSERT INTO ctl_db VALUES (NEW.i)");
        exec("DROP VIEW ctl_dv");
        assertEquals(0, num("SELECT count(*) FROM pg_rules WHERE rulename = 'ctl_rdv'"));

        exec("CREATE VIEW ctl_dv AS SELECT i FROM ctl_db");
        exec("CREATE RULE ctl_rdv AS ON INSERT TO ctl_dv DO INSTEAD"
                + " INSERT INTO ctl_db VALUES (NEW.i)");
        exec("DROP VIEW ctl_dv CASCADE");
        assertEquals(0, num("SELECT count(*) FROM pg_rules WHERE rulename = 'ctl_rdv'"));
    }

    @Test
    void aRolledBackDropBringsTheRulesBack() throws Exception {
        exec("CREATE TABLE ctl_ut (i int)");
        exec("CREATE TABLE ctl_ul (m text)");
        exec("CREATE RULE ctl_rut AS ON INSERT TO ctl_ut DO ALSO"
                + " INSERT INTO ctl_ul VALUES ('x')");
        exec("BEGIN");
        exec("DROP TABLE ctl_ut");
        exec("ROLLBACK");
        assertEquals(1, num("SELECT count(*) FROM pg_rules WHERE rulename = 'ctl_rut'"));
        assertEquals("t", scalar("SELECT relhasrules FROM pg_class WHERE relname = 'ctl_ut'"));
        // The restored rule fires again.
        exec("INSERT INTO ctl_ut VALUES (1)");
        assertEquals(1, num("SELECT count(*) FROM ctl_ul"));

        exec("CREATE TABLE ctl_uvb (i int)");
        exec("CREATE VIEW ctl_uv AS SELECT i FROM ctl_uvb");
        exec("CREATE RULE ctl_ruv AS ON INSERT TO ctl_uv DO INSTEAD"
                + " INSERT INTO ctl_uvb VALUES (NEW.i)");
        exec("BEGIN");
        exec("DROP VIEW ctl_uv");
        exec("ROLLBACK");
        assertEquals(1, num("SELECT count(*) FROM pg_rules WHERE rulename = 'ctl_ruv'"));
        exec("INSERT INTO ctl_uv VALUES (7)");
        assertEquals(1, num("SELECT count(*) FROM ctl_uvb"));
    }

    @Test
    void dropSchemaCascadeTakesTheRulesOfTheRelationsInIt() throws Exception {
        exec("CREATE SCHEMA ctl_sc");
        exec("CREATE TABLE ctl_sc.ctl_sct (i int)");
        exec("CREATE TABLE ctl_sc.ctl_scl (m text)");
        exec("CREATE RULE ctl_rsc AS ON INSERT TO ctl_sc.ctl_sct DO ALSO"
                + " INSERT INTO ctl_sc.ctl_scl VALUES ('z')");
        exec("DROP SCHEMA ctl_sc CASCADE");
        assertEquals(0, num("SELECT count(*) FROM pg_rules WHERE rulename = 'ctl_rsc'"));
        // A relation created under one of those names afterwards starts with none.
        exec("CREATE TABLE ctl_sct (i int)");
        assertEquals("f", scalar("SELECT relhasrules FROM pg_class WHERE relname = 'ctl_sct'"));
    }

    // ------------------------------------------------------------ what the wire says a transaction did

    @Test
    void rollbackWithNoTransactionInProgressWarns() throws Exception {
        // The warning stands before CommandComplete, and fires again on every repetition.
        String warned = "[N[25P01] there is no transaction in progress, C[ROLLBACK], Z[I]]";
        assertEquals(List.of(warned, warned, warned, warned, warned),
                rawSimpleQueries(List.of("ROLLBACK", "ABORT", "ROLLBACK WORK",
                        "ROLLBACK TRANSACTION", "ROLLBACK")));
        // Through JDBC the same warning arrives as an SQLWarning on the statement.
        try (Statement s = conn.createStatement()) {
            s.execute("ROLLBACK");
            SQLWarning w = s.getWarnings();
            assertNotNull(w, "ROLLBACK outside a transaction block warns");
            assertEquals("25P01", w.getSQLState());
            assertTrue(w.getMessage().contains("there is no transaction in progress"),
                    w.getMessage());
        }
    }

    @Test
    void aRollbackThatClosesABlockSaysNothing() throws Exception {
        assertEquals(List.of("[C[BEGIN], Z[T]]",
                        "[C[ROLLBACK], Z[I]]",
                        "[C[BEGIN], Z[T]]",
                        "[C[COMMIT], Z[I]]",
                        "[N[25P01] there is no transaction in progress, C[COMMIT], Z[I]]"),
                rawSimpleQueries(List.of("BEGIN", "ROLLBACK", "BEGIN", "COMMIT", "COMMIT")));
        // A ROLLBACK that closes an aborted block says nothing either; the next one warns.
        assertEquals(List.of("[C[BEGIN], Z[T]]",
                        "[E[42703] column \"ctl_nope\" does not exist, Z[E]]",
                        "[C[ROLLBACK], Z[I]]",
                        "[N[25P01] there is no transaction in progress, C[ROLLBACK], Z[I]]"),
                rawSimpleQueries(List.of("BEGIN", "SELECT ctl_nope", "ROLLBACK", "ROLLBACK")));
    }

    @Test
    void beginInsideAnOpenBlockWarns() throws Exception {
        String warned = "[N[25001] there is already a transaction in progress, C[BEGIN], Z[T]]";
        assertEquals(List.of("[C[BEGIN], Z[T]]", warned, warned, warned, warned,
                        "[C[ROLLBACK], Z[I]]"),
                rawSimpleQueries(List.of("BEGIN", "BEGIN", "BEGIN WORK", "BEGIN TRANSACTION",
                        "BEGIN ISOLATION LEVEL REPEATABLE READ", "ROLLBACK")));
        // BEGIN inside an aborted block is a different answer entirely.
        assertEquals(List.of("[C[BEGIN], Z[T]]",
                        "[E[42703] column \"ctl_nope\" does not exist, Z[E]]",
                        "[E[25P02] current transaction is aborted, commands ignored until end"
                                + " of transaction block, Z[E]]",
                        "[C[ROLLBACK], Z[I]]"),
                rawSimpleQueries(List.of("BEGIN", "SELECT ctl_nope", "BEGIN", "ROLLBACK")));
    }

    @Test
    void startTransactionAnswersWithItsOwnCommandTag() throws Exception {
        assertEquals(List.of("[C[START TRANSACTION], Z[T]]",
                        "[C[COMMIT], Z[I]]",
                        "[C[START TRANSACTION], Z[T]]",
                        "[C[ROLLBACK], Z[I]]",
                        "[C[START TRANSACTION], Z[T]]",
                        "[N[25001] there is already a transaction in progress,"
                                + " C[START TRANSACTION], Z[T]]",
                        "[C[ROLLBACK], Z[I]]"),
                rawSimpleQueries(List.of("START TRANSACTION", "COMMIT",
                        "START TRANSACTION READ ONLY", "ROLLBACK",
                        "START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "START TRANSACTION",
                        "ROLLBACK")));
        // And in a batch, where the tag of each statement follows the one before it.
        assertEquals(List.of("[C[START TRANSACTION], T, D, C[SELECT 1], C[COMMIT], Z[I]]"),
                rawSimpleQueries(List.of("START TRANSACTION;SELECT 1;COMMIT")));
    }

    @Test
    void transactionControlWarnsOverTheExtendedProtocolToo() throws Exception {
        assertEquals("[1, 2, N[25P01] there is no transaction in progress, C[ROLLBACK], Z[I]]",
                rawExtended("ROLLBACK"));
        assertEquals("[1, 2, N[25P01] there is no transaction in progress, C[ROLLBACK], Z[I]]",
                rawExtended("ABORT"));
        assertEquals("[1, 2, N[25P01] there is no transaction in progress, C[COMMIT], Z[I]]",
                rawExtended("COMMIT"));
        assertEquals("[1, 2, C[BEGIN], Z[T]]", rawExtended("BEGIN"));
        assertEquals("[1, 2, C[START TRANSACTION], Z[T]]", rawExtended("START TRANSACTION"));

        assertEquals("[1, 2, C[ROLLBACK], Z[I]]", rawExtended("ROLLBACK", "BEGIN"));
        assertEquals("[1, 2, C[COMMIT], Z[I]]", rawExtended("COMMIT", "BEGIN"));
        assertEquals("[1, 2, N[25001] there is already a transaction in progress,"
                + " C[BEGIN], Z[T]]", rawExtended("BEGIN", "BEGIN"));
        assertEquals("[1, 2, N[25001] there is already a transaction in progress,"
                + " C[START TRANSACTION], Z[T]]", rawExtended("START TRANSACTION", "BEGIN"));
    }

    @Test
    void parseAnswersTheFaultsItCanSee() throws Exception {
        exec("CREATE TABLE ctl_wt (i int, s text)");
        exec("INSERT INTO ctl_wt VALUES (0, 'a')");
        assertEquals("[E[42601] syntax error at or near \"SELEC\", Z[I]]",
                rawExtended("SELEC 1"));
        assertEquals("[E[42P01] relation \"ctl_nosuchrel\" does not exist, Z[I]]",
                rawExtended("SELECT * FROM ctl_nosuchrel"));
        assertEquals("[E[42703] column \"ctl_nosuchcol\" does not exist, Z[I]]",
                rawExtended("SELECT ctl_nosuchcol"));
        String missing = "[E[42703] column \"nosuchcol\" does not exist, Z[I]]";
        assertEquals(missing, rawExtended("SELECT nosuchcol FROM ctl_wt"));
        assertEquals(missing, rawExtended("SELECT * FROM ctl_wt WHERE nosuchcol = 1"));
        assertEquals(missing, rawExtended("SELECT i FROM ctl_wt ORDER BY nosuchcol"));
        assertEquals(missing, rawExtended("SELECT 1 FROM ctl_wt WHERE nosuchcol"));
        assertEquals(missing, rawExtended("SELECT * FROM (SELECT nosuchcol FROM ctl_wt) x"));
        assertEquals(missing,
                rawExtended("WITH q AS (SELECT nosuchcol FROM ctl_wt) SELECT * FROM q"));
        String noRelation = "[E[42P01] relation \"ctl_nosuchrel\" does not exist, Z[I]]";
        assertEquals(noRelation, rawExtended("INSERT INTO ctl_nosuchrel VALUES (1)"));
        assertEquals(noRelation, rawExtended("UPDATE ctl_nosuchrel SET i = 1"));
        assertEquals(noRelation, rawExtended("DELETE FROM ctl_nosuchrel"));
        assertEquals(noRelation, rawExtended("SELECT * FROM ctl_wt, ctl_nosuchrel"));
        String noColumnOfRelation = "[E[42703] column \"nosuchcol\" of relation \"ctl_wt\""
                + " does not exist, Z[I]]";
        assertEquals(noColumnOfRelation, rawExtended("INSERT INTO ctl_wt (nosuchcol) VALUES (1)"));
        assertEquals(noColumnOfRelation, rawExtended("UPDATE ctl_wt SET nosuchcol = 1"));
        assertEquals("[E[42601] cannot insert multiple commands into a prepared statement, Z[I]]",
                rawExtended("SELECT 1; SELECT 2"));

        // Statements that still complete, for contrast: a fault the server can only see when it
        // runs the statement still arrives after ParseComplete and BindComplete.
        assertEquals("[1, 2, D, C[SELECT 1], Z[I]]", rawExtended("SELECT * FROM ctl_wt"));
        assertEquals("[1, 2, E[22012] division by zero, Z[I]]",
                rawExtended("SELECT 1/i FROM ctl_wt"));
        assertEquals("[1, 2, I, Z[I]]", rawExtended(""));
    }

    @Test
    void nothingIsAnsweredBetweenAParseErrorAndTheClientsSync() throws Exception {
        try (RawWireClient wire = new RawWireClient(memgres.getPort())) {
            wire.startup("memgres", "memgres");
            byte[] flush = RawWireClient.frame('H', new byte[0]);
            wire.write(RawWireClient.parse("SELEC 1"));
            wire.write(flush);
            assertEquals("[E[42601] syntax error at or near \"SELEC\", <waiting>]",
                    wire.readUntilQuiet().toString());
            wire.write(RawWireClient.bind());
            wire.write(flush);
            assertEquals("[<waiting>]", wire.readUntilQuiet().toString());
            wire.write(RawWireClient.execute());
            wire.write(flush);
            assertEquals("[<waiting>]", wire.readUntilQuiet().toString());
            wire.write(RawWireClient.sync());
            assertEquals("[Z[I], <waiting>]", wire.readUntilQuiet().toString());
        }
    }

    @Test
    void bindAnswersTheParameterCountItWasGiven() throws Exception {
        assertEquals("[1, E[08P01] bind message supplies 0 parameters, but prepared statement"
                + " \"\" requires 1, Z[I]]", rawBind("SELECT $1::int", 0));
        assertEquals("[1, E[08P01] bind message supplies 1 parameters, but prepared statement"
                + " \"\" requires 0, Z[I]]", rawBind("SELECT 1", 1));
    }

    @Test
    void aNamedPreparedStatementIsNotSilentlyReplaced() throws Exception {
        try (RawWireClient wire = new RawWireClient(memgres.getPort())) {
            wire.startup("memgres", "memgres");
            wire.write(RawWireClient.frame('P', RawWireClient.concat(
                    RawWireClient.cstring("ctl_s2"), RawWireClient.cstring("SELECT 1"),
                    RawWireClient.int16(0))));
            wire.write(RawWireClient.sync());
            assertEquals("[1, Z[I]]", readToReady(wire).toString());
            wire.write(RawWireClient.frame('P', RawWireClient.concat(
                    RawWireClient.cstring("ctl_s2"), RawWireClient.cstring("SELECT 2"),
                    RawWireClient.int16(0))));
            wire.write(RawWireClient.sync());
            assertEquals("[E[42P05] prepared statement \"ctl_s2\" already exists, Z[I]]",
                    readToReady(wire).toString());
        }
    }

    // ------------------------------------------------------------ a composite type is a type

    @Test
    void aTypeOutsideTheSearchPathIsNotReachableByItsBareName() throws Exception {
        exec("CREATE SCHEMA ctl_hidden");
        exec("CREATE DOMAIN ctl_hidden.ctl_d AS int");
        exec("CREATE TYPE ctl_hidden.ctl_ct AS (x int)");
        exec("CREATE TYPE ctl_hidden.ctl_rg AS RANGE (subtype = int4)");
        assertEquals("42704", stateOf("SELECT 1::ctl_d"));
        assertEquals("type \"ctl_d\" does not exist", messageOf("SELECT 1::ctl_d"));
        assertEquals("type \"ctl_ct\" does not exist", messageOf("SELECT row(1)::ctl_ct"));
        assertEquals("type \"ctl_rg\" does not exist", messageOf("SELECT '[1,3)'::ctl_rg"));
        assertEquals("type \"ctl_d\" does not exist",
                messageOf("ALTER DOMAIN ctl_d SET NOT NULL"));
        // The qualified name still reaches it.
        assertEquals("1", scalar("SELECT 1::ctl_hidden.ctl_d"));
        // Putting the schema on the search path makes the bare name reach it, and RESET takes
        // it away again.
        exec("SET search_path TO ctl_hidden, public");
        try {
            assertEquals("1", scalar("SELECT 1::ctl_d"));
        } finally {
            exec("RESET search_path");
        }
        assertEquals("42704", stateOf("SELECT 1::ctl_d"));
    }

    @Test
    void rollbackUndoesEveryAttributeChangeATransactionMade() throws Exception {
        exec("CREATE TYPE ctl_undo AS (x int, y int)");
        exec("BEGIN");
        exec("ALTER TYPE ctl_undo ADD ATTRIBUTE z text");
        exec("ALTER TYPE ctl_undo RENAME ATTRIBUTE x TO xx");
        exec("ALTER TYPE ctl_undo ALTER ATTRIBUTE y TYPE varchar(4)");
        exec("ALTER TYPE ctl_undo DROP ATTRIBUTE y");
        assertEquals("xx/1/false/integer ........pg.dropped.2......../2/true/- z/3/false/text",
                attributesOf("ctl_undo"));
        exec("ROLLBACK");
        assertEquals("x/1/false/integer y/2/false/integer", attributesOf("ctl_undo"));
    }

    @Test
    void rollbackToASavepointUndoesOnlyWhatCameAfterIt() throws Exception {
        exec("CREATE TYPE ctl_sp1 AS (x int, y int)");
        exec("BEGIN");
        exec("ALTER TYPE ctl_sp1 ADD ATTRIBUTE z text");
        exec("SAVEPOINT ctl_sp");
        exec("ALTER TYPE ctl_sp1 ADD ATTRIBUTE w text");
        exec("ROLLBACK TO ctl_sp");
        assertEquals("x/1 y/2 z/3", scalar("SELECT string_agg(a.attname || '/' || a.attnum,"
                + " ' ' ORDER BY a.attnum) FROM pg_attribute a JOIN pg_type t"
                + " ON t.typrelid = a.attrelid WHERE t.typname = 'ctl_sp1' AND a.attnum > 0"));
        exec("COMMIT");
        assertEquals("x/1 y/2 z/3", scalar("SELECT string_agg(a.attname || '/' || a.attnum,"
                + " ' ' ORDER BY a.attnum) FROM pg_attribute a JOIN pg_type t"
                + " ON t.typrelid = a.attrelid WHERE t.typname = 'ctl_sp1' AND a.attnum > 0"));
    }

    @Test
    void aCompositeTypedColumnPointsAtTheComposite() throws Exception {
        exec("CREATE TYPE ctl_pair AS (v varchar(3), n int)");
        exec("CREATE TABLE ctl_holder (c ctl_pair)");
        assertEquals("ctl_pair/-1/x/d/true",
                scalar("SELECT format_type(a.atttypid, a.atttypmod) || '/' || a.attlen"
                        + " || '/' || a.attstorage::text || '/' || a.attalign::text || '/'"
                        + " || (a.atttypid = 'ctl_pair'::regtype) FROM pg_attribute a"
                        + " WHERE a.attrelid = 'ctl_holder'::regclass AND a.attnum > 0"));
        assertEquals("USER-DEFINED/ctl_pair",
                scalar("SELECT data_type || '/' || udt_name FROM information_schema.columns"
                        + " WHERE table_name = 'ctl_holder'"));
        exec("CREATE TABLE ctl_arr (c ctl_pair[])");
        assertEquals("ctl_pair[]",
                scalar("SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a"
                        + " WHERE a.attrelid = 'ctl_arr'::regclass AND a.attnum > 0"));
    }

    @Test
    void aWriteIntoACompositeColumnIsHeldToEachFieldsType() throws Exception {
        exec("CREATE TYPE ctl_wpair AS (v varchar(3), n int)");
        exec("CREATE TABLE ctl_whold (c ctl_wpair)");
        assertEquals("22001", stateOf("INSERT INTO ctl_whold VALUES (row('abcdef', 3))"));
        assertEquals("value too long for type character varying(3)",
                messageOf("INSERT INTO ctl_whold VALUES (row('abcdef', 3))"));
        assertEquals("Input has too many columns.",
                detailOf("INSERT INTO ctl_whold VALUES (row(1,2,3))"));
        assertEquals("cannot cast type record to ctl_wpair",
                messageOf("INSERT INTO ctl_whold VALUES (row(1,2,3))"));
        assertEquals("42846", stateOf("INSERT INTO ctl_whold VALUES (row(1))"));
        assertEquals("Input has too few columns.",
                detailOf("INSERT INTO ctl_whold VALUES (row(1))"));
        // Nothing above was written, and the refused UPDATE leaves the stored row alone.
        exec("INSERT INTO ctl_whold VALUES (row('ab', 3))");
        assertEquals("22001", stateOf("UPDATE ctl_whold SET c = row('abcdef', 3)"));
        assertEquals("(ab,3)", scalar("SELECT c::text FROM ctl_whold"));

        // Each field is padded and rounded as its own type declares.
        exec("CREATE TYPE ctl_pad AS (a char(3), b numeric(4,1))");
        exec("CREATE TABLE ctl_padt (c ctl_pad)");
        exec("INSERT INTO ctl_padt VALUES (row('a', 1.26))");
        assertEquals("(\"a  \",1.3)", scalar("SELECT c::text FROM ctl_padt"));
        assertEquals("1.3", scalar("SELECT (c).b FROM ctl_padt"));
        assertEquals("A field with precision 4, scale 1 must round to an absolute value less"
                        + " than 10^3.",
                detailOf("INSERT INTO ctl_padt VALUES (row('a', 12345.6))"));
        assertEquals("22003", stateOf("INSERT INTO ctl_padt VALUES (row('a', 12345.6))"));
        assertEquals("value too long for type character(3)",
                messageOf("INSERT INTO ctl_padt VALUES (row('abcd', 1))"));
        assertEquals(1, num("SELECT count(*) FROM ctl_padt"));
    }

    @Test
    void aCompositeAttributeKeepsTheIntervalQualifierItWasDeclaredWith() throws Exception {
        exec("CREATE TYPE ctl_iv AS (a interval hour to minute, b interval day, c interval(3),"
                + " d interval second(2), e interval, f interval day to second(4), g interval[])");
        assertEquals("a=interval hour to minute/201392127 b=interval day/589823"
                        + " c=interval(3)/2147418115 d=interval second(2)/268435458"
                        + " e=interval/-1 f=interval day to second(4)/470286340 g=interval[]/-1",
                scalar("SELECT string_agg(a.attname || '='"
                        + " || format_type(a.atttypid, a.atttypmod) || '/' || a.atttypmod,"
                        + " ' ' ORDER BY a.attnum) FROM pg_attribute a JOIN pg_type t"
                        + " ON t.typrelid = a.attrelid WHERE t.typname = 'ctl_iv'"
                        + " AND a.attnum > 0"));
    }

    @Test
    void aCompositeAttributeReportsTheLayoutOfItsDeclaredType() throws Exception {
        exec("CREATE TYPE ctl_lay AS (a int, b varchar(5), c text, d numeric(10,2), e bool,"
                + " f timestamp(3), g uuid, h bytea, i char(3), j float8, k smallint, l date,"
                + " m point, n int[], o varchar(5)[], p bigint, q jsonb, s name, t oid)");
        assertEquals("a=4/p/i/true b=-1/x/i/false c=-1/x/i/false d=-1/m/i/false e=1/p/c/true"
                        + " f=8/p/d/true g=16/p/c/false h=-1/x/i/false i=-1/x/i/false"
                        + " j=8/p/d/true k=2/p/s/true l=4/p/i/true m=16/p/d/false"
                        + " n=-1/x/i/false o=-1/x/i/false p=8/p/d/true q=-1/x/i/false"
                        + " s=64/p/c/false t=4/p/i/true",
                scalar("SELECT string_agg(a.attname || '=' || a.attlen || '/'"
                        + " || a.attstorage::text || '/' || a.attalign::text || '/' || a.attbyval,"
                        + " ' ' ORDER BY a.attnum) FROM pg_attribute a JOIN pg_type t"
                        + " ON t.typrelid = a.attrelid WHERE t.typname = 'ctl_lay'"
                        + " AND a.attnum > 0"));

        // A user-declared type's own layout reaches the attribute, arrays included.
        exec("CREATE TYPE ctl_e AS ENUM ('a','b')");
        exec("CREATE DOMAIN ctl_dom2 AS varchar(4)");
        exec("CREATE TYPE ctl_in AS (q int)");
        exec("CREATE TYPE ctl_u AS (a ctl_e, b ctl_dom2, c ctl_in, d ctl_e[], e ctl_in[])");
        assertEquals("a=ctl_e/4/p/i/true b=ctl_dom2/-1/x/i/false c=ctl_in/-1/x/d/false"
                        + " d=ctl_e[]/-1/x/i/false e=ctl_in[]/-1/x/d/false",
                scalar("SELECT string_agg(a.attname || '='"
                        + " || format_type(a.atttypid, a.atttypmod) || '/' || a.attlen || '/'"
                        + " || a.attstorage::text || '/' || a.attalign::text || '/' || a.attbyval,"
                        + " ' ' ORDER BY a.attnum) FROM pg_attribute a JOIN pg_type t"
                        + " ON t.typrelid = a.attrelid WHERE t.typname = 'ctl_u'"
                        + " AND a.attnum > 0"));
    }

    @Test
    void aDroppedAttributeKeepsItsNumberAndTheLayoutItWasDeclaredWith() throws Exception {
        exec("CREATE TYPE ctl_dr AS (a int, b timestamp(3), c point, d int[])");
        exec("ALTER TYPE ctl_dr DROP ATTRIBUTE b");
        exec("ALTER TYPE ctl_dr DROP ATTRIBUTE c");
        exec("ALTER TYPE ctl_dr DROP ATTRIBUTE d");
        assertEquals("a=1/false/23/-1/4/p/i/true"
                        + " ........pg.dropped.2........=2/true/0/3/8/p/d/true"
                        + " ........pg.dropped.3........=3/true/0/-1/16/p/d/false"
                        + " ........pg.dropped.4........=4/true/0/-1/-1/x/i/false",
                scalar("SELECT string_agg(a.attname || '=' || a.attnum || '/' || a.attisdropped"
                        + " || '/' || a.atttypid || '/' || a.atttypmod || '/' || a.attlen || '/'"
                        + " || a.attstorage::text || '/' || a.attalign::text || '/' || a.attbyval,"
                        + " ' ' ORDER BY a.attnum) FROM pg_attribute a JOIN pg_type t"
                        + " ON t.typrelid = a.attrelid WHERE t.typname = 'ctl_dr'"
                        + " AND a.attnum > 0"));
    }

    @Test
    void alterAttributeTypeIsRefusedWhileARelationUsesTheType() throws Exception {
        exec("CREATE TYPE ctl_used AS (x int)");
        exec("CREATE TABLE ctl_uses (c ctl_used)");
        assertEquals("0A000", stateOf("ALTER TYPE ctl_used ALTER ATTRIBUTE x TYPE text"));
        assertEquals("cannot alter type \"ctl_used\" because column \"ctl_uses.c\" uses it",
                messageOf("ALTER TYPE ctl_used ALTER ATTRIBUTE x TYPE text"));
        assertNull(detailOf("ALTER TYPE ctl_used ALTER ATTRIBUTE x TYPE text"));
        assertNull(hintOf("ALTER TYPE ctl_used ALTER ATTRIBUTE x TYPE text"));
        // CASCADE does not excuse a plain column.
        assertEquals("cannot alter type \"ctl_used\" because column \"ctl_uses.c\" uses it",
                messageOf("ALTER TYPE ctl_used ALTER ATTRIBUTE x TYPE text CASCADE"));
        // The other three attribute actions are allowed while a column uses the type.
        exec("ALTER TYPE ctl_used ADD ATTRIBUTE y text");
        exec("ALTER TYPE ctl_used RENAME ATTRIBUTE y TO z");
        exec("ALTER TYPE ctl_used DROP ATTRIBUTE z");
        exec("DROP TABLE ctl_uses");

        // The column reaches the type through an array, a composite or a domain.
        exec("CREATE TABLE ctl_v1 (c ctl_used[])");
        assertEquals("cannot alter type \"ctl_used\" because column \"ctl_v1.c\" uses it",
                messageOf("ALTER TYPE ctl_used ALTER ATTRIBUTE x TYPE text"));
        exec("DROP TABLE ctl_v1");
        exec("CREATE TYPE ctl_v2t AS (q ctl_used)");
        exec("CREATE TABLE ctl_v2 (c ctl_v2t)");
        assertEquals("cannot alter type \"ctl_used\" because column \"ctl_v2.c\" uses it",
                messageOf("ALTER TYPE ctl_used ALTER ATTRIBUTE x TYPE text"));
        exec("DROP TABLE ctl_v2");
        exec("DROP TYPE ctl_v2t");
        exec("CREATE DOMAIN ctl_v3d AS ctl_used");
        exec("CREATE TABLE ctl_v3 (c ctl_v3d)");
        assertEquals("cannot alter type \"ctl_used\" because column \"ctl_v3.c\" uses it",
                messageOf("ALTER TYPE ctl_used ALTER ATTRIBUTE x TYPE text"));
        exec("DROP TABLE ctl_v3");
        exec("DROP DOMAIN ctl_v3d");
    }

    @Test
    void nothingElseIsRefusedByTheScanThatGuardsAnAttributeType() throws Exception {
        // A view over the type is not a relation that holds values of it.
        exec("CREATE TYPE ctl_vt AS (x int)");
        exec("CREATE VIEW ctl_vw AS SELECT c FROM (SELECT NULL::ctl_vt AS c) s");
        exec("ALTER TYPE ctl_vt ALTER ATTRIBUTE x TYPE text");
        assertEquals("x=text", scalar("SELECT string_agg(a.attname || '='"
                + " || format_type(a.atttypid, a.atttypmod), ' ' ORDER BY a.attnum)"
                + " FROM pg_attribute a JOIN pg_type t ON t.typrelid = a.attrelid"
                + " WHERE t.typname = 'ctl_vt' AND a.attnum > 0"));

        // Nor is a table that has nothing to do with the type.
        exec("CREATE TYPE ctl_free AS (x int)");
        exec("CREATE TABLE ctl_other (a int, b text)");
        exec("ALTER TYPE ctl_free ALTER ATTRIBUTE x TYPE text");
        assertEquals("x=text", scalar("SELECT string_agg(a.attname || '='"
                + " || format_type(a.atttypid, a.atttypmod), ' ' ORDER BY a.attnum)"
                + " FROM pg_attribute a JOIN pg_type t ON t.typrelid = a.attrelid"
                + " WHERE t.typname = 'ctl_free' AND a.attnum > 0"));
    }

    @Test
    void aTypedTableIsRefusedWithItsOwnSqlstateAndHint() throws Exception {
        exec("CREATE TYPE ctl_typ AS (x int)");
        exec("CREATE TABLE ctl_typed OF ctl_typ");
        assertEquals("2BP01", stateOf("ALTER TYPE ctl_typ ALTER ATTRIBUTE x TYPE text"));
        assertEquals("cannot alter type \"ctl_typ\" because it is the type of a typed table",
                messageOf("ALTER TYPE ctl_typ ALTER ATTRIBUTE x TYPE text"));
        assertEquals("Use ALTER ... CASCADE to alter the typed tables too.",
                hintOf("ALTER TYPE ctl_typ ALTER ATTRIBUTE x TYPE text"));
        // The refusal changed nothing.
        assertEquals("x=integer", scalar("SELECT string_agg(a.attname || '='"
                + " || format_type(a.atttypid, a.atttypmod), ' ' ORDER BY a.attnum)"
                + " FROM pg_attribute a WHERE a.attrelid = 'ctl_typed'::regclass"
                + " AND a.attnum > 0"));
    }

    // ------------------------------------------------------------ a definition names what a reader will see

    /**
     * A recursive query reads itself, and that reference cannot be written under the name the
     * query around it already carries, so PostgreSQL gives it one of its own. The first arm names
     * no column, and a definition a reader reads writes the placeholder out rather than leave the
     * column nameless.
     */
    @Test
    void aRecursiveQuerysReferenceToItselfIsNamedApart() throws Exception {
        exec("CREATE VIEW cty_cte AS WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL"
                + " SELECT n + 1 FROM t WHERE n < 5) SELECT n FROM t");
        assertEquals(" WITH RECURSIVE t(n) AS (\n"
                        + "         SELECT 1 AS \"?column?\"\n"
                        + "        UNION ALL\n"
                        + "         SELECT t_1.n + 1\n"
                        + "           FROM t t_1\n"
                        + "          WHERE t_1.n < 5\n"
                        + "        )\n"
                        + " SELECT n\n"
                        + "   FROM t;",
                scalar("SELECT pg_get_viewdef('cty_cte'::regclass, true)"));
        assertEquals(" WITH RECURSIVE t(n) AS (\n"
                        + "         SELECT 1 AS \"?column?\"\n"
                        + "        UNION ALL\n"
                        + "         SELECT (t_1.n + 1)\n"
                        + "           FROM t t_1\n"
                        + "          WHERE (t_1.n < 5)\n"
                        + "        )\n"
                        + " SELECT n\n"
                        + "   FROM t;",
                scalar("SELECT pg_get_viewdef('cty_cte'::regclass, false)"));

        exec("CREATE VIEW cty_cte2 AS WITH RECURSIVE t(n, s) AS (SELECT 1, 'a' UNION ALL"
                + " SELECT t.n + 1, t.s || 'b' FROM t WHERE t.n < 5) SELECT n, s FROM t");
        assertEquals(" WITH RECURSIVE t(n, s) AS (\n"
                        + "         SELECT 1 AS \"?column?\",\n"
                        + "            'a'::text AS \"?column?\"\n"
                        + "        UNION ALL\n"
                        + "         SELECT t_1.n + 1,\n"
                        + "            t_1.s || 'b'::text\n"
                        + "           FROM t t_1\n"
                        + "          WHERE t_1.n < 5\n"
                        + "        )\n"
                        + " SELECT n,\n"
                        + "    s\n"
                        + "   FROM t;",
                scalar("SELECT pg_get_viewdef('cty_cte2'::regclass, true)"));

        // A reference the query already named for itself keeps the name it was written with.
        exec("CREATE VIEW cty_cte3 AS WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL"
                + " SELECT tt.n + 1 FROM t tt WHERE tt.n < 5) SELECT n FROM t");
        assertEquals(" WITH RECURSIVE t(n) AS (\n"
                        + "         SELECT 1 AS \"?column?\"\n"
                        + "        UNION ALL\n"
                        + "         SELECT tt.n + 1\n"
                        + "           FROM t tt\n"
                        + "          WHERE tt.n < 5\n"
                        + "        )\n"
                        + " SELECT n\n"
                        + "   FROM t;",
                scalar("SELECT pg_get_viewdef('cty_cte3'::regclass, true)"));

        // A second WITH item reading the first is not the recursive reference, so it is not
        // renamed: the counter only moves where a name is already taken.
        exec("CREATE VIEW cty_cte4 AS WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL"
                + " SELECT n + 1 FROM t WHERE n < 5), u AS (SELECT n FROM t) SELECT n FROM u");
        assertEquals(" WITH RECURSIVE t(n) AS (\n"
                        + "         SELECT 1 AS \"?column?\"\n"
                        + "        UNION ALL\n"
                        + "         SELECT t.n + 1\n"
                        + "           FROM t\n"
                        + "          WHERE t.n < 5\n"
                        + "        ), u AS (\n"
                        + "         SELECT t.n\n"
                        + "           FROM t\n"
                        + "        )\n"
                        + " SELECT n\n"
                        + "   FROM u;",
                scalar("SELECT pg_get_viewdef('cty_cte4'::regclass, true)"));
    }

    @Test
    void aSubSelectReadingItsCallersRelationIsNamedApart() throws Exception {
        exec("CREATE TABLE cty_r1 (id int, nme text)");
        exec("CREATE VIEW cty_sub AS SELECT id FROM cty_r1 WHERE id IN (SELECT id FROM cty_r1)");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_r1\n"
                        + "  WHERE (id IN ( SELECT cty_r1_1.id\n"
                        + "           FROM cty_r1 cty_r1_1));",
                scalar("SELECT pg_get_viewdef('cty_sub'::regclass, true)"));

        // Two levels down the counter has moved twice.
        exec("CREATE VIEW cty_sub2 AS SELECT id FROM cty_r1 WHERE id IN"
                + " (SELECT id FROM cty_r1 WHERE id IN (SELECT id FROM cty_r1))");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_r1\n"
                        + "  WHERE (id IN ( SELECT cty_r1_1.id\n"
                        + "           FROM cty_r1 cty_r1_1\n"
                        + "          WHERE (cty_r1_1.id IN ( SELECT cty_r1_2.id\n"
                        + "                   FROM cty_r1 cty_r1_2))));",
                scalar("SELECT pg_get_viewdef('cty_sub2'::regclass, true)"));

        // A name an enclosing FROM item already took is taken whether or not it names the same
        // relation: the sub-select's own entry is renamed because the alias beside it is not.
        exec("CREATE VIEW cty_sub3 AS SELECT a.id FROM cty_r1 a, (SELECT id FROM cty_r1) cty_r1");
        assertEquals(" SELECT a.id\n"
                        + "   FROM cty_r1 a,\n"
                        + "    ( SELECT cty_r1_1.id\n"
                        + "           FROM cty_r1 cty_r1_1) cty_r1;",
                scalar("SELECT pg_get_viewdef('cty_sub3'::regclass, true)"));
    }

    @Test
    void aColumnWithNoNameOfItsOwnIsPublishedAsThePlaceholder() throws Exception {
        exec("CREATE TABLE cty_l1 (id int, nme text)");
        exec("CREATE VIEW cty_lbl AS SELECT 1 AS one, id + 1 FROM cty_l1");
        assertEquals(" SELECT 1 AS one,\n"
                        + "    id + 1 AS \"?column?\"\n"
                        + "   FROM cty_l1;",
                scalar("SELECT pg_get_viewdef('cty_lbl'::regclass, true)"));

        // A set operation takes its names from its first arm, so the later arm is written against
        // the name already settled rather than against one of its own.
        exec("CREATE VIEW cty_un AS SELECT 1 AS a UNION ALL SELECT 2");
        assertEquals(" SELECT 1 AS a\n"
                        + "UNION ALL\n"
                        + " SELECT 2 AS a;",
                scalar("SELECT pg_get_viewdef('cty_un'::regclass, true)"));

        // The placeholder written out as a label of its own is a name like any other.
        exec("CREATE VIEW cty_quo AS SELECT id AS \"Mixed\", nme AS \"?column?\" FROM cty_l1");
        assertEquals(" SELECT id AS \"Mixed\",\n"
                        + "    nme AS \"?column?\"\n"
                        + "   FROM cty_l1;",
                scalar("SELECT pg_get_viewdef('cty_quo'::regclass, true)"));
    }

    // ------------------------------------------------------------ a stored constant is the value its type read

    @Test
    void aStoredConstantIsPrintedAsTheValueItsTypeRead() throws Exception {
        exec("CREATE TABLE cty_v1 (id int, ts timestamp, dt date, iv interval, b boolean,"
                + " arr int[], tarr text[], u uuid, jb jsonb, ba bytea, ip inet, mc macaddr,"
                + " bt bit(8), vb bit varying(8))");
        exec("CREATE VIEW cty_ts AS SELECT id FROM cty_v1"
                + " WHERE ts > '2020-01-01' AND ts < '2020-1-2 3:4:5.6'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_v1\n"
                        + "  WHERE ts > '2020-01-01 00:00:00'::timestamp without time zone"
                        + " AND ts < '2020-01-02 03:04:05.6'::timestamp without time zone;",
                scalar("SELECT pg_get_viewdef('cty_ts'::regclass, true)"));

        exec("CREATE VIEW cty_dt AS SELECT id FROM cty_v1"
                + " WHERE dt > '2020-1-2' AND iv > '1 day 2 hours 3 min'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_v1\n"
                        + "  WHERE dt > '2020-01-02'::date AND iv > '1 day 02:03:00'::interval;",
                scalar("SELECT pg_get_viewdef('cty_dt'::regclass, true)"));

        // Both spellings of true are the value they stand for, so the two read back the same.
        exec("CREATE VIEW cty_bool AS SELECT id FROM cty_v1 WHERE b = 't' OR b = 'yes'");
        assertEquals(" SELECT id\n   FROM cty_v1\n  WHERE b = true OR b = true;",
                scalar("SELECT pg_get_viewdef('cty_bool'::regclass, true)"));

        exec("CREATE VIEW cty_val AS SELECT id FROM cty_v1"
                + " WHERE u = '0A0B0C0D-0E0F-1011-1213-141516171819'"
                + " AND jb = '{\"b\":1, \"a\":2}' AND ba = 'abc'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_v1\n"
                        + "  WHERE u = '0a0b0c0d-0e0f-1011-1213-141516171819'::uuid"
                        + " AND jb = '{\"a\": 2, \"b\": 1}'::jsonb AND ba = '\\x616263'::bytea;",
                scalar("SELECT pg_get_viewdef('cty_val'::regclass, true)"));

        exec("CREATE VIEW cty_net AS SELECT id FROM cty_v1 WHERE ip = '010.0.0.1/8'"
                + " AND mc = '08002B010203' AND bt = '10101010' AND vb = '1010'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_v1\n"
                        + "  WHERE ip = '10.0.0.1/8'::inet AND mc = '08:00:2b:01:02:03'::macaddr"
                        + " AND bt = '10101010'::\"bit\" AND vb = '1010'::bit varying;",
                scalar("SELECT pg_get_viewdef('cty_net'::regclass, true)"));

        // A hexadecimal bit constant is stored as the bits it stands for.
        exec("CREATE VIEW cty_bit AS SELECT B'1010' AS a, X'0A' AS b FROM cty_v1");
        assertEquals(" SELECT '1010'::\"bit\" AS a,\n"
                        + "    '00001010'::\"bit\" AS b\n"
                        + "   FROM cty_v1;",
                scalar("SELECT pg_get_viewdef('cty_bit'::regclass, true)"));
    }

    /**
     * An integer and a numeric that reads as a fraction are written bare, because reading the text
     * again settles on the same type; everything else is quoted and labelled, a negative number
     * included, which without the quotes would read as a minus sign applied to a constant.
     */
    @Test
    void aNumericConstantCarriesTheLabelItsTextWouldNotGive() throws Exception {
        exec("CREATE TABLE cty_n1 (id int, sm smallint, bg bigint, nm numeric,"
                + " f4 real, f8 double precision)");
        exec("CREATE VIEW cty_num AS SELECT id FROM cty_n1 WHERE nm > '01.50' AND id > '007'"
                + " AND sm > '3' AND bg > '9999999999'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_n1\n"
                        + "  WHERE nm > 1.50 AND id > 7 AND sm > '3'::smallint"
                        + " AND bg > '9999999999'::bigint;",
                scalar("SELECT pg_get_viewdef('cty_num'::regclass, true)"));

        exec("CREATE VIEW cty_num2 AS SELECT id FROM cty_n1 WHERE f8 > '1.5' AND f4 > '1.5'"
                + " AND nm > '5' AND nm > 1e3");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_n1\n"
                        + "  WHERE f8 > '1.5'::double precision AND f4 > '1.5'::real"
                        + " AND nm > '5'::numeric AND nm > '1000'::numeric;",
                scalar("SELECT pg_get_viewdef('cty_num2'::regclass, true)"));

        exec("CREATE VIEW cty_neg AS SELECT id FROM cty_n1 WHERE id > -1 AND nm > -1.5");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_n1\n"
                        + "  WHERE id > '-1'::integer AND nm > '-1.5'::numeric;",
                scalar("SELECT pg_get_viewdef('cty_neg'::regclass, true)"));
        assertEquals(" SELECT id\n"
                        + "   FROM cty_n1\n"
                        + "  WHERE ((id > '-1'::integer) AND (nm > '-1.5'::numeric));",
                scalar("SELECT pg_get_viewdef('cty_neg'::regclass, false)"));

        exec("CREATE VIEW cty_cst AS SELECT '2020-1-2'::date AS a, '1.50'::numeric AS b,"
                + " '007'::int AS c, '1.50'::numeric(10,2) AS d, (-1)::int AS e,"
                + " 1.5::numeric(10,2) AS f FROM cty_n1");
        assertEquals(" SELECT '2020-01-02'::date AS a,\n"
                        + "    1.50 AS b,\n"
                        + "    7 AS c,\n"
                        + "    1.50::numeric(10,2) AS d,\n"
                        + "    '-1'::integer AS e,\n"
                        + "    1.5::numeric(10,2) AS f\n"
                        + "   FROM cty_n1;",
                scalar("SELECT pg_get_viewdef('cty_cst'::regclass, true)"));
    }

    /**
     * Parse analysis drops a coercion with nothing to do, so the cast is not in the stored query
     * and cannot be printed. A cast that applies a width, or one to another type, is work and
     * stays where it was written.
     */
    @Test
    void aCastToTheTypeAnExpressionAlreadyHasIsNotThere() throws Exception {
        exec("CREATE TABLE cty_f1 (id int, sm smallint, bg bigint, nm numeric, nm2 numeric(10,2),"
                + " f4 real, nme text, vc varchar(10), ch char(5))");
        exec("CREATE VIEW cty_fold AS SELECT upper(nme)::text AS a, nme::text AS b, id::int AS c,"
                + " (id + 1)::int AS d, length(nme)::int AS e, lower(nme)::varchar AS f"
                + " FROM cty_f1");
        assertEquals(" SELECT upper(nme) AS a,\n"
                        + "    nme AS b,\n"
                        + "    id AS c,\n"
                        + "    id + 1 AS d,\n"
                        + "    length(nme) AS e,\n"
                        + "    lower(nme)::character varying AS f\n"
                        + "   FROM cty_f1;",
                scalar("SELECT pg_get_viewdef('cty_fold'::regclass, true)"));
        assertEquals(" SELECT upper(nme) AS a,\n"
                        + "    nme AS b,\n"
                        + "    id AS c,\n"
                        + "    (id + 1) AS d,\n"
                        + "    length(nme) AS e,\n"
                        + "    (lower(nme))::character varying AS f\n"
                        + "   FROM cty_f1;",
                scalar("SELECT pg_get_viewdef('cty_fold'::regclass, false)"));

        // A cast that drops a width is work; one to the width the column already has is not.
        exec("CREATE VIEW cty_fold2 AS SELECT nm::numeric AS a, nm2::numeric AS b,"
                + " ch::bpchar AS c, id::numeric AS d, sm::smallint AS e, bg::bigint AS f"
                + " FROM cty_f1");
        assertEquals(" SELECT nm AS a,\n"
                        + "    nm2::numeric AS b,\n"
                        + "    ch::bpchar AS c,\n"
                        + "    id::numeric AS d,\n"
                        + "    sm AS e,\n"
                        + "    bg AS f\n"
                        + "   FROM cty_f1;",
                scalar("SELECT pg_get_viewdef('cty_fold2'::regclass, true)"));

        exec("CREATE VIEW cty_fold3 AS SELECT count(*)::bigint AS a, sum(id)::numeric AS b,"
                + " max(nme)::text AS c FROM cty_f1");
        assertEquals(" SELECT count(*) AS a,\n"
                        + "    sum(id)::numeric AS b,\n"
                        + "    max(nme) AS c\n"
                        + "   FROM cty_f1;",
                scalar("SELECT pg_get_viewdef('cty_fold3'::regclass, true)"));

        exec("CREATE VIEW cty_fold4 AS SELECT (id * 2)::int AS a, (id / 2)::int AS b,"
                + " (nm + 1)::numeric AS c FROM cty_f1");
        assertEquals(" SELECT id * 2 AS a,\n"
                        + "    id / 2 AS b,\n"
                        + "    nm + 1::numeric AS c\n"
                        + "   FROM cty_f1;",
                scalar("SELECT pg_get_viewdef('cty_fold4'::regclass, true)"));

        // The ELSE a CASE never had is written with the type its arms settled on.
        exec("CREATE VIEW cty_fcase AS SELECT CASE WHEN id = 1 THEN f4 END AS a,"
                + " CASE WHEN id = 1 THEN vc END AS b, CASE WHEN id = 1 THEN ch END AS c"
                + " FROM cty_f1");
        assertEquals(" SELECT\n"
                        + "        CASE\n"
                        + "            WHEN id = 1 THEN f4\n"
                        + "            ELSE NULL::real\n"
                        + "        END AS a,\n"
                        + "        CASE\n"
                        + "            WHEN id = 1 THEN vc\n"
                        + "            ELSE NULL::character varying\n"
                        + "        END AS b,\n"
                        + "        CASE\n"
                        + "            WHEN id = 1 THEN ch\n"
                        + "            ELSE NULL::bpchar\n"
                        + "        END AS c\n"
                        + "   FROM cty_f1;",
                scalar("SELECT pg_get_viewdef('cty_fcase'::regclass, true)"));
    }

    /**
     * The clock part of a timestamp and a time of day are the same text read by the same reader:
     * one digit is enough in any field, the seconds may be left off, a sixtieth second is the
     * next minute and the end of the day is written 24:00:00.
     */
    @Test
    void aTimeIsReadTheWayTheClockPartOfATimestampIs() throws Exception {
        assertEquals("03:04:00", scalar("SELECT ('3:4'::time)::text"));
        assertEquals("03:04:05", scalar("SELECT ('3:4:5'::time)::text"));
        assertEquals("03:04:05.5", scalar("SELECT ('3:4:5.500'::time)::text"));
        assertEquals("24:00:00", scalar("SELECT ('23:59:60'::time)::text"));
        assertEquals("03:05:00", scalar("SELECT ('3:4:60'::time)::text"));
        assertEquals("03:04:05.123457", scalar("SELECT ('3:4:5.123456789'::time)::text"));
        assertEquals("03:04:00+02", scalar("SELECT ('3:4+02'::timetz)::text"));
        assertEquals("true", scalar("SELECT ('3:4'::time = '03:04:00'::time)::text"));
        assertEquals("03:04:00", scalar("SELECT ('03:04'::time)::text"));
        assertEquals("24:00:00", scalar("SELECT ('24:00'::time)::text"));

        // A field the reader takes but the calendar does not is out of range, whatever width it
        // was written at; a fourth field is not a time at all.
        assertEquals("22008", stateOf("SELECT '25:00'::time"));
        assertEquals("22008", stateOf("SELECT '3:60'::time"));
        assertEquals("22008", stateOf("SELECT '3:4:61'::time"));
        assertEquals("date/time field value out of range: \"3:4:61\"",
                messageOf("SELECT '3:4:61'::time"));
        assertEquals("22007", stateOf("SELECT '3:4:5:6'::time"));
        assertEquals("invalid input syntax for type time: \"3:4:5:6\"",
                messageOf("SELECT '3:4:5:6'::time"));
    }

    @Test
    void aTimeConstantInADefinitionPrintsTheValueItRead() throws Exception {
        exec("CREATE TABLE cty_tm (id int, tm time, tz timetz)");
        exec("CREATE VIEW cty_w1 AS SELECT id FROM cty_tm WHERE tm > '3:4'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_tm\n"
                        + "  WHERE tm > '03:04:00'::time without time zone;",
                scalar("SELECT pg_get_viewdef('cty_w1'::regclass, true)"));

        exec("CREATE VIEW cty_w2 AS SELECT id FROM cty_tm WHERE tm > '3:4:5' AND tm < '3:4:5.500'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_tm\n"
                        + "  WHERE tm > '03:04:05'::time without time zone"
                        + " AND tm < '03:04:05.5'::time without time zone;",
                scalar("SELECT pg_get_viewdef('cty_w2'::regclass, true)"));

        exec("CREATE VIEW cty_w3 AS SELECT id FROM cty_tm WHERE tz > '3:4+02'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_tm\n"
                        + "  WHERE tz > '03:04:00+02'::time with time zone;",
                scalar("SELECT pg_get_viewdef('cty_w3'::regclass, true)"));

        // The three spellings of a written-out constant are one value and print alike.
        exec("CREATE VIEW cty_w4 AS SELECT '3:4'::time AS a, '3:4:5'::time AS b, time '3:4' AS c");
        assertEquals(" SELECT '03:04:00'::time without time zone AS a,\n"
                        + "    '03:04:05'::time without time zone AS b,\n"
                        + "    '03:04:00'::time without time zone AS c;",
                scalar("SELECT pg_get_viewdef('cty_w4'::regclass, true)"));
    }

    /**
     * An array constant is read by the rules PostgreSQL reads braces by and written back element
     * by element, so the spacing it was typed with is not part of the value.
     */
    @Test
    void anArrayConstantIsWrittenElementByElement() throws Exception {
        exec("CREATE TABLE cty_ar (id int, arr int[], nm numeric[], txa text[], m int[],"
                + " da date[], e int[])");
        exec("CREATE VIEW cty_a1 AS SELECT id FROM cty_ar WHERE arr = '{ 1, 2 , 3 }'"
                + " AND txa = '{ a , b }' AND nm = '{ 01.50, 2 }'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_ar\n"
                        + "  WHERE arr = '{1,2,3}'::integer[] AND txa = '{a,b}'::text[]"
                        + " AND nm = '{1.50,2}'::numeric[];",
                scalar("SELECT pg_get_viewdef('cty_a1'::regclass, true)"));

        // An element that carries its own quotes keeps them; one that does not is written bare.
        exec("CREATE VIEW cty_a2 AS SELECT id FROM cty_ar WHERE txa = '{\"x y\", z}'"
                + " AND arr = '{ 007, 8 }' AND arr = '{NULL, 1}'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_ar\n"
                        + "  WHERE txa = '{\"x y\",z}'::text[] AND arr = '{7,8}'::integer[]"
                        + " AND arr = '{NULL,1}'::integer[];",
                scalar("SELECT pg_get_viewdef('cty_a2'::regclass, true)"));

        exec("CREATE VIEW cty_a3 AS SELECT '{ 1, 2 }'::int[] AS a, '{ x, y }'::text[] AS b");
        assertEquals(" SELECT '{1,2}'::integer[] AS a,\n"
                        + "    '{x,y}'::text[] AS b;",
                scalar("SELECT pg_get_viewdef('cty_a3'::regclass, true)"));

        // The type of the array on one side of a containment operator is the type of an untyped
        // constant on the other.
        exec("CREATE VIEW cty_a4 AS SELECT id FROM cty_ar WHERE arr @> '{ 1 }'");
        assertEquals(" SELECT id\n   FROM cty_ar\n  WHERE arr @> '{1}'::integer[];",
                scalar("SELECT pg_get_viewdef('cty_a4'::regclass, true)"));

        // Nesting, an element of a type with a reader of its own, and an empty array all survive.
        exec("CREATE VIEW cty_a5 AS SELECT id FROM cty_ar WHERE m = '{ {1,2}, {3,4} }'"
                + " AND da = '{ 2020-1-2 }' AND e = '{}'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_ar\n"
                        + "  WHERE m = '{{1,2},{3,4}}'::integer[] AND da = '{2020-01-02}'::date[]"
                        + " AND e = '{}'::integer[];",
                scalar("SELECT pg_get_viewdef('cty_a5'::regclass, true)"));

        // The bounds a literal declares are part of the value, so they are written back too.
        exec("CREATE VIEW cty_a6 AS SELECT id FROM cty_ar WHERE arr = '[0:2]={ 1, 2, 3 }'");
        assertEquals(" SELECT id\n   FROM cty_ar\n  WHERE arr = '[0:2]={1,2,3}'::integer[];",
                scalar("SELECT pg_get_viewdef('cty_a6'::regclass, true)"));
    }

    /**
     * A column default and a CHECK constraint are printed by machinery of their own, so the same
     * two constants are read there again: the text a default carries has to be text its own type
     * would take back.
     */
    @Test
    void aDefaultAndACheckPrintTheValueTheirTypeRead() throws Exception {
        exec("CREATE TABLE cty_e (a int, t time DEFAULT '3:4', tz timetz DEFAULT '3:4+02',"
                + " arr int[] DEFAULT '{ 1, 2 }', nm numeric[] DEFAULT '{ 01.50 }')");
        String expected = "t='03:04:00'::time without time zone"
                + " tz='03:04:00+02'::time with time zone"
                + " arr='{1,2}'::integer[] nm='{1.50}'::numeric[]";
        assertEquals(expected, scalar("SELECT string_agg(column_name || '=' || column_default,"
                + " ' ' ORDER BY ordinal_position) FROM information_schema.columns"
                + " WHERE table_name = 'cty_e' AND column_default IS NOT NULL"));
        assertEquals("'03:04:00'::time without time zone '03:04:00+02'::time with time zone"
                        + " '{1,2}'::integer[] '{1.50}'::numeric[]",
                scalar("SELECT string_agg(pg_get_expr(d.adbin, d.adrelid), ' ' ORDER BY d.adnum)"
                        + " FROM pg_attrdef d WHERE d.adrelid = 'cty_e'::regclass"));

        exec("CREATE TABLE cty_k (a int, t time, arr int[], CHECK (t > '3:4'),"
                + " CHECK (arr <> '{ 1, 2 }'))");
        assertEquals("CHECK ((arr <> '{1,2}'::integer[]))\n"
                        + "CHECK ((t > '03:04:00'::time without time zone))",
                scalar("SELECT string_agg(pg_get_constraintdef(oid), chr(10)"
                        + " ORDER BY pg_get_constraintdef(oid)) FROM pg_constraint"
                        + " WHERE conrelid = 'cty_k'::regclass AND contype = 'c'"));
    }

    // ------------------------------------------------------------ a value function is the keyword it is

    /**
     * PostgreSQL keeps CURRENT_DATE and its kin as nodes of their own rather than as calls, so a
     * definition writes them the way the grammar spells them: in capitals and without
     * parentheses. An ordinary call of no arguments keeps its parentheses.
     */
    @Test
    void aValueFunctionIsWrittenAsTheKeywordItIs() throws Exception {
        exec("CREATE VIEW cty_kw AS SELECT current_date AS a, current_time AS b,"
                + " current_timestamp AS c, localtime AS d, localtimestamp AS e,"
                + " current_user AS f, session_user AS g, current_role AS h,"
                + " current_catalog AS i, current_schema AS j");
        String expected = " SELECT CURRENT_DATE AS a,\n"
                + "    CURRENT_TIME AS b,\n"
                + "    CURRENT_TIMESTAMP AS c,\n"
                + "    LOCALTIME AS d,\n"
                + "    LOCALTIMESTAMP AS e,\n"
                + "    CURRENT_USER AS f,\n"
                + "    SESSION_USER AS g,\n"
                + "    CURRENT_ROLE AS h,\n"
                + "    CURRENT_CATALOG AS i,\n"
                + "    CURRENT_SCHEMA AS j;";
        assertEquals(expected, scalar("SELECT pg_get_viewdef('cty_kw'::regclass, true)"));
        // The keyword is not an expression the bracket-pruning flag reaches, so both forms agree.
        assertEquals(expected, scalar("SELECT pg_get_viewdef('cty_kw'::regclass, false)"));

        // Each of them names a column after itself, and the name is a keyword, so it is quoted.
        exec("CREATE TABLE cty_kt (id int)");
        exec("CREATE VIEW cty_kw2 AS SELECT current_date, current_user FROM cty_kt");
        assertEquals(" SELECT CURRENT_DATE AS \"current_date\",\n"
                        + "    CURRENT_USER AS \"current_user\"\n"
                        + "   FROM cty_kt;",
                scalar("SELECT pg_get_viewdef('cty_kw2'::regclass, true)"));
        assertEquals("current_date,current_user",
                column("SELECT attname FROM pg_attribute WHERE attrelid = 'cty_kw2'::regclass"
                        + " AND attnum > 0 ORDER BY attnum"));
    }

    @Test
    void aValueFunctionNamesTheTypeOfTheConstantBesideIt() throws Exception {
        exec("CREATE TABLE cty_vf (id int, dt date, tm time, ts timestamp, txt text)");
        exec("CREATE VIEW cty_vf1 AS SELECT id FROM cty_vf WHERE dt > current_date"
                + " AND txt = current_user AND tm > localtime AND ts > localtimestamp"
                + " AND ts > now()");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_vf\n"
                        + "  WHERE dt > CURRENT_DATE AND txt = CURRENT_USER AND tm > LOCALTIME"
                        + " AND ts > LOCALTIMESTAMP AND ts > now();",
                scalar("SELECT pg_get_viewdef('cty_vf1'::regclass, true)"));

        // CURRENT_USER and CURRENT_SCHEMA answer a name, not text, and the constant beside one
        // takes that type.
        exec("CREATE VIEW cty_vf2 AS SELECT id FROM cty_vf WHERE current_date > '2020-01-02'"
                + " AND current_user = 'bob' AND current_schema = 'public'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_vf\n"
                        + "  WHERE CURRENT_DATE > '2020-01-02'::date AND CURRENT_USER = 'bob'::name"
                        + " AND CURRENT_SCHEMA = 'public'::name;",
                scalar("SELECT pg_get_viewdef('cty_vf2'::regclass, true)"));

        exec("CREATE VIEW cty_vf3 AS SELECT id FROM cty_vf WHERE localtime > '3:4'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_vf\n"
                        + "  WHERE LOCALTIME > '03:04:00'::time without time zone;",
                scalar("SELECT pg_get_viewdef('cty_vf3'::regclass, true)"));

        // The same type settles the ELSE the CASE never had.
        exec("CREATE VIEW cty_vf4 AS SELECT CASE WHEN id = 1 THEN current_date END AS a,"
                + " CASE WHEN id = 2 THEN current_user END AS b FROM cty_vf");
        assertEquals(" SELECT\n"
                        + "        CASE\n"
                        + "            WHEN id = 1 THEN CURRENT_DATE\n"
                        + "            ELSE NULL::date\n"
                        + "        END AS a,\n"
                        + "        CASE\n"
                        + "            WHEN id = 2 THEN CURRENT_USER\n"
                        + "            ELSE NULL::name\n"
                        + "        END AS b\n"
                        + "   FROM cty_vf;",
                scalar("SELECT pg_get_viewdef('cty_vf4'::regclass, true)"));
    }

    @Test
    void aValueFunctionInADefaultACheckAndARule() throws Exception {
        exec("CREATE TABLE cty_vd (id int, dt date DEFAULT current_date,"
                + " who text DEFAULT current_user, ts timestamptz DEFAULT current_timestamp,"
                + " sch text DEFAULT current_schema, CHECK (dt <= current_date))");
        assertEquals("id=NULL dt=CURRENT_DATE who=CURRENT_USER ts=CURRENT_TIMESTAMP"
                        + " sch=CURRENT_SCHEMA",
                scalar("SELECT string_agg(column_name || '=' || coalesce(column_default, 'NULL'),"
                        + " ' ' ORDER BY ordinal_position) FROM information_schema.columns"
                        + " WHERE table_name = 'cty_vd'"));
        assertEquals("CURRENT_DATE CURRENT_USER CURRENT_TIMESTAMP CURRENT_SCHEMA",
                scalar("SELECT string_agg(pg_get_expr(d.adbin, d.adrelid), ' ' ORDER BY d.adnum)"
                        + " FROM pg_attrdef d WHERE d.adrelid = 'cty_vd'::regclass"));
        assertEquals("CHECK ((dt <= CURRENT_DATE))",
                scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                        + " WHERE conrelid = 'cty_vd'::regclass AND contype = 'c'"));

        exec("CREATE TABLE cty_rx (a int)");
        exec("CREATE TABLE cty_ry (a int, d date)");
        exec("CREATE RULE cty_rr AS ON INSERT TO cty_rx DO ALSO"
                + " INSERT INTO cty_ry VALUES (NEW.a, current_date)");
        assertEquals("CREATE RULE cty_rr AS\n"
                        + "    ON INSERT TO public.cty_rx DO  INSERT INTO cty_ry (a, d)\n"
                        + "  VALUES (new.a, CURRENT_DATE);",
                scalar("SELECT definition FROM pg_rules WHERE rulename = 'cty_rr'"));

        // An index predicate goes through the same deparser and prints its constant the same way.
        exec("CREATE INDEX cty_ix ON cty_ry (a) WHERE d < '2020-01-02'");
        assertEquals("CREATE INDEX cty_ix ON public.cty_ry USING btree (a)"
                        + " WHERE (d < '2020-01-02'::date)",
                scalar("SELECT pg_get_indexdef('cty_ix'::regclass)"));
    }

    // ------------------------------------------------------------ a star stands for what it stood for

    /**
     * A star is expanded into the columns it stood for when the view was created, and a WITH item
     * publishes names of its own for it to stand for.
     */
    @Test
    void aStarOverAWithItemStandsForTheNamesThatItemPublishes() throws Exception {
        exec("CREATE TABLE cty_s (a int, b text)");
        exec("INSERT INTO cty_s VALUES (1,'x'),(2,'y')");

        exec("CREATE VIEW cty_q1 AS WITH q AS (SELECT 1) SELECT * FROM q");
        assertEquals(" WITH q AS (\n"
                        + "         SELECT 1 AS \"?column?\"\n"
                        + "        )\n"
                        + " SELECT \"?column?\"\n"
                        + "   FROM q;",
                scalar("SELECT pg_get_viewdef('cty_q1'::regclass, true)"));

        exec("CREATE VIEW cty_q2 AS WITH q AS (SELECT a, b FROM cty_s) SELECT * FROM q");
        String ab = " WITH q AS (\n"
                + "         SELECT cty_s.a,\n"
                + "            cty_s.b\n"
                + "           FROM cty_s\n"
                + "        )\n"
                + " SELECT a,\n"
                + "    b\n"
                + "   FROM q;";
        assertEquals(ab, scalar("SELECT pg_get_viewdef('cty_q2'::regclass, true)"));

        // A star inside the WITH item is expanded first, and the star over it stands for what
        // that expansion published.
        exec("CREATE VIEW cty_q3 AS WITH q AS (SELECT * FROM cty_s) SELECT * FROM q");
        assertEquals(ab, scalar("SELECT pg_get_viewdef('cty_q3'::regclass, true)"));

        // The names a WITH item was given override the ones its query would publish.
        exec("CREATE VIEW cty_q4 AS WITH q(x, y) AS (SELECT a, b FROM cty_s) SELECT * FROM q");
        assertEquals(" WITH q(x, y) AS (\n"
                        + "         SELECT cty_s.a,\n"
                        + "            cty_s.b\n"
                        + "           FROM cty_s\n"
                        + "        )\n"
                        + " SELECT x,\n"
                        + "    y\n"
                        + "   FROM q;",
                scalar("SELECT pg_get_viewdef('cty_q4'::regclass, true)"));

        exec("CREATE VIEW cty_q5 AS WITH q AS (SELECT a, b FROM cty_s)"
                + " SELECT q2.* FROM q q2 WHERE q2.a > 1");
        assertEquals(" WITH q AS (\n"
                        + "         SELECT cty_s.a,\n"
                        + "            cty_s.b\n"
                        + "           FROM cty_s\n"
                        + "        )\n"
                        + " SELECT a,\n"
                        + "    b\n"
                        + "   FROM q q2\n"
                        + "  WHERE a > 1;",
                scalar("SELECT pg_get_viewdef('cty_q5'::regclass, true)"));

        exec("CREATE VIEW cty_q6 AS WITH RECURSIVE q(n) AS (SELECT 1 UNION ALL"
                + " SELECT n + 1 FROM q WHERE n < 5) SELECT * FROM q");
        assertEquals(" WITH RECURSIVE q(n) AS (\n"
                        + "         SELECT 1 AS \"?column?\"\n"
                        + "        UNION ALL\n"
                        + "         SELECT q_1.n + 1\n"
                        + "           FROM q q_1\n"
                        + "          WHERE q_1.n < 5\n"
                        + "        )\n"
                        + " SELECT n\n"
                        + "   FROM q;",
                scalar("SELECT pg_get_viewdef('cty_q6'::regclass, true)"));

        // An aggregate publishes the name it will be known by, and the star stands for that.
        exec("CREATE VIEW cty_q7 AS WITH q AS (SELECT count(*) FROM cty_s) SELECT * FROM q");
        assertEquals(" WITH q AS (\n"
                        + "         SELECT count(*) AS count\n"
                        + "           FROM cty_s\n"
                        + "        )\n"
                        + " SELECT count\n"
                        + "   FROM q;",
                scalar("SELECT pg_get_viewdef('cty_q7'::regclass, true)"));

        // A set operation publishes its first arm's names.
        exec("CREATE VIEW cty_q8 AS WITH q AS (SELECT a, b FROM cty_s UNION ALL"
                + " SELECT a, b FROM cty_s) SELECT * FROM q");
        assertEquals(" WITH q AS (\n"
                        + "         SELECT cty_s.a,\n"
                        + "            cty_s.b\n"
                        + "           FROM cty_s\n"
                        + "        UNION ALL\n"
                        + "         SELECT cty_s.a,\n"
                        + "            cty_s.b\n"
                        + "           FROM cty_s\n"
                        + "        )\n"
                        + " SELECT a,\n"
                        + "    b\n"
                        + "   FROM q;",
                scalar("SELECT pg_get_viewdef('cty_q8'::regclass, true)"));

        // A star beside an item of its own keeps the item where it was written.
        exec("CREATE VIEW cty_q9 AS WITH q AS (SELECT * FROM (SELECT a, b FROM cty_s) z)"
                + " SELECT *, a + 1 AS c FROM q");
        assertEquals(" WITH q AS (\n"
                        + "         SELECT z.a,\n"
                        + "            z.b\n"
                        + "           FROM ( SELECT cty_s.a,\n"
                        + "                    cty_s.b\n"
                        + "                   FROM cty_s) z\n"
                        + "        )\n"
                        + " SELECT a,\n"
                        + "    b,\n"
                        + "    a + 1 AS c\n"
                        + "   FROM q;",
                scalar("SELECT pg_get_viewdef('cty_q9'::regclass, true)"));

        // It is a freeze, not only a spelling: a column added afterwards is not in the view.
        exec("ALTER TABLE cty_s ADD COLUMN c int");
        assertEquals(ab, scalar("SELECT pg_get_viewdef('cty_q3'::regclass, true)"));
        assertEquals("1,2", column("SELECT a FROM cty_q3 ORDER BY a"));
        assertEquals("x,y", column("SELECT b FROM cty_q3 ORDER BY a"));
        assertEquals(2, num("SELECT count(*) FROM pg_attribute"
                + " WHERE attrelid = 'cty_q3'::regclass AND attnum > 0"));
    }

    // ------------------------------------------------------------ an operator resolves to an entry

    /**
     * A stored definition shows which entry of pg_operator each spelling resolved to, because
     * parse analysis puts a conversion in front of an operand whose own type that entry does not
     * declare. character varying has no operators of its own at all, so every comparison with one
     * is text's.
     */
    @Test
    void aVarcharIsReadAsTextByTheOperatorThatComparesIt() throws Exception {
        exec("CREATE TABLE cty_o (id int, vc varchar(10), vc2 varchar, txt text, nme name,"
                + " ch char(5))");
        exec("CREATE VIEW cty_o1 AS SELECT id FROM cty_o WHERE vc = 'b'");
        assertEquals(" SELECT id\n   FROM cty_o\n  WHERE vc::text = 'b'::text;",
                scalar("SELECT pg_get_viewdef('cty_o1'::regclass, true)"));
        assertEquals(" SELECT id\n   FROM cty_o\n  WHERE ((vc)::text = 'b'::text);",
                scalar("SELECT pg_get_viewdef('cty_o1'::regclass, false)"));

        // A name has equality against text, so it is not converted; a character has equality of
        // its own, so the character varying is converted to it rather than the other way round.
        exec("CREATE VIEW cty_o2 AS SELECT id FROM cty_o WHERE vc = vc2 AND vc = txt"
                + " AND vc = nme AND vc = ch");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_o\n"
                        + "  WHERE vc::text = vc2::text AND vc::text = txt AND vc::text = nme"
                        + " AND vc::bpchar = ch;",
                scalar("SELECT pg_get_viewdef('cty_o2'::regclass, true)"));

        exec("CREATE VIEW cty_o3 AS SELECT id FROM cty_o WHERE ch = 'b' AND vc < 'b'"
                + " AND vc <> 'c' AND vc >= 'd'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_o\n"
                        + "  WHERE ch = 'b'::bpchar AND vc::text < 'b'::text"
                        + " AND vc::text <> 'c'::text AND vc::text >= 'd'::text;",
                scalar("SELECT pg_get_viewdef('cty_o3'::regclass, true)"));

        // A bare column of that type is nothing for an operator to convert.
        exec("CREATE VIEW cty_o4 AS SELECT vc, vc AS z FROM cty_o ORDER BY vc");
        assertEquals(" SELECT vc,\n    vc AS z\n   FROM cty_o\n  ORDER BY vc;",
                scalar("SELECT pg_get_viewdef('cty_o4'::regclass, true)"));
    }

    @Test
    void aConversionIsPrintedWhereverAnOperandCarriesOne() throws Exception {
        exec("CREATE TABLE cty_cv (id int, vc varchar(10), vc2 varchar, txt text)");
        exec("CREATE VIEW cty_c1 AS SELECT vc || 'x' AS a, vc || vc2 AS b, txt || vc AS c"
                + " FROM cty_cv");
        assertEquals(" SELECT vc::text || 'x'::text AS a,\n"
                        + "    vc::text || vc2::text AS b,\n"
                        + "    txt || vc::text AS c\n"
                        + "   FROM cty_cv;",
                scalar("SELECT pg_get_viewdef('cty_c1'::regclass, true)"));

        exec("CREATE VIEW cty_c2 AS SELECT id FROM cty_cv WHERE vc LIKE 'a%' AND vc ILIKE 'b%'"
                + " AND vc ~ 'c'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_cv\n"
                        + "  WHERE vc::text ~~ 'a%'::text AND vc::text ~~* 'b%'::text"
                        + " AND vc::text ~ 'c'::text;",
                scalar("SELECT pg_get_viewdef('cty_c2'::regclass, true)"));

        exec("CREATE VIEW cty_c3 AS SELECT id FROM cty_cv WHERE vc BETWEEN 'a' AND 'b'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_cv\n"
                        + "  WHERE vc::text >= 'a'::text AND vc::text <= 'b'::text;",
                scalar("SELECT pg_get_viewdef('cty_c3'::regclass, true)"));

        // IS NULL is not an operator and converts nothing; IS DISTINCT FROM is written over one.
        exec("CREATE VIEW cty_c4 AS SELECT id FROM cty_cv"
                + " WHERE vc IS NULL OR vc IS DISTINCT FROM 'a'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_cv\n"
                        + "  WHERE vc IS NULL OR vc::text IS DISTINCT FROM 'a'::text;",
                scalar("SELECT pg_get_viewdef('cty_c4'::regclass, true)"));

        exec("CREATE VIEW cty_c5 AS SELECT id FROM cty_cv GROUP BY id HAVING max(vc) > 'a'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_cv\n"
                        + "  GROUP BY id\n"
                        + " HAVING max(vc::text) > 'a'::text;",
                scalar("SELECT pg_get_viewdef('cty_c5'::regclass, true)"));

        // A constant written with a type of its own is converted where the entry wants another.
        exec("CREATE VIEW cty_c6 AS SELECT id FROM cty_cv WHERE vc = 'b'::varchar"
                + " AND vc = 'c'::text");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_cv\n"
                        + "  WHERE vc::text = 'b'::character varying::text AND vc::text = 'c'::text;",
                scalar("SELECT pg_get_viewdef('cty_c6'::regclass, true)"));
    }

    @Test
    void aCidrIsReadAsInetAndABitVaryingAsBit() throws Exception {
        exec("CREATE TABLE cty_ni (id int, cd cidr, ip inet, vb bit varying(8), bt bit(8))");
        exec("CREATE VIEW cty_n2 AS SELECT id FROM cty_ni WHERE cd = cd AND cd >> ip"
                + " AND cd = '10.0.0.0/8'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_ni\n"
                        + "  WHERE cd::inet = cd::inet AND cd::inet >> ip"
                        + " AND cd::inet = '10.0.0.0/8'::inet;",
                scalar("SELECT pg_get_viewdef('cty_n2'::regclass, true)"));

        // abbrev has a cidr signature of its own, which is why its argument stands unconverted.
        exec("CREATE VIEW cty_n3 AS SELECT host(cd) AS a, masklen(cd) AS b, abbrev(cd) AS c"
                + " FROM cty_ni");
        assertEquals(" SELECT host(cd::inet) AS a,\n"
                        + "    masklen(cd::inet) AS b,\n"
                        + "    abbrev(cd) AS c\n"
                        + "   FROM cty_ni;",
                scalar("SELECT pg_get_viewdef('cty_n3'::regclass, true)"));

        exec("CREATE VIEW cty_n4 AS SELECT ip >>= cd AS a, ip <<= cd AS b, ip << cd AS c,"
                + " ip >> cd AS d FROM cty_ni");
        assertEquals(" SELECT ip >>= cd::inet AS a,\n"
                        + "    ip <<= cd::inet AS b,\n"
                        + "    ip << cd::inet AS c,\n"
                        + "    ip >> cd::inet AS d\n"
                        + "   FROM cty_ni;",
                scalar("SELECT pg_get_viewdef('cty_n4'::regclass, true)"));

        // Equality is declared over both, and bit varying is the preferred type of the category,
        // so there the bit is converted instead.
        exec("CREATE VIEW cty_n5 AS SELECT id FROM cty_ni WHERE vb = B'1010'"
                + " AND bt = B'10101010'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_ni\n"
                        + "  WHERE vb = '1010'::\"bit\"::bit varying AND bt = '10101010'::\"bit\";",
                scalar("SELECT pg_get_viewdef('cty_n5'::regclass, true)"));

        // & | # and << are declared over bit and not over bit varying.
        exec("CREATE VIEW cty_n6 AS SELECT vb & bt AS a, vb | bt AS b, vb # bt AS c,"
                + " vb << 1 AS d, vb || bt AS e FROM cty_ni");
        assertEquals(" SELECT vb::\"bit\" & bt AS a,\n"
                        + "    vb::\"bit\" | bt AS b,\n"
                        + "    vb::\"bit\" # bt AS c,\n"
                        + "    vb::\"bit\" << 1 AS d,\n"
                        + "    vb || bt::bit varying AS e\n"
                        + "   FROM cty_ni;",
                scalar("SELECT pg_get_viewdef('cty_n6'::regclass, true)"));
    }

    @Test
    void aNumberIsConvertedOnlyWhereNoOperatorTakesThePairAsWritten() throws Exception {
        exec("CREATE TABLE cty_nu (id int, nm numeric, nm2 numeric(10,2), bi bigint,"
                + " si smallint, d double precision, dt date, tm time, ts timestamp)");
        exec("CREATE VIEW cty_u1 AS SELECT id FROM cty_nu WHERE nm = 1 AND bi = 2 AND si = 3"
                + " AND d = 4 AND nm2 = 5");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_nu\n"
                        + "  WHERE nm = 1::numeric AND bi = 2 AND si = 3"
                        + " AND d = 4::double precision AND nm2 = 5::numeric;",
                scalar("SELECT pg_get_viewdef('cty_u1'::regclass, true)"));

        // There is an entry for (bigint, integer) and for (bigint, smallint) but none for
        // (numeric, integer) or (double precision, numeric).
        exec("CREATE VIEW cty_u2 AS SELECT id FROM cty_nu WHERE nm = bi AND bi = si AND d = nm");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_nu\n"
                        + "  WHERE nm = bi::numeric AND bi = si AND d = nm::double precision;",
                scalar("SELECT pg_get_viewdef('cty_u2'::regclass, true)"));

        // An entry of exactly the two written types wins outright, which is what keeps the
        // constant beside a timestamp from becoming the category's preferred type.
        exec("CREATE VIEW cty_u3 AS SELECT id FROM cty_nu WHERE ts > '2020-01-01' AND ts > dt"
                + " AND dt > ts AND tm > '3:4'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_nu\n"
                        + "  WHERE ts > '2020-01-01 00:00:00'::timestamp without time zone"
                        + " AND ts > dt AND dt > ts"
                        + " AND tm > '03:04:00'::time without time zone;",
                scalar("SELECT pg_get_viewdef('cty_u3'::regclass, true)"));
    }

    @Test
    void aCallsArgumentsAreReadAgainstTheSignatureItResolvedTo() throws Exception {
        exec("CREATE TABLE cty_ca (id int, vc varchar(10), txt text, nm numeric, si smallint)");
        exec("CREATE VIEW cty_g1 AS SELECT upper(vc) AS a, length(vc) AS b,"
                + " substr(vc, 1, 2) AS c FROM cty_ca");
        assertEquals(" SELECT upper(vc::text) AS a,\n"
                        + "    length(vc::text) AS b,\n"
                        + "    substr(vc::text, 1, 2) AS c\n"
                        + "   FROM cty_ca;",
                scalar("SELECT pg_get_viewdef('cty_g1'::regclass, true)"));

        // count takes "any", so its argument is not converted.
        exec("CREATE VIEW cty_g2 AS SELECT max(vc) AS a, string_agg(vc, ',') AS b,"
                + " count(vc) AS c FROM cty_ca");
        assertEquals(" SELECT max(vc::text) AS a,\n"
                        + "    string_agg(vc::text, ','::text) AS b,\n"
                        + "    count(vc) AS c\n"
                        + "   FROM cty_ca;",
                scalar("SELECT pg_get_viewdef('cty_g2'::regclass, true)"));

        // abs and round are declared over the types they were handed; trunc and to_char are not.
        exec("CREATE VIEW cty_g3 AS SELECT abs(si) AS a, round(nm, 2) AS b, trunc(si) AS c,"
                + " to_char(si, '999') AS d, substr(txt, si) AS e FROM cty_ca");
        assertEquals(" SELECT abs(si) AS a,\n"
                        + "    round(nm, 2) AS b,\n"
                        + "    trunc(si::double precision) AS c,\n"
                        + "    to_char(si::double precision, '999'::text) AS d,\n"
                        + "    substr(txt, si::integer) AS e\n"
                        + "   FROM cty_ca;",
                scalar("SELECT pg_get_viewdef('cty_g3'::regclass, true)"));
    }

    /**
     * COALESCE, GREATEST, LEAST, CASE and an array constructor settle on one type for every arm.
     * text reaches character varying without being asked, so the character varying is what they
     * settle on. NULLIF is different because it is written over an operator and takes its types.
     */
    @Test
    void aConstructSettlesOnOneTypeForEveryArm() throws Exception {
        exec("CREATE TABLE cty_ar2 (id int, vc varchar(10), txt text, si smallint, bi bigint)");
        exec("CREATE VIEW cty_s1 AS SELECT coalesce(vc, 'x') AS a, coalesce(vc, txt) AS b,"
                + " CASE WHEN id = 1 THEN vc ELSE 'y' END AS c FROM cty_ar2");
        assertEquals(" SELECT COALESCE(vc, 'x'::character varying) AS a,\n"
                        + "    COALESCE(vc, txt::character varying) AS b,\n"
                        + "        CASE\n"
                        + "            WHEN id = 1 THEN vc\n"
                        + "            ELSE 'y'::character varying\n"
                        + "        END AS c\n"
                        + "   FROM cty_ar2;",
                scalar("SELECT pg_get_viewdef('cty_s1'::regclass, true)"));

        exec("CREATE VIEW cty_s2 AS SELECT greatest(vc, 'a') AS a, least(vc, txt) AS b,"
                + " nullif(vc, 'z') AS c FROM cty_ar2");
        assertEquals(" SELECT GREATEST(vc, 'a'::character varying) AS a,\n"
                        + "    LEAST(vc, txt::character varying) AS b,\n"
                        + "    NULLIF(vc::text, 'z'::text) AS c\n"
                        + "   FROM cty_ar2;",
                scalar("SELECT pg_get_viewdef('cty_s2'::regclass, true)"));

        exec("CREATE VIEW cty_s3 AS SELECT ARRAY[vc, 'a'] AS a, ARRAY[txt, vc] AS b,"
                + " ARRAY[1, 2.5] AS c, ARRAY[si, bi] AS d FROM cty_ar2");
        assertEquals(" SELECT ARRAY[vc, 'a'::character varying] AS a,\n"
                        + "    ARRAY[txt, vc::text] AS b,\n"
                        + "    ARRAY[1::numeric, 2.5] AS c,\n"
                        + "    ARRAY[si::bigint, bi] AS d\n"
                        + "   FROM cty_ar2;",
                scalar("SELECT pg_get_viewdef('cty_s3'::regclass, true)"));

        // A CASE written over a value compares it with the operator that pair resolves to.
        exec("CREATE VIEW cty_s4 AS SELECT CASE WHEN id=1 THEN vc WHEN id=2 THEN txt"
                + " ELSE 'z' END AS a, CASE vc WHEN 'a' THEN 1 END AS b FROM cty_ar2");
        assertEquals(" SELECT\n"
                        + "        CASE\n"
                        + "            WHEN id = 1 THEN vc\n"
                        + "            WHEN id = 2 THEN txt::character varying\n"
                        + "            ELSE 'z'::character varying\n"
                        + "        END AS a,\n"
                        + "        CASE vc\n"
                        + "            WHEN 'a'::text THEN 1\n"
                        + "            ELSE NULL::integer\n"
                        + "        END AS b\n"
                        + "   FROM cty_ar2;",
                scalar("SELECT pg_get_viewdef('cty_s4'::regclass, true)"));
    }

    @Test
    void aPolymorphicOperatorConvertsNothingAndAJsonbPathIsAnArrayOfText() throws Exception {
        exec("CREATE TABLE cty_jp (id int, jb jsonb, ia int[], ta text[])");
        exec("CREATE VIEW cty_j1 AS SELECT jb #> '{a}' AS a, jb #>> '{a}' AS b, jb ? 'k' AS c,"
                + " jb ?| ARRAY['k'] AS d, jb ?& ARRAY['k'] AS e, jb #- '{a}' AS f FROM cty_jp");
        assertEquals(" SELECT jb #> '{a}'::text[] AS a,\n"
                        + "    jb #>> '{a}'::text[] AS b,\n"
                        + "    jb ? 'k'::text AS c,\n"
                        + "    jb ?| ARRAY['k'::text] AS d,\n"
                        + "    jb ?& ARRAY['k'::text] AS e,\n"
                        + "    jb #- '{a}'::text[] AS f\n"
                        + "   FROM cty_jp;",
                scalar("SELECT pg_get_viewdef('cty_j1'::regclass, true)"));

        // = and @> over arrays are declared over anyarray, which names no type to convert to.
        exec("CREATE VIEW cty_j2 AS SELECT id FROM cty_jp WHERE ta = '{ a , b }'"
                + " AND ia = '{ 1, 2 , 3 }' AND ia @> '{ 1 }'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_jp\n"
                        + "  WHERE ta = '{a,b}'::text[] AND ia = '{1,2,3}'::integer[]"
                        + " AND ia @> '{1}'::integer[];",
                scalar("SELECT pg_get_viewdef('cty_j2'::regclass, true)"));
    }

    // ------------------------------------------------------------ a list written with IN

    /**
     * PostgreSQL does not keep IN: it rewrites the list into what tests it, and the rewrite is
     * what a definition shows. An item that is a column is compared on its own and joined to the
     * rest by OR, the array first.
     */
    @Test
    void aListWrittenWithInIsTheComparisonItStandsFor() throws Exception {
        exec("CREATE TABLE cty_in (id int, vc varchar(10), txt text, nme name, nm numeric,"
                + " bi bigint, si smallint)");
        exec("CREATE VIEW cty_i1 AS SELECT id FROM cty_in WHERE txt IN ('a')"
                + " AND txt IN (nme, 'b') AND id NOT IN (1)");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_in\n"
                        + "  WHERE txt = 'a'::text AND (txt = nme OR txt = 'b'::text)"
                        + " AND id <> 1;",
                scalar("SELECT pg_get_viewdef('cty_i1'::regclass, true)"));

        // Only a plain column reference is compared on its own: 1+1 is an item like any other.
        exec("CREATE VIEW cty_i2 AS SELECT id FROM cty_in WHERE id IN (1, 2, id)"
                + " AND id IN (1+1, 2) AND nm IN (1, 2)");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_in\n"
                        + "  WHERE ((id = ANY (ARRAY[1, 2])) OR id = id)"
                        + " AND (id = ANY (ARRAY[1 + 1, 2]))"
                        + " AND (nm = ANY (ARRAY[1::numeric, 2::numeric]));",
                scalar("SELECT pg_get_viewdef('cty_i2'::regclass, true)"));

        // The list settles the type of its items, and the comparison is resolved over it.
        exec("CREATE VIEW cty_i3 AS SELECT id FROM cty_in WHERE vc IN ('a','b')");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_in\n"
                        + "  WHERE vc::text = ANY (ARRAY['a'::character varying,"
                        + " 'b'::character varying]::text[]);",
                scalar("SELECT pg_get_viewdef('cty_i3'::regclass, true)"));

        exec("CREATE VIEW cty_i4 AS SELECT id FROM cty_in WHERE si + bi IN (1, 2)");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_in\n"
                        + "  WHERE (si + bi) = ANY (ARRAY[1::bigint, 2::bigint]);",
                scalar("SELECT pg_get_viewdef('cty_i4'::regclass, true)"));

        // An array written out settles its own element type, with no say for the value being
        // tested, which is why the same items are text here and character varying above.
        exec("CREATE VIEW cty_i5 AS SELECT id FROM cty_in WHERE vc = ANY (ARRAY['a','b'])"
                + " AND vc = ANY (ARRAY[vc, txt])");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_in\n"
                        + "  WHERE (vc::text = ANY (ARRAY['a'::text, 'b'::text]))"
                        + " AND (vc::text = ANY (ARRAY[vc, txt::character varying]::text[]));",
                scalar("SELECT pg_get_viewdef('cty_i5'::regclass, true)"));
    }

    // ------------------------------------------------------------ the spelling and the parentheses

    @Test
    void everyOperatorIsWrittenUnderItsOwnSpelling() throws Exception {
        exec("CREATE TABLE cty_op (id int, tv tsvector, tq tsquery, pt point, bx box, ln lseg,"
                + " r4 int4range)");
        exec("CREATE VIEW cty_p1 AS SELECT id & 1 AS a, id | 2 AS b, id # 3 AS c,"
                + " id << 1 AS d, id >> 1 AS e, ~ id AS f, - id AS g FROM cty_op");
        assertEquals(" SELECT id & 1 AS a,\n"
                        + "    id | 2 AS b,\n"
                        + "    id # 3 AS c,\n"
                        + "    id << 1 AS d,\n"
                        + "    id >> 1 AS e,\n"
                        + "    ~ id AS f,\n"
                        + "    - id AS g\n"
                        + "   FROM cty_op;",
                scalar("SELECT pg_get_viewdef('cty_p1'::regclass, true)"));
        // A prefix operator is written with a space after it, and bracketed by the other form.
        assertEquals(" SELECT (id & 1) AS a,\n"
                        + "    (id | 2) AS b,\n"
                        + "    (id # 3) AS c,\n"
                        + "    (id << 1) AS d,\n"
                        + "    (id >> 1) AS e,\n"
                        + "    (~ id) AS f,\n"
                        + "    (- id) AS g\n"
                        + "   FROM cty_op;",
                scalar("SELECT pg_get_viewdef('cty_p1'::regclass, false)"));

        exec("CREATE VIEW cty_p2 AS SELECT tv @@ tq AS a, pt <-> pt AS b, bx ~= bx AS c,"
                + " bx <<| bx AS d, ln ## bx AS e, r4 -|- r4 AS f FROM cty_op");
        assertEquals(" SELECT tv @@ tq AS a,\n"
                        + "    pt <-> pt AS b,\n"
                        + "    bx ~= bx AS c,\n"
                        + "    bx <<| bx AS d,\n"
                        + "    ln ## bx AS e,\n"
                        + "    r4 -|- r4 AS f\n"
                        + "   FROM cty_op;",
                scalar("SELECT pg_get_viewdef('cty_p2'::regclass, true)"));
    }

    @Test
    void similarToIsARegularExpressionMatchAndAnEscapeIsACallOfItsOwn() throws Exception {
        exec("CREATE TABLE cty_si (id int, vc varchar(10), txt text)");
        exec("CREATE VIEW cty_e1v AS SELECT id FROM cty_si WHERE txt SIMILAR TO 'a%'"
                + " AND vc NOT SIMILAR TO 'b%'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_si\n"
                        + "  WHERE txt ~ similar_to_escape('a%'::text)"
                        + " AND vc::text !~ similar_to_escape('b%'::text);",
                scalar("SELECT pg_get_viewdef('cty_e1v'::regclass, true)"));

        exec("CREATE VIEW cty_e2v AS SELECT id FROM cty_si WHERE txt SIMILAR TO 'a%' ESCAPE '#'"
                + " AND txt LIKE 'b%' ESCAPE '!'");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_si\n"
                        + "  WHERE txt ~ similar_to_escape('a%'::text, '#'::text)"
                        + " AND txt ~~ like_escape('b%'::text, '!'::text);",
                scalar("SELECT pg_get_viewdef('cty_e2v'::regclass, true)"));
    }

    /**
     * Which operand carries parentheses is decided by what reads it, not by precedence:
     * PostgreSQL brackets an operand that does not already read as one thing, and leaves them off
     * only for arithmetic standing inside arithmetic of its own strength or weaker.
     */
    @Test
    void anOperatorInsideAnotherIsBracketed() throws Exception {
        exec("CREATE TABLE cty_br (id int, id2 int, bi bigint, si smallint)");
        exec("CREATE VIEW cty_b1v AS SELECT id FROM cty_br WHERE si + bi = 1 AND si * 2 + 1 = 2"
                + " AND si + 2 * 3 = 3 AND si - (bi - 1) = 4");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_br\n"
                        + "  WHERE (si + bi) = 1 AND (si * 2 + 1) = 2 AND (si + 2 * 3) = 3"
                        + " AND (si - (bi - 1)) = 4;",
                scalar("SELECT pg_get_viewdef('cty_b1v'::regclass, true)"));

        // Nothing is bracketed at the top of a select list, addition inside addition only on the
        // right, and multiplication inside addition not at all.
        exec("CREATE VIEW cty_b2v AS SELECT si + bi AS a, si * 2 + 1 AS b, (si + 1) * 2 AS c,"
                + " si + bi + si AS d FROM cty_br");
        assertEquals(" SELECT si + bi AS a,\n"
                        + "    si * 2 + 1 AS b,\n"
                        + "    (si + 1) * 2 AS c,\n"
                        + "    si + bi + si AS d\n"
                        + "   FROM cty_br;",
                scalar("SELECT pg_get_viewdef('cty_b2v'::regclass, true)"));

        exec("CREATE VIEW cty_b3v AS SELECT - (si + bi) AS a, - (si * bi) AS b, - (- si) AS c,"
                + " - si + bi AS d, - si * bi AS e FROM cty_br");
        assertEquals(" SELECT - (si + bi) AS a,\n"
                        + "    - (si * bi) AS b,\n"
                        + "    - (- si) AS c,\n"
                        + "    (- si) + bi AS d,\n"
                        + "    (- si) * bi AS e\n"
                        + "   FROM cty_br;",
                scalar("SELECT pg_get_viewdef('cty_b3v'::regclass, true)"));

        exec("CREATE VIEW cty_b4v AS SELECT id FROM cty_br WHERE id > id2 - 7 AND - id > 3"
                + " AND (id + 1) * 2 > 4");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_br\n"
                        + "  WHERE id > (id2 - 7) AND (- id) > 3 AND ((id + 1) * 2) > 4;",
                scalar("SELECT pg_get_viewdef('cty_b4v'::regclass, true)"));
    }

    @Test
    void whatIsNotAnOperatorBracketsItsOperandToo() throws Exception {
        exec("CREATE TABLE cty_nb2 (id int, txt text, bi bigint, si smallint)");
        exec("CREATE VIEW cty_nb1 AS SELECT id FROM cty_nb2 WHERE si + bi IN (1, 2)"
                + " AND si + bi BETWEEN 1 AND 2 AND si + bi IS NULL");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_nb2\n"
                        + "  WHERE ((si + bi) = ANY (ARRAY[1::bigint, 2::bigint]))"
                        + " AND (si + bi) >= 1 AND (si + bi) <= 2 AND (si + bi) IS NULL;",
                scalar("SELECT pg_get_viewdef('cty_nb1'::regclass, true)"));

        // A NOT does not bracket what it reads, because a boolean connective is not an operator.
        exec("CREATE VIEW cty_nb3 AS SELECT id FROM cty_nb2 WHERE (si = 1) = true"
                + " AND NOT (si + bi = 1) AND (txt LIKE 'a%') = true");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_nb2\n"
                        + "  WHERE (si = 1) = true AND NOT (si + bi) = 1"
                        + " AND (txt ~~ 'a%'::text) = true;",
                scalar("SELECT pg_get_viewdef('cty_nb3'::regclass, true)"));
    }

    /**
     * A key that is only a number would read as a column's position, so a sort or grouping key
     * that is not a plain column reference is bracketed, a call included.
     */
    @Test
    void aSortOrGroupingKeyIsBracketedUnlessItIsAColumn() throws Exception {
        exec("CREATE TABLE cty_ky (id int, txt text)");
        exec("CREATE VIEW cty_k1v AS SELECT count(*) AS n FROM cty_ky"
                + " GROUP BY txt || 'x', upper(txt), id + 1, id");
        assertEquals(" SELECT count(*) AS n\n"
                        + "   FROM cty_ky\n"
                        + "  GROUP BY (txt || 'x'::text), (upper(txt)), (id + 1), id;",
                scalar("SELECT pg_get_viewdef('cty_k1v'::regclass, true)"));

        exec("CREATE VIEW cty_k2v AS SELECT id FROM cty_ky ORDER BY txt DESC,"
                + " upper(txt) NULLS FIRST, id + 1 DESC, id");
        assertEquals(" SELECT id\n"
                        + "   FROM cty_ky\n"
                        + "  ORDER BY txt DESC, (upper(txt)) NULLS FIRST, (id + 1) DESC, id;",
                scalar("SELECT pg_get_viewdef('cty_k2v'::regclass, true)"));

        exec("CREATE VIEW cty_k3v AS SELECT DISTINCT ON (id + 1, id) id FROM cty_ky");
        assertEquals(" SELECT DISTINCT ON ((id + 1), id) id\n   FROM cty_ky;",
                scalar("SELECT pg_get_viewdef('cty_k3v'::regclass, true)"));

        exec("CREATE VIEW cty_k4v AS SELECT count(*) OVER (PARTITION BY txt || 'x', id"
                + " ORDER BY upper(txt), id) AS n FROM cty_ky");
        assertEquals(" SELECT count(*) OVER (PARTITION BY (txt || 'x'::text), id"
                        + " ORDER BY (upper(txt)), id) AS n\n"
                        + "   FROM cty_ky;",
                scalar("SELECT pg_get_viewdef('cty_k4v'::regclass, true)"));
    }

    /** A CHECK constraint prints the same conversions, in the form that keeps every bracket. */
    @Test
    void aCheckConstraintCarriesTheOperatorsConversions() throws Exception {
        exec("CREATE TABLE cty_cc (id int, vc varchar(10), txt text, cd cidr, ip inet,"
                + " vb bit varying(8), nm numeric, bi bigint, si smallint, ch char(5), nme name,"
                + " CONSTRAINT cty_c1c CHECK (vc = 'a'),"
                + " CONSTRAINT cty_c2c CHECK (cd = '10.0.0.0/8'),"
                + " CONSTRAINT cty_c3c CHECK (vb = B'1010'),"
                + " CONSTRAINT cty_c4c CHECK (nm = bi),"
                + " CONSTRAINT cty_c5c CHECK (vc = ch),"
                + " CONSTRAINT cty_c6c CHECK (upper(vc) = 'A'),"
                + " CONSTRAINT cty_c7c CHECK (vc || 'x' = 'y'),"
                + " CONSTRAINT cty_c8c CHECK (si + bi = 1),"
                + " CONSTRAINT cty_c9c CHECK (vc IN ('a','b')),"
                + " CONSTRAINT cty_cac CHECK (vc LIKE 'a%'),"
                + " CONSTRAINT cty_cbc CHECK (cd >> ip),"
                + " CONSTRAINT cty_ccc CHECK (vc = nme))");
        assertEquals("cty_c1c CHECK (((vc)::text = 'a'::text))\n"
                        + "cty_c2c CHECK (((cd)::inet = '10.0.0.0/8'::inet))\n"
                        + "cty_c3c CHECK ((vb = ('1010'::\"bit\")::bit varying))\n"
                        + "cty_c4c CHECK ((nm = (bi)::numeric))\n"
                        + "cty_c5c CHECK (((vc)::bpchar = ch))\n"
                        + "cty_c6c CHECK ((upper((vc)::text) = 'A'::text))\n"
                        + "cty_c7c CHECK ((((vc)::text || 'x'::text) = 'y'::text))\n"
                        + "cty_c8c CHECK (((si + bi) = 1))\n"
                        + "cty_c9c CHECK (((vc)::text = ANY ((ARRAY['a'::character varying,"
                        + " 'b'::character varying])::text[])))\n"
                        + "cty_cac CHECK (((vc)::text ~~ 'a%'::text))\n"
                        + "cty_cbc CHECK (((cd)::inet >> ip))\n"
                        + "cty_ccc CHECK (((vc)::text = nme))",
                scalar("SELECT string_agg(conname || ' ' || pg_get_constraintdef(oid), chr(10)"
                        + " ORDER BY conname) FROM pg_constraint"
                        + " WHERE conrelid = 'cty_cc'::regclass AND contype = 'c'"));
    }

    // ------------------------------------------------------------ a rule belongs to a relation in a schema

    /** Two schemas, each holding a relation of the same name with a rule of its own. */
    private static void twoSchemas(String tag) throws SQLException {
        exec("CREATE SCHEMA cty_s" + tag);
        exec("CREATE TABLE public.cty_t" + tag + " (i int)");
        exec("CREATE TABLE public.cty_g" + tag + " (i int)");
        exec("CREATE TABLE cty_s" + tag + ".cty_t" + tag + " (i int)");
        exec("CREATE TABLE cty_s" + tag + ".cty_g" + tag + " (i int)");
        exec("CREATE RULE cty_rp" + tag + " AS ON INSERT TO public.cty_t" + tag
                + " DO ALSO INSERT INTO public.cty_g" + tag + " VALUES (new.i)");
        exec("CREATE RULE cty_rs" + tag + " AS ON INSERT TO cty_s" + tag + ".cty_t" + tag
                + " DO ALSO INSERT INTO cty_s" + tag + ".cty_g" + tag + " VALUES (new.i + 100)");
    }

    @Test
    void pgRulesNamesTheSchemaOfTheRelationTheRuleIsOn() throws Exception {
        twoSchemas("a");
        assertEquals("public/cty_ta/cty_rpa,cty_sa/cty_ta/cty_rsa",
                column("SELECT schemaname || '/' || tablename || '/' || rulename FROM pg_rules"
                        + " WHERE rulename IN ('cty_rpa','cty_rsa') ORDER BY rulename"));
        assertEquals("cty_sa/cty_ta",
                scalar("SELECT n.nspname || '/' || c.relname FROM pg_rewrite r"
                        + " JOIN pg_class c ON c.oid = r.ev_class"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE r.rulename = 'cty_rsa'"));
    }

    @Test
    void aWriteFiresTheRulesOfTheRelationItsNameReaches() throws Exception {
        twoSchemas("b");
        exec("INSERT INTO public.cty_tb VALUES (1)");
        exec("INSERT INTO cty_sb.cty_tb VALUES (2)");
        assertEquals(1, num("SELECT count(*) FROM public.cty_gb"));
        // 102, not 2: the row came from the rule on cty_sb.cty_tb and from no other.
        assertEquals(102, num("SELECT i FROM cty_sb.cty_gb"));
    }

    @Test
    void aRelationInAnotherSchemaLeavesThisOnesRulesAlone() throws Exception {
        twoSchemas("c");
        exec("DROP TABLE cty_sc.cty_tc");
        exec("CREATE TABLE cty_sc.cty_tc (i int)");
        assertEquals(1, num("SELECT count(*) FROM pg_rules WHERE rulename = 'cty_rpc'"));
        assertEquals("t", scalar("SELECT relhasrules FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE c.relname = 'cty_tc' AND n.nspname = 'public'"));
        exec("INSERT INTO public.cty_tc VALUES (4)");
        assertEquals(1, num("SELECT count(*) FROM public.cty_gc"));

        // Nor does dropping the whole schema the other relation is in.
        exec("DROP SCHEMA cty_sc CASCADE");
        assertEquals(1, num("SELECT count(*) FROM pg_rules WHERE rulename = 'cty_rpc'"));
        exec("INSERT INTO public.cty_tc VALUES (6)");
        assertEquals(2, num("SELECT count(*) FROM public.cty_gc"));
    }

    @Test
    void aRolledBackCascadePutsBackTheRuleItTookOffAnotherRelation() throws Exception {
        exec("CREATE TABLE cty_ra (i int)");
        exec("CREATE TABLE cty_rb (i int)");
        exec("CREATE RULE cty_rcr AS ON INSERT TO cty_ra DO ALSO INSERT INTO cty_rb VALUES (new.i)");
        exec("BEGIN");
        exec("DROP TABLE cty_rb CASCADE");
        assertEquals(0, num("SELECT count(*) FROM pg_rules WHERE rulename = 'cty_rcr'"));
        exec("ROLLBACK");
        // The rule sits on cty_ra, which the statement never named.
        assertEquals(1, num("SELECT count(*) FROM pg_rules WHERE rulename = 'cty_rcr'"));
        assertEquals("cty_ra", scalar("SELECT tablename FROM pg_rules"
                + " WHERE rulename = 'cty_rcr'"));
        exec("INSERT INTO cty_ra VALUES (5)");
        assertEquals(1, num("SELECT count(*) FROM cty_rb"));
    }

    @Test
    void dropRuleAlterRuleAndDropTriggerReachTheRelationTheQualifierNames() throws Exception {
        exec("CREATE SCHEMA cty_qs");
        exec("CREATE TABLE cty_qs.cty_qt (i int)");
        exec("CREATE TABLE cty_qs.cty_ql (i int)");
        exec("CREATE RULE cty_qr AS ON INSERT TO cty_qs.cty_qt DO ALSO"
                + " INSERT INTO cty_qs.cty_ql VALUES (new.i)");
        assertEquals("42704", stateOf("DROP RULE cty_nope ON cty_qs.cty_qt"));
        assertEquals("rule \"cty_nope\" for relation \"cty_qt\" does not exist",
                messageOf("DROP RULE cty_nope ON cty_qs.cty_qt"));

        exec("ALTER RULE cty_qr ON cty_qs.cty_qt RENAME TO cty_qr2");
        assertEquals("cty_qr2", scalar("SELECT rulename FROM pg_rules"
                + " WHERE tablename = 'cty_qt'"));
        // The renamed rule still holds what its action names.
        assertEquals("2BP01", stateOf("DROP TABLE cty_qs.cty_ql"));
        assertEquals("rule cty_qr2 on table cty_qs.cty_qt depends on table cty_qs.cty_ql",
                detailOf("DROP TABLE cty_qs.cty_ql"));
        exec("DROP RULE cty_qr2 ON cty_qs.cty_qt");
        assertEquals(0, num("SELECT count(*) FROM pg_rules WHERE tablename = 'cty_qt'"));

        exec("CREATE FUNCTION cty_qs.cty_tf() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$"
                + " LANGUAGE plpgsql");
        exec("CREATE TRIGGER cty_tr BEFORE INSERT ON cty_qs.cty_qt"
                + " FOR EACH ROW EXECUTE FUNCTION cty_qs.cty_tf()");
        assertEquals("42704", stateOf("DROP TRIGGER cty_nope ON cty_qs.cty_qt"));
        assertEquals("trigger \"cty_nope\" for table \"cty_qt\" does not exist",
                messageOf("DROP TRIGGER cty_nope ON cty_qs.cty_qt"));
        exec("DROP TRIGGER cty_tr ON cty_qs.cty_qt");
        assertEquals(0, num("SELECT count(*) FROM pg_trigger WHERE tgname = 'cty_tr'"));
        exec("DROP SCHEMA cty_qs CASCADE");
    }

    // ------------------------------------------------------------ what an action may read

    @Test
    void theRelationHoldingTheNameIsNamedInTheDetail() throws Exception {
        exec("CREATE TABLE cty_ca2 (i int, j int)");
        exec("CREATE TABLE cty_cb2 (i int, k int)");
        exec("CREATE TABLE cty_cc2 (zx int, zy int, zv int)");
        // i is a column of the ruled relation and of the relation being written to; neither is in
        // the namespace an action's values are read in.
        String both = "CREATE RULE cty_x1 AS ON INSERT TO cty_ca2 DO ALSO"
                + " INSERT INTO cty_cb2 VALUES (i, 2)";
        assertEquals("42703", stateOf(both));
        assertEquals("There are columns named \"i\", but they are in tables that cannot be"
                + " referenced from this part of the query.", detailOf(both));
        assertEquals("Try using a table-qualified name.", hintOf(both));
        assertEquals(0, num("SELECT count(*) FROM pg_rules WHERE rulename = 'cty_x1'"));

        // One relation holding the name is named, and there is no hint beside it.
        String one = "CREATE RULE cty_x2 AS ON INSERT TO cty_ca2 DO ALSO"
                + " INSERT INTO cty_cb2 (i) VALUES (k)";
        assertEquals("There is a column named \"k\" in table \"cty_cb2\", but it cannot be"
                + " referenced from this part of the query.", detailOf(one));
        assertNull(hintOf(one));
    }

    @Test
    void aNearMissInsideAnActionCarriesTheSuggestion() throws Exception {
        exec("CREATE TABLE cty_na (i int, j int)");
        exec("CREATE TABLE cty_nb4 (i int, k int)");
        exec("CREATE TABLE cty_nc4 (zx int, zy int, zv int)");
        assertEquals("Perhaps you meant to reference the column \"cty_nb4.k\".",
                hintOf("CREATE RULE cty_x3 AS ON INSERT TO cty_na DO ALSO"
                        + " INSERT INTO cty_nb4 VALUES (kk, 2)"));
        // The relation written to and read from is two entries of the range table, and PostgreSQL
        // offers its column once for each of them.
        assertEquals("Perhaps you meant to reference the column \"cty_nb4.k\" or the column"
                        + " \"cty_nb4.k\".",
                hintOf("CREATE RULE cty_x4 AS ON INSERT TO cty_na DO ALSO"
                        + " INSERT INTO cty_nb4 SELECT i, 2 FROM cty_nb4 WHERE kk = 1"));
        // Three columns equally close is no suggestion at all.
        assertNull(hintOf("CREATE RULE cty_x5 AS ON INSERT TO cty_na DO ALSO"
                + " INSERT INTO cty_nc4 (zx) VALUES (zz)"));

        // The rule's own qualification reads the ruled relation's columns, and misses there are
        // offered under the names that namespace holds them by.
        exec("CREATE RULE cty_ok0 AS ON INSERT TO cty_na WHERE j > 1 DO INSTEAD NOTHING");
        assertEquals(1, num("SELECT count(*) FROM pg_rules WHERE rulename = 'cty_ok0'"));
        assertEquals("Perhaps you meant to reference the column \"old.j\" or the column"
                        + " \"new.j\".",
                hintOf("CREATE RULE cty_ok0b AS ON INSERT TO cty_na WHERE jj > 1"
                        + " DO INSTEAD NOTHING"));
    }

    @Test
    void whatAnActionHandsBackIsCheckedAsTheRuleIsWritten() throws Exception {
        exec("CREATE TABLE cty_ha (i int, j int)");
        exec("CREATE TABLE cty_hb (i int, k int)");
        assertEquals("0A000", stateOf("CREATE RULE cty_x6 AS ON INSERT TO cty_ha DO ALSO"
                + " INSERT INTO cty_hb VALUES (new.i, 2) RETURNING i"));
        assertEquals("RETURNING lists are not supported in non-INSTEAD rules",
                messageOf("CREATE RULE cty_x6 AS ON INSERT TO cty_ha DO ALSO"
                        + " INSERT INTO cty_hb VALUES (new.i, 2) RETURNING i"));
        assertEquals("42P17", stateOf("CREATE RULE cty_x7 AS ON INSERT TO cty_ha DO INSTEAD"
                + " INSERT INTO cty_hb VALUES (new.i, 2) RETURNING i"));
        assertEquals("RETURNING list has too few entries",
                messageOf("CREATE RULE cty_x7 AS ON INSERT TO cty_ha DO INSTEAD"
                        + " INSERT INTO cty_hb VALUES (new.i, 2) RETURNING i"));
        assertEquals("RETURNING list has too many entries",
                messageOf("CREATE RULE cty_x8 AS ON INSERT TO cty_ha DO INSTEAD"
                        + " INSERT INTO cty_hb VALUES (new.i, 2) RETURNING i, k, 3"));

        // A list of the right length, and a star, are accepted.
        exec("CREATE RULE cty_ok1 AS ON INSERT TO cty_ha DO ALSO"
                + " INSERT INTO cty_hb SELECT i, 2 FROM cty_hb");
        exec("CREATE RULE cty_ok2 AS ON UPDATE TO cty_ha DO ALSO"
                + " UPDATE cty_hb SET k = 1 WHERE i = 3");
        exec("CREATE RULE cty_ok3 AS ON INSERT TO cty_ha DO INSTEAD"
                + " INSERT INTO cty_hb VALUES (new.i, 2) RETURNING i, k");
        exec("CREATE RULE cty_ok4 AS ON INSERT TO cty_ha DO INSTEAD"
                + " INSERT INTO cty_hb VALUES (new.i, 2) RETURNING *");
        assertEquals("cty_ok1,cty_ok2,cty_ok3,cty_ok4",
                column("SELECT rulename FROM pg_rules WHERE tablename = 'cty_ha' ORDER BY 1"));
    }

    // ------------------------------------------------------------ one DROP may name several objects

    @Test
    void everyKindThatTakesAListDropsEveryNameInIt() throws Exception {
        exec("CREATE TABLE cty_da (i int)");
        exec("CREATE TABLE cty_db2 (i int)");
        exec("DROP TABLE public.cty_da, public.cty_db2");
        assertEquals(0, num("SELECT count(*) FROM pg_class"
                + " WHERE relname IN ('cty_da','cty_db2')"));

        exec("CREATE VIEW cty_dv1 AS SELECT 1 AS x");
        exec("CREATE VIEW cty_dv2 AS SELECT 2 AS x");
        exec("DROP VIEW cty_dv1, cty_dv2");
        assertEquals(0, num("SELECT count(*) FROM pg_class"
                + " WHERE relname IN ('cty_dv1','cty_dv2')"));

        exec("CREATE SEQUENCE cty_dq1");
        exec("CREATE SEQUENCE cty_dq2");
        exec("DROP SEQUENCE public.cty_dq1, public.cty_dq2");
        assertEquals(0, num("SELECT count(*) FROM pg_class"
                + " WHERE relname IN ('cty_dq1','cty_dq2')"));

        exec("CREATE TABLE cty_dix (i int, j int)");
        exec("CREATE INDEX cty_dx1 ON cty_dix (i)");
        exec("CREATE INDEX cty_dx2 ON cty_dix (j)");
        exec("DROP INDEX public.cty_dx1, public.cty_dx2");
        assertEquals(0, num("SELECT count(*) FROM pg_class"
                + " WHERE relname IN ('cty_dx1','cty_dx2')"));
        exec("DROP TABLE cty_dix");

        exec("CREATE TYPE cty_de1 AS ENUM ('a')");
        exec("CREATE TYPE cty_de2 AS ENUM ('b')");
        exec("DROP TYPE public.cty_de1, public.cty_de2");
        assertEquals(0, num("SELECT count(*) FROM pg_type"
                + " WHERE typname IN ('cty_de1','cty_de2')"));

        exec("CREATE DOMAIN cty_dd1 AS int");
        exec("CREATE DOMAIN cty_dd2 AS int");
        exec("DROP DOMAIN cty_dd1, cty_dd2");
        assertEquals(0, num("SELECT count(*) FROM pg_type"
                + " WHERE typname IN ('cty_dd1','cty_dd2')"));

        exec("CREATE MATERIALIZED VIEW cty_dm1 AS SELECT 1 AS x");
        exec("CREATE MATERIALIZED VIEW cty_dm2 AS SELECT 2 AS x");
        exec("DROP MATERIALIZED VIEW public.cty_dm1, public.cty_dm2");
        assertEquals(0, num("SELECT count(*) FROM pg_class"
                + " WHERE relname IN ('cty_dm1','cty_dm2')"));

        exec("CREATE SCHEMA cty_ds1");
        exec("CREATE SCHEMA cty_ds2");
        exec("DROP SCHEMA cty_ds1, cty_ds2");
        assertEquals(0, num("SELECT count(*) FROM pg_namespace"
                + " WHERE nspname IN ('cty_ds1','cty_ds2')"));

        exec("CREATE FUNCTION cty_df1() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql");
        exec("CREATE FUNCTION cty_df2(int) RETURNS int AS $$ SELECT 2 $$ LANGUAGE sql");
        exec("DROP FUNCTION cty_df1(), cty_df2(int)");
        assertEquals(0, num("SELECT count(*) FROM pg_proc"
                + " WHERE proname IN ('cty_df1','cty_df2')"));
    }

    @Test
    void everyNameIsLookedForInTheSchemaItWasWrittenWith() throws Exception {
        exec("CREATE SCHEMA cty_dsq");
        exec("CREATE TABLE cty_dta (i int)");
        exec("CREATE TABLE cty_dsq.cty_dtb (i int)");
        exec("CREATE TABLE cty_dtc (i int)");
        exec("DROP TABLE public.cty_dta, cty_dsq.cty_dtb, cty_dtc");
        assertEquals(0, num("SELECT count(*) FROM pg_class"
                + " WHERE relname IN ('cty_dta','cty_dtb','cty_dtc')"));
        exec("DROP SCHEMA cty_dsq");
    }

    @Test
    void oneObjectNamedTwiceIsDroppedOnce() throws Exception {
        exec("CREATE VIEW cty_y1 AS SELECT 1 AS x");
        exec("DROP VIEW cty_y1, cty_y1");
        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname = 'cty_y1'"));

        exec("CREATE VIEW cty_y2 AS SELECT 1 AS x");
        exec("DROP VIEW cty_y2, public.cty_y2");
        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname = 'cty_y2'"));

        exec("CREATE TABLE cty_y3 (i int)");
        exec("DROP TABLE cty_y3, cty_y3");
        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname = 'cty_y3'"));
    }

    @Test
    void aNameThatReachesNothingTakesTheWholeStatementWithIt() throws Exception {
        exec("CREATE VIEW cty_kk1 AS SELECT 1 AS x");
        assertEquals("42P01", stateOf("DROP VIEW cty_kk1, cty_nosuch"));
        assertEquals("view \"cty_nosuch\" does not exist",
                messageOf("DROP VIEW cty_kk1, cty_nosuch"));
        assertEquals(1, num("SELECT count(*) FROM pg_class WHERE relname = 'cty_kk1'"));
        assertEquals("42P01", stateOf("DROP VIEW cty_nosuch, cty_kk1"));
        assertEquals(1, num("SELECT count(*) FROM pg_class WHERE relname = 'cty_kk1'"));
        exec("DROP VIEW IF EXISTS cty_nosuch, cty_kk1");
        assertEquals(0, num("SELECT count(*) FROM pg_class WHERE relname = 'cty_kk1'"));
    }

    /**
     * The whole set is settled before anything is looked for that would be left pointing at it,
     * so an object the same statement drops is no reason to refuse.
     */
    @Test
    void whatTheSameDropTakesDownIsNoDependency() throws Exception {
        exec("CREATE TABLE cty_dp2 (i int PRIMARY KEY)");
        exec("CREATE TABLE cty_dc2 (i int REFERENCES cty_dp2(i))");
        exec("DROP TABLE cty_dp2, cty_dc2");
        assertEquals(0, num("SELECT count(*) FROM pg_class"
                + " WHERE relname IN ('cty_dp2','cty_dc2')"));

        exec("CREATE TABLE cty_drt (i int)");
        exec("CREATE TABLE cty_drl (i int)");
        exec("CREATE RULE cty_drr AS ON INSERT TO cty_drt DO ALSO"
                + " INSERT INTO cty_drl VALUES (new.i)");
        exec("DROP TABLE cty_drl, cty_drt");
        assertEquals(0, num("SELECT count(*) FROM pg_class"
                + " WHERE relname IN ('cty_drt','cty_drl')"));

        exec("CREATE VIEW cty_du1 AS SELECT 1 AS x");
        exec("CREATE VIEW cty_du2 AS SELECT x FROM cty_du1");
        exec("DROP VIEW cty_du1, cty_du2");
        assertEquals(0, num("SELECT count(*) FROM pg_class"
                + " WHERE relname IN ('cty_du1','cty_du2')"));
    }

    /**
     * A statement that names several says only that it cannot have what it asked for; one that
     * names a single object names it.
     */
    @Test
    void aDependentTheListDoesNotNameRefusesTheWholeSet() throws Exception {
        exec("CREATE TABLE cty_ep (i int PRIMARY KEY)");
        exec("CREATE TABLE cty_ec (i int REFERENCES cty_ep(i))");
        exec("CREATE TABLE cty_eo (i int)");
        assertEquals("2BP01", stateOf("DROP TABLE cty_ep, cty_eo"));
        assertEquals("cannot drop desired object(s) because other objects depend on them",
                messageOf("DROP TABLE cty_ep, cty_eo"));
        assertEquals("constraint cty_ec_i_fkey on table cty_ec depends on table cty_ep",
                detailOf("DROP TABLE cty_ep, cty_eo"));
        assertEquals("cannot drop table cty_ep because other objects depend on it",
                messageOf("DROP TABLE cty_ep"));
        exec("DROP TABLE cty_ec, cty_ep");
        exec("DROP TABLE cty_eo");

        exec("CREATE VIEW cty_eg1 AS SELECT 1 AS x");
        exec("CREATE VIEW cty_eg2 AS SELECT x FROM cty_eg1");
        exec("CREATE VIEW cty_eg3 AS SELECT x FROM cty_eg2");
        assertEquals("cannot drop desired object(s) because other objects depend on them",
                messageOf("DROP VIEW cty_eg1, cty_eg2"));
        assertEquals("view cty_eg3 depends on view cty_eg2",
                detailOf("DROP VIEW cty_eg1, cty_eg2"));
        exec("DROP VIEW cty_eg3, cty_eg2, cty_eg1");
        assertEquals(0, num("SELECT count(*) FROM pg_class"
                + " WHERE relname IN ('cty_eg1','cty_eg2','cty_eg3')"));
    }

    /** The kinds whose grammar names one object at a time have no list at all. */
    @Test
    void aCommaWhereTheGrammarHasNoListIsASyntaxError() throws Exception {
        exec("CREATE TABLE cty_fo (i int)");
        exec("CREATE TABLE cty_fol (i int)");
        exec("CREATE RULE cty_fo1 AS ON INSERT TO cty_fo DO ALSO"
                + " INSERT INTO cty_fol VALUES (new.i)");
        exec("CREATE RULE cty_fo2 AS ON UPDATE TO cty_fo DO ALSO"
                + " INSERT INTO cty_fol VALUES (new.i)");
        assertEquals("42601", stateOf("DROP RULE cty_fo1 ON cty_fo, cty_fo2 ON cty_fo"));
        assertEquals("syntax error at or near \",\"",
                messageOf("DROP RULE cty_fo1 ON cty_fo, cty_fo2 ON cty_fo"));
        assertEquals(2, num("SELECT count(*) FROM pg_rules"
                + " WHERE rulename IN ('cty_fo1','cty_fo2')"));
        exec("DROP TABLE cty_fo CASCADE");
        exec("DROP TABLE cty_fol");

        assertEquals("42601", stateOf("DROP CAST (int AS text), (text AS int)"));
        assertEquals("42601",
                stateOf("DROP OPERATOR CLASS cty_oc1 USING btree, cty_oc2 USING btree"));
    }

    /** A rule depends on the relations its actions name, and on those alone. */
    @Test
    void aRuleDependsOnTheRelationInTheSchemaItsActionNamed() throws Exception {
        exec("CREATE SCHEMA cty_rsx");
        exec("CREATE TABLE cty_rst (i int)");
        exec("CREATE TABLE cty_rsx.cty_rsb (i int)");
        exec("CREATE TABLE cty_rsb (i int)");
        exec("CREATE RULE cty_rsr AS ON INSERT TO cty_rst DO ALSO"
                + " INSERT INTO cty_rsx.cty_rsb VALUES (new.i)");
        // The public relation of the same name is nothing to the rule.
        exec("DROP TABLE cty_rsb");
        assertEquals("2BP01", stateOf("DROP TABLE cty_rsx.cty_rsb"));
        assertEquals("cannot drop table cty_rsx.cty_rsb because other objects depend on it",
                messageOf("DROP TABLE cty_rsx.cty_rsb"));
        exec("DROP TABLE cty_rst CASCADE");
        exec("DROP SCHEMA cty_rsx CASCADE");
        assertEquals(0, num("SELECT count(*) FROM pg_rules WHERE rulename = 'cty_rsr'"));
    }

    // ------------------------------------------------------------ the scope an ON CONFLICT action is read in

    /**
     * The row already in the relation and the row being written are both in scope, and EXCLUDED
     * holds every column the relation holds, so a column written without a relation name answers
     * to both and PostgreSQL refuses to choose.
     */
    @Test
    void aBareColumnInsideDoUpdateIsAmbiguous() throws Exception {
        exec("CREATE TABLE cty_oc3 (i int PRIMARY KEY, k int)");
        assertEquals("42702", stateOf("INSERT INTO cty_oc3 VALUES (1,2) ON CONFLICT (i)"
                + " DO UPDATE SET k = 9 WHERE i = 1"));
        assertEquals("column reference \"i\" is ambiguous",
                messageOf("INSERT INTO cty_oc3 VALUES (1,2) ON CONFLICT (i)"
                        + " DO UPDATE SET k = 9 WHERE i = 1"));
        assertEquals("column reference \"k\" is ambiguous",
                messageOf("INSERT INTO cty_oc3 VALUES (1,2) ON CONFLICT (i)"
                        + " DO UPDATE SET k = k + 1"));
        assertEquals("42702", stateOf("INSERT INTO cty_oc3 VALUES (1,2) ON CONFLICT (i)"
                + " DO UPDATE SET k = i"));
        // Either name written out settles it.
        exec("INSERT INTO cty_oc3 VALUES (1,2) ON CONFLICT (i) DO UPDATE SET k = 9"
                + " WHERE cty_oc3.k = 1");
        exec("INSERT INTO cty_oc3 VALUES (1,3) ON CONFLICT (i) DO UPDATE SET k = EXCLUDED.k");
        assertEquals(3, num("SELECT k FROM cty_oc3 WHERE i = 1"));
    }

    @Test
    void anAliasOnTheRelationDoesNotSettleABareName() throws Exception {
        exec("CREATE TABLE cty_al (i int PRIMARY KEY, k int)");
        exec("INSERT INTO cty_al VALUES (1,1)");
        assertEquals("42702", stateOf("INSERT INTO cty_al AS bb VALUES (1,2) ON CONFLICT (i)"
                + " DO UPDATE SET k = k + 1"));
        exec("INSERT INTO cty_al AS bb VALUES (1,2) ON CONFLICT (i) DO UPDATE SET k = bb.k + 1");
        assertEquals(2, num("SELECT k FROM cty_al WHERE i = 1"));
    }

    @Test
    void theActionIsReadWhileTheStatementIsPlannedSoNoRowNeedConflict() throws Exception {
        exec("CREATE TABLE cty_pl (i int PRIMARY KEY, k int)");
        // Nothing to conflict with.
        assertEquals("42702", stateOf("INSERT INTO cty_pl VALUES (99,2) ON CONFLICT (i)"
                + " DO UPDATE SET k = k + 1"));
        // And no row written at all.
        assertEquals("42702", stateOf("INSERT INTO cty_pl SELECT 1,2 WHERE false"
                + " ON CONFLICT (i) DO UPDATE SET k = k + 1"));
        assertEquals(0, num("SELECT count(*) FROM cty_pl"));
        // DO NOTHING has no action to read a column in.
        exec("INSERT INTO cty_pl VALUES (1,5) ON CONFLICT (i) DO NOTHING");
        assertEquals(1, num("SELECT count(*) FROM cty_pl"));
    }

    @Test
    void theOrderTheOnConflictClauseIsReadIn() throws Exception {
        exec("CREATE TABLE cty_or (i int PRIMARY KEY, j text, k int UNIQUE)");
        exec("ALTER TABLE cty_or ADD CONSTRAINT cty_ock CHECK (i > 0)");
        // A constraint that is not there comes before the action.
        assertEquals("42704", stateOf("INSERT INTO cty_or VALUES (9,'x',90)"
                + " ON CONFLICT ON CONSTRAINT cty_nosuch DO UPDATE SET j = j"));
        assertEquals("constraint \"cty_nosuch\" for table \"cty_or\" does not exist",
                messageOf("INSERT INTO cty_or VALUES (9,'x',90)"
                        + " ON CONFLICT ON CONSTRAINT cty_nosuch DO UPDATE SET j = j"));
        // A constraint with no index behind it comes after it.
        assertEquals("42702", stateOf("INSERT INTO cty_or VALUES (9,'x',90)"
                + " ON CONFLICT ON CONSTRAINT cty_ock DO UPDATE SET j = j"));
        assertEquals("42809", stateOf("INSERT INTO cty_or VALUES (9,'x',90)"
                + " ON CONFLICT ON CONSTRAINT cty_ock DO UPDATE SET j = 'y'"));
        assertEquals("constraint in ON CONFLICT clause has no associated index",
                messageOf("INSERT INTO cty_or VALUES (9,'x',90)"
                        + " ON CONFLICT ON CONSTRAINT cty_ock DO UPDATE SET j = 'y'"));
        // An arbiter with no unique index behind it comes after it too.
        assertEquals("42702", stateOf("INSERT INTO cty_or VALUES (9,'x',90)"
                + " ON CONFLICT (j) DO UPDATE SET j = j"));
        assertEquals("42P10", stateOf("INSERT INTO cty_or VALUES (9,'x',90)"
                + " ON CONFLICT (j) DO UPDATE SET j = 'y'"));
        // An arbiter naming no column of the relation comes before it, and is not named as the
        // relation's own; a column a SET writes to is.
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("INSERT INTO cty_or VALUES (9,'x',90)"
                        + " ON CONFLICT (nosuchcol) DO UPDATE SET j = j"));
        assertEquals("42703", stateOf("INSERT INTO cty_or VALUES (9,'x',90)"
                + " ON CONFLICT (nosuchcol) DO NOTHING"));
        assertEquals("column \"nosuchcol\" of relation \"cty_or\" does not exist",
                messageOf("INSERT INTO cty_or VALUES (9,'x',90)"
                        + " ON CONFLICT (i) DO UPDATE SET nosuchcol = 'x'"));
        // The assignments are read before the WHERE beside them.
        assertEquals("42702", stateOf("INSERT INTO cty_or VALUES (9,'x',90)"
                + " ON CONFLICT (i) DO UPDATE SET j = j WHERE nosuchcol = 1"));
        assertEquals(0, num("SELECT count(*) FROM cty_or"));
    }

    // ------------------------------------------------------------ a type the search path does not reach

    /**
     * A cast of NULL is still a cast to a named type, so the name is looked up the way every
     * other name is: in the schemas the search path reaches, and nowhere else.
     */
    @Test
    void aNullCastDoesNotReachATypeOutsideTheSearchPath() throws Exception {
        exec("CREATE SCHEMA cty_hid");
        exec("CREATE DOMAIN cty_hid.cty_hd AS int NOT NULL");
        exec("CREATE TYPE cty_hid.cty_hc AS (x int)");
        exec("CREATE TYPE cty_hid.cty_hr AS RANGE (subtype = int4)");
        exec("CREATE TYPE cty_hid.cty_he AS ENUM ('a','b')");
        exec("CREATE TABLE cty_hrel (k int)");

        assertEquals("42704", stateOf("SELECT NULL::cty_hd"));
        assertEquals("type \"cty_hd\" does not exist", messageOf("SELECT NULL::cty_hd"));
        assertEquals("type \"cty_hc\" does not exist", messageOf("SELECT NULL::cty_hc"));
        assertEquals("type \"cty_hr\" does not exist", messageOf("SELECT NULL::cty_hr"));
        assertEquals("type \"cty_he\" does not exist", messageOf("SELECT NULL::cty_he"));
        // The other spelling of a cast, and the array, argument and constructor positions.
        assertEquals("type \"cty_hd\" does not exist", messageOf("SELECT CAST(NULL AS cty_hd)"));
        assertEquals("type \"cty_hd[]\" does not exist", messageOf("SELECT NULL::cty_hd[]"));
        assertEquals("type \"cty_hd\" does not exist",
                messageOf("SELECT coalesce(NULL::cty_hd, 1)"));
        assertEquals("type \"cty_he\" does not exist", messageOf("SELECT array[NULL::cty_he]"));
        // A cast of a value was already refused, and still is.
        assertEquals("type \"cty_hd\" does not exist", messageOf("SELECT 1::cty_hd"));
        assertEquals("type \"cty_hc\" does not exist", messageOf("SELECT row(1)::cty_hc"));
        assertEquals("type \"cty_he\" does not exist", messageOf("SELECT 'a'::cty_he"));

        // A relation carries a row type that answers to the relation namespace, not this one.
        assertEquals("t", scalar("SELECT (NULL::cty_hrel) IS NULL AS n"));
    }

    @Test
    void theQualifiedNameReachesTheTypeAndSoDoesTheSearchPath() throws Exception {
        exec("CREATE SCHEMA cty_hd2");
        exec("CREATE DOMAIN cty_hd2.cty_qd AS int NOT NULL");
        exec("CREATE TYPE cty_hd2.cty_qc AS (x int)");
        // Reaching the domain is the whole of it: its NOT NULL is what refuses the cast.
        org.postgresql.util.ServerErrorMessage m = fieldsOf("SELECT NULL::cty_hd2.cty_qd");
        assertEquals("23502", m.getSQLState());
        assertEquals("domain cty_hd2.cty_qd does not allow null values", m.getMessage());
        assertEquals("cty_qd", m.getDatatype());
        assertEquals("t", scalar("SELECT (NULL::cty_hd2.cty_qc) IS NULL AS n"));

        // On the path the bare name means it again, and the constraint is reported bare too.
        exec("SET search_path = public, cty_hd2");
        try {
            assertEquals("domain cty_qd does not allow null values",
                    messageOf("SELECT NULL::cty_qd"));
            assertEquals("t", scalar("SELECT (NULL::cty_qc) IS NULL AS n"));
            // An array of a NOT NULL domain is not itself refused.
            assertEquals("t", scalar("SELECT (NULL::cty_qd[]) IS NULL AS n"));
        } finally {
            exec("RESET search_path");
        }
        assertEquals("42704", stateOf("SELECT NULL::cty_qc"));
    }

    // ------------------------------------------------------------ a composite value is held to its shape

    /**
     * A record constructor of the wrong width is a cast that cannot be made; the same value
     * written as text is input the composite's own reader cannot read, and the two are told
     * apart.
     */
    @Test
    void aCompositeLiteralIsReadByTheCompositesOwnReader() throws Exception {
        exec("CREATE TYPE cty_pr AS (a int, b int)");
        exec("CREATE TABLE cty_prel (k int)");
        assertEquals("42846", stateOf("SELECT row(1,2,3)::cty_pr"));
        assertEquals("cannot cast type record to cty_pr", messageOf("SELECT row(1,2,3)::cty_pr"));
        assertEquals("Input has too many columns.", detailOf("SELECT row(1,2,3)::cty_pr"));
        assertEquals("Input has too few columns.", detailOf("SELECT row(1)::cty_pr"));

        assertEquals("22P02", stateOf("SELECT '(1,2,3)'::cty_pr"));
        assertEquals("malformed record literal: \"(1,2,3)\"",
                messageOf("SELECT '(1,2,3)'::cty_pr"));
        assertEquals("Too many columns.", detailOf("SELECT '(1,2,3)'::cty_pr"));
        assertEquals("Too few columns.", detailOf("SELECT '(1)'::cty_pr"));
        assertEquals("Too few columns.", detailOf("SELECT '()'::cty_pr"));
        assertEquals("Missing left parenthesis.", detailOf("SELECT 'abc'::cty_pr"));
        assertEquals("Unexpected end of input.", detailOf("SELECT '(1,2'::cty_pr"));

        // Text that reached the cast as a value of a known type is read the same way.
        assertEquals("Too many columns.", detailOf("SELECT '(1,2,3)'::text::cty_pr"));
        assertEquals("Too many columns.", detailOf("SELECT (SELECT '(1,2,3)')::cty_pr"));

        // A relation's row type counts its columns the same way, and so does an array element.
        assertEquals("malformed record literal: \"(1,2,3)\"",
                messageOf("SELECT '(1,2,3)'::cty_prel"));
        assertEquals("Too many columns.", detailOf("SELECT '{\"(1,2,3)\"}'::cty_pr[]"));
        assertEquals("Input has too many columns.",
                detailOf("SELECT ARRAY[row(1,2,3)]::cty_pr[]"));
    }

    @Test
    void aWriteOfTheWrongShapeIsRefusedOnEveryWritePath() throws Exception {
        exec("CREATE TYPE cty_wp AS (a int, b int)");
        exec("CREATE TABLE cty_hp (k int, c cty_wp)");
        assertEquals("Input has too many columns.",
                detailOf("INSERT INTO cty_hp VALUES (1, row(1,2,3))"));
        assertEquals("Input has too few columns.",
                detailOf("INSERT INTO cty_hp VALUES (1, row(1))"));
        assertEquals("Too many columns.", detailOf("INSERT INTO cty_hp VALUES (1, '(1,2,3)')"));
        assertEquals("Too few columns.", detailOf("INSERT INTO cty_hp VALUES (1, '(1)')"));
        assertEquals("Missing left parenthesis.",
                detailOf("INSERT INTO cty_hp VALUES (1, 'abc')"));
        assertEquals("Input has too many columns.",
                detailOf("INSERT INTO cty_hp (k, c) VALUES (1, row(1,2,3))"));

        // The row of the right shape is stored, and nothing above was.
        exec("INSERT INTO cty_hp VALUES (1, row(1,2))");
        assertEquals("(1,2)", scalar("SELECT c::text FROM cty_hp"));
        assertEquals(1, num("SELECT count(*) FROM cty_hp"));

        assertEquals("Input has too many columns.",
                detailOf("UPDATE cty_hp SET c = row(9,8,7)"));
        assertEquals("Too many columns.", detailOf("UPDATE cty_hp SET c = '(9,8,7)'"));
        assertEquals("Input has too few columns.", detailOf("UPDATE cty_hp SET c = row(9)"));
        assertEquals("Too few columns.", detailOf("UPDATE cty_hp SET c = '(9)'"));
        assertEquals("(1,2)", scalar("SELECT c::text FROM cty_hp"));

        // Both arms of a MERGE take the same route.
        assertEquals("Input has too many columns.",
                detailOf("MERGE INTO cty_hp t USING (SELECT 2 AS k) s ON t.k = s.k"
                        + " WHEN NOT MATCHED THEN INSERT (k, c) VALUES (s.k, row(1,2,3))"));
        assertEquals("Too many columns.",
                detailOf("MERGE INTO cty_hp t USING (SELECT 2 AS k) s ON t.k = s.k"
                        + " WHEN NOT MATCHED THEN INSERT (k, c) VALUES (s.k, '(1,2,3)')"));
        assertEquals("Input has too many columns.",
                detailOf("MERGE INTO cty_hp t USING (SELECT 1 AS k) s ON t.k = s.k"
                        + " WHEN MATCHED THEN UPDATE SET c = row(1,2,3)"));
        assertEquals("Too many columns.",
                detailOf("MERGE INTO cty_hp t USING (SELECT 1 AS k) s ON t.k = s.k"
                        + " WHEN MATCHED THEN UPDATE SET c = '(1,2,3)'"));

        // And so does the action of an ON CONFLICT.
        exec("CREATE TABLE cty_ocp (k int PRIMARY KEY, c cty_wp)");
        exec("INSERT INTO cty_ocp VALUES (1, row(1,2))");
        assertEquals("Input has too many columns.",
                detailOf("INSERT INTO cty_ocp VALUES (1, row(3,4)) ON CONFLICT (k)"
                        + " DO UPDATE SET c = row(1,2,3)"));
        assertEquals("Too many columns.",
                detailOf("INSERT INTO cty_ocp VALUES (1, row(3,4)) ON CONFLICT (k)"
                        + " DO UPDATE SET c = '(1,2,3)'"));
        assertEquals("Input has too few columns.",
                detailOf("INSERT INTO cty_ocp VALUES (1, row(3,4)) ON CONFLICT (k)"
                        + " DO UPDATE SET c = row(9)"));
        assertEquals("(1,2)", scalar("SELECT c::text FROM cty_ocp"));
    }

    /** A field of a composite is read by its own type's reader, so the inner type is blamed. */
    @Test
    void aNestedRecordIsBlamedOnTheInnerType() throws Exception {
        exec("CREATE TYPE cty_np AS (a int, b int)");
        exec("CREATE TYPE cty_nst AS (x int, y cty_np)");
        exec("CREATE TABLE cty_hn (k int, c cty_nst)");
        assertEquals("cannot cast type record to cty_np",
                messageOf("SELECT row(1,row(2,3,4))::cty_nst"));
        assertEquals("Input has too many columns.",
                detailOf("INSERT INTO cty_hn VALUES (1, row(1,row(2,3,4)))"));
        assertEquals("cannot cast type record to cty_np",
                messageOf("INSERT INTO cty_hn VALUES (1, row(1,row(2,3,4)))"));

        // A nested literal quotes the inner text, and the inner reader reads it.
        assertEquals("malformed record literal: \"(2,3,4)\"",
                messageOf("SELECT '(1,\"(2,3,4)\")'::cty_nst"));
        assertEquals("Too many columns.", detailOf("SELECT '(1,\"(2,3,4)\")'::cty_nst"));
        assertEquals("Too few columns.", detailOf("SELECT '(1,\"(2)\")'::cty_nst"));
        assertEquals("Too many columns.",
                detailOf("INSERT INTO cty_hn VALUES (1, '(1,\"(2,3,4)\")')"));
        assertEquals("Too few columns.", detailOf("INSERT INTO cty_hn VALUES (1, '(1,\"(2)\")')"));

        // A field handed a string is read by the field's own reader too.
        assertEquals("malformed record literal: \"(2,3,4)\"",
                messageOf("SELECT row(1,'(2,3,4)')::cty_nst"));
        assertEquals("(1,\"(2,3)\")", scalar("SELECT (row(1,'(2,3)')::cty_nst)::text"));

        exec("INSERT INTO cty_hn VALUES (1, row(1,row(2,3)))");
        assertEquals("(1,\"(2,3)\")", scalar("SELECT c::text FROM cty_hn"));
        assertEquals(1, num("SELECT count(*) FROM cty_hn"));
    }

    @Test
    void copyReadsACompositeFieldWithTheCompositesOwnReader() throws Exception {
        exec("CREATE TYPE cty_cpr AS (a int, b int)");
        exec("CREATE TABLE cty_cp (k int, c cty_cpr)");
        assertEquals(1L, copyIn("COPY cty_cp (k, c) FROM STDIN", "1\t(1,2)\n"));
        assertEquals("22P02", copyState("COPY cty_cp (k, c) FROM STDIN", "2\t(1,2,3)\n"));
        assertEquals("malformed record literal: \"(1,2,3)\"",
                copyMessage("COPY cty_cp (k, c) FROM STDIN", "2\t(1,2,3)\n"));
        assertEquals("malformed record literal: \"abc\"",
                copyMessage("COPY cty_cp (k, c) FROM STDIN", "3\tabc\n"));
        assertEquals("malformed record literal: \"(1)\"",
                copyMessage("COPY cty_cp (k, c) FROM STDIN", "4\t(1)\n"));
        // Only the line of the right shape was copied in.
        assertEquals("1", column("SELECT k FROM cty_cp ORDER BY k"));
        assertEquals("(1,2)", scalar("SELECT c::text FROM cty_cp"));
    }

    /** The number of rows a COPY reports having stored. */
    private static long copyIn(String sql, String data) throws Exception {
        org.postgresql.copy.CopyManager cm =
                new org.postgresql.copy.CopyManager((org.postgresql.core.BaseConnection) conn);
        return cm.copyIn(sql, new java.io.ByteArrayInputStream(
                data.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    /** The fields of the error a COPY of {@code data} raises. */
    private static org.postgresql.util.ServerErrorMessage copyFields(String sql, String data) {
        SQLException thrown = assertThrows(SQLException.class, () -> copyIn(sql, data), sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    private static String copyState(String sql, String data) {
        return copyFields(sql, data).getSQLState();
    }

    private static String copyMessage(String sql, String data) {
        return copyFields(sql, data).getMessage();
    }

    /** Every attribute of a composite type, as name/number/dropped/type, in attribute order. */
    private static String attributesOf(String typeName) throws SQLException {
        return scalar("SELECT string_agg(a.attname || '/' || a.attnum || '/' || a.attisdropped"
                + " || '/' || format_type(a.atttypid, a.atttypmod), ' ' ORDER BY a.attnum)"
                + " FROM pg_attribute a JOIN pg_type t ON t.typrelid = a.attrelid"
                + " WHERE t.typname = '" + typeName + "' AND a.attnum > 0");
    }

    // ------------------------------------------------------------ a schema drop reaches only its own relations' rules

    /**
     * A rule depends on the relations its actions name, and a relation belongs to a schema, so
     * dropping a schema takes away exactly the rules that named something in it. A relation of
     * the same name in a schema that is still there is nothing to them, and the relation such a
     * rule sits on goes on taking writes.
     */
    @Test
    void dropSchemaCascadeLeavesTheRuleOfASameNamedRelationInAnotherSchema() throws Exception {
        exec("CREATE SCHEMA crx_n");
        exec("CREATE TABLE public.crx_dep (i int)");
        exec("CREATE TABLE public.crx_keep (i int)");
        exec("CREATE TABLE crx_n.crx_dep (i int)");
        exec("CREATE TABLE crx_n.crx_keep (i int)");
        exec("CREATE RULE crx_rk AS ON INSERT TO public.crx_keep DO ALSO"
                + " INSERT INTO public.crx_dep VALUES (new.i)");
        exec("INSERT INTO public.crx_keep VALUES (1)");
        assertEquals(1, num("SELECT count(*) FROM public.crx_dep"));
        assertEquals(0, num("SELECT count(*) FROM crx_n.crx_dep"));

        exec("DROP SCHEMA crx_n CASCADE");
        assertEquals("public/crx_keep/crx_rk",
                scalar("SELECT schemaname || '/' || tablename || '/' || rulename FROM pg_rules"
                        + " WHERE rulename = 'crx_rk'"));
        // The relation the rule sits on still takes the write, and the rule still fires.
        assertEquals(1, update("INSERT INTO public.crx_keep VALUES (2)"));
        assertEquals("1,2", column("SELECT i FROM public.crx_keep ORDER BY i"));
        assertEquals("1,2", column("SELECT i FROM public.crx_dep ORDER BY i"));
        assertEquals("t", scalar("SELECT relhasrules FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE c.relname = 'crx_keep' AND n.nspname = 'public'"));
        // And it still stands in the way of dropping what its action names.
        assertEquals("2BP01", stateOf("DROP TABLE public.crx_dep"));
        assertEquals("rule crx_rk on table crx_keep depends on table crx_dep",
                detailOf("DROP TABLE public.crx_dep"));
    }

    /** Every write path of the surviving relation reaches it, and the rule fires on each. */
    @Test
    void theSurvivingRelationTakesEveryWriteAfterTheDrop() throws Exception {
        exec("CREATE SCHEMA crx_w");
        exec("CREATE TABLE public.crx_wd (i int)");
        exec("CREATE TABLE public.crx_wk (i int)");
        exec("CREATE TABLE crx_w.crx_wd (i int)");
        exec("CREATE TABLE crx_w.crx_wk (i int)");
        exec("CREATE RULE crx_rw AS ON INSERT TO public.crx_wk DO ALSO"
                + " INSERT INTO public.crx_wd VALUES (new.i)");
        exec("DROP SCHEMA crx_w CASCADE");

        assertEquals(1, update("INSERT INTO public.crx_wk VALUES (1)"));
        assertEquals(2, update("INSERT INTO public.crx_wk VALUES (2), (3)"));
        assertEquals(1, update("INSERT INTO public.crx_wk SELECT 4"));
        assertEquals("1,2,3,4", column("SELECT i FROM public.crx_wk ORDER BY i"));
        assertEquals("1,2,3,4", column("SELECT i FROM public.crx_wd ORDER BY i"));

        // An UPDATE and a DELETE have no rule of their own here, and change only the relation.
        assertEquals(1, update("UPDATE public.crx_wk SET i = i * 10 WHERE i = 1"));
        assertEquals(1, update("DELETE FROM public.crx_wk WHERE i = 3"));
        assertEquals("2,4,10", column("SELECT i FROM public.crx_wk ORDER BY i"));
        assertEquals("1,2,3,4", column("SELECT i FROM public.crx_wd ORDER BY i"));

        // The row a RETURNING write hands back is the row it stored.
        assertEquals("5", scalar("INSERT INTO public.crx_wk VALUES (5) RETURNING i"));
        assertEquals(5, num("SELECT count(*) FROM public.crx_wd"));
    }

    /**
     * Which rules go is decided one action at a time: the rule that named the dropped schema goes
     * and the rule beside it on the same relation stays, still firing on every write.
     */
    @Test
    void theRuleWhoseActionNamedTheDroppedSchemaGoesAndItsNeighbourStays() throws Exception {
        exec("CREATE SCHEMA crx_m");
        exec("CREATE TABLE crx_m.crx_ml (i int)");
        exec("CREATE TABLE public.crx_ml (i int)");
        exec("CREATE TABLE public.crx_mt (i int)");
        exec("CREATE RULE crx_r1 AS ON INSERT TO public.crx_mt DO ALSO"
                + " INSERT INTO crx_m.crx_ml VALUES (new.i)");
        exec("CREATE RULE crx_r2 AS ON INSERT TO public.crx_mt DO ALSO"
                + " INSERT INTO public.crx_ml VALUES (new.i + 100)");
        exec("INSERT INTO public.crx_mt VALUES (1)");
        assertEquals(1, num("SELECT count(*) FROM crx_m.crx_ml"));
        assertEquals(1, num("SELECT count(*) FROM public.crx_ml"));

        exec("DROP SCHEMA crx_m CASCADE");
        assertEquals("crx_r2", column("SELECT rulename FROM pg_rules"
                + " WHERE rulename IN ('crx_r1','crx_r2') ORDER BY 1"));
        exec("INSERT INTO public.crx_mt VALUES (2)");
        assertEquals("1,2", column("SELECT i FROM public.crx_mt ORDER BY i"));
        assertEquals("101,102", column("SELECT i FROM public.crx_ml ORDER BY i"));

        // The name the drop freed can be written again, and the new rule fires.
        exec("CREATE RULE crx_r1 AS ON INSERT TO public.crx_mt DO ALSO"
                + " INSERT INTO public.crx_ml VALUES (new.i + 200)");
        exec("INSERT INTO public.crx_mt VALUES (3)");
        assertEquals("101,102,103,203", column("SELECT i FROM public.crx_ml ORDER BY i"));
    }

    /**
     * A rule written on a relation inside the dropped schema goes with that relation, and the
     * relation of the same name outside it keeps the rule of its own.
     */
    @Test
    void aRuleInsideTheDroppedSchemaGoesAndTheSameNamedRelationOutsideKeepsIts() throws Exception {
        exec("CREATE SCHEMA crx_i");
        exec("CREATE TABLE crx_i.src (i int)");
        exec("CREATE TABLE public.crx_src (i int)");
        exec("CREATE TABLE public.crx_out (i int)");
        exec("CREATE RULE crx_ri AS ON INSERT TO crx_i.src DO ALSO"
                + " INSERT INTO public.crx_out VALUES (new.i)");
        exec("CREATE RULE crx_rp AS ON INSERT TO public.crx_src DO ALSO"
                + " INSERT INTO public.crx_out VALUES (new.i + 100)");
        exec("INSERT INTO crx_i.src VALUES (1)");
        exec("INSERT INTO public.crx_src VALUES (2)");
        assertEquals("1,102", column("SELECT i FROM public.crx_out ORDER BY i"));
        assertEquals("crx_i/src/crx_ri,public/crx_src/crx_rp",
                column("SELECT schemaname || '/' || tablename || '/' || rulename FROM pg_rules"
                        + " WHERE rulename IN ('crx_ri','crx_rp') ORDER BY 1"));

        exec("DROP SCHEMA crx_i CASCADE");
        assertEquals("public/crx_src/crx_rp",
                column("SELECT schemaname || '/' || tablename || '/' || rulename FROM pg_rules"
                        + " WHERE rulename IN ('crx_ri','crx_rp') ORDER BY 1"));
        exec("INSERT INTO public.crx_src VALUES (3)");
        assertEquals("2,3", column("SELECT i FROM public.crx_src ORDER BY i"));
        assertEquals("1,102,103", column("SELECT i FROM public.crx_out ORDER BY i"));
        // The rule that stayed still holds the relation its action names.
        assertEquals("2BP01", stateOf("DROP TABLE public.crx_out"));
    }

    /** An action that only reads a relation in the dropped schema takes the rule with it too. */
    @Test
    void anActionThatOnlyReadsTheDroppedSchemaTakesItsRuleAway() throws Exception {
        exec("CREATE SCHEMA crx_z");
        exec("CREATE TABLE crx_z.rd (i int)");
        exec("CREATE TABLE public.crx_rd (i int)");
        exec("CREATE TABLE public.crx_zs (i int)");
        exec("CREATE TABLE public.crx_zl (i int)");
        exec("CREATE RULE crx_rrd AS ON INSERT TO public.crx_zs DO ALSO"
                + " INSERT INTO public.crx_zl SELECT count(*) FROM crx_z.rd");
        exec("INSERT INTO public.crx_zs VALUES (1)");
        assertEquals(1, num("SELECT count(*) FROM public.crx_zl"));

        exec("DROP SCHEMA crx_z CASCADE");
        assertEquals(0, num("SELECT count(*) FROM pg_rules WHERE rulename = 'crx_rrd'"));
        // The relation the rule sat on takes the write, and nothing is written for the rule.
        assertEquals(1, update("INSERT INTO public.crx_zs VALUES (2)"));
        assertEquals(2, num("SELECT count(*) FROM public.crx_zs"));
        assertEquals(1, num("SELECT count(*) FROM public.crx_zl"));
    }

    /**
     * RESTRICT is refused while the schema holds a relation, and reaches no rule either way: the
     * rule on the relation of the same name elsewhere is untouched by the refusal and by the drop
     * of the emptied schema that follows it.
     */
    @Test
    void restrictReachesNoRuleWhetherItIsRefusedOrCarriedOut() throws Exception {
        exec("CREATE SCHEMA crx_r");
        exec("CREATE TABLE crx_r.crx_rdp (i int)");
        exec("CREATE TABLE public.crx_rdp (i int)");
        exec("CREATE TABLE public.crx_rkp (i int)");
        exec("CREATE RULE crx_rr AS ON INSERT TO public.crx_rkp DO ALSO"
                + " INSERT INTO public.crx_rdp VALUES (new.i)");
        assertEquals("cannot drop schema crx_r because other objects depend on it",
                messageOf("DROP SCHEMA crx_r RESTRICT"));
        assertEquals(1, num("SELECT count(*) FROM pg_rules WHERE rulename = 'crx_rr'"));
        assertEquals(1, num("SELECT count(*) FROM pg_tables WHERE schemaname = 'crx_r'"));
        exec("INSERT INTO public.crx_rkp VALUES (1)");
        assertEquals(1, num("SELECT count(*) FROM public.crx_rdp"));

        // With the relation out of the way the schema goes, and the rule stays where it was.
        exec("DROP TABLE crx_r.crx_rdp");
        exec("DROP SCHEMA crx_r RESTRICT");
        assertEquals(0, num("SELECT count(*) FROM pg_namespace WHERE nspname = 'crx_r'"));
        assertEquals("public/crx_rkp/crx_rr",
                scalar("SELECT schemaname || '/' || tablename || '/' || rulename FROM pg_rules"
                        + " WHERE rulename = 'crx_rr'"));
        exec("INSERT INTO public.crx_rkp VALUES (2)");
        assertEquals(2, num("SELECT count(*) FROM public.crx_rkp"));
        assertEquals(2, num("SELECT count(*) FROM public.crx_rdp"));
    }

    /** A drop rolled back leaves the rule where it was, and the relation still takes writes. */
    @Test
    void aRolledBackSchemaDropLeavesTheRuleAndTheRelationWritable() throws Exception {
        exec("CREATE SCHEMA crx_g");
        exec("CREATE TABLE public.crx_gd (i int)");
        exec("CREATE TABLE public.crx_gk (i int)");
        exec("CREATE TABLE crx_g.crx_gd (i int)");
        exec("CREATE RULE crx_rg AS ON INSERT TO public.crx_gk DO ALSO"
                + " INSERT INTO public.crx_gd VALUES (new.i)");
        exec("BEGIN");
        exec("DROP SCHEMA crx_g CASCADE");
        exec("INSERT INTO public.crx_gk VALUES (1)");
        assertEquals(1, num("SELECT count(*) FROM public.crx_gk"));
        assertEquals(1, num("SELECT count(*) FROM public.crx_gd"));
        exec("ROLLBACK");

        assertEquals("public/crx_gk/crx_rg",
                scalar("SELECT schemaname || '/' || tablename || '/' || rulename FROM pg_rules"
                        + " WHERE rulename = 'crx_rg'"));
        exec("INSERT INTO public.crx_gk VALUES (2)");
        // Only the row written after the rollback is there, on the relation and through the rule.
        assertEquals("2", column("SELECT i FROM public.crx_gk ORDER BY i"));
        assertEquals("2", column("SELECT i FROM public.crx_gd ORDER BY i"));
        exec("DROP SCHEMA IF EXISTS crx_g CASCADE");
    }

    /** What pg_rules holds for the rule the drop left alone, schema and definition and all. */
    @Test
    void pgRulesStillNamesTheSchemaOfTheRelationTheSurvivingRuleIsOn() throws Exception {
        exec("RESET search_path");
        exec("CREATE SCHEMA crx_d");
        exec("CREATE TABLE public.crx_dd (i int)");
        exec("CREATE TABLE public.crx_dk (i int)");
        exec("CREATE TABLE crx_d.crx_dd (i int)");
        exec("CREATE RULE crx_rd AS ON INSERT TO crx_dk DO ALSO INSERT INTO crx_dd VALUES (new.i)");

        exec("DROP SCHEMA crx_d CASCADE");
        assertEquals("public", scalar("SELECT schemaname FROM pg_rules WHERE rulename = 'crx_rd'"));
        assertEquals("crx_dk", scalar("SELECT tablename FROM pg_rules WHERE rulename = 'crx_rd'"));
        assertEquals("CREATE RULE crx_rd AS\n"
                        + "    ON INSERT TO public.crx_dk DO  INSERT INTO crx_dd (i)\n"
                        + "  VALUES (new.i);",
                scalar("SELECT definition FROM pg_rules WHERE rulename = 'crx_rd'"));
        // The relation the rule is filed under is the one in the schema that was not dropped.
        assertEquals("public/crx_dk",
                scalar("SELECT n.nspname || '/' || c.relname FROM pg_rewrite r"
                        + " JOIN pg_class c ON c.oid = r.ev_class"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE r.rulename = 'crx_rd'"));
        exec("INSERT INTO public.crx_dk VALUES (1)");
        assertEquals(1, num("SELECT count(*) FROM public.crx_dd"));
    }

    /**
     * Two schemas holding relations of the same names each keep their own rule, and a drop of one
     * leaves the other's rule firing -- even where one schema's name starts with the other's.
     */
    @Test
    void eachSchemaKeepsTheRuleOnItsOwnRelationOfTheName() throws Exception {
        exec("CREATE SCHEMA crx_ss");
        exec("CREATE SCHEMA crx_ss2");
        exec("CREATE TABLE crx_ss.t (i int)");
        exec("CREATE TABLE crx_ss.l (i int)");
        exec("CREATE TABLE crx_ss2.t (i int)");
        exec("CREATE TABLE crx_ss2.l (i int)");
        exec("CREATE RULE crx_rs AS ON INSERT TO crx_ss.t DO ALSO"
                + " INSERT INTO crx_ss.l VALUES (new.i)");
        exec("CREATE RULE crx_rs2 AS ON INSERT TO crx_ss2.t DO ALSO"
                + " INSERT INTO crx_ss2.l VALUES (new.i + 20)");
        assertEquals("crx_ss/t/crx_rs,crx_ss2/t/crx_rs2",
                column("SELECT schemaname || '/' || tablename || '/' || rulename FROM pg_rules"
                        + " WHERE rulename IN ('crx_rs','crx_rs2') ORDER BY 1"));

        exec("DROP SCHEMA crx_ss CASCADE");
        assertEquals("crx_ss2/t/crx_rs2",
                column("SELECT schemaname || '/' || tablename || '/' || rulename FROM pg_rules"
                        + " WHERE rulename IN ('crx_rs','crx_rs2') ORDER BY 1"));
        exec("INSERT INTO crx_ss2.t VALUES (1)");
        assertEquals(1, num("SELECT count(*) FROM crx_ss2.t"));
        assertEquals("21", column("SELECT i FROM crx_ss2.l ORDER BY i"));
    }

    /** A view is a relation like any other, and the rule on the view of the same name stays. */
    @Test
    void theRuleOnASameNamedViewInAnotherSchemaStaysAndTheViewTakesWrites() throws Exception {
        exec("CREATE SCHEMA crx_vs");
        exec("CREATE TABLE public.crx_vb (i int)");
        exec("CREATE VIEW public.crx_vw AS SELECT i FROM public.crx_vb");
        exec("CREATE RULE crx_rvw AS ON INSERT TO public.crx_vw DO INSTEAD"
                + " INSERT INTO public.crx_vb VALUES (new.i)");
        exec("CREATE TABLE crx_vs.crx_vb (i int)");
        exec("CREATE VIEW crx_vs.crx_vw AS SELECT i FROM crx_vs.crx_vb");
        exec("CREATE RULE crx_rvw AS ON INSERT TO crx_vs.crx_vw DO INSTEAD"
                + " INSERT INTO crx_vs.crx_vb VALUES (new.i + 50)");
        exec("INSERT INTO public.crx_vw VALUES (1)");
        exec("INSERT INTO crx_vs.crx_vw VALUES (2)");
        assertEquals(1, num("SELECT count(*) FROM public.crx_vb"));
        assertEquals(1, num("SELECT count(*) FROM crx_vs.crx_vb"));

        exec("DROP SCHEMA crx_vs CASCADE");
        assertEquals("public/crx_vw/crx_rvw",
                column("SELECT schemaname || '/' || tablename || '/' || rulename FROM pg_rules"
                        + " WHERE rulename = 'crx_rvw' ORDER BY 1"));
        // The view still takes the write its rule turns into one on the table under it.
        exec("INSERT INTO public.crx_vw VALUES (3)");
        assertEquals("1,3", column("SELECT i FROM public.crx_vb ORDER BY i"));
        assertEquals(2, num("SELECT count(*) FROM public.crx_vw"));
    }

    /** The rules that name a view in the dropped schema go with it, wherever they were written. */
    @Test
    void everyRuleThatNamedAViewInTheDroppedSchemaGoesWithIt() throws Exception {
        exec("CREATE SCHEMA crx_c");
        exec("CREATE TABLE crx_c.base (i int)");
        exec("CREATE VIEW crx_c.vw AS SELECT * FROM crx_c.base");
        exec("CREATE TABLE public.crx_o3 (i int)");
        exec("CREATE TABLE public.crx_o3l (i int)");
        exec("CREATE RULE crx_rv AS ON INSERT TO crx_c.vw DO INSTEAD"
                + " INSERT INTO crx_c.base VALUES (new.i)");
        exec("CREATE RULE crx_rdv AS ON INSERT TO public.crx_o3 DO ALSO"
                + " INSERT INTO public.crx_o3l SELECT count(*) FROM crx_c.vw");
        exec("INSERT INTO crx_c.vw VALUES (1)");
        assertEquals(1, num("SELECT count(*) FROM crx_c.base"));

        exec("DROP SCHEMA crx_c CASCADE");
        assertEquals(0, num("SELECT count(*) FROM pg_rules"
                + " WHERE rulename IN ('crx_rv','crx_rdv')"));
        exec("INSERT INTO public.crx_o3 VALUES (1)");
        assertEquals(1, num("SELECT count(*) FROM public.crx_o3"));
        assertEquals(0, num("SELECT count(*) FROM public.crx_o3l"));
        // Nothing depends on the relation the rule read into any more, so it drops on its own.
        exec("DROP TABLE public.crx_o3");
    }

    /**
     * A rule action names a relation the way any other statement does, so with two schemas on the
     * search path holding the name it writes into the one ahead -- and goes when that one goes.
     */
    @Test
    void anActionNamesTheRelationInTheSchemaAheadOnTheSearchPath() throws Exception {
        exec("CREATE SCHEMA crx_p2");
        exec("CREATE TABLE crx_p2.crx_dp (i int)");
        exec("CREATE TABLE public.crx_dp (i int)");
        exec("CREATE TABLE public.crx_kp (i int)");
        exec("SET search_path = crx_p2, public");
        try {
            exec("CREATE RULE crx_ru AS ON INSERT TO public.crx_kp DO ALSO"
                    + " INSERT INTO crx_dp VALUES (new.i)");
            exec("INSERT INTO public.crx_kp VALUES (1)");
            assertEquals(0, num("SELECT count(*) FROM public.crx_dp"));
            assertEquals(1, num("SELECT count(*) FROM crx_p2.crx_dp"));

            exec("DROP SCHEMA crx_p2 CASCADE");
            assertEquals(0, num("SELECT count(*) FROM pg_rules WHERE rulename = 'crx_ru'"));
            // The relation the rule sat on still takes the write, and nothing follows it.
            assertEquals(1, update("INSERT INTO public.crx_kp VALUES (2)"));
            assertEquals(2, num("SELECT count(*) FROM public.crx_kp"));
            assertEquals(0, num("SELECT count(*) FROM public.crx_dp"));
        } finally {
            exec("RESET search_path");
        }
    }

    /** The other way round: the schema behind public on the path is nothing to the action. */
    @Test
    void theSchemaBehindPublicOnTheSearchPathIsNothingToTheAction() throws Exception {
        exec("CREATE SCHEMA crx_p3");
        exec("CREATE TABLE crx_p3.crx_qd (i int)");
        exec("CREATE TABLE public.crx_qd (i int)");
        exec("CREATE TABLE public.crx_qk (i int)");
        exec("SET search_path = public, crx_p3");
        try {
            exec("CREATE RULE crx_rq3 AS ON INSERT TO public.crx_qk DO ALSO"
                    + " INSERT INTO crx_qd VALUES (new.i)");
            exec("INSERT INTO public.crx_qk VALUES (1)");
            assertEquals(1, num("SELECT count(*) FROM public.crx_qd"));
            assertEquals(0, num("SELECT count(*) FROM crx_p3.crx_qd"));

            exec("DROP SCHEMA crx_p3 CASCADE");
            assertEquals("public/crx_qk/crx_rq3",
                    scalar("SELECT schemaname || '/' || tablename || '/' || rulename FROM pg_rules"
                            + " WHERE rulename = 'crx_rq3'"));
            exec("INSERT INTO public.crx_qk VALUES (2)");
            assertEquals(2, num("SELECT count(*) FROM public.crx_qk"));
            assertEquals(2, num("SELECT count(*) FROM public.crx_qd"));
        } finally {
            exec("RESET search_path");
        }
    }

    /** The rules on the other write paths are left alone the same way, and still fire. */
    @Test
    void anUpdateAndADeleteRuleSurviveTheDropOfASameNamedRelationsSchema() throws Exception {
        exec("CREATE SCHEMA crx_u");
        exec("CREATE TABLE public.crx_ud (i int)");
        exec("CREATE TABLE public.crx_uk (i int)");
        exec("CREATE TABLE crx_u.crx_ud (i int)");
        exec("CREATE RULE crx_ru1 AS ON UPDATE TO public.crx_uk DO ALSO"
                + " INSERT INTO public.crx_ud VALUES (new.i)");
        exec("CREATE RULE crx_ru2 AS ON DELETE TO public.crx_uk DO ALSO"
                + " INSERT INTO public.crx_ud VALUES (old.i * -1)");
        exec("INSERT INTO public.crx_uk VALUES (5)");

        exec("DROP SCHEMA crx_u CASCADE");
        assertEquals("crx_ru1,crx_ru2", column("SELECT rulename FROM pg_rules"
                + " WHERE rulename IN ('crx_ru1','crx_ru2') ORDER BY 1"));
        assertEquals(1, update("UPDATE public.crx_uk SET i = 6"));
        assertEquals(1, update("DELETE FROM public.crx_uk"));
        assertEquals("-6,6", column("SELECT i FROM public.crx_ud ORDER BY i"));
        assertEquals(0, num("SELECT count(*) FROM public.crx_uk"));
    }

    /** A rule with no action to name a relation with is left alone, and goes on refusing. */
    @Test
    void aRuleThatDoesInsteadNothingGoesOnRefusingTheWrite() throws Exception {
        exec("CREATE SCHEMA crx_nn");
        exec("CREATE TABLE crx_nn.crx_nk (i int)");
        exec("CREATE TABLE public.crx_nk (i int)");
        exec("CREATE RULE crx_rn AS ON INSERT TO public.crx_nk DO INSTEAD NOTHING");

        exec("DROP SCHEMA crx_nn CASCADE");
        assertEquals("public/crx_nk/crx_rn",
                scalar("SELECT schemaname || '/' || tablename || '/' || rulename FROM pg_rules"
                        + " WHERE rulename = 'crx_rn'"));
        assertEquals(0, update("INSERT INTO public.crx_nk VALUES (1)"));
        assertEquals(0, num("SELECT count(*) FROM public.crx_nk"));

        // Once the rule is gone the write lands, which is how we know it was the rule refusing.
        exec("DROP RULE crx_rn ON public.crx_nk");
        assertEquals(1, update("INSERT INTO public.crx_nk VALUES (2)"));
        assertEquals(1, num("SELECT count(*) FROM public.crx_nk"));
    }

    // ------------------------------------------------------------ what a schema drop refuses for, and how it says so

    /**
     * A schema is what everything in it hangs from, so a DROP SCHEMA that would take something
     * with it is refused unless CASCADE says to take it: SQLSTATE 2BP01, a message naming the
     * schema, a DETAIL naming what depends on it and a HINT saying what to write instead.
     */
    @Test
    void aSchemaHoldingARelationIsNotDroppedAndTheRefusalCarriesEveryField() throws Exception {
        exec("CREATE SCHEMA sdr_a");
        exec("CREATE TABLE sdr_a.r (i int)");
        exec("INSERT INTO sdr_a.r VALUES (1)");
        assertEquals("2BP01", stateOf("DROP SCHEMA sdr_a RESTRICT"));
        assertEquals("cannot drop schema sdr_a because other objects depend on it",
                messageOf("DROP SCHEMA sdr_a RESTRICT"));
        assertEquals("table sdr_a.r depends on schema sdr_a", detailOf("DROP SCHEMA sdr_a RESTRICT"));
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.",
                hintOf("DROP SCHEMA sdr_a RESTRICT"));

        // RESTRICT is the default, so the bare statement is refused with the same four fields.
        assertEquals("2BP01", stateOf("DROP SCHEMA sdr_a"));
        assertEquals("cannot drop schema sdr_a because other objects depend on it",
                messageOf("DROP SCHEMA sdr_a"));
        assertEquals("table sdr_a.r depends on schema sdr_a", detailOf("DROP SCHEMA sdr_a"));
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.",
                hintOf("DROP SCHEMA sdr_a"));

        // The refusal changed nothing: the schema is there and so is its row.
        assertEquals(1, num("SELECT count(*) FROM pg_namespace WHERE nspname = 'sdr_a'"));
        assertEquals(1, num("SELECT count(*) FROM sdr_a.r"));
        exec("DROP SCHEMA sdr_a CASCADE");
        assertEquals(0, num("SELECT count(*) FROM pg_namespace WHERE nspname = 'sdr_a'"));
    }

    /** Every object the schema holds is named in the DETAIL, in the order they were created. */
    @Test
    void everyObjectTheSchemaHoldsIsNamedInTheDetailInCreationOrder() throws Exception {
        exec("CREATE SCHEMA sdr_b");
        exec("CREATE TABLE sdr_b.zt (i int)");
        exec("CREATE TABLE sdr_b.at (i int)");
        exec("CREATE VIEW sdr_b.zv AS SELECT i FROM sdr_b.zt");
        exec("CREATE SEQUENCE sdr_b.asq");
        // Creation order, not name order, and each line says what kind of object it is.
        assertEquals("table sdr_b.zt depends on schema sdr_b\n"
                        + "table sdr_b.at depends on schema sdr_b\n"
                        + "view sdr_b.zv depends on schema sdr_b\n"
                        + "sequence sdr_b.asq depends on schema sdr_b",
                detailOf("DROP SCHEMA sdr_b RESTRICT"));
        exec("DROP SCHEMA sdr_b CASCADE");
    }

    /** A sequence, a type or a function of its own stands in the way just as a table does. */
    @Test
    void aSchemaHoldingOnlyASequenceOrATypeOrAFunctionIsStillARefusal() throws Exception {
        exec("CREATE SCHEMA sdr_c");
        exec("CREATE SEQUENCE sdr_c.sq");
        assertEquals("2BP01", stateOf("DROP SCHEMA sdr_c RESTRICT"));
        assertEquals("sequence sdr_c.sq depends on schema sdr_c",
                detailOf("DROP SCHEMA sdr_c RESTRICT"));

        exec("DROP SEQUENCE sdr_c.sq");
        exec("CREATE TYPE sdr_c.e AS ENUM ('a')");
        assertEquals("2BP01", stateOf("DROP SCHEMA sdr_c RESTRICT"));
        assertEquals("type sdr_c.e depends on schema sdr_c", detailOf("DROP SCHEMA sdr_c RESTRICT"));

        exec("DROP TYPE sdr_c.e");
        exec("CREATE FUNCTION sdr_c.f(a int) RETURNS int AS 'SELECT a' LANGUAGE sql");
        assertEquals("2BP01", stateOf("DROP SCHEMA sdr_c RESTRICT"));
        // A function is named with the types of its arguments, spelled as PostgreSQL spells them.
        assertEquals("function sdr_c.f(integer) depends on schema sdr_c",
                detailOf("DROP SCHEMA sdr_c RESTRICT"));

        // With nothing left to hang from it the schema drops without CASCADE.
        exec("DROP FUNCTION sdr_c.f(int)");
        exec("DROP SCHEMA sdr_c RESTRICT");
        assertEquals(0, num("SELECT count(*) FROM pg_namespace WHERE nspname = 'sdr_c'"));
    }

    /** A hundred dependents are named and the rest are counted, in the refusal and the notice. */
    @Test
    void pastAHundredDependentsTheRestAreCountedRatherThanNamed() throws Exception {
        exec("CREATE SCHEMA sdr_d");
        for (int i = 1; i <= 105; i++) {
            exec(String.format("CREATE TABLE sdr_d.t%03d (i int)", i));
        }
        String[] blocking = detailOf("DROP SCHEMA sdr_d RESTRICT").split("\n", -1);
        assertEquals(101, blocking.length);
        assertEquals("table sdr_d.t001 depends on schema sdr_d", blocking[0]);
        assertEquals("table sdr_d.t100 depends on schema sdr_d", blocking[99]);
        assertEquals("and 5 other objects (see server log for list)", blocking[100]);

        String[] notice = noticeOf("DROP SCHEMA sdr_d CASCADE");
        // The count in the message is the whole number taken; the DETAIL is the one that is cut.
        assertEquals("drop cascades to 105 other objects", notice[0]);
        String[] lines = notice[1].split("\n", -1);
        assertEquals(101, lines.length);
        assertEquals("drop cascades to table sdr_d.t001", lines[0]);
        assertEquals("drop cascades to table sdr_d.t100", lines[99]);
        assertEquals("and 5 other objects (see server log for list)", lines[100]);
    }

    // ------------------------------------------------------------ what a schema drop takes, and what it says it took

    /**
     * CASCADE says what it took: nothing at all for an empty schema, the one object by name when
     * there is one, and a count with the names under DETAIL when there are several.
     */
    @Test
    void cascadeNamesOneObjectAndCountsSeveralWithTheirNamesUnderDetail() throws Exception {
        exec("CREATE SCHEMA sdr_e");
        assertNull(noticeOf("DROP SCHEMA sdr_e CASCADE")[0],
                "an empty schema takes nothing with it, so there is nothing to say");

        exec("CREATE SCHEMA sdr_f");
        exec("CREATE TABLE sdr_f.t (i int)");
        String[] one = noticeOf("DROP SCHEMA sdr_f CASCADE");
        assertEquals("drop cascades to table sdr_f.t", one[0]);
        assertNull(one[1], "a single object is named in the message, so no DETAIL is sent");

        exec("CREATE SCHEMA sdr_g");
        exec("CREATE TABLE sdr_g.zt (i int)");
        exec("CREATE TABLE sdr_g.at (i int)");
        exec("CREATE VIEW sdr_g.zv AS SELECT i FROM sdr_g.zt");
        exec("CREATE SEQUENCE sdr_g.asq");
        String[] many = noticeOf("DROP SCHEMA sdr_g CASCADE");
        assertEquals("drop cascades to 4 other objects", many[0]);
        assertEquals("drop cascades to table sdr_g.zt\n"
                + "drop cascades to table sdr_g.at\n"
                + "drop cascades to view sdr_g.zv\n"
                + "drop cascades to sequence sdr_g.asq", many[1]);
    }

    /**
     * A view outside the schema that reads a relation inside it depends on that relation, so it
     * blocks the drop without CASCADE and goes with it under CASCADE -- and so does whatever
     * reads that view. A view that read nothing of the schema's is left alone and goes on
     * answering.
     */
    @Test
    void cascadeReachesAViewOutsideTheSchemaAndTheViewThatReadsThatOne() throws Exception {
        exec("CREATE SCHEMA sdr_h");
        exec("CREATE TABLE sdr_h.t (i int)");
        exec("CREATE VIEW public.sdr_hv AS SELECT i FROM sdr_h.t");
        exec("CREATE VIEW public.sdr_hv2 AS SELECT i FROM public.sdr_hv");
        exec("CREATE TABLE public.sdr_hk (i int)");
        exec("CREATE VIEW public.sdr_hkv AS SELECT i FROM public.sdr_hk");

        assertEquals("2BP01", stateOf("DROP SCHEMA sdr_h RESTRICT"));
        // The walk goes from the schema to its relation and on to what depends on that.
        assertEquals("table sdr_h.t depends on schema sdr_h\n"
                        + "view sdr_hv depends on table sdr_h.t\n"
                        + "view sdr_hv2 depends on view sdr_hv",
                detailOf("DROP SCHEMA sdr_h RESTRICT"));

        String[] notice = noticeOf("DROP SCHEMA sdr_h CASCADE");
        assertEquals("drop cascades to 3 other objects", notice[0]);
        assertEquals("drop cascades to table sdr_h.t\n"
                + "drop cascades to view sdr_hv\n"
                + "drop cascades to view sdr_hv2", notice[1]);
        assertEquals(0, num("SELECT count(*) FROM pg_views WHERE viewname = 'sdr_hv'"));
        assertEquals(0, num("SELECT count(*) FROM pg_views WHERE viewname = 'sdr_hv2'"));

        assertEquals(1, num("SELECT count(*) FROM pg_views WHERE viewname = 'sdr_hkv'"));
        exec("INSERT INTO public.sdr_hk VALUES (7)");
        assertEquals("7", scalar("SELECT i FROM public.sdr_hkv"));
        exec("DROP VIEW public.sdr_hkv");
        exec("DROP TABLE public.sdr_hk");
    }

    /**
     * A row security policy outside the schema whose expression reads a relation inside it goes
     * the same way, and the relation the policy was on is left standing.
     */
    @Test
    void cascadeReachesAPolicyOutsideTheSchemaAndLeavesItsRelationStanding() throws Exception {
        exec("CREATE SCHEMA sdr_i");
        exec("CREATE TABLE sdr_i.t (i int)");
        exec("CREATE TABLE public.sdr_it (i int)");
        exec("CREATE POLICY sdr_ip ON sdr_it USING (i IN (SELECT i FROM sdr_i.t))");
        exec("CREATE TABLE public.sdr_iu (i int)");
        exec("CREATE POLICY sdr_iq ON sdr_iu USING (i > 0)");

        assertEquals("2BP01", stateOf("DROP SCHEMA sdr_i RESTRICT"));
        assertEquals("table sdr_i.t depends on schema sdr_i\n"
                        + "policy sdr_ip on table sdr_it depends on table sdr_i.t",
                detailOf("DROP SCHEMA sdr_i RESTRICT"));

        String[] notice = noticeOf("DROP SCHEMA sdr_i CASCADE");
        assertEquals("drop cascades to 2 other objects", notice[0]);
        assertEquals("drop cascades to table sdr_i.t\n"
                + "drop cascades to policy sdr_ip on table sdr_it", notice[1]);
        assertEquals(0, num("SELECT count(*) FROM pg_policies WHERE policyname = 'sdr_ip'"));
        // The relation the policy was on stays, and so does a policy that read nothing of the
        // schema's.
        assertEquals(1, num("SELECT count(*) FROM pg_tables WHERE tablename = 'sdr_it'"));
        assertEquals(1, num("SELECT count(*) FROM pg_policies WHERE policyname = 'sdr_iq'"));
        exec("DROP TABLE public.sdr_it");
        exec("DROP TABLE public.sdr_iu");
    }

    /**
     * A column default outside the schema that calls a sequence inside it is a dependent of its
     * own, named as a default rather than as the relation it sits on.
     */
    @Test
    void cascadeReachesTheDefaultOfAColumnOnARelationOutsideTheSchema() throws Exception {
        exec("CREATE SCHEMA sdr_j");
        exec("CREATE SEQUENCE sdr_j.sq");
        exec("CREATE TABLE public.sdr_jd (i int, j int DEFAULT nextval('sdr_j.sq'))");
        exec("INSERT INTO public.sdr_jd (i) VALUES (1)");
        assertEquals("1", scalar("SELECT j FROM public.sdr_jd WHERE i = 1"));

        assertEquals("2BP01", stateOf("DROP SCHEMA sdr_j RESTRICT"));
        assertEquals("sequence sdr_j.sq depends on schema sdr_j\n"
                        + "default value for column j of table sdr_jd depends on sequence sdr_j.sq",
                detailOf("DROP SCHEMA sdr_j RESTRICT"));

        // Rolled back, the sequence is back and the default outside goes on drawing from it.
        exec("BEGIN");
        String[] notice = noticeOf("DROP SCHEMA sdr_j CASCADE");
        assertEquals("drop cascades to 2 other objects", notice[0]);
        assertEquals("drop cascades to sequence sdr_j.sq\n"
                + "drop cascades to default value for column j of table sdr_jd", notice[1]);
        exec("ROLLBACK");
        assertEquals(1, num("SELECT count(*) FROM pg_namespace WHERE nspname = 'sdr_j'"));
        exec("INSERT INTO public.sdr_jd (i) VALUES (2)");
        assertEquals("2", scalar("SELECT j FROM public.sdr_jd WHERE i = 2"));
        exec("DROP TABLE public.sdr_jd");
        exec("DROP SCHEMA sdr_j CASCADE");
    }

    /** IF EXISTS on a schema that is not there says what it skipped; without it, 3F000. */
    @Test
    void ifExistsOnAMissingSchemaSaysWhatItSkippedAndTheBareDropRaises() throws Exception {
        String[] notice = noticeOf("DROP SCHEMA IF EXISTS sdr_nothere CASCADE");
        assertEquals("schema \"sdr_nothere\" does not exist, skipping", notice[0]);
        assertNull(notice[1]);
        assertEquals("3F000", stateOf("DROP SCHEMA sdr_nothere"));
        assertEquals("schema \"sdr_nothere\" does not exist", messageOf("DROP SCHEMA sdr_nothere"));
        assertNull(detailOf("DROP SCHEMA sdr_nothere"));
        assertNull(hintOf("DROP SCHEMA sdr_nothere"));
    }

    // ------------------------------------------------------------ a schema drop that is rolled back never happened

    /**
     * A drop rolled back puts the schema back holding what it held: its relations, the rows in
     * them, the view over them and the rules on them, which go on firing and go on standing in
     * the way of dropping what their actions name.
     */
    @Test
    void aRolledBackDropPutsBackTheSchemaItsRelationsItsRowsAndItsRules() throws Exception {
        exec("CREATE SCHEMA sdr_k");
        exec("CREATE TABLE sdr_k.t (i int)");
        exec("CREATE TABLE sdr_k.l (m text)");
        exec("CREATE SEQUENCE sdr_k.sq");
        exec("CREATE VIEW sdr_k.v AS SELECT i FROM sdr_k.t");
        exec("CREATE RULE sdr_kr AS ON INSERT TO sdr_k.t DO ALSO INSERT INTO sdr_k.l VALUES ('z')");
        exec("INSERT INTO sdr_k.t VALUES (1)");
        assertEquals(1, num("SELECT count(*) FROM sdr_k.l"));

        exec("BEGIN");
        exec("DROP SCHEMA sdr_k CASCADE");
        assertEquals(0, num("SELECT count(*) FROM pg_namespace WHERE nspname = 'sdr_k'"));
        exec("ROLLBACK");

        assertEquals(1, num("SELECT count(*) FROM pg_namespace WHERE nspname = 'sdr_k'"));
        assertEquals(1, num("SELECT count(*) FROM pg_rules WHERE rulename = 'sdr_kr'"));
        assertEquals(1, num("SELECT count(*) FROM sdr_k.t"));
        assertEquals(1, num("SELECT count(*) FROM sdr_k.l"));
        assertEquals(1, num("SELECT count(*) FROM sdr_k.v"));

        // The rule the drop took goes on firing.
        exec("INSERT INTO sdr_k.t VALUES (2)");
        assertEquals(2, num("SELECT count(*) FROM sdr_k.t"));
        assertEquals(2, num("SELECT count(*) FROM sdr_k.l"));
        // And it goes on holding the relation its action names.
        assertEquals("2BP01", stateOf("DROP TABLE sdr_k.l"));
        assertEquals("rule sdr_kr on table sdr_k.t depends on table sdr_k.l",
                detailOf("DROP TABLE sdr_k.l"));
        assertEquals("CREATE RULE sdr_kr AS\n"
                        + "    ON INSERT TO sdr_k.t DO  INSERT INTO sdr_k.l (m)\n"
                        + "  VALUES ('z'::text);",
                scalar("SELECT definition FROM pg_rules WHERE rulename = 'sdr_kr'"));
        // The sequence is back at the place it was never moved from.
        assertEquals("1", scalar("SELECT nextval('sdr_k.sq')"));
        exec("DROP SCHEMA sdr_k CASCADE");
    }

    /**
     * The keys and the indexes behind them come back too, and a sequence comes back at the place
     * it had reached rather than at the beginning.
     */
    @Test
    void aRolledBackDropPutsBackTheKeysTheIndexesAndTheSequencesPlace() throws Exception {
        exec("CREATE SCHEMA sdr_l");
        exec("CREATE TABLE sdr_l.k (i int PRIMARY KEY, m text UNIQUE)");
        exec("CREATE SEQUENCE sdr_l.sq");
        exec("INSERT INTO sdr_l.k VALUES (1, 'a')");
        assertEquals("1", scalar("SELECT nextval('sdr_l.sq')"));
        assertEquals("2", scalar("SELECT nextval('sdr_l.sq')"));

        exec("BEGIN");
        exec("DROP SCHEMA sdr_l CASCADE");
        exec("ROLLBACK");

        assertEquals("3", scalar("SELECT nextval('sdr_l.sq')"));
        assertEquals("23505", stateOf("INSERT INTO sdr_l.k VALUES (1, 'b')"));
        assertEquals("duplicate key value violates unique constraint \"k_pkey\"",
                messageOf("INSERT INTO sdr_l.k VALUES (1, 'b')"));
        assertEquals("23505", stateOf("INSERT INTO sdr_l.k VALUES (2, 'a')"));
        assertEquals("duplicate key value violates unique constraint \"k_m_key\"",
                messageOf("INSERT INTO sdr_l.k VALUES (2, 'a')"));
        exec("INSERT INTO sdr_l.k VALUES (2, 'b')");
        assertEquals(2, num("SELECT count(*) FROM sdr_l.k"));
        assertEquals(2, num("SELECT count(*) FROM pg_indexes WHERE schemaname = 'sdr_l'"));
        exec("DROP SCHEMA sdr_l CASCADE");
    }

    /** What CASCADE reached outside the schema comes back with it, and works again. */
    @Test
    void aRolledBackDropPutsBackWhatCascadeReachedOutsideTheSchema() throws Exception {
        exec("CREATE SCHEMA sdr_m");
        exec("CREATE TABLE sdr_m.t (i int)");
        exec("CREATE VIEW public.sdr_mv AS SELECT i FROM sdr_m.t");
        exec("CREATE TABLE public.sdr_mt (i int)");
        exec("CREATE POLICY sdr_mp ON sdr_mt USING (i IN (SELECT i FROM sdr_m.t))");

        exec("BEGIN");
        exec("DROP SCHEMA sdr_m CASCADE");
        exec("ROLLBACK");

        assertEquals(1, num("SELECT count(*) FROM pg_namespace WHERE nspname = 'sdr_m'"));
        assertEquals(1, num("SELECT count(*) FROM pg_views WHERE viewname = 'sdr_mv'"));
        assertEquals(1, num("SELECT count(*) FROM pg_policies WHERE policyname = 'sdr_mp'"));
        exec("INSERT INTO sdr_m.t VALUES (3)");
        assertEquals("3", scalar("SELECT i FROM public.sdr_mv"));
        exec("DROP SCHEMA sdr_m CASCADE");
        exec("DROP TABLE public.sdr_mt");
    }

    /** A schema that held nothing is put back by a rollback the same way. */
    @Test
    void aRolledBackDropOfAnEmptySchemaPutsItBackToo() throws Exception {
        exec("CREATE SCHEMA sdr_n");
        exec("BEGIN");
        exec("DROP SCHEMA sdr_n");
        exec("ROLLBACK");
        assertEquals(1, num("SELECT count(*) FROM pg_namespace WHERE nspname = 'sdr_n'"));
        exec("DROP SCHEMA sdr_n");
        assertEquals(0, num("SELECT count(*) FROM pg_namespace WHERE nspname = 'sdr_n'"));
    }

    /**
     * A savepoint undoes the drop and no more: the writes made after it are committed with the
     * rest of the transaction, and a drop the transaction does commit is done.
     */
    @Test
    void rollingBackToASavepointPutsTheSchemaBackAndTheTransactionGoesOn() throws Exception {
        exec("CREATE SCHEMA sdr_o");
        exec("CREATE TABLE sdr_o.t (i int)");
        exec("INSERT INTO sdr_o.t VALUES (9)");

        exec("BEGIN");
        exec("SAVEPOINT s1");
        exec("DROP SCHEMA sdr_o CASCADE");
        exec("ROLLBACK TO SAVEPOINT s1");
        exec("INSERT INTO sdr_o.t VALUES (10)");
        exec("COMMIT");

        assertEquals(1, num("SELECT count(*) FROM pg_namespace WHERE nspname = 'sdr_o'"));
        assertEquals(2, num("SELECT count(*) FROM sdr_o.t"));

        exec("BEGIN");
        exec("DROP SCHEMA sdr_o CASCADE");
        exec("COMMIT");
        assertEquals(0, num("SELECT count(*) FROM pg_namespace WHERE nspname = 'sdr_o'"));
    }

    /** A refused drop inside a transaction block aborts it, and leaves the schema where it was. */
    @Test
    void aRefusedDropInsideATransactionBlockAbortsIt() throws Exception {
        exec("CREATE SCHEMA sdr_p");
        exec("CREATE TABLE sdr_p.t (i int)");
        exec("BEGIN");
        assertEquals("2BP01", stateOf("DROP SCHEMA sdr_p RESTRICT"));
        assertEquals("25P02", stateOf("SELECT 1"));
        exec("ROLLBACK");
        assertEquals(1, num("SELECT count(*) FROM pg_namespace WHERE nspname = 'sdr_p'"));
        assertEquals(1, num("SELECT count(*) FROM pg_tables WHERE schemaname = 'sdr_p'"));
        exec("DROP SCHEMA sdr_p CASCADE");
    }

    // ------------------------------------------------------------ a rule's action names its relation when the rule is written

    /**
     * A rule's action is analysed when the rule is written, so the relation it writes to is the
     * one the path reached then -- however the path stands when the rule fires.
     */
    @Test
    void aRuleActionNamesItsRelationWhenTheRuleIsWrittenAndNotWhenItFires() throws Exception {
        exec("RESET search_path");
        exec("CREATE SCHEMA sdr_q");
        exec("CREATE TABLE sdr_q.d (m text)");
        exec("CREATE TABLE public.sdr_qs (i int)");
        exec("SET search_path = sdr_q, public");
        try {
            exec("CREATE RULE sdr_qr AS ON INSERT TO public.sdr_qs DO ALSO"
                    + " INSERT INTO d VALUES ('x')");
            // Read by the session that wrote it, the action's relation is written bare, because
            // the schema holding it is on that session's path.
            assertEquals("CREATE RULE sdr_qr AS\n"
                            + "    ON INSERT TO public.sdr_qs DO  INSERT INTO d (m)\n"
                            + "  VALUES ('x'::text);",
                    scalar("SELECT definition FROM pg_rules WHERE rulename = 'sdr_qr'"));

            exec("SET search_path = public");
            assertEquals("CREATE RULE sdr_qr AS\n"
                            + "    ON INSERT TO public.sdr_qs DO  INSERT INTO sdr_q.d (m)\n"
                            + "  VALUES ('x'::text);",
                    scalar("SELECT definition FROM pg_rules WHERE rulename = 'sdr_qr'"));

            exec("CREATE TABLE public.sdr_qd (m text)");
            exec("INSERT INTO public.sdr_qs VALUES (1)");
            assertEquals(1, num("SELECT count(*) FROM sdr_q.d"));
            assertEquals(0, num("SELECT count(*) FROM public.sdr_qd"));
            exec("DROP TABLE public.sdr_qd");
            exec("DROP TABLE public.sdr_qs CASCADE");
            exec("DROP SCHEMA sdr_q CASCADE");
        } finally {
            exec("RESET search_path");
        }
    }

    /**
     * The other way round: a relation of the name appearing later in a schema ahead on the path
     * is nothing to a rule already written, whose action goes on writing where it always did.
     */
    @Test
    void aRelationOfTheNameThatAppearsLaterIsNothingToARuleAlreadyWritten() throws Exception {
        exec("RESET search_path");
        exec("CREATE TABLE public.sdr_rd (m text)");
        exec("CREATE TABLE public.sdr_rk (i int)");
        exec("CREATE RULE sdr_rr AS ON INSERT TO public.sdr_rk DO ALSO"
                + " INSERT INTO sdr_rd VALUES ('t')");
        exec("CREATE SCHEMA sdr_r");
        exec("CREATE TABLE sdr_r.sdr_rd (m text)");
        exec("SET search_path = sdr_r, public");
        try {
            exec("INSERT INTO public.sdr_rk VALUES (1)");
            assertEquals(1, num("SELECT count(*) FROM public.sdr_rd"));
            assertEquals(0, num("SELECT count(*) FROM sdr_r.sdr_rd"));
        } finally {
            exec("RESET search_path");
        }
        // And with the schema that shared the name gone, the rule fires exactly as before.
        exec("DROP SCHEMA sdr_r CASCADE");
        exec("INSERT INTO public.sdr_rk VALUES (2)");
        assertEquals(2, num("SELECT count(*) FROM public.sdr_rd"));
        exec("DROP TABLE public.sdr_rk CASCADE");
        exec("DROP TABLE public.sdr_rd CASCADE");
    }

    /**
     * The stored definition is printed for the session reading it: a relation whose schema is on
     * that session's path is written without it, through either reader.
     */
    @Test
    void theDefinitionWritesTheActionsRelationTheReadersSearchPathCallsFor() throws Exception {
        exec("RESET search_path");
        exec("CREATE SCHEMA sdr_s");
        exec("CREATE TABLE sdr_s.l (m text)");
        exec("CREATE TABLE public.sdr_sk (i int)");
        exec("CREATE RULE sdr_sr AS ON INSERT TO public.sdr_sk DO ALSO"
                + " INSERT INTO sdr_s.l VALUES ('q')");
        String qualified = "CREATE RULE sdr_sr AS\n"
                + "    ON INSERT TO public.sdr_sk DO  INSERT INTO sdr_s.l (m)\n"
                + "  VALUES ('q'::text);";
        assertEquals(qualified, scalar("SELECT definition FROM pg_rules WHERE rulename = 'sdr_sr'"));
        assertEquals(qualified, scalar("SELECT pg_get_ruledef(r.oid) FROM pg_rewrite r"
                + " WHERE r.rulename = 'sdr_sr'"));

        exec("SET search_path = sdr_s, public");
        try {
            String bare = "CREATE RULE sdr_sr AS\n"
                    + "    ON INSERT TO public.sdr_sk DO  INSERT INTO l (m)\n"
                    + "  VALUES ('q'::text);";
            assertEquals(bare, scalar("SELECT definition FROM pg_rules WHERE rulename = 'sdr_sr'"));
            assertEquals(bare, scalar("SELECT pg_get_ruledef(r.oid) FROM pg_rewrite r"
                    + " WHERE r.rulename = 'sdr_sr'"));
        } finally {
            exec("RESET search_path");
        }
        // What the definition is printed as is nothing to where the action writes.
        exec("INSERT INTO public.sdr_sk VALUES (1)");
        assertEquals(1, num("SELECT count(*) FROM sdr_s.l"));
        exec("DROP TABLE public.sdr_sk CASCADE");
        exec("DROP SCHEMA sdr_s CASCADE");
    }

    /**
     * Two sessions reading the same rule at the same time each get the qualification their own
     * search path calls for, and neither reading settles anything for the other.
     */
    @Test
    void twoSessionsReadTheSameRuleThroughTheirOwnSearchPaths() throws Exception {
        exec("RESET search_path");
        exec("CREATE SCHEMA sdr_two");
        exec("CREATE TABLE sdr_two.l (m text)");
        exec("CREATE TABLE public.sdr_twok (i int)");
        exec("CREATE RULE sdr_twor AS ON INSERT TO public.sdr_twok DO ALSO"
                + " INSERT INTO sdr_two.l VALUES ('t')");
        String read = "SELECT definition FROM pg_rules WHERE rulename = 'sdr_twor'";
        String bare = "CREATE RULE sdr_twor AS\n"
                + "    ON INSERT TO public.sdr_twok DO  INSERT INTO l (m)\n"
                + "  VALUES ('t'::text);";
        String qualified = "CREATE RULE sdr_twor AS\n"
                + "    ON INSERT TO public.sdr_twok DO  INSERT INTO sdr_two.l (m)\n"
                + "  VALUES ('t'::text);";
        try (Connection other = otherSession()) {
            exec("SET search_path = sdr_two, public");
            execOn(other, "SET search_path = public");
            assertEquals(bare, scalar(read));
            assertEquals(qualified, scalarOn(other, read));
            // The other session moves its own path and only its own answer moves.
            execOn(other, "SET search_path = sdr_two");
            assertEquals(bare, scalarOn(other, read));
            assertEquals(bare, scalar(read));
        } finally {
            exec("RESET search_path");
        }
        exec("DROP TABLE public.sdr_twok CASCADE");
        exec("DROP SCHEMA sdr_two CASCADE");
    }

    /**
     * public is on every session's path, so a rule written with that qualifier reads back without
     * it -- the stored definition is the tree the server analysed, not the text that was typed.
     */
    @Test
    void aRuleWrittenWithTheQualifierPublicReadsBackWithoutIt() throws Exception {
        exec("RESET search_path");
        exec("CREATE TABLE public.sdr_td (i int)");
        exec("CREATE TABLE public.sdr_tk (i int)");
        exec("CREATE RULE sdr_tr AS ON INSERT TO public.sdr_tk DO ALSO"
                + " INSERT INTO public.sdr_td VALUES (new.i)");
        assertEquals("CREATE RULE sdr_tr AS\n"
                        + "    ON INSERT TO public.sdr_tk DO  INSERT INTO sdr_td (i)\n"
                        + "  VALUES (new.i);",
                scalar("SELECT definition FROM pg_rules WHERE rulename = 'sdr_tr'"));
        exec("INSERT INTO public.sdr_tk VALUES (5)");
        assertEquals("5", scalar("SELECT i FROM public.sdr_td"));
        exec("DROP TABLE public.sdr_tk CASCADE");
        exec("DROP TABLE public.sdr_td CASCADE");
    }

    /** The number of rows a write reports having changed. */
    private static int update(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            return st.executeUpdate(sql);
        }
    }

    /** A session of its own, so its search path is nobody else's. */
    private static Connection otherSession() throws SQLException {
        Connection other = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        other.setAutoCommit(true);
        return other;
    }

    private static void execOn(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static String scalarOn(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }
}
