package com.memgres.session;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Residual bug fixes verified against real PostgreSQL 18:
 * <ul>
 *   <li>H35 — explicit {@code public.x} qualifier in UPDATE/INSERT/DELETE/MERGE must not
 *       hit a same-named temp table (SELECT was already fixed).</li>
 *   <li>H36 — {@code RESET SESSION AUTHORIZATION} restores current_user/session_user.</li>
 *   <li>H37 — {@code SET datestyle='ISO, DMY'} (and YMD) applied to date INPUT parsing.</li>
 *   <li>L7 — {@code current_setting('custom.x', true)} after RESET returns '' not NULL.</li>
 *   <li>L8 — bare {@code CLUSTER t} after {@code CLUSTER t USING idx} succeeds.</li>
 * </ul>
 */
class SessionNameGucResidualsTest {

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
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    // === H35: explicit schema qualifier in DML must not hit a shadowing temp table ===

    @Test
    void h35_updateQualifiedHitsPublicNotTemp() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE public.h35_u(v text)");
            st.execute("INSERT INTO public.h35_u VALUES ('p')");
            st.execute("CREATE TEMP TABLE h35_u(v text)");
            st.execute("INSERT INTO h35_u VALUES ('t')");
            try {
                st.execute("UPDATE public.h35_u SET v = 'updated'");
                // public.h35_u should be updated
                try (ResultSet rs = st.executeQuery("SELECT v FROM public.h35_u")) {
                    assertTrue(rs.next());
                    assertEquals("updated", rs.getString(1));
                }
                // temp table must be untouched
                try (ResultSet rs = st.executeQuery("SELECT v FROM pg_temp.h35_u")) {
                    assertTrue(rs.next());
                    assertEquals("t", rs.getString(1));
                }
            } finally {
                st.execute("DROP TABLE IF EXISTS h35_u");
                st.execute("DROP TABLE IF EXISTS public.h35_u");
            }
        }
    }

    @Test
    void h35_insertQualifiedHitsPublicNotTemp() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE public.h35_i(v text)");
            st.execute("CREATE TEMP TABLE h35_i(v text)");
            try {
                st.execute("INSERT INTO public.h35_i VALUES ('into-public')");
                try (ResultSet rs = st.executeQuery("SELECT count(*) FROM public.h35_i")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
                try (ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_temp.h35_i")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1));
                }
            } finally {
                st.execute("DROP TABLE IF EXISTS h35_i");
                st.execute("DROP TABLE IF EXISTS public.h35_i");
            }
        }
    }

    @Test
    void h35_deleteQualifiedHitsPublicNotTemp() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE public.h35_d(v text)");
            st.execute("INSERT INTO public.h35_d VALUES ('p1'), ('p2')");
            st.execute("CREATE TEMP TABLE h35_d(v text)");
            st.execute("INSERT INTO h35_d VALUES ('t1')");
            try {
                st.execute("DELETE FROM public.h35_d");
                try (ResultSet rs = st.executeQuery("SELECT count(*) FROM public.h35_d")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1));
                }
                // temp untouched
                try (ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_temp.h35_d")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
            } finally {
                st.execute("DROP TABLE IF EXISTS h35_d");
                st.execute("DROP TABLE IF EXISTS public.h35_d");
            }
        }
    }

    // === H36: RESET SESSION AUTHORIZATION restores identity ===

    @Test
    void h36_resetSessionAuthorizationRestoresIdentity() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP ROLE IF EXISTS resid_h36");
            st.execute("CREATE ROLE resid_h36");
            String connecting;
            try (ResultSet rs = st.executeQuery("SELECT current_user")) {
                rs.next();
                connecting = rs.getString(1);
            }
            try {
                st.execute("SET SESSION AUTHORIZATION resid_h36");
                try (ResultSet rs = st.executeQuery("SELECT current_user, session_user")) {
                    rs.next();
                    assertEquals("resid_h36", rs.getString(1));
                    assertEquals("resid_h36", rs.getString(2));
                }
                // RESET must restore the connecting user for current_user AND session_user
                st.execute("RESET SESSION AUTHORIZATION");
                try (ResultSet rs = st.executeQuery("SELECT current_user, session_user")) {
                    rs.next();
                    assertEquals(connecting, rs.getString(1));
                    assertEquals(connecting, rs.getString(2));
                }
            } finally {
                st.execute("RESET SESSION AUTHORIZATION");
                st.execute("DROP ROLE IF EXISTS resid_h36");
            }
        }
    }

    // === H37: DateStyle field order applied to date INPUT parsing ===

    @Test
    void h37_dmyDateInput() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try {
                st.execute("SET datestyle = 'ISO, DMY'");
                try (ResultSet rs = st.executeQuery("SELECT '01/02/2026'::date, '13/07/2026'::date")) {
                    rs.next();
                    assertEquals("2026-02-01", rs.getString(1));
                    assertEquals("2026-07-13", rs.getString(2));
                }
            } finally {
                st.execute("SET datestyle = 'ISO, MDY'");
            }
        }
    }

    @Test
    void h37_mdyDefaultUnchanged() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("SET datestyle = 'ISO, MDY'");
            try (ResultSet rs = st.executeQuery("SELECT '01/02/2026'::date")) {
                rs.next();
                assertEquals("2026-01-02", rs.getString(1));
            }
        }
    }

    @Test
    void h37_ymdDateInput() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            try {
                st.execute("SET datestyle = 'ISO, YMD'");
                try (ResultSet rs = st.executeQuery("SELECT '2026/07/13'::date")) {
                    rs.next();
                    assertEquals("2026-07-13", rs.getString(1));
                }
            } finally {
                st.execute("SET datestyle = 'ISO, MDY'");
            }
        }
    }

    @Test
    void h37_dmyAppliesToColumnInsert() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE h37_dt(d date)");
            try {
                st.execute("SET datestyle = 'ISO, DMY'");
                st.execute("INSERT INTO h37_dt VALUES ('13/07/2026')");
                try (ResultSet rs = st.executeQuery("SELECT d FROM h37_dt")) {
                    rs.next();
                    assertEquals("2026-07-13", rs.getString(1));
                }
            } finally {
                st.execute("SET datestyle = 'ISO, MDY'");
                st.execute("DROP TABLE h37_dt");
            }
        }
    }

    // === L7: current_setting after RESET returns '' not NULL ===

    @Test
    void l7_currentSettingEmptyAfterReset() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("SET \"custom.resid_var\" = 'hello'");
            st.execute("RESET \"custom.resid_var\"");
            try (ResultSet rs = st.executeQuery("SELECT current_setting('custom.resid_var', true)")) {
                assertTrue(rs.next());
                // PG keeps the placeholder defined as '' after RESET
                assertEquals("", rs.getString(1));
                assertNotNull(rs.getString(1));
            }
        }
    }

    // === L8: bare CLUSTER after CLUSTER USING remembers the clustered index ===

    @Test
    void l8_bareClusterAfterClusterUsingSucceeds() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE l8_resid(id int primary key, v text)");
            st.execute("CREATE INDEX l8_resid_idx ON l8_resid(v)");
            try {
                st.execute("CLUSTER l8_resid USING l8_resid_idx");
                // bare CLUSTER must remember the clustered index and succeed
                st.execute("CLUSTER l8_resid");
            } finally {
                st.execute("DROP TABLE l8_resid");
            }
        }
    }
}
