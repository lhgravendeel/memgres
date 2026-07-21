package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Residual type-facade bugs from bugs-review.md (PRs #79-#81 follow-up):
 * H18 (abbreviated cidr / single-octet inet), H20 (IPv6 cidr abbrev, inet_merge cross-family),
 * H21 (macaddr8-&gt;macaddr range check), H22 (bit(n) exact length on INSERT),
 * H23 (position(bytea in bytea), set_byte returns bytea), H26 (regexp_replace backref,
 * SQL-regex substring), M23 (concat_ws arity), M24 (COPY CSV custom escape),
 * M1 (jsonb scalar -&gt; 0), L1 (negative bytea substring), L3 (text(inet) keeps /32).
 * Expected values verified against real PostgreSQL 18.
 */
class TypeFacadeResidualsTest {

    static Memgres memgres;
    Connection conn;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void connect() throws SQLException {
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterEach
    void disconnect() throws SQLException {
        if (conn != null) conn.close();
    }

    private String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private void expectError(String sql, String sqlState) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            fail("Expected error " + sqlState + " for: " + sql);
        } catch (SQLException e) {
            assertEquals(sqlState, e.getSQLState(), "Wrong SQLSTATE for: " + sql + " — " + e.getMessage());
        }
    }

    // ---- H18: abbreviated cidr input, single-octet inet is cidr-only ----

    @Test void cidr_single_octet() throws SQLException { assertEquals("10.0.0.0/8", q("SELECT '10'::cidr::text")); }
    @Test void cidr_two_octets() throws SQLException { assertEquals("10.1.0.0/16", q("SELECT '10.1'::cidr::text")); }
    @Test void cidr_two_octets_high() throws SQLException { assertEquals("128.1.0.0/16", q("SELECT '128.1'::cidr::text")); }
    @Test void cidr_three_octets() throws SQLException { assertEquals("192.168.1.0/24", q("SELECT '192.168.1'::cidr::text")); }
    @Test void cidr_octet_with_slash() throws SQLException { assertEquals("10.0.0.0/8", q("SELECT '10/8'::cidr::text")); }
    @Test void cidr_full_form_unchanged() throws SQLException { assertEquals("192.168.1.0/24", q("SELECT '192.168.1.0/24'::cidr::text")); }
    @Test void cidr_host_bits_still_rejected() throws SQLException { expectError("SELECT '192.168.1.5/24'::cidr", "22P02"); }

    @Test void inet_single_octet_rejected() throws SQLException { expectError("SELECT '10'::inet", "22P02"); }
    @Test void inet_two_octets_rejected() throws SQLException { expectError("SELECT '10.1'::inet", "22P02"); }
    @Test void inet_three_octets_rejected() throws SQLException { expectError("SELECT '10.1.2'::inet", "22P02"); }
    @Test void inet_full_form_ok() throws SQLException { assertEquals("1.2.3.4/32", q("SELECT '1.2.3.4'::inet::text")); }

    // ---- H20: abbrev() on IPv6 cidr, inet_merge cross-family SQLSTATE ----

    @Test void abbrev_ipv6_cidr_32() throws SQLException { assertEquals("2001:db8/32", q("SELECT abbrev('2001:db8::/32'::cidr)")); }
    @Test void abbrev_ipv6_cidr_48() throws SQLException { assertEquals("2001:db8::/48", q("SELECT abbrev('2001:db8::/48'::cidr)")); }
    @Test void abbrev_ipv6_cidr_64() throws SQLException { assertEquals("2001:db8::1/64", q("SELECT abbrev('2001:db8:0:1::/64'::cidr)")); }
    @Test void abbrev_ipv6_cidr_104() throws SQLException { assertEquals("2001:db8::ff00/104", q("SELECT abbrev('2001:db8::ff00:0/104'::cidr)")); }
    @Test void abbrev_ipv6_cidr_zero() throws SQLException { assertEquals("::/0", q("SELECT abbrev('::/0'::cidr)")); }
    @Test void abbrev_ipv4_cidr_unchanged() throws SQLException { assertEquals("10.1.0/20", q("SELECT abbrev('10.1.0.0/20'::cidr)")); }

    @Test void inet_merge_cross_family_22023() throws SQLException {
        expectError("SELECT inet_merge('10.0.0.0/8'::inet, '2001:db8::/32'::inet)", "22023");
    }

    // ---- H21: macaddr8 -> macaddr conversion only when ff:fe in the middle ----

    @Test void macaddr8_to_macaddr_valid() throws SQLException {
        assertEquals("01:02:03:04:05:06", q("SELECT ('01:02:03:ff:fe:04:05:06'::macaddr8)::macaddr::text"));
    }
    @Test void macaddr8_to_macaddr_out_of_range() throws SQLException {
        expectError("SELECT ('01:02:03:04:05:06:07:08'::macaddr8)::macaddr", "22003");
    }
    @Test void macaddr8_to_macaddr_ffff_out_of_range() throws SQLException {
        expectError("SELECT ('01:02:03:ff:ff:04:05:06'::macaddr8)::macaddr", "22003");
    }

    // ---- L3: text(inet) keeps the /32 suffix ----

    @Test void text_inet_keeps_prefix() throws SQLException { assertEquals("1.2.3.4/32", q("SELECT text('1.2.3.4'::inet)")); }
    @Test void text_inet_ipv6() throws SQLException { assertEquals("2001:db8::1/128", q("SELECT text('2001:db8::1'::inet)")); }
    @Test void text_inet_with_mask() throws SQLException { assertEquals("192.168.1.5/24", q("SELECT text('192.168.1.5/24'::inet)")); }

    // ---- H22: bit(n) exact-length enforcement on INSERT (cast still pads) ----

    @Test void bit_column_too_short_rejected() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zz_bit_h22 (b bit(3))");
            try {
                expectError("INSERT INTO zz_bit_h22 VALUES (B'10')", "22026");
                expectError("INSERT INTO zz_bit_h22 VALUES (B'1000')", "22026");
                // Exact length and explicit padded cast both succeed.
                s.execute("INSERT INTO zz_bit_h22 VALUES (B'100')");
                s.execute("INSERT INTO zz_bit_h22 VALUES (B'10'::bit(3))");
                try (ResultSet rs = s.executeQuery("SELECT b::text FROM zz_bit_h22 ORDER BY b")) {
                    assertTrue(rs.next()); assertEquals("100", rs.getString(1));
                    assertTrue(rs.next()); assertEquals("100", rs.getString(1));
                }
            } finally {
                s.execute("DROP TABLE zz_bit_h22");
            }
        }
    }

    // ---- H23: position(bytea in bytea), set_byte returns bytea ----

    @Test void position_bytea_in_bytea() throws SQLException {
        assertEquals("2", q("SELECT position('\\x34'::bytea in '\\x123456'::bytea)"));
    }
    @Test void position_bytea_not_found() throws SQLException {
        assertEquals("0", q("SELECT position('\\x99'::bytea in '\\x123456'::bytea)"));
    }
    @Test void set_byte_returns_bytea() throws SQLException {
        assertEquals("\\xff34", q("SELECT set_byte('\\x1234'::bytea, 0, 255)::text"));
    }

    // ---- H26: regexp_replace backref to missing group; SQL-regex substring ----

    @Test void regexp_replace_missing_backref_empty() throws SQLException {
        assertEquals("ac", q("SELECT regexp_replace('abc','b','\\1')"));
    }
    @Test void regexp_replace_valid_backref() throws SQLException {
        assertEquals("he[ll]o", q("SELECT regexp_replace('hello','(ll)','[\\1]')"));
    }
    @Test void substring_sql_regex_escape() throws SQLException {
        assertEquals("oob", q("SELECT substring('foobar' from '%#\"o_b#\"%' for '#')"));
    }

    // ---- M23: concat_ws with only the separator argument errors ----

    @Test void concat_ws_arity() throws SQLException { expectError("SELECT concat_ws(',')", "42883"); }
    @Test void concat_ws_normal_still_works() throws SQLException { assertEquals("a,b", q("SELECT concat_ws(',', 'a', 'b')")); }

    // ---- M1: jsonb scalar -> 0 echoes the scalar; [] subscript stays NULL ----

    @Test void jsonb_scalar_number_arrow_zero() throws SQLException { assertEquals("123", q("SELECT '123'::jsonb -> 0")); }
    @Test void jsonb_scalar_string_arrow_zero() throws SQLException { assertEquals("\"abc\"", q("SELECT '\"abc\"'::jsonb -> 0")); }
    @Test void jsonb_scalar_arrow_other_index_null() throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT '123'::jsonb -> 1")) {
            assertTrue(rs.next()); assertNull(rs.getString(1));
        }
    }
    @Test void jsonb_scalar_subscript_still_null() throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT ('123'::jsonb)[0]")) {
            assertTrue(rs.next()); assertNull(rs.getString(1));
        }
    }
    @Test void jsonb_array_arrow_still_works() throws SQLException { assertEquals("10", q("SELECT '[10,20]'::jsonb -> 0")); }
    @Test void text_subscript_still_rejected() throws SQLException { expectError("SELECT ('hello'::text)[0]", "42883"); }

    // ---- L1: negative substring length on bytea ----

    @Test void bytea_negative_substring_length() throws SQLException {
        expectError("SELECT substring('\\x123456'::bytea from 1 for -1)", "22011");
    }

    // ---- M24: COPY CSV with a custom ESCAPE escapes an embedded quote (not doubled) ----

    @Test void copy_csv_custom_escape_roundtrips() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zz_csv_m24 (t text)");
            s.execute("INSERT INTO zz_csv_m24 VALUES ('quo\"te')"); // field value: quo"te
            try {
                org.postgresql.copy.CopyManager cm =
                        ((org.postgresql.core.BaseConnection) conn).getCopyAPI();
                java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                cm.copyOut("COPY zz_csv_m24 TO STDOUT WITH (FORMAT csv, ESCAPE E'\\\\')", out);
                // Embedded quote escaped with the custom ESCAPE char, not doubled.
                assertEquals("\"quo\\\"te\"", out.toString().trim());
                // Round-trips back in under the same options.
                s.execute("DELETE FROM zz_csv_m24");
                cm.copyIn("COPY zz_csv_m24 FROM STDIN WITH (FORMAT csv, ESCAPE E'\\\\')",
                        new java.io.ByteArrayInputStream(out.toByteArray()));
                try (ResultSet rs = s.executeQuery("SELECT t FROM zz_csv_m24")) {
                    assertTrue(rs.next());
                    assertEquals("quo\"te", rs.getString(1));
                }
            } finally {
                s.execute("DROP TABLE zz_csv_m24");
            }
        }
    }
}
