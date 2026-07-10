package com.memgres.pgwire;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Group 8 regression (mtask-8 Wave 3 / Wave 4): reading an array-of-custom-enum column via real
 * pgjdbc fails with {@code PSQLException: No results were returned by the query} from
 * {@code TypeInfoCache.getArrayDelimiter}, in both text and binary transfer modes.
 *
 * <p>Root cause: {@code DdlExecutor.resolveColumnType} never marked a {@code CREATE TABLE ...
 * col enum_type[]} column as an array (its {@code arrayElementType} stayed {@code null} because
 * {@code DataType.fromPgName} silently returns {@code null}, not an exception, for a custom enum
 * base type name). So the column's {@link com.memgres.engine.Column} looked byte-for-byte
 * identical to a <em>scalar</em> enum column. {@code PgWireValueFormatter.columnTypeOid}
 * (mtask-6 Bug 7's fix) therefore advertised the enum <em>element</em> type's own OID for the
 * array column too — not a distinct array-type OID. pgjdbc's
 * {@code TypeInfoCache.getArrayDelimiter(oid)} issues {@code SELECT e.typdelim FROM pg_type t,
 * pg_type e WHERE t.oid = ? AND t.typelem = e.oid} for that oid; the element row's own
 * {@code typelem} is 0 (it has no element — it IS the element), so the join finds no rows and
 * pgjdbc raises "No results were returned by the query". This previously "worked" only because
 * pre-Bug-7 the placeholder OID was always 0 ({@code Oid.UNSPECIFIED}), which pgjdbc special-cases
 * to skip the pg_type query entirely and assume a comma delimiter.
 *
 * <p>Fix: mark enum array columns via {@code arrayElementType == DataType.ENUM} (mirroring the
 * existing convention used by {@code DmlValidationHelper}/{@code DmlExecutor}/{@code
 * SelectExecutor}/{@code CatalogSystemFunctions}, which already branch on
 * {@code col.getArrayElementType() != null} to detect array columns), advertise a distinct
 * per-enum array OID ({@code session.resolveOid("type:" + enumTypeName + "[]")}) for such columns,
 * and synthesize the corresponding {@code pg_type} row ({@code typtype='b'}, {@code
 * typcategory='A'}, {@code typelem} = the element's oid, {@code typdelim=','}) plus point the
 * element row's own {@code typarray} at it (real PG's element↔array pg_type linkage).
 */
class EnumArrayColumnOidResolutionTest {

    static Memgres memgres;
    static Connection textConn;
    static Connection binaryConn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        String base = memgres.getJdbcUrl();
        textConn = DriverManager.getConnection(base + "?binaryTransfer=false", memgres.getUser(), memgres.getPassword());
        textConn.setAutoCommit(true);
        binaryConn = DriverManager.getConnection(base + "?binaryTransfer=true&prepareThreshold=1", memgres.getUser(), memgres.getPassword());
        binaryConn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (textConn != null) textConn.close();
        if (binaryConn != null) binaryConn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(Connection conn, String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    /**
     * How the enum-array column comes to exist. The downstream app's real schema adds
     * {@code sellers.regions region_type[]} via {@code ALTER TABLE sellers ADD ...}, not
     * CREATE TABLE — and {@code DdlAlterTableExecutor.executeAddColumn}/{@code executeSetType}
     * each rebuild the {@link com.memgres.engine.Column} without carrying the resolved
     * {@code arrayElementType} (and, for SetType, without the resolved {@code enumTypeName}),
     * so all three DDL paths must be pinned independently.
     */
    enum ColumnDdlPath { CREATE_TABLE, ALTER_TABLE_ADD, ALTER_COLUMN_TYPE }

    static void setupSellers(Connection conn, ColumnDdlPath path) throws SQLException {
        exec(conn, "DROP TABLE IF EXISTS eacor_sellers");
        exec(conn, "DROP TYPE IF EXISTS eacor_region");
        exec(conn, "CREATE TYPE eacor_region AS ENUM ('north', 'south', 'east', 'west')");
        switch (path) {
            case CREATE_TABLE:
                exec(conn, "CREATE TABLE eacor_sellers (id serial primary key, name text, regions eacor_region[])");
                break;
            case ALTER_TABLE_ADD:
                exec(conn, "CREATE TABLE eacor_sellers (id serial primary key, name text)");
                exec(conn, "ALTER TABLE eacor_sellers ADD regions eacor_region[]");
                break;
            case ALTER_COLUMN_TYPE:
                exec(conn, "CREATE TABLE eacor_sellers (id serial primary key, name text, regions text)");
                exec(conn, "ALTER TABLE eacor_sellers ALTER COLUMN regions TYPE eacor_region[]");
                break;
        }
        exec(conn, "INSERT INTO eacor_sellers(name, regions) VALUES ('acme', ARRAY['north','south']::eacor_region[])");
    }

    static void assertArrayRoundTrips(Connection conn, ColumnDdlPath path) throws SQLException {
        setupSellers(conn, path);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name, regions FROM eacor_sellers ORDER BY name")) {
            assertTrue(rs.next());
            Array array = assertDoesNotThrow(() -> rs.getArray("regions"),
                    "rs.getArray(\"regions\") must not throw for an enum-array column");
            ResultSet elems = assertDoesNotThrow((org.junit.jupiter.api.function.ThrowingSupplier<ResultSet>) array::getResultSet,
                    "Array.getResultSet() must not throw TypeInfoCache.getArrayDelimiter's "
                            + "\"No results were returned by the query\"");
            java.util.List<String> values = new java.util.ArrayList<>();
            while (elems.next()) {
                values.add(elems.getString(2));
            }
            assertEquals(java.util.List.of("north", "south"), values);
        } finally {
            exec(conn, "DROP TABLE IF EXISTS eacor_sellers");
            exec(conn, "DROP TYPE IF EXISTS eacor_region");
        }
    }

    static void assertColumnTypeNameIsArrayTypeName(ColumnDdlPath path) throws SQLException {
        setupSellers(textConn, path);
        try (Statement s = textConn.createStatement();
             ResultSet rs = s.executeQuery("SELECT regions FROM eacor_sellers")) {
            ResultSetMetaData md = rs.getMetaData();
            String typeName = assertDoesNotThrow(() -> md.getColumnTypeName(1));
            // pgjdbc returns the raw pg_type.typname for array types, which PG itself always
            // spells with a leading underscore (e.g. "_text", "_int4") rather than "region[]".
            assertEquals("_eacor_region", typeName);
        } finally {
            exec(textConn, "DROP TABLE IF EXISTS eacor_sellers");
            exec(textConn, "DROP TYPE IF EXISTS eacor_region");
        }
    }

    @Test
    void enumArrayColumn_readsCorrectly_overTextTransfer() throws SQLException {
        assertArrayRoundTrips(textConn, ColumnDdlPath.CREATE_TABLE);
    }

    @Test
    void enumArrayColumn_readsCorrectly_overBinaryTransfer() throws SQLException {
        assertArrayRoundTrips(binaryConn, ColumnDdlPath.CREATE_TABLE);
    }

    @Test
    void getColumnTypeName_on_enum_array_column_resolves_array_type_name() throws SQLException {
        assertColumnTypeNameIsArrayTypeName(ColumnDdlPath.CREATE_TABLE);
    }

    @Test
    void alterTableAddColumn_enumArray_readsCorrectly_overTextTransfer() throws SQLException {
        assertArrayRoundTrips(textConn, ColumnDdlPath.ALTER_TABLE_ADD);
    }

    @Test
    void alterTableAddColumn_enumArray_readsCorrectly_overBinaryTransfer() throws SQLException {
        assertArrayRoundTrips(binaryConn, ColumnDdlPath.ALTER_TABLE_ADD);
    }

    @Test
    void alterTableAddColumn_enumArray_columnTypeName_isArrayTypeName() throws SQLException {
        assertColumnTypeNameIsArrayTypeName(ColumnDdlPath.ALTER_TABLE_ADD);
    }

    @Test
    void alterColumnType_toEnumArray_readsCorrectly_overTextTransfer() throws SQLException {
        assertArrayRoundTrips(textConn, ColumnDdlPath.ALTER_COLUMN_TYPE);
    }

    @Test
    void alterColumnType_toEnumArray_readsCorrectly_overBinaryTransfer() throws SQLException {
        assertArrayRoundTrips(binaryConn, ColumnDdlPath.ALTER_COLUMN_TYPE);
    }

    @Test
    void alterColumnType_toEnumArray_columnTypeName_isArrayTypeName() throws SQLException {
        assertColumnTypeNameIsArrayTypeName(ColumnDdlPath.ALTER_COLUMN_TYPE);
    }

    /** Regression guard: the scalar enum OID path (mtask-6 Bug 7) must be unaffected. */
    @Test
    void scalarEnumColumn_stillResolvesElementTypeName() throws SQLException {
        exec(textConn, "DROP TABLE IF EXISTS eacor_scalar");
        exec(textConn, "DROP TYPE IF EXISTS eacor_scalar_enum");
        exec(textConn, "CREATE TYPE eacor_scalar_enum AS ENUM ('a', 'b')");
        exec(textConn, "CREATE TABLE eacor_scalar (id int primary key, v eacor_scalar_enum)");
        exec(textConn, "INSERT INTO eacor_scalar VALUES (1, 'a')");
        try {
            try (Statement s = textConn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT v FROM eacor_scalar")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals("eacor_scalar_enum", md.getColumnTypeName(1));
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
            }
        } finally {
            exec(textConn, "DROP TABLE IF EXISTS eacor_scalar");
            exec(textConn, "DROP TYPE IF EXISTS eacor_scalar_enum");
        }
    }
}
