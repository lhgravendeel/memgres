# Memgres

In-memory PostgreSQL-compatible database for Java testing. No Docker, no external processes — just add the dependency and write tests against real PostgreSQL SQL.

Memgres implements the PostgreSQL wire protocol (v3) and speaks directly to standard PostgreSQL JDBC drivers. Your tests run the same SQL that runs in production.

**Website:** [memgres.com](https://memgres.com)

## Quick Start

### Maven

```xml
<dependency>
    <groupId>com.memgres</groupId>
    <artifactId>memgres-junit5</artifactId>
    <version>0.2.0</version>
    <scope>test</scope>
</dependency>
```

### Gradle

```groovy
testImplementation 'com.memgres:memgres-junit5:0.2.0'
testImplementation 'org.postgresql:postgresql:42.7.5'
```

### JUnit 5

```java
@MemgresTest
class MyTest {

    @Test
    void insertAndQuery(Connection conn) throws SQLException {
        conn.createStatement().execute(
            "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)");
        conn.createStatement().execute(
            "INSERT INTO users (name) VALUES ('Alice'), ('Bob')");

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT name FROM users ORDER BY id");
        rs.next();
        assertEquals("Alice", rs.getString(1));
    }
}
```

That's it. `@MemgresTest` starts an in-memory database, injects a `Connection`, and tears it down after the test class.

## Features

### SQL Compatibility

Memgres targets PostgreSQL 18 compatibility with 9,500+ passing tests. Supported features include:

- **Queries**: SELECT, JOIN (all types), subqueries, CTEs (recursive), window functions, GROUPING SETS / ROLLUP / CUBE, DISTINCT ON, LATERAL, set operations (UNION / INTERSECT / EXCEPT)
- **DML**: INSERT (ON CONFLICT / RETURNING), UPDATE (FROM), DELETE (USING), MERGE, COPY (text / CSV / binary, FROM STDIN, SELECT ... TO), TRUNCATE
- **DDL**: tables, views, materialized views, indexes, sequences, schemas, domains, constraints (PK, FK, UNIQUE, CHECK, exclusion), partitioning (RANGE / LIST / HASH)
- **Types**: 40+ data types — integer, bigint, numeric, text, boolean, date, timestamp(tz), interval, UUID, JSON/JSONB, arrays, enums, composite types, range types, bytea, bit strings, geometric types, network types (inet/cidr/macaddr), money, XML, tsvector/tsquery
- **Functions**: 200+ built-in functions across string, math, date/time, JSON/JSONB, array, full-text search, geometric, network, range, XML, and more
- **PL/pgSQL**: stored procedures and functions, DO blocks, variable scoping, control flow (IF/ELSIF/ELSE, WHILE, FOR, LOOP), exception handling with SQLSTATE, RAISE NOTICE/WARNING/ERROR, cursors, RETURN QUERY, PERFORM, EXECUTE, GET DIAGNOSTICS, IN/OUT/INOUT parameters
- **Transactions**: BEGIN / COMMIT / ROLLBACK, savepoints, isolation levels (READ COMMITTED, REPEATABLE READ, SERIALIZABLE)
- **Locking**: SELECT FOR UPDATE / SHARE, SKIP LOCKED, NOWAIT, table-level LOCK, advisory locks (session and transaction scoped), deadlock detection
- **Security**: roles (CREATE / ALTER / DROP ROLE), GRANT / REVOKE, ALTER DEFAULT PRIVILEGES, row-level security policies
- **EXPLAIN**: supports ANALYZE, VERBOSE, and output formats (TEXT, JSON, YAML, XML)
- **LISTEN / NOTIFY**: channel-based notifications with transaction semantics (deferred until COMMIT, discarded on ROLLBACK)
- **Multiple databases**: CREATE / DROP / ALTER DATABASE, database isolation, DROP DATABASE WITH (FORCE), ALTER DATABASE RENAME TO, pg_database catalog, configurable default database name and auto-create behavior
- **Other**: triggers (BEFORE / AFTER / INSTEAD OF, transition tables), rules, prepared statements, cursors, full-text search (to_tsvector, to_tsquery, ts_rank, websearch_to_tsquery, @@)

### Protocol

Memgres implements the PostgreSQL wire protocol v3, including the extended query protocol (Parse / Bind / Describe / Execute / Sync) and the COPY sub-protocol. This means it works with:

- Standard PostgreSQL JDBC drivers (`org.postgresql:postgresql`)
- Any client that speaks the PostgreSQL protocol (psql, psycopg2, node-postgres, etc.)
- Connection pools (HikariCP, etc.)
- ORMs and query builders (jOOQ, MyBatis, Hibernate, etc.)
- **pg_dump / pg_restore** (see below)

### System Catalogs

Memgres implements 80+ `pg_catalog` tables and 13+ `information_schema` views, including `pg_class`, `pg_attribute`, `pg_type`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_namespace`, `pg_roles`, `information_schema.tables`, `information_schema.columns`, and more. This provides compatibility with ORMs, migration tools (Flyway, Liquibase), and database introspection tools that query system catalogs.

### Test Isolation

```java
// Per-class (default) — one database per test class, tests share state
@MemgresTest(isolation = IsolationMode.PER_CLASS)

// Per-method — fresh database per test method
@MemgresTest(isolation = IsolationMode.PER_METHOD)
```

### Migrations and Init Scripts

```java
@MemgresTest(
    migrationDirs = "db/migrations",       // sorted .sql files from classpath
    initScripts = "seed.sql",              // runs after migrations
    snapshotAfterInit = true               // restore before each test
)
class MyTest { ... }
```

With `snapshotAfterInit = true`, Memgres captures the database state after initialization and restores it before each test method. Schema is preserved; only row data and sequence values are rolled back — making it fast.

### Builder API

For more control, use `MemgresExtension` directly:

```java
class MyTest {

    @RegisterExtension
    static MemgresExtension db = MemgresExtension.builder()
        .migrationDir("db/migrations")
        .initScript("seed.sql")
        .isolation(IsolationMode.PER_CLASS)
        .snapshotAfterInit(true)
        .restoreBeforeEach(true)
        .build();

    @Test
    void test(Connection conn) { ... }
}
```

### Parameter Injection

`@MemgresTest` and `MemgresExtension` support injecting:

- `java.sql.Connection` — fresh connection per test method (auto-closed)
- `javax.sql.DataSource` — for connection pooling or framework integration
- `com.memgres.core.Memgres` — the underlying database instance

### pg_dump & pg_restore

Memgres is compatible with the real `pg_dump` and `pg_restore` binaries. You can dump from Memgres and restore into Memgres (or a real PostgreSQL instance, and vice versa). Supported formats:

- **Custom format** (`-Fc`) — including compressed (`-Fc -Z9`)
- **Directory format** (`-Fd`)
- **Plain SQL** (`-Fp`)
- **Tar format** (`-Ft`)

Round-trip data integrity is verified across schemas, tables, enums, foreign keys, views, and row data.

> **Note:** Parallel dump/restore (`-j`) is not yet supported — it requires `pg_export_snapshot()`, which Memgres does not currently implement.

### Spring Boot Integration

Use `@DynamicPropertySource` to point Spring Boot at Memgres:

```java
@SpringBootTest
class MyIntegrationTest {

    @RegisterExtension
    static MemgresExtension db = MemgresExtension.builder()
        .snapshotAfterInit(true)
        .restoreBeforeEach(true)
        .build();

    @DynamicPropertySource
    static void configureDataSource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", db::getJdbcUrl);
        registry.add("spring.datasource.username", () -> "memgres");
        registry.add("spring.datasource.password", () -> "memgres");
    }

    @Test
    void test(@Autowired JdbcTemplate jdbc) {
        // Spring manages the DataSource, Flyway runs migrations, tests get a clean DB each time
    }
}
```

### Quarkus Integration

Use a `QuarkusTestResourceLifecycleManager` to wire Memgres into Quarkus integration tests:

```java
public class MemgresResource implements QuarkusTestResourceLifecycleManager {

    static MemgresExtension db = MemgresExtension.builder()
        .isolation(IsolationMode.GLOBAL)
        .build();

    @Override
    public Map<String, String> start() {
        db.startGlobal();
        return Map.of(
            "quarkus.datasource.jdbc.url", db.getJdbcUrl(),
            "quarkus.datasource.username", "memgres",
            "quarkus.datasource.password", "memgres",
            "quarkus.datasource.devservices.enabled", "false"
        );
    }

    @Override
    public void stop() {}
}
```

```java
@QuarkusTest
@WithTestResource(MemgresResource.class)  // or @QuarkusTestResource on Quarkus < 3.13
class MyIT {
    // Quarkus runs Flyway/Liquibase migrations on startup against Memgres
}
```

> **Note:** `devservices.enabled=false` prevents Quarkus Dev Services from starting a Testcontainers PostgreSQL alongside Memgres. See the [memgres-junit5 README](memgres-junit5/README.md) for snapshot/restore patterns.

### Standalone Usage

Memgres can also be used without JUnit, for local development or prototyping:

```java
try (Memgres db = Memgres.builder().port(5432).build().start()) {
    System.out.println("JDBC URL: " + db.getJdbcUrl());
    // Connect with any PostgreSQL client
    Thread.currentThread().join(); // keep running
}
```

## Modules

| Module | Description |
|--------|-------------|
| `memgres-core` | SQL engine, PgWire server, type system |
| `memgres-junit5` | JUnit 5 extension (`@MemgresTest`, parameter injection, isolation modes) |

## Connection Details

| Property | Value |
|----------|-------|
| JDBC URL | `jdbc:postgresql://localhost:{port}/memgres` (default; configurable via `defaultDatabaseName`) |
| Username | `memgres` |
| Password | `memgres` |
| Driver | `org.postgresql.Driver` (standard PostgreSQL JDBC) |

The port is randomly assigned by default (pass `port(0)` or omit). Use `db.getPort()` or `db.getJdbcUrl()` to retrieve it.

## Configuration

```java
Memgres db = Memgres.builder()
    .port(0)                      // 0 = random available port (default)
    .maxConnections(100)          // max concurrent connections (default: 100, 0 = unlimited)
    .bindAddress("localhost")     // listen address (default: localhost)
    .logAllStatements(false)      // log SQL at INFO level (default: false)
    .defaultDatabaseName("mydb")  // name of the default database (default: "memgres")
    .autoCreateDatabases(true)    // create databases on connect if they don't exist (default: true)
    .build()
    .start();
```

### Multiple Databases

Memgres supports multiple databases within a single server instance, just like PostgreSQL. Each database is fully isolated with its own tables, schemas, sequences, functions, views, and enums.

```java
try (Memgres db = Memgres.builder().port(0).build().start()) {
    try (Connection conn = DriverManager.getConnection(db.getJdbcUrl())) {
        conn.createStatement().execute("CREATE DATABASE analytics");
        conn.createStatement().execute("CREATE DATABASE reporting");
    }

    // Connect to a specific database
    try (Connection conn = DriverManager.getConnection(
            "jdbc:postgresql://localhost:" + db.getPort() + "/analytics")) {
        conn.createStatement().execute("CREATE TABLE events (id SERIAL, name TEXT)");
    }
}
```

By default, databases are auto-created when a client connects to a name that doesn't exist yet. To disable this and match strict PostgreSQL behavior (where the database must be created first), set `autoCreateDatabases(false)`:

```java
Memgres db = Memgres.builder()
    .autoCreateDatabases(false)   // connecting to a non-existent database returns error 3D000
    .build()
    .start();
```

Supported database management commands:

| Command | Description |
|---------|-------------|
| `CREATE DATABASE name` | Create a new database |
| `DROP DATABASE name` | Drop a database (fails if other sessions are connected) |
| `DROP DATABASE IF EXISTS name` | Drop if exists, no error if missing |
| `DROP DATABASE name WITH (FORCE)` | Terminate all sessions and drop |
| `ALTER DATABASE name RENAME TO newname` | Rename a database |
| `ALTER DATABASE name SET ...` | Accepted (no-op) |
| `ALTER DATABASE name OWNER TO ...` | Accepted (no-op) |

## Known Limitations

Memgres is designed for testing, not production. The following PostgreSQL features are **not supported**:

| Category | Details |
|----------|---------|
| **Persistence** | Fully in-memory. All data is lost on shutdown (by design). |
| **Index types** | Only btree indexes. No GiST, GIN, BRIN, SP-GiST, or hash indexes. |
| **Extensions** | `CREATE EXTENSION` enables built-in support for **uuid-ossp**, **pgcrypto**, **pg_trgm**, **fuzzystrmatch**, and **unaccent**. C-based extensions (PostGIS, etc.) are not available. |
| **COPY PROGRAM** | `COPY TO/FROM PROGRAM` (external command execution) is not supported. Use `COPY TO/FROM` with files or stdin/stdout instead. |
| **Replication** | No WAL, streaming replication, or logical replication. |
| **Foreign data wrappers** | No FDW or foreign tables. |
| **Tablespaces** | Not supported (all storage is in-memory). |
| **SSL/TLS** | Connections are unencrypted (not needed for local testing). |
| **Statistics** | `pg_stat_*` tables exist as stubs for compatibility but do not collect real statistics. |
| **Parallel queries** | No parallel query execution. Parallel pg_dump/pg_restore (`-j`) not yet supported. |
| **Large objects** | `lo_*` functions are parsed but have minimal support. |
| **Collations** | Basic collation only. No ICU or custom collation support. |
| **Cross-database queries** | Multiple databases are supported, but cross-database queries are not (same as PostgreSQL). |
| **Event triggers** | DDL event triggers are not supported. |

## Comparison to Alternatives

| | Memgres | Testcontainers (PostgreSQL) | H2 (PG compat mode) | embedded-postgres |
|---|---|---|---|---|
| **Docker required** | No | Yes | No | No |
| **Native binary** | No | No | No | Yes (downloads PG) |
| **Startup time** | Milliseconds | Seconds | Milliseconds | Seconds |
| **Wire protocol** | Real PG v3 | Real PG | H2 protocol | Real PG |
| **pg_dump compatible** | Yes | Yes | No | Yes |
| **SQL fidelity** | PG 18 subset | Full PG | Partial PG syntax | Full PG |
| **System catalogs** | 80+ tables | Full PG | Limited | Full PG |
| **PL/pgSQL** | Yes | Yes | No | Yes |
| **CI-friendly** | Yes (no deps) | Needs Docker | Yes (no deps) | Needs network |

**Choose Memgres when** you want fast, zero-dependency PostgreSQL-compatible tests that don't require Docker or native binaries, with enough SQL fidelity for most application testing.

**Choose Testcontainers when** you need 100% PostgreSQL compatibility, including extensions (PostGIS, etc.), advanced index types, or features Memgres doesn't support.

## Requirements

- Java 8+
- PostgreSQL JDBC driver (`org.postgresql:postgresql`) on the test classpath

## Building from Source

```bash
git clone https://github.com/lhgravendeel/memgres.git
cd memgres
mvn clean verify
```

## License

[Apache License 2.0](LICENSE)
