# memgres-junit5

JUnit 5 extension for [Memgres](https://github.com/memgres/memgres) — an in-memory PostgreSQL-compatible database. No Docker, no external processes. Tests start in milliseconds.

## Setup

### Maven

```xml
<dependency>
    <groupId>com.memgres</groupId>
    <artifactId>memgres-junit5</artifactId>
    <version>0.1.0</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.7.5</version>
    <scope>test</scope>
</dependency>
```

### Gradle

```groovy
testImplementation 'com.memgres:memgres-junit5:0.1.0'
testImplementation 'org.postgresql:postgresql:42.7.5'
```

## Quick Start

```java
@MemgresTest
class MyTest {
    @Test
    void query(Connection conn) throws SQLException {
        conn.createStatement().execute("CREATE TABLE t (id SERIAL, name TEXT)");
        conn.createStatement().execute("INSERT INTO t (name) VALUES ('hello')");

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t");
        assertTrue(rs.next());
        assertEquals("hello", rs.getString("name"));
    }
}
```

`Connection`, `DataSource`, and `Memgres` can all be injected as test method parameters.

## Migrations + Test Data

Point at a directory of SQL files (executed in alphabetical order) and/or init scripts:

```java
@MemgresTest(
    migrationDirs = "db/migrations",   // V001__create_users.sql, V002__create_orders.sql, ...
    initScripts = "test-data.sql"      // INSERT test fixtures
)
class MyTest { ... }
```

Order: migration dirs (sorted) → init scripts.

## Builder API

For more control, use `@RegisterExtension` instead of the annotation:

```java
@RegisterExtension
static MemgresExtension db = MemgresExtension.builder()
    .migrationDir("db/migrations")
    .initScript("test-data.sql")
    .build();

@Test
void test() throws SQLException {
    try (Connection conn = db.getConnection()) {
        // ...
    }
}
```

## Isolation Modes

### PER_CLASS (default)

One database per test class. Tests share state.

```java
.isolation(IsolationMode.PER_CLASS)
```

### PER_METHOD

Fresh database for each test method. Slower but completely isolated.

```java
.isolation(IsolationMode.PER_METHOD)
```

### GLOBAL

One database for the entire test suite (JVM lifetime). Ideal for integration tests where an app under test connects to the database.

```java
.isolation(IsolationMode.GLOBAL)
```

## Snapshot & Restore

Take a snapshot after init and automatically restore before each test. Tests can freely mutate data — it resets every time.

```java
@RegisterExtension
static MemgresExtension db = MemgresExtension.builder()
    .migrationDir("db/migrations")
    .initScript("test-data.sql")
    .snapshotAfterInit(true)
    .restoreBeforeEach(true)
    .build();
```

Or via annotation:

```java
@MemgresTest(
    migrationDirs = "db/migrations",
    initScripts = "test-data.sql",
    snapshotAfterInit = true
)
```

Restore only touches row data and sequences — schema/DDL is untouched. This is fast.

You can also call `db.snapshot()` and `db.restore()` manually at any point.

## Integration Tests (Quarkus, etc.)

For integration tests where a real app starts and connects to the database:

```java
@RegisterExtension
static MemgresExtension db = MemgresExtension.builder()
    .migrationDir("db/migrations")
    .initScript("test-data.sql")
    .isolation(IsolationMode.GLOBAL)
    .snapshotAfterInit(true)
    .restoreBeforeEach(true)
    .systemProperty("test.db.url")   // sets System.setProperty on start
    .build();
```

The app reads the JDBC URL from `System.getProperty("test.db.url")`.

### Quarkus TestResource

For Quarkus, where the framework controls app startup:

```java
public class MemgresResource implements QuarkusTestResourceLifecycleManager {

    static MemgresExtension db = MemgresExtension.builder()
        .initScript("test-data.sql")
        .build();

    @Override
    public Map<String, String> start() {
        db.startGlobal();
        // Quarkus starts the app, which runs Flyway migrations
        return Map.of(
            "quarkus.datasource.jdbc.url", db.getJdbcUrl(),
            "quarkus.datasource.username", "memgres",
            "quarkus.datasource.password", "memgres"
        );
    }

    @Override
    public void stop() {
        // cleaned up by JVM shutdown hook
    }
}
```

Then in your test base class, snapshot after Quarkus finishes starting:

```java
@QuarkusTest
@QuarkusTestResource(MemgresResource.class)
class BaseIT {
    @BeforeAll
    static void snapshotAfterAppStart() {
        MemgresResource.db.runInitScripts(); // test data
        MemgresResource.db.snapshot();
    }

    @BeforeEach
    void resetData() {
        MemgresResource.db.restore();
    }
}
```

## Spring Boot Integration

Use `@DynamicPropertySource` to wire Memgres into a Spring Boot test:

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
        // Spring manages the DataSource, Flyway runs migrations automatically
    }
}
```

## All Builder Options

| Method | Description |
|---|---|
| `migrationDir(path)` | Classpath directory of `.sql` files, executed in sorted order |
| `initScript(path)` | Classpath SQL script, runs after migrations |
| `initStatements(sql...)` | Inline SQL, runs after scripts |
| `isolation(mode)` | `PER_CLASS`, `PER_METHOD`, or `GLOBAL` |
| `snapshotAfterInit(true)` | Capture snapshot after init completes |
| `restoreBeforeEach(true)` | Restore snapshot before each test method |
| `systemProperty(name)` | Publish JDBC URL as a system property |
| `port(n)` | Fixed port (default: `0` = random) |
