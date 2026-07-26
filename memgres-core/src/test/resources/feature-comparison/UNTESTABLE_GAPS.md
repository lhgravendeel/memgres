# Memgres vs PostgreSQL 18: Gaps That Cannot Be Unit-Tested

This document describes known compatibility gaps between Memgres and PostgreSQL 18
that **cannot be covered by automated unit tests or SQL verification files**, with
explanations for each.

---

## 1. Wire Protocol SSL/TLS Negotiation

**Gap:** Memgres responds with `'N'` (reject) to all SSLRequest messages. PG 18
supports full SSL/TLS with certificate-based authentication.

**Why untestable:** JDBC's `sslmode=require` would simply refuse to connect to
Memgres, but that's a connection-level failure, not a behavioral test. Testing
SSL negotiation would require a raw socket client that speaks the PG wire protocol
directly, which is outside the scope of JDBC-based tests. The gap is architectural
(no TLS implementation) rather than behavioral.

---

## 2. SCRAM-SHA-256 Authentication

**Gap:** Memgres uses cleartext password authentication (AuthenticationCleartextPassword).
PG 18 defaults to SCRAM-SHA-256, the modern standard for password hashing.

**Why untestable:** The JDBC driver auto-negotiates authentication with the server.
When Memgres sends AuthenticationCleartextPassword, the driver complies silently.
There's no JDBC API to query which authentication method was used. Testing this
would require intercepting wire protocol messages, which JDBC doesn't expose.

---

## 3. Binary Wire Protocol Codec Gaps

**Gap:** Memgres only supports binary encoding/decoding for ~15 types (int, float,
text, UUID, timestamp, date, bytea, numeric, int4[]/text[]). PG 18 supports
binary codec for ALL types including range, composite, geometric, network, tsvector,
tsquery, jsonb path, etc.

**Why untestable:** The JDBC driver defaults to text format for most types. To trigger
binary codec, you'd need to use `preferQueryMode=extended` and set binary transfer
mode, but the JDBC driver only uses binary for a subset of types anyway. When a type
isn't supported in binary, the driver gracefully falls back to text. There's no
observable failure from the client side — just a performance difference.

---

## 4. Streaming/Logical Replication Protocol

**Gap:** Memgres does not implement the streaming replication protocol or logical
replication protocol. PG 18 supports both for high availability and CDC.

**Why untestable:** These are entirely separate wire protocols that require specialized
clients (pg_basebackup, pg_recvlogical, Debezium, etc.), not standard SQL connections.
JDBC cannot initiate replication streams. The gap is at the protocol level and can't
be expressed as a SQL query or JDBC call.

---

## 5. ParameterStatus Messages Are Hardcoded

**Gap:** At connection startup, Memgres sends hardcoded ParameterStatus messages
(e.g., `TimeZone=UTC`, `server_encoding=UTF8`) regardless of actual GUC settings.
PG 18 sends the real current values.

**Why untestable:** While some settings are accessible via `SHOW timezone`, the
actual ParameterStatus messages in the wire protocol handshake are consumed by the
JDBC driver during connection setup and not exposed to application code. The JDBC
`Connection.getClientInfo()` API doesn't correspond to PG's ParameterStatus messages.
Testing would require wire protocol inspection.

---

## 6. Query Planning & Cost Estimation (Excluded by Design)

**Gap:** Memgres has no query planner/optimizer. All queries use full table scans.
EXPLAIN returns synthetic plans with fake costs. PG 18 has a sophisticated cost-based
optimizer with statistics, join reordering, index selection, and parallel query.

**Why excluded:** The user explicitly asked to skip query planning gaps. This is a
fundamental architectural difference that would require building an entire query
optimizer, not fixing a bug or adding a feature.

---

## 7. VACUUM/ANALYZE Internal Effects

**Gap:** VACUUM and ANALYZE are no-ops in Memgres. In PG 18, VACUUM reclaims dead
tuple space, freezes old tuples, and updates the visibility map. ANALYZE collects
column statistics used by the planner.

**Why untestable:** The effects of VACUUM are internal to the storage engine (dead
tuple reclamation, page compaction). There's no SQL query that observes "VACUUM
worked correctly" — the effects are about disk space and internal tuple state.
ANALYZE effects are tested indirectly via `pg_statistic` (see PgStatViewsCompatTest),
but the actual impact on query plans requires a working optimizer (excluded above).

---

## 8. CREATE INDEX CONCURRENTLY Blocking Behavior

**Gap:** Memgres accepts `CREATE INDEX CONCURRENTLY` but runs it synchronously,
blocking all concurrent writes. PG 18 builds the index in the background, allowing
concurrent DML during index creation.

**Why untestable:** From a single connection's perspective, `CREATE INDEX CONCURRENTLY`
behaves identically — it creates an index and returns. The difference is only
observable under concurrent load: in PG, other connections can INSERT/UPDATE/DELETE
during index creation; in Memgres, they'd block. Testing this reliably requires:
1. Starting a long-running index build (needs a large dataset)
2. Attempting DML from another connection during the build
3. Measuring whether the DML blocks or proceeds

This is inherently timing-dependent and flaky. The blocking behavior depends on
thread scheduling and cannot be deterministically asserted.

---

## 9. Deadlock Detection Edge Cases

**Gap:** Memgres has deadlock detection for advisory locks and row-level locks, but
the detection algorithm may differ from PG's sophisticated wait-for graph analysis
with timeout-based cycle detection.

**Why untestable:** Deadlock scenarios are inherently non-deterministic. Whether a
deadlock is detected depends on thread scheduling, lock acquisition timing, and the
detection algorithm's polling interval. PG has a configurable `deadlock_timeout`
(default 1s) that controls when the detector runs. Writing a reliable deadlock test
that distinguishes Memgres's behavior from PG's would require precise timing control
that JDBC doesn't provide.

---

## 10. Large Object Import/Export to Filesystem

**Gap:** `lo_import()` always returns 1 (stub). `lo_export()` always returns 1 (stub).
PG 18 reads/writes real files on the server filesystem.

**Why untestable:** Large object import/export operates on the **server's** filesystem,
not the client's. In a test environment, the Memgres server runs in the same JVM,
but there's no standardized filesystem path to test with. The JDBC `LargeObject` API
can test `lo_read`/`lo_write` (which work via `LargeObjectStore`), but filesystem
import/export is a server-side operation that would require filesystem mocking.

---

## 11. pg_stat_activity Real Backend Information

**Gap:** Memgres shows basic session info in `pg_stat_activity` but lacks real
backend statistics like `backend_start`, `wait_event`, `wait_event_type`,
`backend_type`, `query_start`, etc.

**Why partially untestable:** We CAN test that `pg_stat_activity` has rows (and we do
in PgStatViewsCompatTest), but verifying the **accuracy** of timing fields
(`backend_start`, `query_start`, `state_change`) would require comparing against
real wall-clock time with tolerance windows, making tests flaky. The `wait_event`
fields are inherently transient and depend on what the backend is doing at the
exact moment of the query.

---

## Summary

| Gap | Reason Untestable |
|-----|-------------------|
| SSL/TLS | Connection-level; needs raw socket |
| SCRAM-SHA-256 | Auth negotiation invisible to JDBC |
| Binary codec gaps | Driver falls back to text silently |
| Replication protocol | Entirely different protocol |
| ParameterStatus | Wire messages not exposed to JDBC |
| Query planning | Excluded by design |
| VACUUM/ANALYZE effects | Internal storage engine state |
| CREATE INDEX CONCURRENTLY | Timing-dependent concurrency |
| Deadlock detection | Non-deterministic timing |
| Large object filesystem | Server-side filesystem access |
| pg_stat_activity accuracy | Timing-dependent transient state |
