-- ============================================================================
-- Feature Comparison: LISTEN/NOTIFY registration and self-delivery
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- The push half of this fix -- a listener sitting idle on the socket being
-- woken by another connection's NOTIFY -- needs two connections and lives in
-- NotifyIdleListenerPushTest. What one connection can see is covered here:
-- the channel registry, and pg_notify returning void rather than null.
-- ============================================================================

LISTEN nlp_chan;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_listening_channels() c WHERE c = 'nlp_chan';

NOTIFY nlp_chan, 'x';

-- pg_notify returns void, which is an empty string over the wire, never null
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT (pg_notify('nlp_chan','y') IS NULL) AS a;

LISTEN nlp_other;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM pg_listening_channels();

UNLISTEN nlp_chan;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_listening_channels();

UNLISTEN *;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_listening_channels();
