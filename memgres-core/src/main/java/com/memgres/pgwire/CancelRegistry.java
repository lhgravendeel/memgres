package com.memgres.pgwire;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Registry mapping (processId, secretKey) pairs to the Thread currently
 * executing a query on that connection.  Used to implement the PG cancel
 * protocol (CancelRequest, protocol code 80877102).
 */
public class CancelRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(CancelRegistry.class);
    private static final AtomicInteger PID_SEQ = new AtomicInteger(1000);

        private static final class Key {
        public final int pid;
        public final int secretKey;

        public Key(int pid, int secretKey) {
            this.pid = pid;
            this.secretKey = secretKey;
        }

        public int pid() { return pid; }
        public int secretKey() { return secretKey; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key that = (Key) o;
            return pid == that.pid
                && secretKey == that.secretKey;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(pid, secretKey);
        }

        @Override
        public String toString() {
            return "Key[pid=" + pid + ", " + "secretKey=" + secretKey + "]";
        }
    }

    /** Maps (pid, secretKey) → the Thread running the current query (or null if idle). */
    private final ConcurrentHashMap<Key, Thread> executing = new ConcurrentHashMap<>();

    /** Maps (pid, secretKey) → presence, so we know which keys are registered. */
    private final ConcurrentHashMap<Key, Boolean> registered = new ConcurrentHashMap<>();

    /** Allocate a unique process ID for a new connection. */
    public int nextPid() {
        return PID_SEQ.incrementAndGet();
    }

    /** Register a connection. Called once during startup. */
    public void register(int pid, int secretKey) {
        registered.put(new Key(pid, secretKey), Boolean.TRUE);
    }

    /** Unregister a connection. Called when the connection closes. */
    public void unregister(int pid, int secretKey) {
        Key key = new Key(pid, secretKey);
        registered.remove(key);
        executing.remove(key);
    }

    /** Mark the thread that is currently executing a query on this connection. */
    public void setExecutingThread(int pid, int secretKey, Thread thread) {
        Key key = new Key(pid, secretKey);
        if (thread != null) {
            executing.put(key, thread);
        } else {
            executing.remove(key);
        }
    }

    /**
     * Handle a CancelRequest: interrupt the thread executing on the connection
     * identified by (pid, secretKey).  Returns true if the cancel was delivered.
     */
    public boolean cancel(int pid, int secretKey) {
        Key key = new Key(pid, secretKey);
        if (!registered.containsKey(key)) {
            LOG.debug("CancelRequest for unknown pid={} secretKey={}", pid, secretKey);
            return false; // unknown or stale cancel key
        }
        Thread t = executing.get(key);
        if (t != null) {
            t.interrupt();
            LOG.debug("CancelRequest delivered to thread {} for pid={}", t.getName(), pid);
            return true;
        }
        LOG.debug("CancelRequest for idle pid={}", pid);
        return false; // connection idle, nothing to cancel
    }
}
