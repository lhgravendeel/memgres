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
        final int pid;
        final byte[] secret;

        Key(int pid, byte[] secret) {
            this.pid = pid;
            this.secret = secret;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key that = (Key) o;
            return pid == that.pid && java.util.Arrays.equals(secret, that.secret);
        }

        @Override
        public int hashCode() {
            return pid * 31 + java.util.Arrays.hashCode(secret);
        }

        @Override
        public String toString() {
            return "Key[pid=" + pid + ", secret=" + secret.length + " bytes]";
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
    public void register(int pid, byte[] secretKey) {
        registered.put(new Key(pid, secretKey), Boolean.TRUE);
    }

    /** Unregister a connection. Called when the connection closes. */
    public void unregister(int pid, byte[] secretKey) {
        Key key = new Key(pid, secretKey);
        registered.remove(key);
        executing.remove(key);
    }

    /** Mark the thread that is currently executing a query on this connection. */
    public void setExecutingThread(int pid, byte[] secretKey, Thread thread) {
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
     *
     * <p>The key is as many bytes as the connection was given: four under protocol 3.0, and the
     * thirty-two protocol 3.2 asks for.
     */
    public boolean cancel(int pid, byte[] secretKey) {
        Key key = new Key(pid, secretKey);
        if (!registered.containsKey(key)) {
            LOG.debug("CancelRequest for unknown pid={}", pid);
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
