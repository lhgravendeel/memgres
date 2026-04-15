package com.memgres.engine;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory large object storage for lo_* functions.
 * Stores large objects as byte arrays keyed by OID, and provides
 * file-descriptor-based access with position tracking.
 */
class LargeObjectStore {

    private final Map<Long, byte[]> objects = new ConcurrentHashMap<>();
    private final AtomicLong nextOid = new AtomicLong(100000);
    private final AtomicInteger nextFd = new AtomicInteger(1);
    private final Map<Integer, FdState> openFds = new ConcurrentHashMap<>();

    /** State for an open file descriptor. */
    static class FdState {
        final long oid;
        int position;

        FdState(long oid) {
            this.oid = oid;
            this.position = 0;
        }
    }

    // ---- Large Object functions ----

    /**
     * lo_from_bytea(loid, data): create a large object from bytea.
     * If loid is 0, a new OID is assigned.
     */
    long loFromBytea(long loid, byte[] data) {
        if (loid == 0) {
            loid = nextOid.getAndIncrement();
        }
        objects.put(loid, data != null ? data.clone() : new byte[0]);
        return loid;
    }

    /**
     * lo_get(loid): read entire large object.
     */
    byte[] loGet(long loid) {
        byte[] data = objects.get(loid);
        if (data == null) {
            throw new MemgresException("large object " + loid + " does not exist", "42704");
        }
        return data.clone();
    }

    /**
     * lo_get(loid, offset, length): read a slice of a large object.
     */
    byte[] loGet(long loid, int offset, int length) {
        byte[] data = objects.get(loid);
        if (data == null) {
            throw new MemgresException("large object " + loid + " does not exist", "42704");
        }
        int start = Math.min(offset, data.length);
        int end = Math.min(offset + length, data.length);
        return Arrays.copyOfRange(data, start, end);
    }

    /**
     * lo_put(loid, offset, data): write data at offset, extending if necessary.
     */
    void loPut(long loid, int offset, byte[] newData) {
        byte[] existing = objects.get(loid);
        if (existing == null) {
            throw new MemgresException("large object " + loid + " does not exist", "42704");
        }
        int needed = offset + newData.length;
        if (needed > existing.length) {
            existing = Arrays.copyOf(existing, needed);
        }
        System.arraycopy(newData, 0, existing, offset, newData.length);
        objects.put(loid, existing);
    }

    /**
     * lo_unlink(loid): delete a large object.
     */
    int loUnlink(long loid) {
        byte[] removed = objects.remove(loid);
        if (removed == null) {
            throw new MemgresException("large object " + loid + " does not exist", "42704");
        }
        return 1;
    }

    /**
     * lo_open(loid, mode): open a large object and return a file descriptor.
     */
    int loOpen(long loid, int mode) {
        if (!objects.containsKey(loid)) {
            throw new MemgresException("large object " + loid + " does not exist", "42704");
        }
        int fd = nextFd.getAndIncrement();
        openFds.put(fd, new FdState(loid));
        return fd;
    }

    /**
     * loread(fd, len): read len bytes from current position of open large object.
     */
    byte[] loRead(int fd, int len) {
        FdState state = openFds.get(fd);
        if (state == null) {
            throw new MemgresException("invalid large-object descriptor: " + fd, "42704");
        }
        byte[] data = objects.get(state.oid);
        if (data == null) {
            throw new MemgresException("large object " + state.oid + " does not exist", "42704");
        }
        int start = Math.min(state.position, data.length);
        int end = Math.min(start + len, data.length);
        byte[] result = Arrays.copyOfRange(data, start, end);
        state.position = end;
        return result;
    }

    /**
     * lo_tell(fd): return current position of an open large object.
     */
    int loTell(int fd) {
        FdState state = openFds.get(fd);
        if (state == null) {
            throw new MemgresException("invalid large-object descriptor: " + fd, "42704");
        }
        return state.position;
    }

    /**
     * lowrite(fd, data): write data at current position of open large object.
     * Returns the number of bytes written.
     */
    int loWrite(int fd, byte[] data) {
        FdState state = openFds.get(fd);
        if (state == null) {
            throw new MemgresException("invalid large-object descriptor: " + fd, "42704");
        }
        byte[] existing = objects.get(state.oid);
        if (existing == null) {
            throw new MemgresException("large object " + state.oid + " does not exist", "42704");
        }
        int needed = state.position + data.length;
        if (needed > existing.length) {
            existing = Arrays.copyOf(existing, needed);
        }
        System.arraycopy(data, 0, existing, state.position, data.length);
        objects.put(state.oid, existing);
        state.position += data.length;
        return data.length;
    }

    /**
     * lo_close(fd): close a file descriptor.
     */
    int loClose(int fd) {
        FdState removed = openFds.remove(fd);
        if (removed == null) {
            throw new MemgresException("invalid large-object descriptor: " + fd, "42704");
        }
        return 0;
    }

    /**
     * lo_lseek(fd, offset, whence): seek within an open large object.
     * whence: 0=SEEK_SET, 1=SEEK_CUR, 2=SEEK_END
     */
    int loLseek(int fd, int offset, int whence) {
        FdState state = openFds.get(fd);
        if (state == null) {
            throw new MemgresException("invalid large-object descriptor: " + fd, "42704");
        }
        byte[] data = objects.get(state.oid);
        int size = data != null ? data.length : 0;
        switch (whence) {
            case 0: // SEEK_SET
                state.position = offset;
                break;
            case 1: // SEEK_CUR
                state.position += offset;
                break;
            case 2: // SEEK_END
                state.position = size + offset;
                break;
            default:
                throw new MemgresException("invalid whence: " + whence, "22023");
        }
        return state.position;
    }

    /**
     * lo_truncate(fd, len): truncate a large object to len bytes.
     */
    int loTruncate(int fd, int len) {
        FdState state = openFds.get(fd);
        if (state == null) {
            throw new MemgresException("invalid large-object descriptor: " + fd, "42704");
        }
        byte[] data = objects.get(state.oid);
        if (data == null) {
            throw new MemgresException("large object " + state.oid + " does not exist", "42704");
        }
        objects.put(state.oid, Arrays.copyOf(data, len));
        return 0;
    }

    /**
     * Returns the set of OIDs of all stored large objects.
     */
    Set<Long> getOids() {
        return objects.keySet();
    }

    /**
     * Check if a large object with the given OID exists.
     */
    boolean exists(long loid) {
        return objects.containsKey(loid);
    }
}
