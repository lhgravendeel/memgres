package com.memgres.engine;

import java.util.concurrent.atomic.AtomicLong;

/**
 * PostgreSQL-compatible sequence (auto-increment counter).
 */
public class Sequence {

    private String name;
    /**
     * The schema this sequence lives in. A sequence is a relation, so it belongs to exactly one
     * schema and two schemas may each hold one of the same name; without this the engine keeps a
     * single global counter and two tables with a {@code serial} column of the same name share it.
     */
    private String schemaName = "public";
    private long startWith;
    private long incrementBy;
    private long minValue;
    private long maxValue;
    private boolean cycle;
    private String dataType = "bigint";
    private int cache = 1;
    private final AtomicLong currentValue;
    private volatile boolean called = false;
    private String ownedByTable;
    private String ownedByColumn;

    /**
     * The sequence name a column default draws from, or null when the default draws from none.
     *
     * <p>A default is stored as the text it was written as — {@code nextval('s'::regclass)} for a
     * serial column, {@code __identity__:...:seq:s} for an identity one — and every caller that
     * has to know which sequence a column belongs to was picking the name out of that text again,
     * each with its own idea of where the quotes are.
     */
    public static String nameInDefault(String defaultValue) {
        if (defaultValue == null) return null;
        if (defaultValue.contains(":seq:")) {
            return defaultValue.substring(defaultValue.indexOf(":seq:") + 5);
        }
        if (!defaultValue.contains("nextval(")) return null;
        int q1 = defaultValue.indexOf('\'');
        int q2 = q1 < 0 ? -1 : defaultValue.indexOf('\'', q1 + 1);
        return q2 > q1 ? defaultValue.substring(q1 + 1, q2) : null;
    }

    public Sequence(String name, Long startWith, Long incrementBy, Long minValue, Long maxValue) {
        this.name = name;
        this.startWith = startWith != null ? startWith : 1;
        this.incrementBy = incrementBy != null ? incrementBy : 1;
        this.minValue = minValue != null ? minValue : 1;
        this.maxValue = maxValue != null ? maxValue : Long.MAX_VALUE;
        this.cycle = false;
        this.currentValue = new AtomicLong(this.startWith);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName == null ? "public" : schemaName;
    }

    /** {@code schema.name}, the name that identifies this sequence among all of them. */
    public String qualifiedName() {
        return schemaName + "." + name;
    }

    public synchronized long nextVal() {
        if (!called) {
            called = true;
            return currentValue.get();
        }
        long next = currentValue.get() + incrementBy;
        if (incrementBy > 0) {
            if (next > maxValue) {
                if (!cycle) {
                    throw new MemgresException("nextval: reached maximum value of sequence \"" + name + "\" (" + maxValue + ")", "2200H");
                }
                next = minValue;
            }
        } else {
            if (next < minValue) {
                if (!cycle) {
                    throw new MemgresException("nextval: reached minimum value of sequence \"" + name + "\" (" + minValue + ")", "2200H");
                }
                next = maxValue;
            }
        }
        currentValue.set(next);
        return next;
    }

    /**
     * Bumped whenever the counter moves by something other than nextval.
     *
     * <p>A session that has reserved a block of CACHE values is holding values the sequence has
     * already handed out. setval and ALTER SEQUENCE RESTART mean those are no longer the values
     * the sequence would produce, so the block has to be given up — otherwise resetting a
     * sequence, which is how a test fixture starts over, quietly keeps serving the old numbers.
     */
    private long resetGeneration;

    /** The counter's reset generation; a block cached under an older one is stale. */
    public synchronized long getResetGeneration() { return resetGeneration; }

    /**
     * Reserve a run of up to {@code count} consecutive values for one caller.
     *
     * <p>CACHE is an allocation hint, not a promise: PostgreSQL claims what is left of the
     * sequence and no more, so a sequence whose cache is wider than its remaining range still
     * hands out every value it has. Claiming the whole cache up front would exhaust such a
     * sequence on its first call.
     *
     * @return {@code {firstValue, reservedCount}}; reservedCount is at least 1
     * @throws MemgresException 2200H when not even one value is left
     */
    public synchronized long[] nextValBlock(int count) {
        long first = nextVal();
        long reserved = 1;
        while (reserved < count) {
            long candidate = currentValue.get() + incrementBy;
            if (incrementBy > 0 ? candidate > maxValue : candidate < minValue) break;
            currentValue.set(candidate);
            reserved++;
        }
        return new long[]{first, reserved};
    }

    public synchronized long currVal() {
        if (!called) {
            throw new MemgresException("currval of sequence \"" + name + "\" is not yet defined in this session", "55000");
        }
        return currentValue.get();
    }

    public synchronized long setVal(long value) {
        resetGeneration++;
        if (value > maxValue || value < minValue) {
            throw new MemgresException("setval: value " + value + " is out of bounds for sequence \""
                    + name + "\" (" + minValue + ".." + maxValue + ")", "22003");
        }
        currentValue.set(value);
        called = true;
        return value;
    }

    public synchronized long setVal(long value, boolean isCalled) {
        resetGeneration++;
        if (value > maxValue || value < minValue) {
            throw new MemgresException("setval: value " + value + " is out of bounds for sequence \""
                    + name + "\" (" + minValue + ".." + maxValue + ")", "22003");
        }
        currentValue.set(value);
        if (isCalled) {
            called = true;
        } else {
            // Next nextval() should return this value (not value + incrementBy)
            // We set called=false so the next nextval returns currentValue directly
            called = false;
        }
        return value;
    }

    public long getStartWith() { return startWith; }
    public long getIncrementBy() { return incrementBy; }
    public long getMinValue() { return minValue; }
    public long getMaxValue() { return maxValue; }
    public boolean isCycle() { return cycle; }

    public synchronized void restart() {
        resetGeneration++;
        currentValue.set(startWith);
        called = false;
    }

    public synchronized void restart(long value) {
        resetGeneration++;
        currentValue.set(value);
        called = false;
    }

    /** Set the counter and its called flag together, as ALTER SEQUENCE resolves them as a pair. */
    public synchronized void setCurrentValue(long value, boolean isCalled) {
        resetGeneration++;
        currentValue.set(value);
        this.called = isCalled;
    }

    /**
     * Returns the current internal value without checking the 'called' flag.
     * Used for snapshot/restore.
     */
    public long currValRaw() {
        return currentValue.get();
    }

    public boolean isCalled() {
        return called;
    }

    public synchronized void setIncrementBy(long inc) { this.incrementBy = inc; }
    public synchronized void setMinValue(long min) { this.minValue = min; }
    public synchronized void setMaxValue(long max) { this.maxValue = max; }
    public synchronized void setStartWith(long start) { this.startWith = start; }
    public synchronized void setCycle(boolean cycle) { this.cycle = cycle; }
    public String getDataType() { return dataType; }
    public void setDataType(String dataType) { this.dataType = dataType; }
    public int getCache() { return cache; }
    public synchronized void setCache(int cache) { this.cache = Math.max(1, cache); }
    public String getOwnedByTable() { return ownedByTable; }
    public void setOwnedByTable(String ownedByTable) { this.ownedByTable = ownedByTable; }
    public String getOwnedByColumn() { return ownedByColumn; }
    public void setOwnedByColumn(String ownedByColumn) { this.ownedByColumn = ownedByColumn; }
}
