package com.memgres.engine;

import java.util.concurrent.atomic.AtomicLong;

/**
 * PostgreSQL-compatible sequence (auto-increment counter).
 */
public class Sequence {

    private final String name;
    private long startWith;
    private long incrementBy;
    private long minValue;
    private long maxValue;
    private boolean cycle;
    private final AtomicLong currentValue;
    private volatile boolean called = false;

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

    public synchronized long nextVal() {
        if (!called) {
            called = true;
            return currentValue.get();
        }
        long next = currentValue.get() + incrementBy;
        if (incrementBy > 0) {
            if (next > maxValue) {
                if (!cycle) {
                    throw new MemgresException("nextval: reached maximum value of sequence \"" + name + "\" (" + maxValue + ")");
                }
                next = minValue;
            }
        } else {
            if (next < minValue) {
                if (!cycle) {
                    throw new MemgresException("nextval: reached minimum value of sequence \"" + name + "\" (" + minValue + ")");
                }
                next = maxValue;
            }
        }
        currentValue.set(next);
        return next;
    }

    public long currVal() {
        if (!called) {
            throw new MemgresException("currval of sequence \"" + name + "\" is not yet defined in this session", "55000");
        }
        return currentValue.get();
    }

    public long setVal(long value) {
        if (value > maxValue) {
            throw new MemgresException("setval: value " + value + " is out of bounds for sequence (max=" + maxValue + ")", "22003");
        }
        if (value < minValue) {
            throw new MemgresException("setval: value " + value + " is out of bounds for sequence (min=" + minValue + ")", "22003");
        }
        currentValue.set(value);
        called = true;
        return value;
    }

    public long setVal(long value, boolean isCalled) {
        if (value > maxValue) {
            throw new MemgresException("setval: value " + value + " is out of bounds for sequence (max=" + maxValue + ")", "22003");
        }
        if (value < minValue) {
            throw new MemgresException("setval: value " + value + " is out of bounds for sequence (min=" + minValue + ")", "22003");
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

    public void restart() {
        currentValue.set(startWith);
        called = false;
    }

    public void restart(long value) {
        currentValue.set(value);
        called = false;
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

    public void setIncrementBy(long inc) { this.incrementBy = inc; }
    public void setMinValue(long min) { this.minValue = min; }
    public void setMaxValue(long max) { this.maxValue = max; }
    public void setStartWith(long start) { this.startWith = start; }
    public void setCycle(boolean cycle) { this.cycle = cycle; }
}
