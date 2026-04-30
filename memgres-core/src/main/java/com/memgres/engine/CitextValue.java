package com.memgres.engine;

/**
 * Wrapper for citext (case-insensitive text) values.
 * Preserves the original case for display but compares case-insensitively.
 */
public final class CitextValue {
    private final String value;

    public CitextValue(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return value; // preserve original case for display
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof CitextValue) {
            return value.equalsIgnoreCase(((CitextValue) o).value);
        }
        if (o instanceof String) {
            return value.equalsIgnoreCase((String) o);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return value.toLowerCase().hashCode();
    }
}
