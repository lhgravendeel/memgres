package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.List;

/**
 * Represents a PostgreSQL custom ENUM type.
 */
public class CustomEnum {

    private final String name;
    private final List<String> labels;

    public CustomEnum(String name, List<String> labels) {
        this.name = name;
        this.labels = Cols.listCopyOf(labels);
    }

    public String getName() {
        return name;
    }

    public List<String> getLabels() {
        return labels;
    }

    public boolean isValidLabel(String label) {
        return labels.contains(label);
    }

    public int ordinal(String label) {
        return labels.indexOf(label);
    }
}
