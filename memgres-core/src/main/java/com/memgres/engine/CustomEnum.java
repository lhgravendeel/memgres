package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a PostgreSQL custom ENUM type.
 */
public class CustomEnum {

    private final String name;
    /** The schema the type was created in; two schemas may each hold an enum of this name. */
    private String schemaName = "public";
    private final List<String> labels;
    /** PG-compatible sort orders (fractional for ADD VALUE BEFORE). */
    private final List<Double> sortOrders;

    public CustomEnum(String schemaName, String name, List<String> labels) {
        this(name, labels);
        if (schemaName != null && !schemaName.isEmpty()) this.schemaName = schemaName.toLowerCase();
    }

    public CustomEnum(String name, List<String> labels) {
        this.name = name;
        this.labels = new ArrayList<>(labels);
        // Initialize with integer sort orders 1.0, 2.0, ...
        List<Double> orders = new ArrayList<>(labels.size());
        for (int i = 0; i < labels.size(); i++) {
            orders.add((double) (i + 1));
        }
        this.sortOrders = orders;
    }

    public String getName() {
        return name;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        if (schemaName != null && !schemaName.isEmpty()) this.schemaName = schemaName.toLowerCase();
    }

    public List<String> getLabels() {
        return labels;
    }

    /** Returns the PG-compatible enumsortorder for each label. */
    public List<Double> getSortOrders() {
        return sortOrders;
    }

    public boolean isValidLabel(String label) {
        return labels.contains(label);
    }

    public int ordinal(String label) {
        return labels.indexOf(label);
    }

    /** Add a new label at the end. */
    public void addLabel(String label) {
        double newOrder = sortOrders.isEmpty() ? 1.0 : sortOrders.get(sortOrders.size() - 1) + 1.0;
        labels.add(label);
        sortOrders.add(newOrder);
    }

    /** Add a new label before an existing label (fractional sort order). */
    public void addLabelBefore(String newLabel, String existingLabel) {
        int idx = labels.indexOf(existingLabel);
        if (idx < 0) throw new MemgresException("\"" + existingLabel + "\" is not an existing enum label", "22023");
        double before = idx > 0 ? sortOrders.get(idx - 1) : 0.0;
        double after = sortOrders.get(idx);
        double newOrder = (before + after) / 2.0;
        labels.add(idx, newLabel);
        sortOrders.add(idx, newOrder);
    }

    /** Add a new label after an existing label (fractional sort order). */
    public void addLabelAfter(String newLabel, String existingLabel) {
        int idx = labels.indexOf(existingLabel);
        if (idx < 0) throw new MemgresException("\"" + existingLabel + "\" is not an existing enum label", "22023");
        double current = sortOrders.get(idx);
        double next = idx < sortOrders.size() - 1 ? sortOrders.get(idx + 1) : current + 2.0;
        double newOrder = (current + next) / 2.0;
        labels.add(idx + 1, newLabel);
        sortOrders.add(idx + 1, newOrder);
    }
}
