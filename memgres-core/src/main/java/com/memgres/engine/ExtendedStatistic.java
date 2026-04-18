package com.memgres.engine;

import java.util.List;

/**
 * Metadata for an extended statistics object (CREATE STATISTICS).
 */
public class ExtendedStatistic {
    private String name;
    private final String tableName;
    private final List<String> columns;
    private final List<String> kinds; // "ndistinct", "dependencies", "mcv"
    private int stattarget = -1; // default
    private boolean analyzed; // true after ANALYZE populates data

    public ExtendedStatistic(String name, String tableName, List<String> columns, List<String> kinds) {
        this.name = name;
        this.tableName = tableName;
        this.columns = columns;
        this.kinds = kinds;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getTableName() { return tableName; }
    public List<String> getColumns() { return columns; }
    public List<String> getKinds() { return kinds; }
    public int getStattarget() { return stattarget; }
    public void setStattarget(int stattarget) { this.stattarget = stattarget; }
    public boolean isAnalyzed() { return analyzed; }
    public void setAnalyzed(boolean analyzed) { this.analyzed = analyzed; }
}
