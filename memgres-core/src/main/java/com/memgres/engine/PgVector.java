package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.AbstractList;
import java.util.List;

/**
 * Represents a PG int2vector or oidvector, a 0-based array type.
 * Regular PG arrays are 1-based, but int2vector/oidvector use 0-based subscripts.
 * This wrapper signals the AstExecutor to use 0-based indexing for subscript access.
 */
public class PgVector extends AbstractList<Object> {
    private final List<Object> elements;

    public PgVector(List<Object> elements) {
        this.elements = Cols.listCopyOf(elements);
    }

    @Override
    public Object get(int index) {
        return elements.get(index);
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public String toString() {
        // PG int2vector format: space-separated values
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < elements.size(); i++) {
            if (i > 0) sb.append(' ');
            sb.append(elements.get(i));
        }
        return sb.toString();
    }
}
