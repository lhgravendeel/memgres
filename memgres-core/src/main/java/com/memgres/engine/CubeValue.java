package com.memgres.engine;

import java.util.Arrays;

/**
 * Represents a PostgreSQL cube value — an n-dimensional point or box.
 * A single-point cube is defined by one coordinate array;
 * a box cube is defined by two coordinate arrays (lower-left and upper-right corners).
 */
public class CubeValue {
    private final double[] coords;

    /** Create a single-point cube from coordinates. */
    public CubeValue(double[] coords) {
        this.coords = coords.clone();
    }

    /** Return the number of dimensions. */
    public int dim() {
        return coords.length;
    }

    public double[] coords() {
        return coords.clone();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < coords.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(coords[i]);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CubeValue)) return false;
        return Arrays.equals(coords, ((CubeValue) o).coords);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(coords);
    }
}
