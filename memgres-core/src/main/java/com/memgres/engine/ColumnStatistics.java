package com.memgres.engine;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What ANALYZE learns about one column, and how it learns it.
 *
 * <p>A planner reads these numbers and nothing else about the data: how often the column is null,
 * how wide its values are, how many distinct ones there are, which ones are common enough to be
 * worth naming, where the rest fall, and whether the column's order follows the table's. Left as a
 * distinct count and zeros, {@code pg_stats} said a column of three values repeated a hundred
 * times each had no common values at all, and {@code n_distinct} named a count where PostgreSQL
 * names a fraction.
 *
 * <p>The counts here are PostgreSQL's own, taken from its analyse pass: a distinct count above a
 * tenth of the table is stored as the negative fraction it is, because such a count grows with the
 * table rather than staying put; a value is common enough to name when it beats a threshold set
 * from the average, and what is left over becomes the histogram.
 */
final class ColumnStatistics {

    /** PostgreSQL's default statistics target, which is the number of histogram bins. */
    private static final int TARGET = 100;

    final float nullFrac;
    final int width;
    final float nDistinct;
    final List<Object> commonValues;
    final List<Float> commonFrequencies;
    final List<Object> histogramBounds;
    final float correlation;

    /**
     * A list of values as an array literal, which is how the catalogue holds one. Handed over as
     * a Java array instead, every reader was shown the array object's identity.
     */
    static String arrayLiteral(List<?> values) {
        if (values == null) return null;
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append(arrayElement(values.get(i)));
        }
        return sb.append('}').toString();
    }

    /** One element of an array literal, quoted when what it is written with would end it. */
    private static String arrayElement(Object value) {
        if (value == null) return "NULL";
        String text = value instanceof Boolean
                ? (((Boolean) value) ? "t" : "f") : String.valueOf(value);
        boolean needsQuotes = text.isEmpty() || text.equalsIgnoreCase("NULL");
        for (int i = 0; i < text.length() && !needsQuotes; i++) {
            char c = text.charAt(i);
            if (c == '{' || c == '}' || c == ',' || c == '"' || c == '\\'
                    || Character.isWhitespace(c)) {
                needsQuotes = true;
            }
        }
        if (!needsQuotes) return text;
        return '"' + text.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }

    private ColumnStatistics(float nullFrac, int width, float nDistinct, List<Object> commonValues,
                             List<Float> commonFrequencies, List<Object> histogramBounds,
                             float correlation) {
        this.nullFrac = nullFrac;
        this.width = width;
        this.nDistinct = nDistinct;
        this.commonValues = commonValues;
        this.commonFrequencies = commonFrequencies;
        this.histogramBounds = histogramBounds;
        this.correlation = correlation;
    }

    /**
     * What ANALYZE would record for every column of this table, keyed by column name. A table with
     * no rows has nothing to describe, and PostgreSQL writes no {@code pg_statistic} row for it.
     */
    static Map<String, ColumnStatistics> gather(Table table) {
        Map<String, ColumnStatistics> gathered = new LinkedHashMap<>();
        List<Object[]> rows = table.getRows();
        if (rows.isEmpty()) return gathered;
        for (int c = 0; c < table.getColumns().size(); c++) {
            Column column = table.getColumns().get(c);
            ColumnStatistics stats = ofColumn(rows, c, column);
            if (stats != null) gathered.put(column.getName().toLowerCase(), stats);
        }
        return gathered;
    }

    /** One value and the rows it stands on, kept so it can be ordered by either. */
    private static final class Seen {
        final Object value;
        int count;
        final List<Integer> positions = new ArrayList<>();

        Seen(Object value) {
            this.value = value;
        }
    }

    private static ColumnStatistics ofColumn(List<Object[]> rows, int index, Column column) {
        int nulls = 0;
        long widthTotal = 0;
        Map<Object, Seen> seen = new LinkedHashMap<>();
        int nonNullSoFar = 0;
        for (int r = 0; r < rows.size(); r++) {
            Object[] row = rows.get(r);
            Object value = index < row.length ? row[index] : null;
            if (value == null) {
                nulls++;
                continue;
            }
            widthTotal += storedWidth(value, column);
            Seen entry = seen.get(key(value));
            if (entry == null) {
                entry = new Seen(value);
                seen.put(key(value), entry);
            }
            entry.count++;
            // The position a value stands at is its place among the values that are there. A row
            // holding a null holds no value, so it takes no place in the order being compared.
            entry.positions.add(nonNullSoFar++);
        }
        int sampled = rows.size();
        int nonNull = sampled - nulls;
        if (nonNull == 0) return null;
        float nullFrac = (float) nulls / sampled;
        int width = (int) (widthTotal / nonNull);

        int distinct = seen.size();
        int repeated = 0;
        for (Seen s : seen.values()) {
            if (s.count > 1) repeated++;
        }

        // A column whose values never repeat has as many distinct values as it has rows, and
        // always will; PostgreSQL records that as the negative fraction rather than as a count.
        double stadistinct;
        if (repeated == 0) {
            stadistinct = -1.0 * (1.0 - nullFrac);
        } else {
            stadistinct = distinct;
        }
        if (stadistinct > 0.1 * sampled) stadistinct = -(stadistinct / sampled);

        List<Seen> byFrequency = new ArrayList<>(seen.values());
        // Ties keep the order the values sort in, which is what makes the list reproducible.
        List<Seen> sortedByValue = sortedByValue(seen.values());
        byFrequency = new ArrayList<>(sortedByValue);
        byFrequency.sort(Comparator.comparingInt((Seen s) -> -s.count));

        int commonCount;
        if (distinct == seen.size() && stadistinct > 0 && distinct <= TARGET) {
            commonCount = distinct;
        } else {
            double distinctInTable = stadistinct < 0 ? -stadistinct * sampled : stadistinct;
            double average = (double) sampled / distinctInTable;
            double minimum = average * 1.25;
            double ceiling = (double) nonNull / TARGET;
            if (minimum > ceiling) minimum = ceiling;
            if (minimum < 2) minimum = 2;
            commonCount = Math.min(TARGET, distinct);
            for (int i = 0; i < commonCount; i++) {
                if (byFrequency.get(i).count < minimum) {
                    commonCount = i;
                    break;
                }
            }
        }

        List<Object> commonValues = null;
        List<Float> commonFrequencies = null;
        int rowsNamed = 0;
        if (commonCount > 0) {
            commonValues = new ArrayList<>();
            commonFrequencies = new ArrayList<>();
            for (int i = 0; i < commonCount; i++) {
                Seen s = byFrequency.get(i);
                commonValues.add(s.value);
                commonFrequencies.add((float) s.count / sampled);
                rowsNamed += s.count;
            }
        }

        // The histogram describes what the common-value list does not, and there has to be
        // enough of it left over for two bounds before there is a histogram at all.
        List<Object> histogram = null;
        int remaining = distinct - commonCount;
        int bounds = remaining > TARGET ? TARGET + 1 : remaining;
        if (bounds >= 2) {
            List<Object> ordered = new ArrayList<>();
            for (Seen s : sortedByValue) {
                boolean named = false;
                if (commonValues != null) {
                    for (int i = 0; i < commonCount; i++) {
                        if (byFrequency.get(i) == s) { named = true; break; }
                    }
                }
                if (named) continue;
                for (int k = 0; k < s.count; k++) ordered.add(s.value);
            }
            int values = nonNull - rowsNamed;
            if (values >= 2 && ordered.size() == values) {
                histogram = new ArrayList<>();
                for (int i = 0; i < bounds; i++) {
                    histogram.add(ordered.get((int) ((long) i * (values - 1) / (bounds - 1))));
                }
            }
        }

        return new ColumnStatistics(nullFrac, width, (float) stadistinct,
                commonValues, commonFrequencies, histogram, correlationOf(sortedByValue, nonNull));
    }

    /**
     * How closely the column's order follows the table's: the correlation between where a value
     * sits when the column is sorted and where its row sits in the table. Equal values keep the
     * order their rows are in, which is what makes a column of few values correlate at all.
     */
    private static float correlationOf(List<Seen> sortedByValue, int nonNull) {
        if (nonNull < 2) return 0f;
        double sumXy = 0;
        double sumXx = 0;
        double sumYy = 0;
        double meanX = (nonNull - 1) / 2.0;
        double meanY = 0;
        List<Integer> positions = new ArrayList<>(nonNull);
        for (Seen s : sortedByValue) positions.addAll(s.positions);
        for (int position : positions) meanY += position;
        meanY /= nonNull;
        for (int i = 0; i < positions.size(); i++) {
            double dx = i - meanX;
            double dy = positions.get(i) - meanY;
            sumXy += dx * dy;
            sumXx += dx * dx;
            sumYy += dy * dy;
        }
        if (sumXx == 0 || sumYy == 0) return 0f;
        return (float) (sumXy / Math.sqrt(sumXx * sumYy));
    }

    private static List<Seen> sortedByValue(java.util.Collection<Seen> values) {
        List<Seen> sorted = new ArrayList<>(values);
        sorted.sort((a, b) -> compareValues(a.value, b.value));
        return sorted;
    }

    @SuppressWarnings("unchecked")
    private static int compareValues(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            return new BigDecimal(a.toString()).compareTo(new BigDecimal(b.toString()));
        }
        if (a instanceof Comparable && a.getClass() == b.getClass()) {
            return ((Comparable<Object>) a).compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }

    /** Two values that compare equal are one value, however they were spelled. */
    private static Object key(Object value) {
        if (value instanceof Number) return new BigDecimal(value.toString()).stripTrailingZeros();
        return value;
    }

    /**
     * How wide a value is stored. A variable-length value carries a length word — one byte while
     * it is short enough, four when it is not — and PostgreSQL counts that in the width it
     * records, which is why a two-character string is three bytes wide and not two.
     */
    private static int storedWidth(Object value, Column column) {
        DataType type = column.getType();
        int fixed = fixedWidth(type);
        if (fixed > 0) return fixed;
        int length = value instanceof byte[] ? ((byte[]) value).length
                : value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        return length + (length + 1 <= 126 ? 1 : 4);
    }

    /** The width of a type stored by value, or 0 for the ones stored by length. */
    private static int fixedWidth(DataType type) {
        if (type == null) return 0;
        switch (type) {
            case BOOLEAN:
            case INTERNAL_CHAR:
                return 1;
            case SMALLINT:
                return 2;
            case INTEGER:
            case REAL:
            case DATE:
            case OID:
                return 4;
            case BIGINT:
            case DOUBLE_PRECISION:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMPTZ:
            case MONEY:
                return 8;
            case TIMETZ:
                return 12;
            case INTERVAL:
                return 16;
            case UUID:
                return 16;
            default:
                return 0;
        }
    }
}
