package com.memgres.engine;

import java.time.*;
import java.time.temporal.ChronoUnit;

/**
 * Date/time arithmetic operations, extracted from AstExecutor to reduce class size.
 */
class DateTimeArithmetic {
    private final AstExecutor executor;

    DateTimeArithmetic(AstExecutor executor) {
        this.executor = executor;
    }

    Object dateTimeAdd(Object left, Object right) {
        if (left == null || right == null) return null;

        // interval + interval
        if (left instanceof PgInterval && right instanceof PgInterval) return ((PgInterval) left).plus(((PgInterval) right));

        // date/timestamp + interval (PG: date + interval returns timestamp)
        if (left instanceof LocalDate && right instanceof PgInterval) return ((PgInterval) right).addTo(((LocalDate) left).atStartOfDay());
        if (left instanceof LocalDateTime && right instanceof PgInterval) return ((PgInterval) right).addTo(((LocalDateTime) left));
        if (left instanceof OffsetDateTime && right instanceof PgInterval) return ((PgInterval) right).addTo(((OffsetDateTime) left));

        // interval + date/timestamp (commutative)
        if (left instanceof PgInterval && right instanceof LocalDate) return ((PgInterval) left).addTo(((LocalDate) right).atStartOfDay());
        if (left instanceof PgInterval && right instanceof LocalDateTime) return ((PgInterval) left).addTo(((LocalDateTime) right));
        if (left instanceof PgInterval && right instanceof OffsetDateTime) return ((PgInterval) left).addTo(((OffsetDateTime) right));

        // time + interval
        if (left instanceof LocalTime && right instanceof PgInterval) {
            PgInterval iv = (PgInterval) right;
            LocalTime lt = (LocalTime) left;
            LocalTime result = lt;
            long totalMicros = iv.getMicroseconds();
            long totalSeconds = totalMicros / 1_000_000;
            result = result.plusHours(totalSeconds / 3600);
            result = result.plusMinutes((totalSeconds % 3600) / 60);
            result = result.plusSeconds(totalSeconds % 60);
            return result;
        }
        // interval + time (commutative)
        if (left instanceof PgInterval && right instanceof LocalTime) {
            LocalTime rt = (LocalTime) right;
            PgInterval iv = (PgInterval) left;
            return dateTimeAdd(rt, iv);
        }

        // integer + interval: PG rejects this (operator does not exist: integer + interval)
        if (left instanceof Number && !(left instanceof Float) && !(left instanceof Double)
                && right instanceof PgInterval) {
            throw new MemgresException("operator does not exist: integer + interval", "42883");
        }
        if (left instanceof PgInterval && right instanceof Number
                && !(right instanceof Float) && !(right instanceof Double)) {
            throw new MemgresException("operator does not exist: interval + integer", "42883");
        }

        // date + integer (days)
        if (left instanceof LocalDate && right instanceof Number) return ((LocalDate) left).plusDays(((Number) right).longValue());
        if (left instanceof Number && right instanceof LocalDate) return ((LocalDate) right).plusDays(((Number) left).longValue());

        // Geometric arithmetic: geom + point = translation
        if (left instanceof String && right instanceof String
                && GeometricOperations.isGeometricString(((String) left))) {
            String rs = (String) right;
            String ls = (String) left;
            return GeometricOperations.add(ls, rs);
        }

        // Multirange + Multirange → union
        if (left instanceof String && right instanceof String
                && RangeOperations.isMultirangeOrEmpty(((String) left)) && RangeOperations.isMultirangeOrEmpty(((String) right))) {
            return RangeOperations.multirangeUnion((String) left, (String) right);
        }
        // Multirange + Range → add range to multirange
        if (left instanceof String && right instanceof String
                && RangeOperations.isMultirangeOrEmpty(((String) left)) && RangeOperations.isRangeString(((String) right))) {
            return RangeOperations.multirangeUnion((String) left, "{" + right + "}");
        }
        if (left instanceof String && right instanceof String
                && RangeOperations.isRangeString(((String) left)) && RangeOperations.isMultirangeOrEmpty(((String) right))) {
            return RangeOperations.multirangeUnion("{" + left + "}", (String) right);
        }
        // Range + Range → union
        if (left instanceof String && right instanceof String
                && RangeOperations.isRangeString(((String) left)) && RangeOperations.isRangeString(((String) right))) {
            String rs = (String) right;
            String ls = (String) left;
            return RangeOperations.union(RangeOperations.parse(ls), RangeOperations.parse(rs)).toString();
        }
        // Range + non-range → error
        if (left instanceof String && RangeOperations.isRangeString(((String) left))) {
            String ls = (String) left;
            throw new MemgresException("operator does not exist: int4range + integer", "42883");
        }
        if (right instanceof String && RangeOperations.isRangeString(((String) right))) {
            String rs = (String) right;
            throw new MemgresException("operator does not exist: integer + int4range", "42883");
        }

        // String concatenation when one side is a string (not numeric)
        if (left instanceof String && !(right instanceof Number)) {
            return left.toString() + right.toString();
        }

        // Fall back to numeric
        return executor.numericOp(left, right, Double::sum, Math::addExact, java.math.BigDecimal::add);
    }

    Object dateTimeSubtract(Object left, Object right) {
        if (left == null || right == null) return null;

        // Multirange - Multirange → set difference
        if (left instanceof String && right instanceof String
                && RangeOperations.isMultirangeOrEmpty(((String) left)) && RangeOperations.isMultirangeOrEmpty(((String) right))) {
            return RangeOperations.multirangeSubtract((String) left, (String) right);
        }
        // Multirange - Range → subtract range from multirange
        if (left instanceof String && right instanceof String
                && RangeOperations.isMultirangeOrEmpty(((String) left)) && RangeOperations.isRangeString(((String) right))) {
            return RangeOperations.multirangeSubtract((String) left, "{" + right + "}");
        }
        // Range - Multirange → subtract multirange from range
        if (left instanceof String && right instanceof String
                && RangeOperations.isRangeString(((String) left)) && RangeOperations.isMultirangeOrEmpty(((String) right))) {
            return RangeOperations.multirangeSubtract("{" + left + "}", (String) right);
        }
        // Range - Range → set difference
        // Exclude geometric strings (which also match range-like patterns)
        if (left instanceof String && right instanceof String
                && !GeometricOperations.isGeometricString(((String) left)) && !GeometricOperations.isGeometricString(((String) right))
                && RangeOperations.isRangeString(((String) left)) && RangeOperations.isRangeString(((String) right))) {
            String rs = (String) right;
            String ls = (String) left;
            RangeOperations.PgRange lr = RangeOperations.parse(ls);
            RangeOperations.PgRange rr = RangeOperations.parse(rs);
            RangeOperations.PgRange result = RangeOperations.subtract(lr, rr);
            return result.toString();
        }

        // interval - interval
        if (left instanceof PgInterval && right instanceof PgInterval) return ((PgInterval) left).minus(((PgInterval) right));

        // date/timestamp - interval (PG: date - interval returns timestamp)
        if (left instanceof LocalDate && right instanceof PgInterval) return ((PgInterval) right).negate().addTo(((LocalDate) left).atStartOfDay());
        if (left instanceof LocalDateTime && right instanceof PgInterval) return ((PgInterval) right).negate().addTo(((LocalDateTime) left));
        if (left instanceof OffsetDateTime && right instanceof PgInterval) return ((PgInterval) right).negate().addTo(((OffsetDateTime) left));

        // time - interval
        if (left instanceof LocalTime && right instanceof PgInterval) {
            PgInterval iv = (PgInterval) right;
            LocalTime lt = (LocalTime) left;
            return dateTimeAdd(lt, iv.negate());
        }

        // date - date → integer (days between)
        if (left instanceof LocalDate && right instanceof LocalDate) {
            LocalDate rd = (LocalDate) right;
            LocalDate ld = (LocalDate) left;
            return (int) ChronoUnit.DAYS.between(rd, ld);
        }

        // timestamp - timestamp → interval
        if (left instanceof LocalDateTime && right instanceof LocalDateTime) {
            LocalDateTime rdt = (LocalDateTime) right;
            LocalDateTime ldt = (LocalDateTime) left;
            long micros = ChronoUnit.MICROS.between(rdt, ldt);
            int days = (int) (micros / (24L * 3600 * 1_000_000));
            long remainingMicros = micros % (24L * 3600 * 1_000_000);
            return new PgInterval(0, days, remainingMicros);
        }

        // date - integer (days)
        if (left instanceof LocalDate && right instanceof Number) return ((LocalDate) left).minusDays(((Number) right).longValue());

        // Geometric subtraction: geom - point = translation
        if (left instanceof String && right instanceof String
                && GeometricOperations.isGeometricString(((String) left))) {
            String rs = (String) right;
            String ls = (String) left;
            return GeometricOperations.subtract(ls, rs);
        }

        // JSONB subtraction: jsonb - text (delete key) or jsonb - int (delete by index)
        if (left instanceof String) {
            String ls = (String) left;
            String trimmed = ls.trim();
            if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
                return JsonOperations.deleteKey(ls, right.toString());
            }
        }

        // Fall back to numeric
        return executor.numericOp(left, right, (a, b) -> a - b, Math::subtractExact, java.math.BigDecimal::subtract);
    }

    Object numericOrIntervalMul(Object left, Object right) {
        if (left == null || right == null) return null;
        // Geometric scale/rotate: geom * point (check before range because point "(1,2)" also matches range pattern)
        if (left instanceof String && right instanceof String
                && GeometricOperations.isGeometricString(((String) left))) {
            String rs = (String) right;
            String ls = (String) left;
            return GeometricOperations.multiply(ls, rs);
        }
        // Multirange * Multirange → intersection
        if (left instanceof String && right instanceof String
                && RangeOperations.isMultirangeOrEmpty(((String) left)) && RangeOperations.isMultirangeOrEmpty(((String) right))) {
            return RangeOperations.multirangeIntersect((String) left, (String) right);
        }
        // Multirange * Range → intersection
        if (left instanceof String && right instanceof String
                && RangeOperations.isMultirangeOrEmpty(((String) left)) && RangeOperations.isRangeString(((String) right))) {
            return RangeOperations.multirangeIntersect((String) left, "{" + right + "}");
        }
        if (left instanceof String && right instanceof String
                && RangeOperations.isRangeString(((String) left)) && RangeOperations.isMultirangeOrEmpty(((String) right))) {
            return RangeOperations.multirangeIntersect("{" + left + "}", (String) right);
        }
        // Range * Range → intersection
        if (left instanceof String && right instanceof String
                && RangeOperations.isRangeString(((String) left)) && RangeOperations.isRangeString(((String) right))) {
            String rs = (String) right;
            String ls = (String) left;
            return RangeOperations.intersection(RangeOperations.parse(ls), RangeOperations.parse(rs)).toString();
        }
        // interval * number
        if (left instanceof PgInterval && right instanceof Number) return ((PgInterval) left).multiply(((Number) right).doubleValue());
        if (left instanceof Number && right instanceof PgInterval) return ((PgInterval) right).multiply(((Number) left).doubleValue());
        return executor.numericOp(left, right, (a, b) -> a * b, Math::multiplyExact, java.math.BigDecimal::multiply);
    }
}
