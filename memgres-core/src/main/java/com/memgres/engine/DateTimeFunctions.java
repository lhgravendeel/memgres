package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

/**
 * Date/time function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class DateTimeFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    /** Units below a day, which a date value cannot answer. */
    private static final java.util.Set<String> SUB_DAY_UNITS = new java.util.HashSet<>(
            java.util.Arrays.asList("hour", "hours", "minute", "minutes", "second", "seconds",
                    "millisecond", "milliseconds", "microsecond", "microseconds",
                    "timezone", "timezone_hour", "timezone_minute"));

    private final AstExecutor executor;

    DateTimeFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "age": {
                if (fn.args().size() == 1) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    if (arg == null) return null; // age(NULL) → NULL
                    // age(xid) → int4: transaction age (return small constant for memgres)
                    if (arg instanceof Number) return 1;
                    // PG: single-arg age() uses current_date (midnight), not now()
                    java.time.LocalDateTime dt1 = executor.currentInstant()
                            .atZoneSameInstant(TypeCoercion.sessionZone())
                            .toLocalDate().atStartOfDay();
                    java.time.LocalDateTime dt2 = TypeCoercion.toLocalDateTime(arg);
                    return computeAge(dt1, dt2);
                }
                Object a1 = executor.evalExpr(fn.args().get(0), ctx);
                Object a2 = executor.evalExpr(fn.args().get(1), ctx);
                if (a1 == null || a2 == null) return null; // age(NULL, x) or age(x, NULL) → NULL
                java.time.LocalDateTime dt1 = TypeCoercion.toLocalDateTime(a1);
                java.time.LocalDateTime dt2 = TypeCoercion.toLocalDateTime(a2);
                return computeAge(dt1, dt2);
            }
            case "date_part":
            case "extract": {
                Object fieldObj = executor.evalExpr(fn.args().get(0), ctx);
                Object source = executor.evalExpr(fn.args().get(1), ctx);
                if (source instanceof Number && !(source instanceof Double)) {
                    throw new MemgresException("function date_part(unknown, integer) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                String field = fieldObj.toString().toLowerCase();
                return extractDatePart(field, source);
            }
            case "date_trunc": {
                Object fieldObj = executor.evalExpr(fn.args().get(0), ctx);
                Object source = executor.evalExpr(fn.args().get(1), ctx);
                String field = fieldObj.toString().toLowerCase();
                // A timestamptz truncates in the session's zone, or in the zone the third
                // argument names: midnight is a local idea, not a UTC one.
                java.time.ZoneId zone = TypeCoercion.sessionZone();
                if (fn.args().size() > 2) {
                    Object zoneObj = executor.evalExpr(fn.args().get(2), ctx);
                    if (zoneObj == null) return null;
                    try {
                        zone = java.time.ZoneId.of(zoneObj.toString().trim());
                    } catch (RuntimeException e) {
                        throw new MemgresException("time zone \"" + zoneObj + "\" not recognized", "22023");
                    }
                }
                return truncateDate(field, source, zone);
            }
            case "make_date": {
                int year = executor.toInt(executor.evalExpr(fn.args().get(0), ctx));
                int month = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                int day = executor.toInt(executor.evalExpr(fn.args().get(2), ctx));
                // A negative year names a BC year, which is one lower as a proleptic year
                if (year < 0) year = year + 1;
                try {
                    return java.time.LocalDate.of(year, month, day);
                } catch (java.time.DateTimeException e) {
                    throw new MemgresException("date field value out of range: " + year + "-" + String.format("%02d", month) + "-" + String.format("%02d", day), "22008");
                }
            }
            case "make_timestamp": {
                int year = executor.toInt(executor.evalExpr(fn.args().get(0), ctx));
                int month = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                int day = executor.toInt(executor.evalExpr(fn.args().get(2), ctx));
                int hour = executor.toInt(executor.evalExpr(fn.args().get(3), ctx));
                int minute = executor.toInt(executor.evalExpr(fn.args().get(4), ctx));
                double sec = executor.toDouble(executor.evalExpr(fn.args().get(5), ctx));
                int secs = (int) sec;
                int nanos = (int) Math.round((sec - secs) * 1_000_000_000);
                return java.time.LocalDateTime.of(year, month, day, hour, minute, secs, nanos);
            }
            case "make_timestamptz": {
                int year = executor.toInt(executor.evalExpr(fn.args().get(0), ctx));
                int month = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                int day = executor.toInt(executor.evalExpr(fn.args().get(2), ctx));
                int hour = executor.toInt(executor.evalExpr(fn.args().get(3), ctx));
                int minute = executor.toInt(executor.evalExpr(fn.args().get(4), ctx));
                double sec = executor.toDouble(executor.evalExpr(fn.args().get(5), ctx));
                int secs = (int) sec;
                int nanos = (int) Math.round((sec - secs) * 1_000_000_000);
                String tz = fn.args().size() > 6 ? executor.evalExpr(fn.args().get(6), ctx).toString() : "UTC";
                java.time.ZoneId zone = java.time.ZoneId.of(tz);
                return java.time.LocalDateTime.of(year, month, day, hour, minute, secs, nanos).atZone(zone).toOffsetDateTime();
            }
            case "make_time": {
                int hour = executor.toInt(executor.evalExpr(fn.args().get(0), ctx));
                int minute = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                double sec = executor.toDouble(executor.evalExpr(fn.args().get(2), ctx));
                int secs = (int) sec;
                int nanos = (int) Math.round((sec - secs) * 1_000_000_000);
                return java.time.LocalTime.of(hour, minute, secs, nanos);
            }
            case "isfinite": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof String && (((String) arg).trim().equalsIgnoreCase("infinity") || ((String) arg).trim().equalsIgnoreCase("-infinity"))) {
                    String si = (String) arg;
                    return false;
                }
                if (arg instanceof PgInterval) return !((PgInterval) arg).isInfinite();
                if (arg instanceof java.time.LocalDateTime) {
                    java.time.LocalDateTime dt = (java.time.LocalDateTime) arg;
                    return !dt.equals(TypeCoercion.TIMESTAMP_INFINITY) && !dt.equals(TypeCoercion.TIMESTAMP_NEG_INFINITY);
                }
                if (arg instanceof java.time.LocalDate) {
                    java.time.LocalDate d = (java.time.LocalDate) arg;
                    return !d.equals(java.time.LocalDate.MAX) && !d.equals(java.time.LocalDate.MIN);
                }
                return true;
            }
            case "date_bin": {
                Object intervalObj = executor.evalExpr(fn.args().get(0), ctx);
                Object sourceObj = executor.evalExpr(fn.args().get(1), ctx);
                Object originObj = executor.evalExpr(fn.args().get(2), ctx);
                PgInterval iv = TypeCoercion.toInterval(intervalObj);
                java.time.LocalDateTime source = TypeCoercion.toLocalDateTime(sourceObj);
                java.time.LocalDateTime origin = TypeCoercion.toLocalDateTime(originObj);
                long intervalMicros = iv.getDays() * 24L * 3600 * 1_000_000 + iv.getMicroseconds();
                long sourceMicros = java.time.Duration.between(origin, source).toNanos() / 1000;
                // Use Math.floorDiv to floor toward -infinity for pre-origin values
                long bins = Math.floorDiv(sourceMicros, intervalMicros);
                long binStartMicros = bins * intervalMicros;
                return origin.plusNanos(binStartMicros * 1000);
            }
            case "make_interval": {
                int years = 0, months = 0, weeks = 0, days = 0, hours = 0, mins = 0;
                double secs = 0;
                boolean hasNamedArgs = !fn.args().isEmpty() && fn.args().get(0) instanceof NamedArgExpr;
                if (hasNamedArgs) {
                    for (Expression arg : fn.args()) {
                        if (arg instanceof NamedArgExpr) {
                            NamedArgExpr na = (NamedArgExpr) arg;
                            int val = executor.toInt(executor.evalExpr(na.value(), ctx));
                            switch (na.name()) {
                                case "years":
                                    years = val;
                                    break;
                                case "months":
                                    months = val;
                                    break;
                                case "weeks":
                                    weeks = val;
                                    break;
                                case "days":
                                    days = val;
                                    break;
                                case "hours":
                                    hours = val;
                                    break;
                                case "mins":
                                    mins = val;
                                    break;
                                case "secs":
                                    secs = executor.toDouble(executor.evalExpr(na.value(), ctx));
                                    break;
                            }
                        }
                    }
                } else {
                    years = fn.args().size() > 0 ? executor.toInt(executor.evalExpr(fn.args().get(0), ctx)) : 0;
                    months = fn.args().size() > 1 ? executor.toInt(executor.evalExpr(fn.args().get(1), ctx)) : 0;
                    weeks = fn.args().size() > 2 ? executor.toInt(executor.evalExpr(fn.args().get(2), ctx)) : 0;
                    days = fn.args().size() > 3 ? executor.toInt(executor.evalExpr(fn.args().get(3), ctx)) : 0;
                    hours = fn.args().size() > 4 ? executor.toInt(executor.evalExpr(fn.args().get(4), ctx)) : 0;
                    mins = fn.args().size() > 5 ? executor.toInt(executor.evalExpr(fn.args().get(5), ctx)) : 0;
                    secs = fn.args().size() > 6 ? executor.toDouble(executor.evalExpr(fn.args().get(6), ctx)) : 0;
                }
                return new PgInterval(years * 12 + months,
                        days + weeks * 7,
                        (hours * 3600L + mins * 60L) * 1_000_000L + Math.round(secs * 1_000_000));
            }
            case "transaction_timestamp": {
                if (executor.session != null && executor.session.getTransactionTimestamp() != null) {
                    return executor.session.getTransactionTimestamp();
                }
                return executor.currentStatementTimestamp != null ? executor.currentStatementTimestamp : java.time.OffsetDateTime.now();
            }
            case "statement_timestamp":
                return executor.currentStatementTimestamp != null ? executor.currentStatementTimestamp : java.time.OffsetDateTime.now();
            case "clock_timestamp":
                return java.time.OffsetDateTime.now();
            case "timeofday":
                return java.time.OffsetDateTime.now().toString();
            case "to_char": {
                Object source = executor.evalExpr(fn.args().get(0), ctx);
                if (source == null) return null;
                if (fn.args().size() < 2) {
                    return source.toString();
                }
                Object fmtObj = executor.evalExpr(fn.args().get(1), ctx);
                if (fmtObj == null) return null; // to_char(x, NULL) is NULL, not a template error
                return formatToChar(source, fmtObj.toString());
            }
            case "to_date": {
                Object source = executor.evalExpr(fn.args().get(0), ctx);
                if (source == null) return null;
                if (fn.args().size() >= 2) {
                    Object fmtObj = executor.evalExpr(fn.args().get(1), ctx);
                    if (fmtObj == null) return null;
                    return DateTimeTemplate.parse(source.toString(), fmtObj.toString())
                            .toLocalDate();
                }
                return TypeCoercion.toLocalDate(source);
            }
            case "to_timestamp": {
                Object source = executor.evalExpr(fn.args().get(0), ctx);
                if (source == null) return null;
                if (source instanceof Number) {
                    Number n = (Number) source;
                    return java.time.OffsetDateTime.ofInstant(java.time.Instant.ofEpochSecond(n.longValue()), java.time.ZoneOffset.UTC);
                }
                if (fn.args().size() >= 2) {
                    Object fmtObj = executor.evalExpr(fn.args().get(1), ctx);
                    if (fmtObj == null) return null;
                    java.time.LocalDateTime ldt =
                            DateTimeTemplate.parse(source.toString(), fmtObj.toString());
                    return ldt.atOffset(java.time.ZoneOffset.UTC);
                }
                return TypeCoercion.toOffsetDateTime(source);
            }
            case "to_number": {
                Object source = executor.evalExpr(fn.args().get(0), ctx);
                if (source == null) return null;
                return parseNumberWithFormat(source.toString(),
                        fn.args().size() > 1 ? String.valueOf(executor.evalExpr(fn.args().get(1), ctx)) : null);
            }
            case "justify_hours": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                PgInterval iv = TypeCoercion.toInterval(arg);
                if (iv.isInfinite()) return iv; // nothing to redistribute
                long micros = iv.getMicroseconds();
                int extraDays = (int) (micros / (24L * 3600 * 1_000_000));
                long remainMicros = micros % (24L * 3600 * 1_000_000);
                return new PgInterval(iv.getMonths(), iv.getDays() + extraDays, remainMicros);
            }
            case "justify_days": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                PgInterval iv = TypeCoercion.toInterval(arg);
                if (iv.isInfinite()) return iv; // nothing to redistribute
                int extraMonths = iv.getDays() / 30;
                int remainDays = iv.getDays() % 30;
                return new PgInterval(iv.getMonths() + extraMonths, remainDays, iv.getMicroseconds());
            }
            case "justify_interval": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                PgInterval iv = TypeCoercion.toInterval(arg);
                if (iv.isInfinite()) return iv; // nothing to redistribute
                long microPerDay = 24L * 3600 * 1_000_000;
                long micros = iv.getMicroseconds();
                int extraDays = (int) (micros / microPerDay);
                long remainMicros = micros % microPerDay;
                int totalDays = iv.getDays() + extraDays;
                int extraMonths = totalDays / 30;
                int remainDays = totalDays % 30;
                int totalMonths = iv.getMonths() + extraMonths;
                if (totalMonths > 0 && remainMicros < 0) {
                    remainMicros += microPerDay;
                    remainDays -= 1;
                }
                if (totalMonths > 0 && remainDays < 0) {
                    remainDays += 30;
                    totalMonths -= 1;
                }
                if (totalMonths < 0 && remainMicros > 0) {
                    remainMicros -= microPerDay;
                    remainDays += 1;
                }
                if (totalMonths < 0 && remainDays > 0) {
                    remainDays -= 30;
                    totalMonths += 1;
                }
                return new PgInterval(totalMonths, remainDays, remainMicros);
            }
            default:
                return NOT_HANDLED;
        }
    }

    private PgInterval computeAge(java.time.LocalDateTime dt1, java.time.LocalDateTime dt2) {
        java.time.Period p = java.time.Period.between(dt2.toLocalDate(), dt1.toLocalDate());
        long timeMicros = java.time.Duration.between(dt2.toLocalTime(), dt1.toLocalTime()).toNanos() / 1000;
        // If time part is negative but date part is positive, borrow a day
        if (timeMicros < 0 && (p.getYears() > 0 || p.getMonths() > 0 || p.getDays() > 0)) {
            p = p.minusDays(1);
            timeMicros += 24L * 3600 * 1_000_000;
        } else if (timeMicros > 0 && (p.getYears() < 0 || p.getMonths() < 0 || p.getDays() < 0)) {
            p = p.plusDays(1);
            timeMicros -= 24L * 3600 * 1_000_000;
        }
        return new PgInterval(p.getYears() * 12 + p.getMonths(), p.getDays(), timeMicros);
    }

    private Object extractDatePart(String field, Object source) {
        if (source == null) return null;
        if (source instanceof PgInterval) {
            PgInterval iv = (PgInterval) source;
            if (iv.isInfinite()) {
                // Only epoch has a meaningful answer for an infinite interval
                if (field.equals("epoch")) {
                    return iv.isPositiveInfinity() ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
                }
                return java.math.BigDecimal.ZERO;
            }
            switch (field) {
                case "year":
                case "years":
                    return java.math.BigDecimal.valueOf(iv.getMonths() / 12);
                case "month":
                case "months":
                    return java.math.BigDecimal.valueOf(iv.getMonths() % 12);
                case "day":
                case "days":
                    return java.math.BigDecimal.valueOf(iv.getDays());
                case "hour":
                case "hours":
                    return java.math.BigDecimal.valueOf(iv.getMicroseconds() / 3_600_000_000L);
                case "minute":
                case "minutes":
                    return java.math.BigDecimal.valueOf((iv.getMicroseconds() % 3_600_000_000L) / 60_000_000L);
                case "second":
                case "seconds":
                    return java.math.BigDecimal.valueOf((iv.getMicroseconds() % 60_000_000L)).divide(java.math.BigDecimal.valueOf(1_000_000), 6, java.math.RoundingMode.HALF_UP).stripTrailingZeros();
                case "epoch": {
                    // PG splits the months: whole years count 365.25 days each, the leftover
                    // months 30 days each. Always reported with 6 fractional digits.
                    long totalSecs = (long) (iv.getMonths() / 12) * 31557600L
                            + (long) (iv.getMonths() % 12) * 2592000L
                            + (long) iv.getDays() * 86400L;
                    java.math.BigDecimal secsPart = java.math.BigDecimal.valueOf(totalSecs);
                    java.math.BigDecimal microsPart = java.math.BigDecimal.valueOf(iv.getMicroseconds())
                            .divide(java.math.BigDecimal.valueOf(1_000_000), 6, java.math.RoundingMode.HALF_UP);
                    return secsPart.add(microsPart).setScale(6, java.math.RoundingMode.HALF_UP);
                }
                default:
                    throw new MemgresException("unit \"" + field + "\" not recognized for type interval", "22023");
            }
        }
        Object originalSource = source;
        java.time.LocalDateTime dt;
        if (source instanceof java.time.LocalDate) dt = ((java.time.LocalDate) source).atStartOfDay();
        else if (source instanceof java.time.LocalDateTime) dt = ((java.time.LocalDateTime) source);
        else if (source instanceof java.time.OffsetDateTime) dt = ((java.time.OffsetDateTime) source).atZoneSameInstant(TypeCoercion.sessionZone()).toLocalDateTime();
        else dt = TypeCoercion.toLocalDateTime(source);

        // A date has no time of day, so PG refuses the sub-day units outright rather than
        // reporting the zero that midnight would give
        if (originalSource instanceof java.time.LocalDate && SUB_DAY_UNITS.contains(field)) {
            throw new MemgresException(
                    "unit \"" + field + "\" not supported for type date", "0A000");
        }

        switch (field) {
            case "year":
            case "years":
                // PG has no year zero, so a BC proleptic year reports one lower
                return java.math.BigDecimal.valueOf(
                        dt.getYear() > 0 ? dt.getYear() : dt.getYear() - 1);
            case "month":
            case "months":
                return java.math.BigDecimal.valueOf(dt.getMonthValue());
            case "day":
            case "days":
                return java.math.BigDecimal.valueOf(dt.getDayOfMonth());
            case "hour":
            case "hours":
                return java.math.BigDecimal.valueOf(dt.getHour());
            case "minute":
            case "minutes":
                return java.math.BigDecimal.valueOf(dt.getMinute());
            case "second":
            case "seconds": {
                long sec = dt.getSecond();
                int nano = dt.getNano();
                if (nano == 0) return java.math.BigDecimal.valueOf(sec);
                return java.math.BigDecimal.valueOf(sec).add(java.math.BigDecimal.valueOf(nano, 9)).stripTrailingZeros();
            }
            case "dow":
                return java.math.BigDecimal.valueOf(dt.getDayOfWeek().getValue() % 7);
            case "doy":
                return java.math.BigDecimal.valueOf(dt.getDayOfYear());
            case "week":
                return java.math.BigDecimal.valueOf(dt.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR));
            case "quarter":
                return java.math.BigDecimal.valueOf((dt.getMonthValue() - 1) / 3 + 1);
            case "epoch": {
                long epochSec;
                if (originalSource instanceof java.time.OffsetDateTime) {
                    java.time.OffsetDateTime odt = (java.time.OffsetDateTime) originalSource;
                    epochSec = odt.toEpochSecond();
                } else {
                    epochSec = dt.toEpochSecond(java.time.ZoneOffset.UTC);
                }
                int nano = dt.getNano();
                if (nano == 0) return java.math.BigDecimal.valueOf(epochSec);
                return java.math.BigDecimal.valueOf(epochSec).add(java.math.BigDecimal.valueOf(nano, 9)).setScale(6, java.math.RoundingMode.HALF_UP);
            }
            case "isodow":
                return java.math.BigDecimal.valueOf(dt.getDayOfWeek().getValue());
            case "isoyear":
                return java.math.BigDecimal.valueOf(dt.get(java.time.temporal.IsoFields.WEEK_BASED_YEAR));
            case "century":
                return java.math.BigDecimal.valueOf(dt.getYear() > 0 ? (dt.getYear() - 1) / 100 + 1 : dt.getYear() / 100 - 1);
            case "decade":
                return java.math.BigDecimal.valueOf(dt.getYear() / 10);
            case "millennium":
                return java.math.BigDecimal.valueOf(dt.getYear() > 0 ? (dt.getYear() - 1) / 1000 + 1 : dt.getYear() / 1000 - 1);
            case "microsecond":
            case "microseconds":
                return java.math.BigDecimal.valueOf(dt.getSecond() * 1_000_000L + dt.getNano() / 1000);
            case "millisecond":
            case "milliseconds":
                return java.math.BigDecimal.valueOf(dt.getSecond() * 1000L + dt.getNano() / 1_000_000);
            case "julian": {
                // Julian Day Number: PostgreSQL uses midnight-based Julian days.
                // Java's JulianFields.JULIAN_DAY also starts at midnight, so no
                // offset adjustment is needed — just add the fractional time-of-day.
                long julianDay = dt.toLocalDate().getLong(java.time.temporal.JulianFields.JULIAN_DAY);
                long dayMicros = (dt.getHour() * 3600L + dt.getMinute() * 60L + dt.getSecond()) * 1_000_000L + dt.getNano() / 1000;
                java.math.BigDecimal frac = java.math.BigDecimal.valueOf(dayMicros).divide(
                        java.math.BigDecimal.valueOf(86400_000_000L), 6, java.math.RoundingMode.HALF_UP);
                return java.math.BigDecimal.valueOf(julianDay)
                        .add(frac).stripTrailingZeros();
            }
            case "timezone":
            case "timezone_hour":
            case "timezone_minute": {
                // PG converts timestamptz to session timezone first, so timezone fields
                // reflect the session timezone, not the original literal offset.
                // Memgres session timezone is UTC (offset 0).
                int totalSeconds = 0;
                if (originalSource instanceof java.time.OffsetTime) {
                    // For timetz, use the actual offset from the value
                    totalSeconds = ((java.time.OffsetTime) originalSource).getOffset().getTotalSeconds();
                }
                // For timestamptz (OffsetDateTime), session timezone applies (UTC = 0)
                switch (field) {
                    case "timezone":
                        return java.math.BigDecimal.valueOf(totalSeconds);
                    case "timezone_hour":
                        return java.math.BigDecimal.valueOf(totalSeconds / 3600);
                    case "timezone_minute":
                        return java.math.BigDecimal.valueOf((totalSeconds % 3600) / 60);
                    default:
                        return java.math.BigDecimal.ZERO;
                }
            }
            default:
                throw new MemgresException("unit \"" + field + "\" not recognized for type timestamp without time zone", "22023");
        }
    }

    private Object truncateDate(String field, Object source) {
        return truncateDate(field, source, TypeCoercion.sessionZone());
    }

    private Object truncateDate(String field, Object source, java.time.ZoneId zone) {
        if (source == null) return null;
        // date_trunc(unit, interval) zeroes every field below the unit; there is no calendar
        // involved, so it is a separate rule from the timestamp one
        if (source instanceof PgInterval) return truncateInterval(field, (PgInterval) source);
        java.time.LocalDateTime dt;
        boolean isDate = source instanceof java.time.LocalDate;
        if (source instanceof java.time.LocalDate) dt = ((java.time.LocalDate) source).atStartOfDay();
        else if (source instanceof java.time.LocalDateTime) dt = ((java.time.LocalDateTime) source);
        else if (source instanceof java.time.OffsetDateTime) dt = ((java.time.OffsetDateTime) source).atZoneSameInstant(zone).toLocalDateTime();
        else dt = TypeCoercion.toLocalDateTime(source);

        java.time.LocalDateTime result;
        switch (field) {
            case "year":
                result = java.time.LocalDateTime.of(dt.getYear(), 1, 1, 0, 0);
                break;
            case "decade":
            case "decades":
                result = java.time.LocalDateTime.of(Math.floorDiv(dt.getYear(), 10) * 10, 1, 1, 0, 0);
                break;
            case "century":
            case "centuries":
                // The first century runs 1..100, so 2026 truncates to 2001, not 2000
                result = java.time.LocalDateTime.of(centuryStart(dt.getYear(), 100), 1, 1, 0, 0);
                break;
            case "millennium":
            case "millennia":
                result = java.time.LocalDateTime.of(centuryStart(dt.getYear(), 1000), 1, 1, 0, 0);
                break;
            case "quarter": {
                int q = (dt.getMonthValue() - 1) / 3;
                result = java.time.LocalDateTime.of(dt.getYear(), q * 3 + 1, 1, 0, 0);
                break;
            }
            case "month":
                result = java.time.LocalDateTime.of(dt.getYear(), dt.getMonthValue(), 1, 0, 0);
                break;
            case "week":
                result = dt.with(java.time.DayOfWeek.MONDAY).withHour(0).withMinute(0).withSecond(0).withNano(0);
                break;
            case "day":
                result = dt.withHour(0).withMinute(0).withSecond(0).withNano(0);
                break;
            case "hour":
                result = dt.withMinute(0).withSecond(0).withNano(0);
                break;
            case "minute":
                result = dt.withSecond(0).withNano(0);
                break;
            case "second":
                result = dt.withNano(0);
                break;
            case "millisecond":
            case "milliseconds":
                result = dt.withNano((dt.getNano() / 1_000_000) * 1_000_000);
                break;
            case "microsecond":
            case "microseconds":
                result = dt.withNano((dt.getNano() / 1_000) * 1_000);
                break;
            default:
                throw new MemgresException("unit \"" + field + "\" not recognized for type timestamp", "22023");
        }
        if (isDate) return result.toLocalDate();
        if (source instanceof java.time.OffsetDateTime) return result.atZone(zone).toOffsetDateTime();
        return result;
    }

    /** The first year of the century/millennium containing {@code year} (PG counts from 1). */
    private static int centuryStart(int year, int width) {
        return Math.floorDiv(year - 1, width) * width + 1;
    }

    /** Zero every interval field smaller than the requested unit. */
    private Object truncateInterval(String field, PgInterval iv) {
        if (iv.isInfinite()) return iv;
        int months = iv.getMonths();
        int days = iv.getDays();
        long micros = iv.getMicroseconds();
        switch (field) {
            case "millennium": case "millennia": months -= months % 12000; days = 0; micros = 0; break;
            case "century": case "centuries":    months -= months % 1200;  days = 0; micros = 0; break;
            case "decade": case "decades":       months -= months % 120;   days = 0; micros = 0; break;
            case "year": case "years":           months -= months % 12;    days = 0; micros = 0; break;
            case "quarter": case "quarters":     months -= months % 3;     days = 0; micros = 0; break;
            case "month": case "months":         days = 0; micros = 0; break;
            case "day": case "days":             micros = 0; break;
            case "hour": case "hours":           micros -= micros % 3_600_000_000L; break;
            case "minute": case "minutes":       micros -= micros % 60_000_000L; break;
            case "second": case "seconds":       micros -= micros % 1_000_000L; break;
            case "millisecond": case "milliseconds": micros -= micros % 1_000L; break;
            case "microsecond": case "microseconds": break;
            default:
                throw new MemgresException("unit \"" + field + "\" not recognized for type interval", "22023");
        }
        return new PgInterval(months, days, micros);
    }

    /** to_char dispatches on the value: a number takes the numeric templates, a date the others. */
    private String formatToChar(Object source, String fmt) {
        if (source instanceof Number) return NumericTemplate.format((Number) source, fmt);
        return DateTimeTemplate.toChar(source, fmt);
    }

    /**
     * PG's to_number reads the digits and separators the format describes and returns numeric.
     * The sign may lead or trail (S/MI/PL) or be parenthesised (PR), which a plain numeric parse
     * would drop — turning '123-' into 123 instead of -123.
     */
    private java.math.BigDecimal parseNumberWithFormat(String input, String fmt) {
        String s = input.trim();
        boolean negative = false;
        if (s.startsWith("(") && s.endsWith(")")) {
            negative = true;
            s = s.substring(1, s.length() - 1).trim();
        }
        if (s.endsWith("-")) {
            negative = true;
            s = s.substring(0, s.length() - 1).trim();
        } else if (s.startsWith("-")) {
            negative = true;
            s = s.substring(1).trim();
        } else if (s.startsWith("+")) {
            s = s.substring(1).trim();
        } else if (s.endsWith("+")) {
            s = s.substring(0, s.length() - 1).trim();
        }
        // Group separators are positional noise; only the decimal marker carries meaning
        s = s.replaceAll("[^0-9.]", "");
        if (s.isEmpty()) return java.math.BigDecimal.ZERO;
        java.math.BigDecimal value;
        try {
            value = new java.math.BigDecimal(s);
        } catch (NumberFormatException e) {
            throw new MemgresException("invalid input syntax for type numeric: \"" + input + "\"", "22P02");
        }
        return negative ? value.negate() : value;
    }

}
