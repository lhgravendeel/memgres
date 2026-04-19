package com.memgres.engine;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.util.Strs;

/**
 * Date/time function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class DateTimeFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

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
                    java.time.LocalDateTime dt1 = java.time.LocalDateTime.now();
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
                return truncateDate(field, source);
            }
            case "make_date": {
                int year = executor.toInt(executor.evalExpr(fn.args().get(0), ctx));
                int month = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                int day = executor.toInt(executor.evalExpr(fn.args().get(2), ctx));
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
                long bins = sourceMicros / intervalMicros;
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
                String fmt = executor.evalExpr(fn.args().get(1), ctx).toString();
                return formatToChar(source, fmt);
            }
            case "to_date": {
                Object source = executor.evalExpr(fn.args().get(0), ctx);
                if (source == null) return null;
                if (fn.args().size() >= 2) {
                    String dateStr = source.toString();
                    String fmt = executor.evalExpr(fn.args().get(1), ctx).toString();
                    return parseDateWithFormat(dateStr, fmt);
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
                    String fmt = executor.evalExpr(fn.args().get(1), ctx).toString();
                    java.time.LocalDateTime ldt = parseTimestampWithFormat(source.toString(), fmt);
                    return ldt.atOffset(java.time.ZoneOffset.UTC);
                }
                return TypeCoercion.toOffsetDateTime(source);
            }
            case "to_number": {
                Object source = executor.evalExpr(fn.args().get(0), ctx);
                if (source == null) return null;
                String s = source.toString().replaceAll("[^0-9.\\-+eE]", "");
                if (s.isEmpty()) return 0.0;
                return Double.parseDouble(s);
            }
            case "justify_hours": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                PgInterval iv = TypeCoercion.toInterval(arg);
                long micros = iv.getMicroseconds();
                int extraDays = (int) (micros / (24L * 3600 * 1_000_000));
                long remainMicros = micros % (24L * 3600 * 1_000_000);
                return new PgInterval(iv.getMonths(), iv.getDays() + extraDays, remainMicros);
            }
            case "justify_days": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                PgInterval iv = TypeCoercion.toInterval(arg);
                int extraMonths = iv.getDays() / 30;
                int remainDays = iv.getDays() % 30;
                return new PgInterval(iv.getMonths() + extraMonths, remainDays, iv.getMicroseconds());
            }
            case "justify_interval": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                PgInterval iv = TypeCoercion.toInterval(arg);
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
                    // PG uses 2629800 seconds per month (= 365.25/12 * 86400), not 30*86400
                    long totalSecs = (long) iv.getMonths() * 2629800L + (long) iv.getDays() * 86400L;
                    java.math.BigDecimal secsPart = java.math.BigDecimal.valueOf(totalSecs);
                    java.math.BigDecimal microsPart = java.math.BigDecimal.valueOf(iv.getMicroseconds())
                            .divide(java.math.BigDecimal.valueOf(1_000_000), 6, java.math.RoundingMode.HALF_UP);
                    java.math.BigDecimal result = secsPart.add(microsPart);
                    // Use toPlainString-compatible representation: strip trailing fractional zeros but keep integer form
                    result = result.stripTrailingZeros();
                    if (result.scale() < 0) result = result.setScale(0);
                    return result;
                }
                default:
                    throw new MemgresException("unit \"" + field + "\" not recognized for type interval", "22023");
            }
        }
        Object originalSource = source;
        java.time.LocalDateTime dt;
        if (source instanceof java.time.LocalDate) dt = ((java.time.LocalDate) source).atStartOfDay();
        else if (source instanceof java.time.LocalDateTime) dt = ((java.time.LocalDateTime) source);
        else if (source instanceof java.time.OffsetDateTime) dt = ((java.time.OffsetDateTime) source).toLocalDateTime();
        else dt = TypeCoercion.toLocalDateTime(source);

        switch (field) {
            case "year":
            case "years":
                return java.math.BigDecimal.valueOf(dt.getYear());
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
                // Julian Day Number: PostgreSQL returns a fractional Julian day where
                // Julian days start at noon (12:00). Java's JulianFields.JULIAN_DAY
                // gives the integer Julian day that starts at midnight, so we need to
                // subtract 0.5 (to shift to the noon-based epoch) and then add
                // the fractional time-of-day.
                long julianDay = dt.toLocalDate().getLong(java.time.temporal.JulianFields.JULIAN_DAY);
                long dayMicros = (dt.getHour() * 3600L + dt.getMinute() * 60L + dt.getSecond()) * 1_000_000L + dt.getNano() / 1000;
                java.math.BigDecimal frac = java.math.BigDecimal.valueOf(dayMicros).divide(
                        java.math.BigDecimal.valueOf(86400_000_000L), 6, java.math.RoundingMode.HALF_UP);
                // Subtract 0.5 because Julian days begin at noon, not midnight
                return java.math.BigDecimal.valueOf(julianDay)
                        .subtract(new java.math.BigDecimal("0.5"))
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
        if (source == null) return null;
        java.time.LocalDateTime dt;
        boolean isDate = source instanceof java.time.LocalDate;
        if (source instanceof java.time.LocalDate) dt = ((java.time.LocalDate) source).atStartOfDay();
        else if (source instanceof java.time.LocalDateTime) dt = ((java.time.LocalDateTime) source);
        else if (source instanceof java.time.OffsetDateTime) dt = ((java.time.OffsetDateTime) source).toLocalDateTime();
        else dt = TypeCoercion.toLocalDateTime(source);

        java.time.LocalDateTime result;
        switch (field) {
            case "year":
                result = java.time.LocalDateTime.of(dt.getYear(), 1, 1, 0, 0);
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
        if (source instanceof java.time.OffsetDateTime) return result.atOffset(java.time.ZoneOffset.UTC);
        return result;
    }

    private String formatToChar(Object source, String fmt) {
        java.time.LocalDateTime dt;
        if (source instanceof java.time.LocalDate) dt = ((java.time.LocalDate) source).atStartOfDay();
        else if (source instanceof java.time.LocalDateTime) dt = ((java.time.LocalDateTime) source);
        else if (source instanceof java.time.OffsetDateTime) dt = ((java.time.OffsetDateTime) source).toLocalDateTime();
        else if (source instanceof Number) {
            Number n = (Number) source;
            return formatNumber(n, fmt);
        }
        else dt = TypeCoercion.toLocalDateTime(source);

        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (i < fmt.length()) {
            String rest = fmt.substring(i);
            if (rest.startsWith("YYYY")) { sb.append(String.format("%04d", dt.getYear())); i += 4; }
            else if (rest.startsWith("YY")) { sb.append(String.format("%02d", dt.getYear() % 100)); i += 2; }
            else if (rest.startsWith("Month")) {
                String mname = dt.getMonth().getDisplayName(java.time.format.TextStyle.FULL, java.util.Locale.ENGLISH);
                String formatted = mname.substring(0, 1).toUpperCase() + mname.substring(1).toLowerCase();
                sb.append(String.format("%-9s", formatted));
                i += 5;
            }
            else if (rest.startsWith("MONTH")) {
                sb.append(dt.getMonth().getDisplayName(java.time.format.TextStyle.FULL, java.util.Locale.ENGLISH).toUpperCase());
                i += 5;
            }
            else if (rest.startsWith("month")) {
                sb.append(dt.getMonth().getDisplayName(java.time.format.TextStyle.FULL, java.util.Locale.ENGLISH).toLowerCase());
                i += 5;
            }
            else if (rest.startsWith("Mon")) {
                String mname = dt.getMonth().getDisplayName(java.time.format.TextStyle.SHORT, java.util.Locale.ENGLISH);
                sb.append(mname.substring(0, 1).toUpperCase()).append(mname.substring(1).toLowerCase());
                i += 3;
            }
            else if (rest.startsWith("MM")) { sb.append(String.format("%02d", dt.getMonthValue())); i += 2; }
            else if (rest.startsWith("DD")) { sb.append(String.format("%02d", dt.getDayOfMonth())); i += 2; }
            else if (rest.startsWith("Day")) {
                String dname = dt.getDayOfWeek().getDisplayName(java.time.format.TextStyle.FULL, java.util.Locale.ENGLISH);
                String formatted = dname.substring(0, 1).toUpperCase() + dname.substring(1).toLowerCase();
                sb.append(String.format("%-9s", formatted));
                i += 3;
            }
            else if (rest.startsWith("DAY")) {
                sb.append(dt.getDayOfWeek().getDisplayName(java.time.format.TextStyle.FULL, java.util.Locale.ENGLISH).toUpperCase());
                i += 3;
            }
            else if (rest.startsWith("day")) {
                sb.append(dt.getDayOfWeek().getDisplayName(java.time.format.TextStyle.FULL, java.util.Locale.ENGLISH).toLowerCase());
                i += 3;
            }
            else if (rest.startsWith("Dy")) {
                String dname = dt.getDayOfWeek().getDisplayName(java.time.format.TextStyle.SHORT, java.util.Locale.ENGLISH);
                sb.append(dname.substring(0, 1).toUpperCase()).append(dname.substring(1).toLowerCase());
                i += 2;
            }
            else if (rest.startsWith("DY")) {
                sb.append(dt.getDayOfWeek().getDisplayName(java.time.format.TextStyle.SHORT, java.util.Locale.ENGLISH).toUpperCase());
                i += 2;
            }
            else if (rest.startsWith("dy")) {
                sb.append(dt.getDayOfWeek().getDisplayName(java.time.format.TextStyle.SHORT, java.util.Locale.ENGLISH).toLowerCase());
                i += 2;
            }
            else if (rest.startsWith("HH24")) { sb.append(String.format("%02d", dt.getHour())); i += 4; }
            else if (rest.startsWith("HH12") || rest.startsWith("HH")) {
                int h = dt.getHour() % 12; if (h == 0) h = 12;
                sb.append(String.format("%02d", h));
                i += rest.startsWith("HH12") ? 4 : 2;
            }
            else if (rest.startsWith("MI")) { sb.append(String.format("%02d", dt.getMinute())); i += 2; }
            else if (rest.startsWith("SS")) { sb.append(String.format("%02d", dt.getSecond())); i += 2; }
            else if (rest.startsWith("Q")) { sb.append((dt.getMonthValue() - 1) / 3 + 1); i += 1; }
            else if (rest.startsWith("D")) {
                int dow = dt.getDayOfWeek().getValue() % 7 + 1;
                sb.append(dow);
                i += 1;
            }
            else if (rest.startsWith("AM") || rest.startsWith("PM")) {
                sb.append(dt.getHour() < 12 ? "AM" : "PM"); i += 2;
            }
            else if (rest.startsWith("am") || rest.startsWith("pm")) {
                sb.append(dt.getHour() < 12 ? "am" : "pm"); i += 2;
            }
            else if (rest.startsWith("TZH")) {
                if (source instanceof java.time.OffsetDateTime) {
                    java.time.ZoneOffset offset = ((java.time.OffsetDateTime) source).getOffset();
                    int totalSeconds = offset.getTotalSeconds();
                    String sign = totalSeconds >= 0 ? "+" : "-";
                    int hours = Math.abs(totalSeconds) / 3600;
                    sb.append(String.format("%s%02d", sign, hours));
                } else {
                    sb.append("+00");
                }
                i += 3;
            }
            else if (rest.startsWith("TZM")) {
                if (source instanceof java.time.OffsetDateTime) {
                    java.time.ZoneOffset offset = ((java.time.OffsetDateTime) source).getOffset();
                    int minutes = (Math.abs(offset.getTotalSeconds()) % 3600) / 60;
                    sb.append(String.format("%02d", minutes));
                } else {
                    sb.append("00");
                }
                i += 3;
            }
            else if (rest.startsWith("TZ")) {
                if (source instanceof java.time.OffsetDateTime) {
                    java.time.ZoneOffset offset = ((java.time.OffsetDateTime) source).getOffset();
                    if (offset.getTotalSeconds() == 0) {
                        sb.append("UTC");
                    } else {
                        sb.append(offset.getId().replace("Z", "UTC"));
                    }
                } else {
                    sb.append("UTC");
                }
                i += 2;
            }
            else if (rest.startsWith("OF")) {
                if (source instanceof java.time.OffsetDateTime) {
                    java.time.ZoneOffset offset = ((java.time.OffsetDateTime) source).getOffset();
                    int totalSeconds = offset.getTotalSeconds();
                    String sign = totalSeconds >= 0 ? "+" : "-";
                    int absSeconds = Math.abs(totalSeconds);
                    int hours = absSeconds / 3600;
                    int minutes = (absSeconds % 3600) / 60;
                    if (minutes != 0) {
                        sb.append(String.format("%s%02d:%02d", sign, hours, minutes));
                    } else {
                        sb.append(String.format("%s%02d", sign, hours));
                    }
                } else {
                    sb.append("+00");
                }
                i += 2;
            }
            else { sb.append(fmt.charAt(i)); i++; }
        }
        return sb.toString();
    }

    private java.time.LocalDate parseDateWithFormat(String dateStr, String pgFmt) {
        String javaPattern = pgFmt
                .replace("YYYY", "yyyy")
                .replace("MM", "MM")
                .replace("DD", "dd");
        try {
            return java.time.LocalDate.parse(dateStr, java.time.format.DateTimeFormatter.ofPattern(javaPattern));
        } catch (Exception e) {
            return TypeCoercion.toLocalDate(dateStr);
        }
    }

    private java.time.LocalDateTime parseTimestampWithFormat(String tsStr, String pgFmt) {
        String javaPattern = pgFmt
                .replace("YYYY", "yyyy")
                .replace("MM", "MM")
                .replace("DD", "dd")
                .replace("HH24", "HH")
                .replace("HH12", "hh")
                .replace("MI", "mm")
                .replace("SS", "ss");
        try {
            return java.time.LocalDateTime.parse(tsStr, java.time.format.DateTimeFormatter.ofPattern(javaPattern));
        } catch (Exception e) {
            return TypeCoercion.toLocalDateTime(tsStr);
        }
    }

    private String formatNumber(Number n, String fmt) {
        double val = n.doubleValue();
        boolean negative = val < 0;
        double absVal = Math.abs(val);

        boolean fillMode = false;
        String fmtWork = fmt;
        if (fmtWork.toUpperCase().startsWith("FM")) {
            fillMode = true;
            fmtWork = fmtWork.substring(2);
        }

        boolean miSuffix = false;
        if (fmtWork.toUpperCase().endsWith("MI")) {
            miSuffix = true;
            fmtWork = fmtWork.substring(0, fmtWork.length() - 2);
        }

        boolean prSuffix = false;
        if (fmtWork.toUpperCase().endsWith("PR")) {
            prSuffix = true;
            fmtWork = fmtWork.substring(0, fmtWork.length() - 2);
        }

        int intDigits9 = 0, intDigits0 = 0, fracDigits = 0;
        boolean hasDecimal = fmtWork.contains(".");
        boolean hasComma = fmtWork.contains(",");
        String intPart, fracPart = "";
        if (hasDecimal) {
            int dotIdx = fmtWork.indexOf('.');
            intPart = fmtWork.substring(0, dotIdx);
            fracPart = fmtWork.substring(dotIdx + 1);
            fracDigits = fracPart.length();
        } else {
            intPart = fmtWork;
        }
        for (char c : intPart.toCharArray()) {
            if (c == '9') intDigits9++;
            else if (c == '0') intDigits0++;
        }
        int totalIntDigits = intDigits9 + intDigits0;

        String formatted;
        if (hasDecimal) {
            formatted = String.format(java.util.Locale.US, "%." + fracDigits + "f", absVal);
        } else {
            formatted = String.valueOf((long) absVal);
        }

        String intStr, fracStr = "";
        if (formatted.contains(".")) {
            int dotIdx = formatted.indexOf('.');
            intStr = formatted.substring(0, dotIdx);
            fracStr = formatted.substring(dotIdx + 1);
        } else {
            intStr = formatted;
        }

        while (intStr.length() < totalIntDigits) {
            intStr = (intDigits0 > 0 && intStr.length() < totalIntDigits) ? "0" + intStr : " " + intStr;
        }

        if (hasComma) {
            StringBuilder grouped = new StringBuilder();
            int count = 0;
            for (int i = intStr.length() - 1; i >= 0; i--) {
                char c = intStr.charAt(i);
                if (c == ' ') {
                    grouped.insert(0, c);
                } else {
                    if (count > 0 && count % 3 == 0) grouped.insert(0, ',');
                    grouped.insert(0, c);
                    count++;
                }
            }
            intStr = grouped.toString();
        }

        StringBuilder result = new StringBuilder();
        if (!fillMode) result.append(' ');
        if (negative && !miSuffix && !prSuffix) result.append('-');
        if (prSuffix && negative) result.append('<');
        result.append(intStr);
        if (hasDecimal) result.append('.').append(fracStr);
        if (miSuffix) result.append(negative ? '-' : ' ');
        if (prSuffix) result.append(negative ? '>' : ' ');

        String res = result.toString();
        if (fillMode) res = Strs.stripLeading(res);
        return res;
    }
}
