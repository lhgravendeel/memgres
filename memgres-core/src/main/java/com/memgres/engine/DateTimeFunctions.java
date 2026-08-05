package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

/**
 * Date/time function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class DateTimeFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    /**
     * PostgreSQL's interval-unit token table, keyed by spelling and valued by canonical unit.
     * A spelling matches a token when the two agree on their first ten characters, which is
     * why "microseconds" and "millenniums" are accepted while "microsecs" is not.
     */
    private static final java.util.Map<String, String> DELTA_UNITS = new java.util.HashMap<>();

    /** The further units only the field-extracting functions read; date_trunc does not. */
    private static final java.util.Map<String, String> FIELD_UNITS = new java.util.HashMap<>();

    /** The reserved words among {@link #FIELD_UNITS}: of these only "epoch" ever has a value. */
    private static final java.util.Set<String> RESERVED_UNITS = new java.util.HashSet<>(
            java.util.Arrays.asList("epoch", "now", "today", "tomorrow", "yesterday",
                    "infinity", "-infinity", "allballs"));

    static {
        alias(DELTA_UNITS, "microsecond", "microsecon", "us", "usec", "usecond", "useconds", "usecs");
        alias(DELTA_UNITS, "millisecond", "millisecon", "ms", "msec", "msecond", "mseconds", "msecs");
        alias(DELTA_UNITS, "second", "s", "sec", "second", "seconds", "secs");
        alias(DELTA_UNITS, "minute", "m", "min", "mins", "minute", "minutes");
        alias(DELTA_UNITS, "hour", "h", "hour", "hours", "hr", "hrs");
        alias(DELTA_UNITS, "day", "d", "day", "days");
        alias(DELTA_UNITS, "week", "w", "week", "weeks");
        alias(DELTA_UNITS, "month", "mon", "mons", "month", "months");
        alias(DELTA_UNITS, "quarter", "qtr", "quarter");
        alias(DELTA_UNITS, "year", "y", "year", "years", "yr", "yrs");
        alias(DELTA_UNITS, "decade", "dec", "decade", "decades", "decs");
        alias(DELTA_UNITS, "century", "c", "cent", "centuries", "century");
        alias(DELTA_UNITS, "millennium", "mil", "millennia", "millennium", "mils");
        alias(DELTA_UNITS, "timezone", "timezone");
        alias(DELTA_UNITS, "timezone_hour", "timezone_h");
        alias(DELTA_UNITS, "timezone_minute", "timezone_m");

        alias(FIELD_UNITS, "dow", "dow");
        alias(FIELD_UNITS, "doy", "doy");
        alias(FIELD_UNITS, "isodow", "isodow");
        alias(FIELD_UNITS, "isoyear", "isoyear");
        alias(FIELD_UNITS, "julian", "j", "jd", "julian");
        for (String reserved : RESERVED_UNITS) FIELD_UNITS.put(reserved, reserved);
    }

    private static void alias(java.util.Map<String, String> table, String canonical, String... spellings) {
        for (String spelling : spellings) table.put(spelling, canonical);
    }

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
                if (fieldObj == null) return null;
                if (source instanceof Number && !(source instanceof Double)) {
                    throw new MemgresException("function date_part(unknown, integer) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                return extractDatePart(fieldObj.toString().toLowerCase(), source, name.equals("extract"));
            }
            case "date_trunc": {
                Object fieldObj = executor.evalExpr(fn.args().get(0), ctx);
                Object source = executor.evalExpr(fn.args().get(1), ctx);
                if (fieldObj == null) return null;
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
                // A field outside its range is the caller's mistake, reported as one rather than
                // as the internal fault java.time raises for it.
                if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || sec < 0 || sec >= 60) {
                    throw new MemgresException("time field value out of range: " + hour + ":"
                            + twoDigits(minute) + ":" + twoDigits(secs), "22008");
                }
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
                if (intervalObj == null || sourceObj == null || originObj == null) return null;
                PgInterval iv = TypeCoercion.toInterval(intervalObj);
                // A month is not a fixed number of microseconds, so there is no bin width to
                // count with; PG refuses the stride rather than picking a length for it.
                if (iv.isInfinite() || iv.getMonths() != 0) {
                    throw new MemgresException(
                            "timestamps cannot be binned into intervals containing months or years", "0A000");
                }
                long strideMicros = iv.getDays() * 24L * 3600 * 1_000_000 + iv.getMicroseconds();
                if (strideMicros <= 0) {
                    throw new MemgresException("stride must be greater than zero", "22008");
                }
                if (sourceObj instanceof java.time.OffsetDateTime) {
                    // A timestamptz bins on the instant line, so a zone with a non-hour offset
                    // cannot shift which bin a value lands in
                    java.time.Instant src = ((java.time.OffsetDateTime) sourceObj).toInstant();
                    java.time.Instant org = TypeCoercion.toOffsetDateTime(originObj).toInstant();
                    java.time.Instant binStart = org.plusNanos(
                            binOffsetMicros(microsBetween(org, src), strideMicros) * 1000);
                    return binStart.atZone(TypeCoercion.sessionZone()).toOffsetDateTime();
                }
                java.time.LocalDateTime source = TypeCoercion.toLocalDateTime(sourceObj);
                java.time.LocalDateTime origin = TypeCoercion.toLocalDateTime(originObj);
                return origin.plusNanos(
                        binOffsetMicros(microsBetween(origin, source), strideMicros) * 1000);
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
                String fmt = fmtObj.toString();
                // An empty format leaves a number with nothing to print, while the date/time
                // form of to_char returns NULL outright
                if (fmt.isEmpty()) return (source instanceof Number) ? "" : null;
                return formatToChar(source, fmt);
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

    /** Microseconds from {@code from} to {@code to}, without {@code Duration.toNanos}'s range. */
    private static long microsBetween(java.time.temporal.Temporal from, java.time.temporal.Temporal to) {
        java.time.Duration d = java.time.Duration.between(from, to);
        return d.getSeconds() * 1_000_000L + d.getNano() / 1000;
    }

    /** How far past the origin the bin containing an offset starts; floors below the origin too. */
    private static long binOffsetMicros(long offsetMicros, long strideMicros) {
        return Math.floorDiv(offsetMicros, strideMicros) * strideMicros;
    }

    /** The text shape memgres writes a timetz in, which is how it carries the offset around. */
    private static final java.util.regex.Pattern TIMETZ_TEXT =
            java.util.regex.Pattern.compile("\\d{1,2}:\\d{2}:\\d{2}(\\.\\d+)?([+-]\\d{2})(:?\\d{2})?");

    /** The timetz a value stands for, or null if it is not one. */
    private static java.time.OffsetTime asOffsetTime(Object source) {
        if (source instanceof java.time.OffsetTime) return (java.time.OffsetTime) source;
        if (!(source instanceof String)) return null;
        String s = ((String) source).trim();
        java.util.regex.Matcher m = TIMETZ_TEXT.matcher(s);
        if (!m.matches()) return null;
        // ISO_OFFSET_TIME wants a whole offset, where memgres writes a bare hour for a whole one
        if (m.group(3) == null) s = s + ":00";
        try {
            return java.time.OffsetTime.parse(s, java.time.format.DateTimeFormatter.ISO_OFFSET_TIME);
        } catch (java.time.format.DateTimeParseException e) {
            return null;
        }
    }

    /** The canonical unit a spelling names, or null when PostgreSQL would not recognise it. */
    private static String resolveUnit(java.util.Map<String, String> table, String unit) {
        return table.get(unit.length() > 10 ? unit.substring(0, 10) : unit);
    }

    /** The unit lookup the field-extracting functions do: the delta table, then the wider one. */
    private static String fieldUnit(String unit) {
        String canonical = resolveUnit(DELTA_UNITS, unit);
        return canonical != null ? canonical : resolveUnit(FIELD_UNITS, unit);
    }

    /** The unit is a word PostgreSQL knows, but this type has no such field. */
    private static MemgresException notSupported(String unit, String typeName) {
        return new MemgresException("unit \"" + unit + "\" not supported for type " + typeName, "0A000");
    }

    /** The unit is not a word PostgreSQL knows at all. */
    private static MemgresException notRecognized(String unit, String typeName) {
        return new MemgresException("unit \"" + unit + "\" not recognized for type " + typeName, "22023");
    }

    /** extract() answers a numeric of a fixed scale where date_part() answers a float8. */
    private static Object scaled(long micros, int scale, boolean extractForm) {
        java.math.BigDecimal value = java.math.BigDecimal.valueOf(micros, scale);
        return extractForm ? (Object) value : (Object) Double.valueOf(value.doubleValue());
    }

    private static Object whole(long value, boolean extractForm) {
        return extractForm ? (Object) java.math.BigDecimal.valueOf(value) : (Object) Double.valueOf(value);
    }

    /**
     * Fields of an infinite value: the ones that only ever grow answer infinity, the ones that
     * cycle have no answer at all. PG spells both the same way for numeric and float8.
     */
    private static final java.util.Set<String> MONOTONIC_UNITS = new java.util.HashSet<>(
            java.util.Arrays.asList("year", "decade", "century", "millennium", "isoyear",
                    "julian", "epoch"));

    /** As {@link #MONOTONIC_UNITS}, for an interval — which counts hours and days, not weeks. */
    private static final java.util.Set<String> MONOTONIC_INTERVAL_UNITS = new java.util.HashSet<>(
            java.util.Arrays.asList("year", "decade", "century", "millennium", "epoch",
                    "hour", "day"));

    private Object extractDatePart(String rawUnit, Object source, boolean extractForm) {
        if (source == null) return null;
        String unit = rawUnit.toLowerCase();
        if (source instanceof PgInterval) return intervalField(unit, (PgInterval) source, extractForm);
        if (source instanceof java.time.LocalTime) {
            return timeField(unit, (java.time.LocalTime) source, null, extractForm);
        }
        java.time.OffsetTime ot = asOffsetTime(source);
        if (ot != null) return timeField(unit, ot.toLocalTime(), ot.getOffset(), extractForm);
        // extract() has an entry point of its own for date, which refuses every sub-day unit;
        // date_part() has none, so a date reaches the timestamp code and answers zero for them.
        if (extractForm && source instanceof java.time.LocalDate) {
            return dateField(unit, (java.time.LocalDate) source);
        }
        return timestampField(unit, source, extractForm);
    }

    private Object timestampField(String unit, Object source, boolean extractForm) {
        boolean tzAware = source instanceof java.time.OffsetDateTime;
        String typeName = tzAware ? "timestamp with time zone" : "timestamp without time zone";
        String canonical = fieldUnit(unit);
        if (canonical == null) throw notRecognized(unit, typeName);

        java.time.LocalDateTime dt;
        int offsetSeconds = 0;
        if (source instanceof java.time.LocalDate) dt = ((java.time.LocalDate) source).atStartOfDay();
        else if (source instanceof java.time.LocalDateTime) dt = (java.time.LocalDateTime) source;
        else if (tzAware) {
            // PG resolves a timestamptz in the session zone before taking any field of it,
            // so the offset reported is the session's at that instant, not the literal's
            java.time.ZonedDateTime zoned = ((java.time.OffsetDateTime) source)
                    .atZoneSameInstant(TypeCoercion.sessionZone());
            dt = zoned.toLocalDateTime();
            offsetSeconds = zoned.getOffset().getTotalSeconds();
        } else dt = TypeCoercion.toLocalDateTime(source);

        if (dt.equals(TypeCoercion.TIMESTAMP_INFINITY) || dt.equals(TypeCoercion.TIMESTAMP_NEG_INFINITY)) {
            return infiniteField(canonical, MONOTONIC_UNITS, CYCLING_UNITS,
                    dt.equals(TypeCoercion.TIMESTAMP_INFINITY), unit, typeName);
        }

        long timeMicros = dt.toLocalTime().toNanoOfDay() / 1000;
        long secondMicros = timeMicros % 60_000_000L;
        if (canonical.equals("microsecond")) return whole(secondMicros, extractForm);
        if (canonical.equals("millisecond")) return scaled(secondMicros, 3, extractForm);
        if (canonical.equals("second")) return scaled(secondMicros, 6, extractForm);
        if (canonical.equals("minute")) return whole(dt.getMinute(), extractForm);
        if (canonical.equals("hour")) return whole(dt.getHour(), extractForm);
        if (canonical.equals("epoch")) {
            long epochSeconds = dt.toEpochSecond(java.time.ZoneOffset.ofTotalSeconds(offsetSeconds));
            return scaled(epochSeconds * 1_000_000L + dt.getNano() / 1000, 6, extractForm);
        }
        if (canonical.equals("julian")) return julian(dt.toLocalDate(), timeMicros, extractForm);
        if (canonical.equals("timezone")) {
            if (!tzAware) throw notSupported(unit, typeName);
            return whole(offsetSeconds, extractForm);
        }
        if (canonical.equals("timezone_hour")) {
            if (!tzAware) throw notSupported(unit, typeName);
            return whole(offsetSeconds / 3600, extractForm);
        }
        if (canonical.equals("timezone_minute")) {
            if (!tzAware) throw notSupported(unit, typeName);
            return whole((offsetSeconds % 3600) / 60, extractForm);
        }
        Long calendar = calendarField(canonical, dt.toLocalDate());
        if (calendar == null) throw notSupported(unit, typeName);
        return whole(calendar.longValue(), extractForm);
    }

    /** extract() over a date: PG's own function for it, which has no sub-day fields at all. */
    private Object dateField(String unit, java.time.LocalDate date) {
        String canonical = fieldUnit(unit);
        if (canonical == null) throw notRecognized(unit, "date");
        if (canonical.equals("epoch")) {
            return java.math.BigDecimal.valueOf(date.toEpochDay() * 86400L);
        }
        if (canonical.equals("julian")) {
            return java.math.BigDecimal.valueOf(
                    date.getLong(java.time.temporal.JulianFields.JULIAN_DAY));
        }
        Long calendar = calendarField(canonical, date);
        if (calendar == null) throw notSupported(unit, "date");
        return java.math.BigDecimal.valueOf(calendar.longValue());
    }

    /** The fields a calendar date can answer, or null when this unit is not one of them. */
    private static Long calendarField(String canonical, java.time.LocalDate date) {
        int year = date.getYear();
        switch (canonical) {
            // PG has no year zero, so a BC proleptic year reports one lower
            case "year": return (long) (year > 0 ? year : year - 1);
            case "month": return (long) date.getMonthValue();
            case "day": return (long) date.getDayOfMonth();
            case "week": return (long) date.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            case "quarter": return (long) ((date.getMonthValue() - 1) / 3 + 1);
            case "dow": return (long) (date.getDayOfWeek().getValue() % 7);
            case "doy": return (long) date.getDayOfYear();
            case "isodow": return (long) date.getDayOfWeek().getValue();
            case "isoyear": return (long) date.get(java.time.temporal.IsoFields.WEEK_BASED_YEAR);
            case "decade": return (long) (year / 10);
            case "century": return (long) (year > 0 ? (year - 1) / 100 + 1 : year / 100 - 1);
            case "millennium": return (long) (year > 0 ? (year - 1) / 1000 + 1 : year / 1000 - 1);
            default: return null;
        }
    }

    /**
     * The Julian day with the time of day as its fraction. PG divides in numeric for extract()
     * and in double precision for date_part().
     */
    private static Object julian(java.time.LocalDate date, long timeMicros, boolean extractForm) {
        long julianDay = date.getLong(java.time.temporal.JulianFields.JULIAN_DAY);
        if (!extractForm) {
            return Double.valueOf(julianDay + timeMicros / 86_400_000_000.0);
        }
        return java.math.BigDecimal.valueOf(julianDay).add(
                java.math.BigDecimal.valueOf(timeMicros).divide(
                        java.math.BigDecimal.valueOf(MICROS_PER_DAY),
                        numericDivScale(timeMicros, MICROS_PER_DAY), java.math.RoundingMode.HALF_UP));
    }

    private static final long MICROS_PER_DAY = 86_400_000_000L;

    /**
     * The scale PG's numeric division would choose: sixteen significant digits past the
     * estimated weight of the quotient, counted in the base-10000 digits numeric stores.
     */
    private static int numericDivScale(long numerator, long denominator) {
        int quotientWeight = base10000Weight(numerator) - base10000Weight(denominator);
        if (leadingGroup(numerator) <= leadingGroup(denominator)) quotientWeight--;
        return Math.max(0, Math.min(1000, 16 - quotientWeight * 4));
    }

    private static int base10000Weight(long value) {
        return value == 0 ? 0 : (Long.toString(Math.abs(value)).length() - 1) / 4;
    }

    private static long leadingGroup(long value) {
        long abs = Math.abs(value);
        for (int i = base10000Weight(value); i > 0; i--) abs /= 10000;
        return abs;
    }

    private Object timeField(String unit, java.time.LocalTime time,
                             java.time.ZoneOffset offset, boolean extractForm) {
        String typeName = offset == null ? "time without time zone" : "time with time zone";
        String canonical = fieldUnit(unit);
        if (canonical == null) throw notRecognized(unit, typeName);
        // A time answers no reserved word but epoch; the rest are not even the right kind of name
        if (RESERVED_UNITS.contains(canonical) && !canonical.equals("epoch")) {
            throw notRecognized(unit, typeName);
        }
        // The end-of-day value counts as a full day, so its hour is 24 rather than the 23 the
        // clock underneath it reads.
        long dayMicros = TypeCoercion.timeMicros(time);
        long secondMicros = dayMicros % 60_000_000L;
        long hourOfDay = dayMicros / 3_600_000_000L;
        long minuteOfHour = (dayMicros % 3_600_000_000L) / 60_000_000L;
        int offsetSeconds = offset == null ? 0 : offset.getTotalSeconds();
        switch (canonical) {
            case "microsecond": return whole(secondMicros, extractForm);
            case "millisecond": return scaled(secondMicros, 3, extractForm);
            case "second": return scaled(secondMicros, 6, extractForm);
            case "minute": return whole(minuteOfHour, extractForm);
            case "hour": return whole(hourOfDay, extractForm);
            // A timetz's epoch is its time of day taken back to UTC
            case "epoch": return scaled(dayMicros - offsetSeconds * 1_000_000L, 6, extractForm);
            case "timezone":
                if (offset == null) throw notSupported(unit, typeName);
                return whole(offsetSeconds, extractForm);
            case "timezone_hour":
                if (offset == null) throw notSupported(unit, typeName);
                return whole(offsetSeconds / 3600, extractForm);
            case "timezone_minute":
                if (offset == null) throw notSupported(unit, typeName);
                return whole((offsetSeconds % 3600) / 60, extractForm);
            default: throw notSupported(unit, typeName);
        }
    }

    private Object intervalField(String unit, PgInterval iv, boolean extractForm) {
        String canonical = fieldUnit(unit);
        if (canonical == null) throw notRecognized(unit, "interval");
        if (RESERVED_UNITS.contains(canonical) && !canonical.equals("epoch")) {
            throw notRecognized(unit, "interval");
        }
        if (iv.isInfinite()) {
            return infiniteField(canonical, MONOTONIC_INTERVAL_UNITS, CYCLING_INTERVAL_UNITS,
                    iv.isPositiveInfinity(), unit, "interval");
        }
        int months = iv.getMonths();
        long micros = iv.getMicroseconds();
        long secondMicros = micros % 60_000_000L;
        switch (canonical) {
            case "microsecond": return whole(secondMicros, extractForm);
            case "millisecond": return scaled(secondMicros, 3, extractForm);
            case "second": return scaled(secondMicros, 6, extractForm);
            case "minute": return whole((micros % 3_600_000_000L) / 60_000_000L, extractForm);
            case "hour": return whole(micros / 3_600_000_000L, extractForm);
            case "day": return whole(iv.getDays(), extractForm);
            case "week": return whole(iv.getDays() / 7, extractForm);
            case "month": return whole(months % 12, extractForm);
            // The quarter counts away from zero in both directions, so -5 months is quarter -2
            case "quarter": return whole(months % 12 / 3 + (months < 0 ? -1 : 1), extractForm);
            case "year": return whole(months / 12, extractForm);
            case "decade": return whole(months / 12 / 10, extractForm);
            case "century": return whole(months / 12 / 100, extractForm);
            case "millennium": return whole(months / 12 / 1000, extractForm);
            case "epoch": {
                // PG splits the months: whole years count 365.25 days each, the leftover
                // months 30 days each. Always reported with 6 fractional digits.
                long totalSecs = (long) (months / 12) * 31557600L
                        + (long) (months % 12) * 2592000L
                        + (long) iv.getDays() * 86400L;
                return scaled(totalSecs * 1_000_000L + micros, 6, extractForm);
            }
            default: throw notSupported(unit, "interval");
        }
    }

    private static Object infiniteField(String canonical, java.util.Set<String> monotonic,
                                        java.util.Set<String> cycling, boolean positive,
                                        String unit, String typeName) {
        if (monotonic.contains(canonical)) {
            return positive ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
        }
        // A cycling field of an infinite value has no answer; PG returns NULL rather than erroring
        if (cycling.contains(canonical)) return null;
        throw notSupported(unit, typeName);
    }

    /** Fields that repeat, and so say nothing about an infinite timestamp. */
    private static final java.util.Set<String> CYCLING_UNITS = new java.util.HashSet<>(
            java.util.Arrays.asList("microsecond", "millisecond", "second", "minute", "hour",
                    "day", "month", "quarter", "week", "dow", "isodow", "doy",
                    "timezone", "timezone_hour", "timezone_minute"));

    /** As {@link #CYCLING_UNITS}, for an interval — whose hours and days keep counting up. */
    private static final java.util.Set<String> CYCLING_INTERVAL_UNITS = new java.util.HashSet<>(
            java.util.Arrays.asList("microsecond", "millisecond", "second", "minute",
                    "month", "quarter", "week"));

    private Object truncateDate(String rawUnit, Object source, java.time.ZoneId zone) {
        if (source == null) return null;
        String unit = rawUnit.toLowerCase();
        // PG has no date_trunc over timetz at all, and reaches the interval form for time
        if (asOffsetTime(source) != null) {
            throw new MemgresException("function date_trunc(unknown, time with time zone) does not exist"
                    + "\n  Hint: No function matches the given name and argument types."
                    + " You might need to add explicit type casts.", "42883");
        }
        if (source instanceof java.time.LocalTime) {
            return truncateInterval(unit,
                    new PgInterval(0, 0, ((java.time.LocalTime) source).toNanoOfDay() / 1000));
        }
        // date_trunc(unit, interval) zeroes every field below the unit; there is no calendar
        // involved, so it is a separate rule from the timestamp one
        if (source instanceof PgInterval) return truncateInterval(unit, (PgInterval) source);
        // A date has no date_trunc of its own either; PG resolves it to the timestamptz form
        boolean fromDate = source instanceof java.time.LocalDate;
        if (fromDate) {
            java.time.LocalDate date = (java.time.LocalDate) source;
            if (date.equals(java.time.LocalDate.MAX) || date.equals(java.time.LocalDate.MIN)) return date;
            source = date.atStartOfDay(zone).toOffsetDateTime();
        }
        boolean tzAware = fromDate || source instanceof java.time.OffsetDateTime;
        String typeName = tzAware ? "timestamp with time zone" : "timestamp without time zone";
        String canonical = resolveUnit(DELTA_UNITS, unit);
        if (canonical == null) throw notRecognized(unit, typeName);

        java.time.LocalDateTime dt;
        if (source instanceof java.time.LocalDateTime) dt = (java.time.LocalDateTime) source;
        else if (source instanceof java.time.OffsetDateTime) {
            dt = ((java.time.OffsetDateTime) source).atZoneSameInstant(zone).toLocalDateTime();
        } else dt = TypeCoercion.toLocalDateTime(source);
        if (dt.equals(TypeCoercion.TIMESTAMP_INFINITY) || dt.equals(TypeCoercion.TIMESTAMP_NEG_INFINITY)) {
            return source;
        }

        java.time.LocalDateTime result;
        switch (canonical) {
            case "year":
                result = java.time.LocalDateTime.of(dt.getYear(), 1, 1, 0, 0);
                break;
            case "decade":
                result = java.time.LocalDateTime.of(Math.floorDiv(dt.getYear(), 10) * 10, 1, 1, 0, 0);
                break;
            case "century":
                // The first century runs 1..100, so 2026 truncates to 2001, not 2000
                result = java.time.LocalDateTime.of(centuryStart(dt.getYear(), 100), 1, 1, 0, 0);
                break;
            case "millennium":
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
                result = dt.withNano((dt.getNano() / 1_000_000) * 1_000_000);
                break;
            case "microsecond":
                result = dt.withNano((dt.getNano() / 1_000) * 1_000);
                break;
            default:
                throw notSupported(unit, typeName);
        }
        if (tzAware) return result.atZone(zone).toOffsetDateTime();
        return result;
    }

    /** The first year of the century/millennium containing {@code year} (PG counts from 1). */
    private static int centuryStart(int year, int width) {
        return Math.floorDiv(year - 1, width) * width + 1;
    }

    /** Zero every interval field smaller than the requested unit. */
    private Object truncateInterval(String unit, PgInterval iv) {
        String canonical = resolveUnit(DELTA_UNITS, unit);
        if (canonical == null) throw notRecognized(unit, "interval");
        if (iv.isInfinite()) return iv;
        int months = iv.getMonths();
        int days = iv.getDays();
        long micros = iv.getMicroseconds();
        switch (canonical) {
            case "millennium": months -= months % 12000; days = 0; micros = 0; break;
            case "century":    months -= months % 1200;  days = 0; micros = 0; break;
            case "decade":     months -= months % 120;   days = 0; micros = 0; break;
            case "year":       months -= months % 12;    days = 0; micros = 0; break;
            case "quarter":    months -= months % 3;     days = 0; micros = 0; break;
            case "month":      days = 0; micros = 0; break;
            case "day":        micros = 0; break;
            case "hour":       micros -= micros % 3_600_000_000L; break;
            case "minute":     micros -= micros % 60_000_000L; break;
            case "second":     micros -= micros % 1_000_000L; break;
            case "millisecond": micros -= micros % 1_000L; break;
            case "microsecond": break;
            default:
                throw notSupported(unit, "interval");
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
        if (s.isEmpty()) {
            // Nothing the format could read as a number was there. PG reports what it was left
            // holding, which is a blank, rather than quietly answering zero.
            throw new MemgresException("invalid input syntax for type numeric: \" \"", "22P02");
        }
        java.math.BigDecimal value;
        try {
            value = new java.math.BigDecimal(s);
        } catch (NumberFormatException e) {
            throw new MemgresException("invalid input syntax for type numeric: \"" + input + "\"", "22P02");
        }
        return negative ? value.negate() : value;
    }


    /** A clock field padded the way PostgreSQL writes it back in a range complaint. */
    private static String twoDigits(int value) {
        return value < 10 && value >= 0 ? "0" + value : String.valueOf(value);
    }

}
