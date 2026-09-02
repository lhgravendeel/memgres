package com.memgres.engine;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The zone abbreviations PostgreSQL knows, in both directions.
 *
 * <p>Two different questions are asked of an abbreviation and they have two different answers.
 * Read one — in {@code AT TIME ZONE 'EST'}, in a timestamptz literal, in a {@code TZ} template —
 * and it names a fixed displacement out of PostgreSQL's abbreviation table, which is a list of
 * names and offsets that has nothing to do with which zones are in force where. Write one — in a
 * {@code TZ} template, in {@code pg_timezone_names} — and it is the abbreviation the zone itself
 * carries at that moment, which changes with the season and which the zone database, not this
 * table, decides.
 */
final class PgTimeZones {

    private PgTimeZones() {
    }

    /** An abbreviation's displacement and whether the table marks it as a summer-time name. */
    static final class Abbrev {
        final int offsetSeconds;
        final boolean daylight;

        Abbrev(int offsetSeconds, boolean daylight) {
            this.offsetSeconds = offsetSeconds;
            this.daylight = daylight;
        }
    }

    /**
     * PostgreSQL's default abbreviation table, in the order the catalogue reports it. The names
     * overlap and contradict — IST is Israel here and India in the zone database, CST is the
     * American one and not the Chinese — because this is a fixed list PostgreSQL ships, not a
     * reading of the zones.
     */
    private static final Object[] TABLE = {
            "ACDT", 37800, true, "ACSST", 37800, true, "ACST", 34200, false, "ACT", -18000, false, "ACWST", 
            31500, false, "ADT", -10800, true, "AEDT", 39600, true, "AESST", 39600, true, "AEST", 36000, 
            false, "AFT", 16200, false, "AKDT", -28800, true, "AKST", -32400, false, "ALMST", 25200, true, 
            "ALMT", 21600, false, "AMST", 14400, false, "AMT", -14400, false, "ANAST", 43200, false, 
            "ANAT", 43200, false, "ARST", -10800, false, "ART", -10800, false, "AST", -14400, false, 
            "AWSST", 32400, true, "AWST", 28800, false, "AZOST", 0, true, "AZOT", -3600, false, "AZST", 
            14400, false, "AZT", 14400, false, "BDST", 7200, true, "BDT", 21600, false, "BNT", 28800, 
            false, "BORT", 28800, false, "BOT", -14400, false, "BRA", -10800, false, "BRST", -7200, true, 
            "BRT", -10800, false, "BST", 3600, true, "BTT", 21600, false, "CADT", 37800, true, "CAST", 
            34200, false, "CCT", 28800, false, "CDT", -18000, true, "CEST", 7200, true, "CET", 3600, false, 
            "CETDST", 7200, true, "CHADT", 49500, true, "CHAST", 45900, false, "CHUT", 36000, false, "CKT", 
            -36000, false, "CLST", -10800, true, "CLT", -14400, false, "COT", -18000, false, "CST", -21600, 
            false, "CXT", 25200, false, "DAVT", 25200, false, "DDUT", 36000, false, "EASST", -21600, false, 
            "EAST", -21600, false, "EAT", 10800, false, "EDT", -14400, true, "EEST", 10800, true, "EET", 
            7200, false, "EETDST", 10800, true, "EGST", 0, true, "EGT", -3600, false, "EST", -18000, false, 
            "FET", 10800, false, "FJST", 46800, true, "FJT", 43200, false, "FKST", -10800, false, "FKT", 
            -10800, false, "FNST", -3600, true, "FNT", -7200, false, "GALT", -21600, false, "GAMT", -32400, 
            false, "GEST", 14400, false, "GET", 14400, false, "GFT", -10800, false, "GILT", 43200, false, 
            "GMT", 0, false, "GYT", -14400, false, "HKT", 28800, false, "HST", -36000, false, "ICT", 25200, 
            false, "IDT", 10800, true, "IOT", 21600, false, "IRKST", 28800, false, "IRKT", 28800, false, 
            "IRT", 12600, false, "IST", 7200, false, "JAYT", 32400, false, "JST", 32400, false, "KDT", 
            36000, true, "KGST", 21600, true, "KGT", 21600, false, "KOST", 39600, false, "KRAST", 25200, 
            false, "KRAT", 25200, false, "KST", 32400, false, "LHDT", 37800, false, "LHST", 37800, false, 
            "LIGT", 36000, false, "LINT", 50400, false, "LKT", 19800, false, "MAGST", 39600, false, "MAGT", 
            39600, false, "MART", -34200, false, "MAWT", 18000, false, "MDT", -21600, true, "MEST", 7200, 
            true, "MESZ", 7200, true, "MET", 3600, false, "METDST", 7200, true, "MEZ", 3600, false, "MHT", 
            43200, false, "MMT", 23400, false, "MPT", 36000, false, "MSD", 14400, true, "MSK", 10800, 
            false, "MST", -25200, false, "MUST", 18000, true, "MUT", 14400, false, "MVT", 18000, false, 
            "MYT", 28800, false, "NDT", -9000, true, "NFT", -12600, false, "NOVST", 25200, false, "NOVT", 
            25200, false, "NPT", 20700, false, "NST", -12600, false, "NUT", -39600, false, "NZDT", 46800, 
            true, "NZST", 43200, false, "NZT", 43200, false, "OMSST", 21600, false, "OMST", 21600, false, 
            "PDT", -25200, true, "PET", -18000, false, "PETST", 43200, false, "PETT", 43200, false, "PGT", 
            36000, false, "PHT", 28800, false, "PKST", 21600, true, "PKT", 18000, false, "PMDT", -7200, 
            true, "PMST", -10800, false, "PONT", 39600, false, "PST", -28800, false, "PWT", 32400, false, 
            "PYST", -10800, true, "PYT", -10800, false, "RET", 14400, false, "SADT", 37800, true, "SAST", 
            7200, false, "SCT", 14400, false, "SGT", 28800, false, "TAHT", -36000, false, "TFT", 18000, 
            false, "TJT", 18000, false, "TKT", 46800, false, "TMT", 18000, false, "TOT", 46800, false, 
            "TRUT", 36000, false, "TVT", 43200, false, "UCT", 0, false, "ULAST", 32400, true, "ULAT", 
            28800, false, "UT", 0, false, "UTC", 0, false, "UYST", -7200, true, "UYT", -10800, false, 
            "UZST", 21600, true, "UZT", 18000, false, "VET", -14400, false, "VLAST", 36000, false, "VLAT", 
            36000, false, "VOLT", 10800, false, "VUT", 39600, false, "WADT", 28800, true, "WAKT", 43200, 
            false, "WAST", 25200, false, "WAT", 3600, false, "WDT", 32400, true, "WET", 0, false, "WETDST", 
            3600, true, "WFT", 43200, false, "WGST", -7200, true, "WGT", -10800, false, "XJT", 21600, 
            false, "YAKST", 32400, false, "YAKT", 32400, false, "YAPT", 36000, false, "YEKST", 21600, true, 
            "YEKT", 18000, false, "Z", 0, false, "ZULU", 0, false,
    };

    private static final Map<String, Abbrev> BY_NAME;
    private static final Map<String, String> CANONICAL;

    static {
        Map<String, Abbrev> byName = new LinkedHashMap<String, Abbrev>();
        Map<String, String> canonical = new HashMap<String, String>();
        for (int i = 0; i < TABLE.length; i += 3) {
            String name = (String) TABLE[i];
            byName.put(name, new Abbrev(((Integer) TABLE[i + 1]).intValue(),
                    ((Boolean) TABLE[i + 2]).booleanValue()));
            canonical.put(name.toUpperCase(Locale.ROOT), name);
        }
        BY_NAME = Collections.unmodifiableMap(byName);
        CANONICAL = Collections.unmodifiableMap(canonical);
    }

    /**
     * The zones PostgreSQL reports that Java does not offer as zone identifiers, and the rules
     * each of them follows. Java keeps the SystemV names PostgreSQL's zone database drops, so
     * those come off the list in the other direction.
     */
    private static final Map<String, String> EXTRA_ZONES;

    static {
        Map<String, String> extra = new LinkedHashMap<String, String>();
        extra.put("EST", "-05:00");
        extra.put("Factory", "Z");
        extra.put("GMT+0", "GMT");
        extra.put("GMT-0", "GMT");
        extra.put("HST", "-10:00");
        extra.put("MST", "-07:00");
        extra.put("ROC", "Asia/Taipei");
        EXTRA_ZONES = Collections.unmodifiableMap(extra);
    }

    /** The names the catalogue reports, in order. */
    static Map<String, Abbrev> table() {
        return BY_NAME;
    }

    /** Every zone name the catalogue reports, sorted. */
    static java.util.List<String> zoneNames() {
        java.util.List<String> names = new java.util.ArrayList<String>();
        for (String id : ZoneId.getAvailableZoneIds()) {
            if (!id.startsWith("SystemV/")) names.add(id);
        }
        for (String id : EXTRA_ZONES.keySet()) {
            if (!names.contains(id)) names.add(id);
        }
        Collections.sort(names);
        return names;
    }

    /** The rules a catalogue name stands for. */
    static ZoneId zoneFor(String name) {
        String follows = EXTRA_ZONES.get(name);
        return ZoneId.of(follows == null ? name : follows);
    }

    /**
     * The abbreviation the catalogue reports for one of its own names. Three of the names Java
     * does not carry are abbreviations in their own right, and Factory names no place at all, so
     * the zone database writes it as a displacement of nothing.
     */
    static String catalogAbbreviationOf(String name, Instant at) {
        if (name.equals("EST") || name.equals("HST") || name.equals("MST")) return name;
        if (name.equals("Factory")) return "-00";
        if (name.equals("GMT+0") || name.equals("GMT-0")) return "GMT";
        if (name.equals("ROC")) return "CST";
        return abbreviationOf(zoneFor(name), at);
    }

    /**
     * The abbreviation the session's zone writes at an instant.
     *
     * <p>Asked by the name the session was set to, so a zone whose name is all that distinguishes
     * it — EST resolves to the same fixed displacement a dozen other names do — still writes the
     * abbreviation PostgreSQL writes for it.
     */
    static String sessionAbbreviationAt(Instant at) {
        String name = TypeCoercion.rawSessionZoneName();
        if (name != null && EXTRA_ZONES.containsKey(name)) {
            return catalogAbbreviationOf(name, at);
        }
        return abbreviationOf(TypeCoercion.sessionZone(), at);
    }

    /**
     * The abbreviation table's entry for a name, matched without regard to case the way
     * PostgreSQL matches it, or null when the table does not carry the name.
     */
    static Abbrev lookup(String name) {
        if (name == null) return null;
        String canon = CANONICAL.get(name.trim().toUpperCase(Locale.ROOT));
        return canon == null ? null : BY_NAME.get(canon);
    }

    /** The displacement a name stands for, or null when the table does not carry the name. */
    static ZoneOffset offsetOf(String name) {
        Abbrev a = lookup(name);
        return a == null ? null : ZoneOffset.ofTotalSeconds(a.offsetSeconds);
    }

    /** The abbreviation a zone writes at an instant: its own name, or its displacement. */
    static String abbreviationOf(ZoneId zone, Instant at) {
        String written = PgZoneAbbreviations.at(zone, at);
        if (written != null) return written;
        return displacement(zone.getRules().getOffset(at));
    }

    /**
     * A displacement written the POSIX way, where a positive number is west of Greenwich: the
     * bare form {@code -05:30}, and the form with a name in front of it, {@code UTC+5}. The
     * shape is only accepted when the sign is written or the whole operand is a number, so the
     * zone names that carry a digit — {@code EST5EDT}, {@code MST7MDT} — are left to the zone
     * database, which is where PostgreSQL leaves them.
     */
    private static final java.util.regex.Pattern POSIX_DISPLACEMENT =
            java.util.regex.Pattern.compile(
                    "(?:[A-Za-z]+([+-])|([+-])?)(\\d{1,2})(?::(\\d{1,2}))?(?::(\\d{1,2}))?");

    /**
     * The zone an {@code AT TIME ZONE} operand names. The abbreviation table is asked first, so
     * {@code CET} is the fixed hour PostgreSQL's table says it is rather than the zone of that
     * name and its summer time; only then is the zone database asked.
     */
    static ZoneId zoneOperand(Object value) {
        if (value instanceof PgInterval) {
            PgInterval iv = (PgInterval) value;
            if (iv.getMonths() != 0 || iv.getDays() != 0) {
                throw new MemgresException("interval time zone \"" + iv
                        + "\" must not include months or days", "22023");
            }
            return ZoneOffset.ofTotalSeconds((int) (iv.getMicroseconds() / 1_000_000L));
        }
        String name = String.valueOf(value);
        ZoneOffset abbreviated = offsetOf(name);
        if (abbreviated != null) return abbreviated;
        java.util.regex.Matcher m = POSIX_DISPLACEMENT.matcher(name.trim());
        if (m.matches()) {
            String sign = m.group(1) != null ? m.group(1) : m.group(2);
            int secs = Integer.parseInt(m.group(3)) * 3600
                    + (m.group(4) == null ? 0 : Integer.parseInt(m.group(4)) * 60)
                    + (m.group(5) == null ? 0 : Integer.parseInt(m.group(5)));
            // West of Greenwich is the positive direction here and the negative one everywhere
            // else, so the displacement the operand names is the one it does not spell.
            return ZoneOffset.ofTotalSeconds("-".equals(sign) ? secs : -secs);
        }
        try {
            return zoneFor(name);
        } catch (RuntimeException e) {
            throw new MemgresException("time zone \"" + name + "\" not recognized", "22023");
        }
    }

    /** A displacement written the way the zone database writes one: +05, -0330, +1245. */
    static String displacement(ZoneOffset offset) {
        int secs = offset.getTotalSeconds();
        int abs = Math.abs(secs);
        StringBuilder sb = new StringBuilder(secs < 0 ? "-" : "+");
        int hours = abs / 3600;
        if (hours < 10) sb.append('0');
        sb.append(hours);
        int minutes = (abs % 3600) / 60;
        if (minutes != 0) {
            if (minutes < 10) sb.append('0');
            sb.append(minutes);
        }
        return sb.toString();
    }
}
