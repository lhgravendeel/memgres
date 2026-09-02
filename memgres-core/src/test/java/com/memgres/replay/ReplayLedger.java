package com.memgres.replay;

import java.util.*;

/** Every statement of the replay corpus that memgres does not yet answer as PostgreSQL does. */
public final class ReplayLedger {

    public static final class Entry {
        public final String script;
        public final String finding;
        public final String sql;
        public final String expected;
        public final String actual;

        Entry(String script, String finding, String sql, String expected, String actual) {
            this.script = script;
            this.finding = finding;
            this.sql = sql;
            this.expected = expected;
            this.actual = actual;
        }
    }

    public final List<Entry> open = new ArrayList<>();
    public int agreed;
    public int allowed;
    public int unrunnable;
    public int scripts;

    void agree() { agreed++; }
    void allow() { allowed++; }

    void differ(String script, String finding, String sql, String expected, String actual) {
        open.add(new Entry(script, finding, sql, expected, actual));
    }

    public int total() { return agreed + allowed + open.size(); }

    /** The ledger written out, newest measurement first, for a reader and for review. */
    public String render() {
        StringBuilder b = new StringBuilder();
        b.append("# Replay ledger\n\n");
        b.append("Every reproducer in `investigation.md`, `investigation-2026-08.md` and\n");
        b.append("`review-2026-08.md`, replayed statement by statement against the answers\n");
        b.append("PostgreSQL 18 gave for them.\n\n");
        b.append("| | |\n|---|---:|\n");
        b.append("| scripts | ").append(scripts).append(" |\n");
        b.append("| statements replayed | ").append(total()).append(" |\n");
        b.append("| agreeing | ").append(agreed).append(" |\n");
        b.append("| open | ").append(open.size()).append(" |\n");
        b.append("| written divergences | ").append(allowed).append(" |\n");
        b.append("| scripts the report wrote abbreviated | ").append(unrunnable).append(" |\n\n");

        Map<String, List<Entry>> byScript = new LinkedHashMap<>();
        for (Entry e : open) {
            byScript.computeIfAbsent(e.script, k -> new ArrayList<>()).add(e);
        }
        b.append("## Open — ").append(open.size()).append(" statements across ")
                .append(byScript.size()).append(" scripts\n\n");
        for (Map.Entry<String, List<Entry>> e : byScript.entrySet()) {
            b.append("### ").append(e.getKey()).append("  (")
                    .append(e.getValue().size()).append(")\n\n");
            for (Entry x : e.getValue()) {
                b.append("- `").append(oneLine(x.sql)).append("`\n");
                b.append("  - PG:      ").append(oneLine(x.expected)).append('\n');
                b.append("  - memgres: ").append(oneLine(x.actual)).append('\n');
            }
            b.append('\n');
        }
        return b.toString();
    }

    private static String oneLine(String s) {
        String t = s == null ? "" : s.replace('\n', ' ').replaceAll("\\s+", " ").trim();
        return t.length() > 300 ? t.substring(0, 300) + " …" : t;
    }
}
