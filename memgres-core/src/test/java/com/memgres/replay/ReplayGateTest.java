package com.memgres.replay;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The gate: every reproducer the investigation reports recorded, replayed against the answers
 * PostgreSQL 18 gave for them.
 *
 * <p>The August plan closed ten branches and left more than half of these reproducers still
 * disagreeing. It could, because the thing it was measured against was written to assert what had
 * just been fixed — a corpus like that can only ever agree with itself. This one is written from
 * the reports instead, so it says what is left rather than what was done.
 *
 * <p>The baseline file lists the statements known to differ. A statement that starts differing
 * fails the build; a statement that stops differing fails it too, with the instruction to take it
 * out of the baseline, so the list can only ever get shorter.
 *
 * <p>No PostgreSQL is needed to run this: the answers are recorded in the scripts. Re-record them
 * with {@code tools/replay/annotate.sh} when the reference server is upgraded.
 */
class ReplayGateTest {

    private static final Path BASELINE =
            Paths.get("src/test/resources/replay-baseline.txt");

    @Test
    void everyRecordedReproducerAnswersAsPostgresOrIsAKnownGap() throws Exception {
        Path dir = ReplayRunner.corpusDir();
        ReplayLedger ledger = ReplayRunner.run(dir, ReplayRunner.manifest(dir));

        Set<String> nowOpen = identities(ledger);
        Set<String> wasOpen = new TreeSet<>(Files.readAllLines(BASELINE, StandardCharsets.UTF_8));
        wasOpen.removeIf(l -> l.trim().isEmpty() || l.startsWith("#"));

        Set<String> regressed = new TreeSet<>(nowOpen);
        regressed.removeAll(wasOpen);
        Set<String> fixed = new TreeSet<>(wasOpen);
        fixed.removeAll(nowOpen);

        // Written before the assertions, so a failing run still leaves a readable ledger behind.
        Files.write(Paths.get("target/replay-ledger.md"),
                ledger.render().getBytes(StandardCharsets.UTF_8));

        assertTrue(regressed.isEmpty(),
                regressed.size() + " statement(s) that used to answer as PostgreSQL no longer do."
                        + " See target/replay-ledger.md.\n  "
                        + String.join("\n  ", head(regressed, 20)));

        assertTrue(fixed.isEmpty(),
                fixed.size() + " statement(s) now answer as PostgreSQL does. Take them out of "
                        + BASELINE + " so the gate keeps them closed.\n  "
                        + String.join("\n  ", head(fixed, 20)));

        assertEquals(0, ledger.scripts - ledger.unrunnable - runnableScripts(ledger),
                "every script is either replayed or recorded as unrunnable");
    }

    private static int runnableScripts(ReplayLedger l) {
        return l.scripts - l.unrunnable;
    }

    private static List<String> head(Set<String> s, int n) {
        List<String> out = new ArrayList<>();
        for (String x : s) {
            if (out.size() >= n) { out.add("… and " + (s.size() - n) + " more"); break; }
            out.add(x);
        }
        return out;
    }

    /**
     * The baseline name of every open statement.
     *
     * <p>A script may hold the same statement more than once — a reproducer often reads a table
     * before and after a change — and the two are not interchangeable, so each occurrence is
     * counted. Keyed on the text alone, fixing the second occurrence would have looked like
     * fixing the first.
     */
    static Set<String> identities(ReplayLedger ledger) {
        Map<String, Integer> seen = new HashMap<>();
        Set<String> out = new TreeSet<>();
        for (ReplayLedger.Entry e : ledger.open) {
            String base = identity(e.script, e.sql);
            int n = seen.merge(base, 1, Integer::sum);
            out.add(n == 1 ? base : base + "#" + n);
        }
        return out;
    }

    /**
     * What names a statement in the baseline: the script it is in and a digest of the statement
     * itself, so editing an unrelated line of the script does not renumber every entry after it.
     */
    static String identity(String script, String sql) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] d = md.digest(sql.replaceAll("\\s+", " ").trim().getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (int i = 0; i < 6; i++) hex.append(String.format("%02x", d[i]));
            return script + " " + hex;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
