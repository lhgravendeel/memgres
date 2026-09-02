package com.memgres.replay;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

/**
 * Rewrite the baseline to the statements that are open right now.
 *
 * <p>The gate fails when a statement stops differing, so that a fix is recorded rather than
 * silently absorbed. This is how it gets recorded: run it after a fix, and the entries that now
 * answer as PostgreSQL does leave the list. It only ever writes what it measured, so it cannot
 * be used to hide a regression — the gate is run again afterwards and would say so.
 */
public final class ReplayRebaseline {
    private ReplayRebaseline() {}

    public static void main(String[] args) throws Exception {
        Path baseline = Paths.get("src/test/resources/replay-baseline.txt");
        Path dir = ReplayRunner.corpusDir();
        ReplayLedger ledger = ReplayRunner.run(dir, ReplayRunner.manifest(dir));
        Set<String> open = ReplayGateTest.identities(ledger);

        int before = 0;
        if (Files.exists(baseline)) {
            for (String line : Files.readAllLines(baseline, StandardCharsets.UTF_8)) {
                if (!line.trim().isEmpty() && !line.startsWith("#")) before++;
            }
        }
        Files.write(baseline, (String.join("\n", open) + "\n").getBytes(StandardCharsets.UTF_8));
        Files.write(Paths.get("target/replay-ledger.md"),
                ledger.render().getBytes(StandardCharsets.UTF_8));
        System.out.println("open " + open.size() + " (was " + before + "), agreeing "
                + ledger.agreed);
    }
}
