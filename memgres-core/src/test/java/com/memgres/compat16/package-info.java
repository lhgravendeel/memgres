/**
 * Compatibility tests that assert PostgreSQL 18 behavior on areas where
 * Memgres is known to differ. These tests are <b>expected to fail</b> on
 * Memgres and serve as a tracking mechanism for PG compatibility gaps.
 *
 * <h2>Test Results Summary (as of creation)</h2>
 *
 * <h3>Tests that FAIL (confirming real Memgres gaps):</h3>
 * <ul>
 *   <li>{@code SystemColumnCompatTest} (7 errors) - xmin/xmax/cmin/cmax/ctid not implemented</li>
 *   <li>{@code CollationOrderCompatTest} (4 failures) - COLLATE has no effect on sort order</li>
 *   <li>{@code SerializableWriteSkewTest} (2 failures) - no SSI write-skew detection</li>
 *   <li>{@code RepeatableReadCrossTableTest} (1 failure) - per-table snapshots, not per-transaction</li>
 *   <li>{@code SetParameterStrictnessTest} (6 failures) - unknown SET params silently accepted</li>
 *   <li>{@code IdentifierLengthCompatTest} (4 failures) - no 63-char truncation</li>
 *   <li>{@code SequenceCacheCompatTest} (2 failures) - CACHE parsed but not implemented</li>
 *   <li>{@code TwoPhaseCommitCompatTest} (2 failures) - PREPARE TRANSACTION is no-op</li>
 *   <li>{@code PgStatViewsCompatTest} (6 failures/errors) - stat views are empty stubs</li>
 *   <li>{@code CustomOperatorDispatchTest} (1 error) - unary prefix operator parsing</li>
 *   <li>{@code PlpgsqlMissingFeaturesTest} (1 error) - FOREACH SLICE not parsed</li>
 *   <li>{@code ErrorDetailCompatTest} (2 failures) - missing DETAIL/key info in constraint errors</li>
 * </ul>
 *
 * <h3>Tests that PASS (gaps already fixed or analysis was wrong):</h3>
 * <ul>
 *   <li>{@code WindowFrameExcludeTest} (5/5 pass) - EXCLUDE clause works</li>
 *   <li>{@code ReturningOldNewTest} (7/7 pass) - RETURNING OLD/NEW works</li>
 *   <li>{@code InfoSchemaCatalogNameTest} (4/4 pass) - catalog name matches current_database()</li>
 * </ul>
 *
 * <h2>Corresponding SQL verification files</h2>
 * <p>See {@code src/test/resources/feature-comparison/compat-*.sql} for annotated SQL
 * files covering the same gaps in a format runnable against both PG 18 and Memgres.</p>
 *
 * <h2>Gaps that cannot be unit-tested</h2>
 * <p>See {@code UNTESTABLE_GAPS.md} in this package for gaps that cannot be
 * expressed as automated tests and the reasons why.</p>
 */
package com.memgres.compat16;
