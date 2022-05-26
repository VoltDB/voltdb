/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.dr2.conflicts;

import com.google_voltpatches.common.collect.Lists;
import org.junit.Test;
import org.voltdb.PartitionDRGateway;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class DRConflictsStatsWithTrackerTest {

    public static final String HOST_NAME = "";
    public static final int HOST_ID = 0;

    public static final long NOW_MILLIS = 1024L;
    public static final long NOW_MICROS = NOW_MILLIS * 1000;
    public static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(NOW_MILLIS), ZoneId.systemDefault());

    public static final int CLUSTER_ID = 0;
    public static final int REMOTE_CLUSTER_ID = 1;
    public static final int PARTITION_ID = 2;
    public static final String TABLE_NAME = "TABLE";

    @Test
    public void shouldMarkMissingRowConflict() {
        // Given
        DRConflictsTracker drConflictsTracker = new DRConflictsTracker(CLOCK);
        DRConflictsStats drConflictsStats = new DRConflictsStats(drConflictsTracker, CLUSTER_ID);

        // When
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING,
                REMOTE_CLUSTER_ID,
                PARTITION_ID,
                "TABLE",
                false,
                false);
        Object[][] actual = drConflictsStats.getStatsRows(false, NOW_MILLIS);

        // Then
        assertArrayEquals(
                createStatsWithSingleMetricsRow(1, 0, 1, 0, 0),
                actual
        );
    }

    @Test
    public void shouldMarkTimestampMismatchConflict() {
        // Given
        DRConflictsTracker drConflictsTracker = new DRConflictsTracker(CLOCK);
        DRConflictsStats drConflictsStats = new DRConflictsStats(drConflictsTracker, CLUSTER_ID);

        // When
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.EXPECTED_ROW_TIMESTAMP_MISMATCH,
                REMOTE_CLUSTER_ID,
                PARTITION_ID,
                "TABLE",
                false,
                false);
        Object[][] actual = drConflictsStats.getStatsRows(false, NOW_MILLIS);

        // Then
        assertArrayEquals(
                createStatsWithSingleMetricsRow(1, 0, 0, 1, 0),
                actual
        );
    }

    @Test
    public void shouldMarkConstraintViolationConflict() {
        // Given
        DRConflictsTracker drConflictsTracker = new DRConflictsTracker(CLOCK);
        DRConflictsStats drConflictsStats = new DRConflictsStats(drConflictsTracker, CLUSTER_ID);

        // When
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.EXPECTED_ROW_TIMESTAMP_MISMATCH,
                REMOTE_CLUSTER_ID,
                PARTITION_ID,
                "TABLE",
                false,
                false);
        Object[][] actual = drConflictsStats.getStatsRows(false, NOW_MILLIS);

        // Then
        assertArrayEquals(
                createStatsWithSingleMetricsRow(1, 0, 0, 1, 0),
                actual
        );
    }

    @Test
    public void shouldMarkConflictThatIsDivergent() {
        // Given
        DRConflictsTracker drConflictsTracker = new DRConflictsTracker(CLOCK);
        DRConflictsStats drConflictsStats = new DRConflictsStats(drConflictsTracker, CLUSTER_ID);

        // When
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING,
                REMOTE_CLUSTER_ID,
                PARTITION_ID,
                "TABLE",
                true,
                false);
        Object[][] actual = drConflictsStats.getStatsRows(false, NOW_MILLIS);

        // Then
        assertArrayEquals(
                createStatsWithSingleMetricsRow(1, 1, 1, 0, 0),
                actual
        );
    }

    @Test
    public void shouldMarkTheSameConflictTwice() {
        // Given
        DRConflictsTracker drConflictsTracker = new DRConflictsTracker(CLOCK);
        DRConflictsStats drConflictsStats = new DRConflictsStats(drConflictsTracker, CLUSTER_ID);

        // When
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING,
                REMOTE_CLUSTER_ID,
                PARTITION_ID,
                "TABLE",
                false,
                false);
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING,
                REMOTE_CLUSTER_ID,
                PARTITION_ID,
                "TABLE",
                false,
                false);
        Object[][] actual = drConflictsStats.getStatsRows(false, NOW_MILLIS);

        // Then
        assertArrayEquals(
                createStatsWithSingleMetricsRow(2, 0, 2, 0, 0),
                actual
        );
    }

    @Test
    public void shouldMarkDifferentConflictsForOneKey() {
        // Given
        DRConflictsTracker drConflictsTracker = new DRConflictsTracker(CLOCK);
        DRConflictsStats drConflictsStats = new DRConflictsStats(drConflictsTracker, CLUSTER_ID);

        // When
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING,
                REMOTE_CLUSTER_ID,
                PARTITION_ID,
                "TABLE",
                false,
                false);
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.EXPECTED_ROW_TIMESTAMP_MISMATCH,
                REMOTE_CLUSTER_ID,
                PARTITION_ID,
                "TABLE",
                false,
                false);
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.CONSTRAINT_VIOLATION,
                REMOTE_CLUSTER_ID,
                PARTITION_ID,
                "TABLE",
                false,
                false);
        Object[][] actual = drConflictsStats.getStatsRows(false, NOW_MILLIS);

        // Then
        assertArrayEquals(
                createStatsWithSingleMetricsRow(3, 0, 1, 1, 1),
                actual
        );
    }

    @Test
    public void shouldMarkConflictsOnDifferentKeys() {
        // Given
        DRConflictsTracker drConflictsTracker = new DRConflictsTracker(CLOCK);
        DRConflictsStats drConflictsStats = new DRConflictsStats(drConflictsTracker, CLUSTER_ID);

        // When
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING,
                1,
                10,
                "TABLE_1",
                false,
                false);
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING,
                2,
                20,
                "TABLE_2",
                false,
                false);
        Object[][] actual = drConflictsStats.getStatsRows(false, NOW_MILLIS);

        // Then
        assertEquals(actual.length, 2);
        assertThat(
                Lists.newArrayList(actual),
                containsInAnyOrder(
                        createMetricsRow(CLUSTER_ID, 1, 10, "TABLE_1", 1, 0, 1, 0, 0),
                        createMetricsRow(CLUSTER_ID, 2, 20, "TABLE_2", 1, 0, 1, 0, 0)
                )
        );
    }

    @Test
    public void shouldResetIntervalStats() {
        // Given
        DRConflictsTracker drConflictsTracker = new DRConflictsTracker(CLOCK);
        DRConflictsStats drConflictsStats = new DRConflictsStats(drConflictsTracker, CLUSTER_ID);

        // When
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING,
                REMOTE_CLUSTER_ID,
                PARTITION_ID,
                "TABLE",
                false,
                false);
        Object[][] actual = drConflictsStats.getStatsRows(true, NOW_MILLIS);

        // Then
        assertArrayEquals(
                createStatsWithSingleMetricsRow(1, 0, 1, 0, 0),
                actual
        );

        // When
        actual = drConflictsStats.getStatsRows(true, NOW_MILLIS);


        // Then
        assertEquals(actual.length, 0);
    }

    @Test
    public void shouldNotResetIntervalStatsWhenYouCallTotalStats() {
        // Given
        DRConflictsTracker drConflictsTracker = new DRConflictsTracker(CLOCK);
        DRConflictsStats drConflictsStats = new DRConflictsStats(drConflictsTracker, CLUSTER_ID);

        // When
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING,
                REMOTE_CLUSTER_ID,
                PARTITION_ID,
                "TABLE",
                false,
                false);
        Object[][] actual = drConflictsStats.getStatsRows(false, NOW_MILLIS);

        // Then
        assertArrayEquals(
                createStatsWithSingleMetricsRow(1, 0, 1, 0, 0),
                actual
        );

        // When
        actual = drConflictsStats.getStatsRows(true, NOW_MILLIS);


        // Then
        assertArrayEquals(
                createStatsWithSingleMetricsRow(1, 0, 1, 0, 0),
                actual
        );
    }

    @Test
    public void shouldReturnCorrectTotalStatsAfterCallingIntervalStats() {
        // Given
        DRConflictsTracker drConflictsTracker = new DRConflictsTracker(CLOCK);
        DRConflictsStats drConflictsStats = new DRConflictsStats(drConflictsTracker, CLUSTER_ID);

        // When
        drConflictsTracker.markConflict(
                PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING,
                REMOTE_CLUSTER_ID,
                PARTITION_ID,
                "TABLE",
                false,
                false);
        Object[][] actual = drConflictsStats.getStatsRows(true, NOW_MILLIS);

        // Then
        assertArrayEquals(
                createStatsWithSingleMetricsRow(1, 0, 1, 0, 0),
                actual
        );

        // When
        actual = drConflictsStats.getStatsRows(false, NOW_MILLIS);


        // Then
        assertArrayEquals(
                createStatsWithSingleMetricsRow(1, 0, 1, 0, 0),
                actual
        );
    }

    private Object[][] createStatsWithSingleMetricsRow(long totalConflictCount,
                                                       long divergenceCount,
                                                       long missingRowCount,
                                                       long timestampMismatchCount,
                                                       long constraintViolationCount) {
        return new Object[][]{
                createMetricsRow(
                        totalConflictCount,
                        divergenceCount,
                        missingRowCount,
                        timestampMismatchCount,
                        constraintViolationCount
                )
        };
    }

    private Object[] createMetricsRow(int clusterId,
                                      int remoteClusterId,
                                      int partitionId,
                                      String tableName,
                                      long totalConflictCount,
                                      long divergenceCount,
                                      long missingRowCount,
                                      long timestampMismatchCount,
                                      long constraintViolationCount) {
        return new Object[]{
                NOW_MILLIS,
                HOST_ID,
                HOST_NAME,
                clusterId,
                remoteClusterId,
                partitionId,
                tableName,
                NOW_MICROS,
                totalConflictCount,
                divergenceCount,
                missingRowCount,
                timestampMismatchCount,
                constraintViolationCount
        };
    }

    private Object[] createMetricsRow(long totalConflictCount,
                                      long divergenceCount,
                                      long missingRowCount,
                                      long timestampMismatchCount,
                                      long constraintViolationCount) {
        return createMetricsRow(
                CLUSTER_ID,
                REMOTE_CLUSTER_ID,
                PARTITION_ID,
                TABLE_NAME,
                totalConflictCount,
                divergenceCount,
                missingRowCount,
                timestampMismatchCount,
                constraintViolationCount
        );
    }
}