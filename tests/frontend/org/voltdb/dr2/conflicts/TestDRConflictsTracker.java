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

import org.junit.Test;
import org.voltdb.PartitionDRGateway;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestDRConflictsTracker {

    private static final int REMOTE_CLUSTER_ID = 1;
    private static final int PARTITION_ID = 2;
    private static final String TABLE_NAME = "TABLE_NAME";

    private static long NOW_MILLIS = 10_000;
    private static long NOW_MICRO = NOW_MILLIS * 1000;
    public static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(NOW_MILLIS), ZoneId.systemDefault());

    @Test
    public void shouldNotCountNoConflictType() {
        // Given
        DRConflictsTracker tracker = new DRConflictsTracker(CLOCK);

        // When
        tracker.markConflict(PartitionDRGateway.DRConflictType.NO_CONFLICT, REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME, false, false);

        // Then
        assertEquals(0, tracker.getTotalMetricsSnapshot().size());
    }

    @Test
    public void shouldNotCountOnReplicatedTableWithPartitionDifferentThanZero() {
        // Given
        DRConflictsTracker tracker = new DRConflictsTracker(CLOCK);

        // When
        tracker.markConflict(PartitionDRGateway.DRConflictType.NO_CONFLICT, REMOTE_CLUSTER_ID, 1, TABLE_NAME, true, false);

        // Then
        assertEquals(0, tracker.getTotalMetricsSnapshot().size());
    }

    @Test
    public void shouldCountReplicatedTableWithPartition0() {
        // Given
        DRConflictsTracker tracker = new DRConflictsTracker(CLOCK);

        // When
        tracker.markConflict(PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING, REMOTE_CLUSTER_ID, 0, TABLE_NAME, true, false);

        // Then
        Map<DRConflictsMetricKey, DRConflictsMetricValue> snapshot = tracker.getTotalMetricsSnapshot();
        assertEquals(1, snapshot.size());
    }

    @Test
    public void shouldCountExpectedRowMissingConflict() {
        // Given
        DRConflictsTracker tracker = new DRConflictsTracker(CLOCK);

        // When
        tracker.markConflict(PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING, REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME, false, false);

        // Then
        Map<DRConflictsMetricKey, DRConflictsMetricValue> snapshot = tracker.getTotalMetricsSnapshot();
        assertEquals(1, snapshot.size());
        assertEquals(
                new DRConflictsMetricValue(NOW_MICRO, 1, 0, 1, 0, 0),
                snapshot.get(new DRConflictsMetricKey(REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME))
        );
    }

    @Test
    public void shouldCountConstraintViolationConflict() {
        // Given
        DRConflictsTracker tracker = new DRConflictsTracker(CLOCK);

        // When
        tracker.markConflict(PartitionDRGateway.DRConflictType.CONSTRAINT_VIOLATION, REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME, false, false);

        // Then
        Map<DRConflictsMetricKey, DRConflictsMetricValue> snapshot = tracker.getTotalMetricsSnapshot();
        assertEquals(1, snapshot.size());
        assertEquals(
                new DRConflictsMetricValue(NOW_MICRO, 1, 0, 0, 0, 1),
                snapshot.get(new DRConflictsMetricKey(REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME))
        );
    }

    @Test
    public void shouldCountExpectedRowTimestampMismatchConflict() {
        // Given
        DRConflictsTracker tracker = new DRConflictsTracker(CLOCK);

        // When
        tracker.markConflict(PartitionDRGateway.DRConflictType.EXPECTED_ROW_TIMESTAMP_MISMATCH, REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME, false, false);

        // Then
        Map<DRConflictsMetricKey, DRConflictsMetricValue> snapshot = tracker.getTotalMetricsSnapshot();
        assertEquals(1, snapshot.size());
        assertEquals(
                new DRConflictsMetricValue(NOW_MICRO, 1, 0, 0, 1, 0),
                snapshot.get(new DRConflictsMetricKey(REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME))
        );
    }

    @Test
    public void shouldCountConflictThatIsDivergent() {
        // Given
        DRConflictsTracker tracker = new DRConflictsTracker(CLOCK);

        // When
        tracker.markConflict(PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING, REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME, true, false);

        // Then
        Map<DRConflictsMetricKey, DRConflictsMetricValue> snapshot = tracker.getTotalMetricsSnapshot();
        assertEquals(1, snapshot.size());
        assertEquals(
                new DRConflictsMetricValue(NOW_MICRO, 1, 1, 1, 0, 0),
                snapshot.get(new DRConflictsMetricKey(REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME))
        );
    }

    @Test
    public void shouldCountMultipleConflictForOneKey() {
        // Given
        DRConflictsTracker tracker = new DRConflictsTracker(CLOCK);

        // When
        tracker.markConflict(PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING, REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME, false, false);
        tracker.markConflict(PartitionDRGateway.DRConflictType.CONSTRAINT_VIOLATION, REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME, false, false);
        tracker.markConflict(PartitionDRGateway.DRConflictType.EXPECTED_ROW_TIMESTAMP_MISMATCH, REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME, false, false);

        // Then
        Map<DRConflictsMetricKey, DRConflictsMetricValue> snapshot = tracker.getTotalMetricsSnapshot();
        assertEquals(1, snapshot.size());
        assertEquals(
                new DRConflictsMetricValue(NOW_MICRO, 3, 0, 1, 1, 1),
                snapshot.get(new DRConflictsMetricKey(REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME))
        );
    }

    @Test
    public void shouldMarkConflictsOnDifferentKeys() {
        // Given
        DRConflictsTracker tracker = new DRConflictsTracker(CLOCK);

        // When
        tracker.markConflict(PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING, 0, 0, "TABLE_0", false, false);
        tracker.markConflict(PartitionDRGateway.DRConflictType.CONSTRAINT_VIOLATION, 1, 1, "TABLE_1", false, false);

        // Then
        Map<DRConflictsMetricKey, DRConflictsMetricValue> snapshot = tracker.getTotalMetricsSnapshot();
        assertEquals(2, snapshot.size());
        assertEquals(
                new DRConflictsMetricValue(NOW_MICRO, 1, 0, 1, 0, 0),
                snapshot.get(new DRConflictsMetricKey(0, 0, "TABLE_0"))
        );
        assertEquals(
                new DRConflictsMetricValue(NOW_MICRO, 1, 0, 0, 0, 1),
                snapshot.get(new DRConflictsMetricKey(1 , 1, "TABLE_1"))
        );
    }

    @Test
    public void shouldGetCorrectValuesFromLastSnapshot() {
        // Given
        DRConflictsTracker tracker = new DRConflictsTracker(CLOCK);

        // When
        tracker.markConflict(PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING, REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME, false, false);

        // Then
        Map<DRConflictsMetricKey, DRConflictsMetricValue> snapshot = tracker.getLastMetricsSnapshot();
        assertEquals(1, snapshot.size());
        assertEquals(
                new DRConflictsMetricValue(NOW_MICRO, 1, 0, 1, 0, 0),
                snapshot.get(new DRConflictsMetricKey(REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME))
        );
    }

    @Test
    public void shouldNotResetTotalMetricsSnapshotAfterGettingLastMetricsSnapshot() {
        // Given
        DRConflictsTracker tracker = new DRConflictsTracker(CLOCK);

        // When
        tracker.markConflict(PartitionDRGateway.DRConflictType.EXPECTED_ROW_MISSING, REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME, false, false);
        tracker.getLastMetricsSnapshot();

        // Then
        Map<DRConflictsMetricKey, DRConflictsMetricValue> snapshot = tracker.getTotalMetricsSnapshot();
        assertEquals(1, snapshot.size());
        assertEquals(
                new DRConflictsMetricValue(NOW_MICRO, 1, 0, 1, 0, 0),
                snapshot.get(new DRConflictsMetricKey(REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME))
        );
    }

    @Test
    public void shouldResetLatestSnapshot() {
        // Given
        DRConflictsTracker tracker = new DRConflictsTracker(CLOCK);
        tracker.markConflict(PartitionDRGateway.DRConflictType.EXPECTED_ROW_TIMESTAMP_MISMATCH, REMOTE_CLUSTER_ID, PARTITION_ID, TABLE_NAME, false, false);

        // When & Then
        assertEquals(1, tracker.getLastMetricsSnapshot().size());
        assertEquals(0, tracker.getLastMetricsSnapshot().size());
    }
}
