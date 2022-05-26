/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.dr2.conflicts;

import org.voltdb.PartitionDRGateway;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DRConflictsTracker {

    private final Clock m_clock;

    private final ConcurrentHashMap<DRConflictsMetricKey, DRConflictsMetricInternalValue> m_counters = new ConcurrentHashMap<>();

    public DRConflictsTracker(Clock clock) {
        this.m_clock = clock;
    }

    public void markConflict(PartitionDRGateway.DRConflictType conflictType,
                             int remoteClusterId,
                             int partitionId,
                             String tableName,
                             boolean isResolutionDivergent,
                             boolean isReplicatedTable) {
        // Replicated tables save conflict log files only on a node with partition 0.
        // We mimic that behavior, so conflict log files and metrics are consistent.
        if (isReplicatedTable && partitionId != 0) {
            return;
        }

        if (conflictType == PartitionDRGateway.DRConflictType.NO_CONFLICT) {
            return;
        }

        m_counters.compute(
                new DRConflictsMetricKey(remoteClusterId, partitionId, tableName),
                (key, value) -> updateConflictsCount(conflictType, value, isResolutionDivergent)
        );
    }

    public Map<DRConflictsMetricKey, DRConflictsMetricValue> getLastMetricsSnapshot() {
        Map<DRConflictsMetricKey, DRConflictsMetricValue> snapshot = new HashMap<>();
        m_counters.forEach((key, value) -> {
            if (value.lastConflictsCount > 0) {
                snapshot.put(key, value.toLastDRConflictsMetricValue());
                value.reset();
            }
        });
        return snapshot;
    }

    public Map<DRConflictsMetricKey, DRConflictsMetricValue> getTotalMetricsSnapshot() {
        Map<DRConflictsMetricKey, DRConflictsMetricValue> snapshot = new HashMap<>();
        m_counters.forEach((key, value) -> {
            snapshot.put(key, value.toTotalDRConflictsMetricValue());
        });
        return snapshot;
    }

    private DRConflictsMetricInternalValue updateConflictsCount(PartitionDRGateway.DRConflictType conflictType,
                                                                DRConflictsMetricInternalValue value,
                                                                boolean isResolutionDivergent) {
        if (value == null) {
            value = new DRConflictsMetricInternalValue();
        }
        value.updateConflictCounts(conflictType, m_clock.millis(), isResolutionDivergent);
        return value;
    }

    private static class DRConflictsMetricInternalValue {
        private long totalLastConflictTimestamp;
        private long lastLastConflictTimestamp;

        private long totalConflictsCount;
        private long lastConflictsCount;
        private long totalDivergenceCount;
        private long lastDivergenceCount;

        private long totalMissingRowCount;
        private long lastMissingRowCount;
        private long totalRowTimestampMismatchCount;
        private long lastRowTimestampMismatchCount;
        private long totalConstraintViolationCount;
        private long lastConstraintViolationCount;

        public void updateConflictCounts(PartitionDRGateway.DRConflictType conflictType,
                                         long currentMillis,
                                         boolean isResolutionDivergent) {
            switch (conflictType) {
                case EXPECTED_ROW_MISSING:
                    lastMissingRowCount++;
                    totalMissingRowCount++;
                    break;
                case CONSTRAINT_VIOLATION:
                    lastConstraintViolationCount++;
                    totalConstraintViolationCount++;
                    break;
                case EXPECTED_ROW_TIMESTAMP_MISMATCH:
                    lastRowTimestampMismatchCount++;
                    totalRowTimestampMismatchCount++;
            }
            if (isResolutionDivergent) {
                lastDivergenceCount++;
                totalDivergenceCount++;
            }
            lastConflictsCount++;
            totalConflictsCount++;
            lastLastConflictTimestamp = MILLISECONDS.toMicros(currentMillis);
            totalLastConflictTimestamp = MILLISECONDS.toMicros(currentMillis);
        }

        public DRConflictsMetricValue toLastDRConflictsMetricValue() {
            return new DRConflictsMetricValue(
                    lastLastConflictTimestamp,
                    lastConflictsCount,
                    lastDivergenceCount,
                    lastMissingRowCount,
                    lastRowTimestampMismatchCount,
                    lastConstraintViolationCount
            );
        }

        public DRConflictsMetricValue toTotalDRConflictsMetricValue() {
            return new DRConflictsMetricValue(
                    totalLastConflictTimestamp,
                    totalConflictsCount,
                    totalDivergenceCount,
                    totalMissingRowCount,
                    totalRowTimestampMismatchCount,
                    totalConstraintViolationCount
            );
        }

        public void reset() {
            lastLastConflictTimestamp = 0;
            lastConflictsCount = 0;
            lastDivergenceCount = 0;
            lastMissingRowCount = 0;
            lastRowTimestampMismatchCount = 0;
            lastConstraintViolationCount = 0;
        }
    }
}
