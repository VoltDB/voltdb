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

import org.voltdb.StatsSource;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class DRConflictsStats extends StatsSource {

    private final DRConflictsTracker m_drConflictsTracker;
    private final int m_clusterId;
    private Map<DRConflictsMetricKey, DRConflictsMetricValue> m_metricsSnapshot;

    public enum DRConflicts {
        CLUSTER_ID(VoltType.INTEGER),
        REMOTE_CLUSTER_ID(VoltType.INTEGER),
        PARTITION_ID(VoltType.INTEGER),
        TABLE_NAME(VoltType.STRING),
        LAST_CONFLICT_TIMESTAMP(VoltType.TIMESTAMP),
        TOTAL_CONFLICT_COUNT(VoltType.BIGINT),
        DIVERGENCE_COUNT(VoltType.BIGINT),
        MISSING_ROW_COUNT(VoltType.BIGINT),
        TIMESTAMP_MISMATCH_COUNT(VoltType.BIGINT),
        CONSTRAINT_VIOLATION_COUNT(VoltType.BIGINT);

        public final VoltType m_type;

        DRConflicts(VoltType type) {
            m_type = type;
        }
    }

    public DRConflictsStats(DRConflictsTracker drConflictsTracker, int clusterId) {
        super(false);
        m_drConflictsTracker = drConflictsTracker;
        m_clusterId = clusterId;
    }

    @Override
    protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
        super.populateColumnSchema(columns, DRConflicts.class);
    }

    @Override
    protected synchronized int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        DRConflictsMetricKey key = (DRConflictsMetricKey) rowKey;
        DRConflictsMetricValue value = m_metricsSnapshot.get(key);

        rowValues[offset + DRConflicts.CLUSTER_ID.ordinal()] = m_clusterId;
        rowValues[offset + DRConflicts.REMOTE_CLUSTER_ID.ordinal()] = key.getRemoteClusterId();
        rowValues[offset + DRConflicts.PARTITION_ID.ordinal()] = key.getPartitionId();
        rowValues[offset + DRConflicts.TABLE_NAME.ordinal()] = key.getTableName();
        rowValues[offset + DRConflicts.LAST_CONFLICT_TIMESTAMP.ordinal()] = value.getLastConflictTimestamp();
        rowValues[offset + DRConflicts.DIVERGENCE_COUNT.ordinal()] = value.getDivergenceCount();
        rowValues[offset + DRConflicts.TOTAL_CONFLICT_COUNT.ordinal()] = value.getConflictsCount();
        rowValues[offset + DRConflicts.MISSING_ROW_COUNT.ordinal()] = value.getMissingRowCount();
        rowValues[offset + DRConflicts.TIMESTAMP_MISMATCH_COUNT.ordinal()] = value.getRowTimestampMismatchCount();
        rowValues[offset + DRConflicts.CONSTRAINT_VIOLATION_COUNT.ordinal()] = value.getConstraintViolationCount();
        return offset + DRConflicts.values().length;
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        if (interval) {
            m_metricsSnapshot = m_drConflictsTracker.getLastMetricsSnapshot();
        } else {
            m_metricsSnapshot = m_drConflictsTracker.getTotalMetricsSnapshot();
        }
        return cast(m_metricsSnapshot.keySet().iterator());
    }

    @SuppressWarnings("unchecked")
    private static <T> Iterator<T> cast(Iterator<?> p) {
        return (Iterator<T>) p;
    }
}
