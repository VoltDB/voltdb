/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NavigableSet;

import org.voltdb.StatsSource;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.ImmutableSortedSet;

public class KSafetyStats extends StatsSource {
    private volatile NavigableSet<StatsPoint> m_kSafetySet;

    public KSafetyStats() {
        super(false);
        m_kSafetySet = ImmutableSortedSet.<StatsPoint>of();
    }

    public static interface Constants {
        public final static String TIMESTAMP = "TIMESTAMP";
        public final static String PARTITION_ID = "PARTITION_ID";
        public final static String MISSING_REPLICA = "MISSING_REPLICA";
    }

    NavigableSet<StatsPoint> getSafetySet() {
        return m_kSafetySet;
    }

    void setSafetySet(NavigableSet<StatsPoint> kSafetySet) {
        Preconditions.checkArgument(kSafetySet != null, "specified null map");
        this.m_kSafetySet = kSafetySet;
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        columns.add(new ColumnInfo(Constants.TIMESTAMP, VoltType.BIGINT));
        columns.add(new ColumnInfo(Constants.PARTITION_ID, VoltType.INTEGER));
        columns.add(new ColumnInfo(Constants.MISSING_REPLICA, VoltType.INTEGER));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        StatsPoint sp = StatsPoint.class.cast(rowKey);
        rowValues[columnNameToIndex.get(Constants.TIMESTAMP)] = sp.getTimestamp();
        rowValues[columnNameToIndex.get(Constants.PARTITION_ID)] = sp.getPartitionId();
        rowValues[columnNameToIndex.get(Constants.MISSING_REPLICA)] = sp.getMissingCount();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        @SuppressWarnings("rawtypes")
        Iterator iter = m_kSafetySet.iterator();
        return (Iterator<Object>)iter;
    }

    public static class StatsPoint implements Comparable<StatsPoint>{
        private final long timestamp;
        private final int partitionId;
        private final int missingCount;

        public StatsPoint(long timestamp, int partitionId, int missingCount) {
            this.timestamp = timestamp;
            this.partitionId = partitionId;
            this.missingCount = missingCount;
        }

        long getTimestamp() {
            return timestamp;
        }

        int getPartitionId() {
            return partitionId;
        }

        int getMissingCount() {
            return missingCount;
        }

        @Override
        public String toString() {
            return "StatsPoint [timestamp=" + timestamp + ", partitionId="
                    + partitionId + ", missingCount=" + missingCount + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + partitionId;
            result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            StatsPoint other = (StatsPoint) obj;
            if (partitionId != other.partitionId)
                return false;
            if (timestamp != other.timestamp)
                return false;
            return true;
        }

        @Override
        public int compareTo(StatsPoint o) {
            int cmp = this.partitionId - o.partitionId;
            if (cmp == 0 ) {
                long lcmp = this.timestamp - o.timestamp;
                if (lcmp != 0) cmp = lcmp < 0 ? -1 : 1;
            }
            return cmp;
        }
    }
}
