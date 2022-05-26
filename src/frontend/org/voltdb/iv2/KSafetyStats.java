/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.StatsSource;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.ImmutableSortedSet;

public class KSafetyStats extends StatsSource {
    private final AtomicReference<NavigableSet<StatsPoint>> m_kSafetySet;

    // undocumented
    public enum KSafety {
        TIMESTAMP                   (VoltType.BIGINT),
        PARTITION_ID                (VoltType.INTEGER),
        MISSING_REPLICA             (VoltType.INTEGER);

        public final VoltType m_type;
        KSafety(VoltType type) { m_type = type; }
    }

    public KSafetyStats() {
        super(false);
        NavigableSet<StatsPoint> empty = ImmutableSortedSet.<StatsPoint>of();
        m_kSafetySet = new AtomicReference<>(empty);
    }

    NavigableSet<StatsPoint> getSafetySet() {
        return m_kSafetySet.get();
    }

    boolean setSafetySet(NavigableSet<StatsPoint> kSafetySet) {
        Preconditions.checkArgument(kSafetySet != null, "specified null map");
        final NavigableSet<StatsPoint> expect = m_kSafetySet.get();
        return m_kSafetySet.compareAndSet(expect, kSafetySet);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        for (KSafety col : KSafety.values()) {
            columns.add(new VoltTable.ColumnInfo(col.name(), col.m_type));
        }
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        StatsPoint sp = StatsPoint.class.cast(rowKey);
        rowValues[KSafety.TIMESTAMP.ordinal()] = sp.getTimestamp();
        rowValues[KSafety.PARTITION_ID.ordinal()] = sp.getPartitionId();
        rowValues[KSafety.MISSING_REPLICA.ordinal()] = sp.getMissingCount();
        return KSafety.values().length;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        @SuppressWarnings("rawtypes")
        Iterator iter = m_kSafetySet.get().iterator();
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

        public long getTimestamp() {
            return timestamp;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public int getMissingCount() {
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
