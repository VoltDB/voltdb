/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.voltdb.ClientInterface;
import org.voltdb.SiteStatsSource;
import org.voltdb.StatsSelector;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import com.google_voltpatches.common.collect.ImmutableList;

/**
 * Class that provides latency information in buckets. Each bucket contains the
 * number of procedures with latencies in the range.
 */
public class LatencyStats extends SiteStatsSource {
    /**
     * A dummy iterator that wraps and int and provides the
     * Iterator<Object> necessary for getStatsRowKeyIterator()
     *
     */
    private static class BucketIterator implements Iterator<Object> {
        private final int buckets;
        private int current = -1;

        private BucketIterator(int buckets) {
            // Store the 0-indexed size
            this.buckets = buckets - 1;
        }

        @Override
        public boolean hasNext() {
            if (current == buckets) {
                return false;
            }
            return true;
        }

        @Override
        public Object next() {
            return ++current;
        }

        @Override
        public void remove() {

        }
    }

    public static class LatencyInfo
    {
        private volatile ImmutableList<Long> m_latencyStats;
        private volatile long m_max;

        public LatencyInfo()
        {
            Long[] buckets = {0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l,
                0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l,
                0l, 0l, 0l, 0l, 0l, 0l};
            m_latencyStats = ImmutableList.copyOf(buckets);
            m_max = (m_latencyStats.size() - 1) * BUCKET_RANGE;
        }

        public void addSample(int delta)
        {
            int bucketIndex = Math.min((int) (delta / BUCKET_RANGE), m_latencyStats.size() - 1);

            // if the host's clock moves backwards, bucketIndex can be negative
            // this next line of code is a lie, but a beautiful one that keeps your server up
            if (bucketIndex < 0) bucketIndex = 0;

            m_max = Math.max(delta, m_max);
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            for (int i = 0; i < m_latencyStats.size(); i++) {
                if (i == bucketIndex) {
                    builder.add(m_latencyStats.get(i) + 1);
                }
                else {
                    builder.add(m_latencyStats.get(i));
                }
            }
            m_latencyStats = builder.build();
        }

        void mergeLatencyInfo(LatencyInfo other)
        {
            assert(other != null);
            assert(m_latencyStats.size() == other.m_latencyStats.size());
            m_max = Math.max(m_max, other.m_max);
            ImmutableList.Builder<Long> builder = ImmutableList.builder();
            for (int i = 0; i < m_latencyStats.size(); i++) {
                builder.add(m_latencyStats.get(i) + other.m_latencyStats.get(i));
            }
            m_latencyStats = builder.build();
        }

        List<Long> getBuckets()
        {
            return m_latencyStats;
        }

        long getMax()
        {
            return m_max;
        }
    }

    private static final long BUCKET_RANGE = 10; // 10ms
    private LatencyInfo m_totals;

    public LatencyStats(long siteId) {
        super(siteId, false);
        VoltDB.instance().getStatsAgent().registerStatsSource(StatsSelector.LATENCY, 0, this);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval)
    {
        m_totals = new LatencyInfo();
        ClientInterface ci = VoltDB.instance().getClientInterface();
        if (ci != null) {
            List<LatencyInfo> thisci = ci.getLatencyStats();
            for (LatencyInfo info : thisci) {
                m_totals.mergeLatencyInfo(info);
            }
        }
        return new BucketIterator(m_totals.getBuckets().size());
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo("BUCKET_MIN", VoltType.INTEGER));
        columns.add(new ColumnInfo("BUCKET_MAX", VoltType.INTEGER));
        columns.add(new ColumnInfo("INVOCATIONS", VoltType.BIGINT));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        final int bucket = (Integer) rowKey;

        rowValues[columnNameToIndex.get("BUCKET_MIN")] = bucket * BUCKET_RANGE;
        if (bucket < m_totals.getBuckets().size() - 1) {
            rowValues[columnNameToIndex.get("BUCKET_MAX")] = (bucket + 1) * BUCKET_RANGE;
        } else {
            // max for the last bucket is the max of the largest latency
            rowValues[columnNameToIndex.get("BUCKET_MAX")] = m_totals.getMax();
        }
        rowValues[columnNameToIndex.get("INVOCATIONS")] = m_totals.getBuckets().get(bucket);
        super.updateStatsRow(rowKey, rowValues);
    }
}
