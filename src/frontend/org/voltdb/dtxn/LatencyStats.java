/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.Iterator;

import org.voltdb.SiteStatsSource;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

/**
 * Class that provides latency information in buckets. Each bucket contains the
 * number of procedures with latencies in the range.
 */
public class LatencyStats extends SiteStatsSource {
    /**
     * A dummy iterator that wraps an integer and provides the
     * Iterator<Object> necessary for getStatsRowKeyIterator()
     *
     */
    private static class BucketIterator implements Iterator<Object> {
        private final int max;
        private int current = -1;

        private BucketIterator(int max) {
            this.max = max - 1; // minus one so that it's easier to compare later
        }

        @Override
        public boolean hasNext() {
            if (current == max) {
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

    /**
     * Latency buckets to store overall latency distribution. It's divided into
     * 26 buckets, each stores 10ms latency range invocation info. The last
     * bucket covers invocations with latencies larger than 250ms.
     */
    private final long[] m_latencyBuckets = {0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l,
                                             0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l,
                                             0l, 0l, 0l, 0l, 0l, 0l};
    private long m_max = (m_latencyBuckets.length - 1) * BUCKET_RANGE;
    private static final long BUCKET_RANGE = 10; // 10ms

    public LatencyStats(String name, int siteId) {
        super(name, siteId, false);
        VoltDB.instance().getStatsAgent().registerStatsSource(SysProcSelector.LATENCY, 0, this);
    }

    /**
     * Called by the Initiator every time a transaction is completed
     * @param delta Time the procedure took to round trip intra cluster
     */
    public synchronized void logTransactionCompleted(int delta) {
        int bucketIndex = Math.min((int) (delta / BUCKET_RANGE), m_latencyBuckets.length - 1);
        m_max = Math.max(delta, m_max);
        m_latencyBuckets[bucketIndex]++;
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new BucketIterator(m_latencyBuckets.length);
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
        if (bucket < m_latencyBuckets.length - 1) {
            rowValues[columnNameToIndex.get("BUCKET_MAX")] = (bucket + 1) * BUCKET_RANGE;
        } else {
            // max for the last bucket is the max of the largest latency
            rowValues[columnNameToIndex.get("BUCKET_MAX")] = m_max;
        }
        rowValues[columnNameToIndex.get("INVOCATIONS")] = m_latencyBuckets[bucket];
        super.updateStatsRow(rowKey, rowValues);
    }
}
