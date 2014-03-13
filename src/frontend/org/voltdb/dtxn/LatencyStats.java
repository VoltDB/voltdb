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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.HdrHistogram_voltpatches.AbstractHistogram;
import org.HdrHistogram_voltpatches.AtomicHistogram;
import org.HdrHistogram_voltpatches.Histogram;
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
    private static class DummyIterator implements Iterator<Object> {
        boolean oneRow = false;

        @Override
        public boolean hasNext() {
            if (!oneRow) {
                oneRow = true;
                return true;
            }
            return false;
        }

        @Override
        public Object next() {
            return null;
        }

        @Override
        public void remove() {

        }
    }

    public static AbstractHistogram constructHistogram(boolean threadSafe) {
        final long highestTrackableValue = 60L * 60L * 1000000L;
        final int numberOfSignificantValueDigits = 2;
        if (threadSafe) {
            return new AtomicHistogram( highestTrackableValue, numberOfSignificantValueDigits);
        } else {
            return new Histogram( highestTrackableValue, numberOfSignificantValueDigits);
        }
    }

    private AbstractHistogram m_totals = constructHistogram(false);

    public LatencyStats(long siteId) {
        super(siteId, false);
        VoltDB.instance().getStatsAgent().registerStatsSource(StatsSelector.LATENCY, 0, this);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval)
    {
        m_totals.reset();
        ClientInterface ci = VoltDB.instance().getClientInterface();
        if (ci != null) {
            List<AbstractHistogram> thisci = ci.getLatencyStats();
            for (AbstractHistogram info : thisci) {
                m_totals.add(info);
                info.reset();
            }
        }
        return new DummyIterator();
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo("HISTOGRAM", VoltType.VARBINARY));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        rowValues[columnNameToIndex.get("HISTOGRAM")] = m_totals.toCompressedBytes();
        super.updateStatsRow(rowKey, rowValues);
    }
}
