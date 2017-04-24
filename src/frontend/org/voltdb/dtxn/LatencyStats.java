/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.HdrHistogram_voltpatches.AbstractHistogram;
import org.HdrHistogram_voltpatches.Histogram;
import org.voltdb.ClientInterface;
import org.voltdb.SiteStatsSource;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;
import org.voltdb.VoltTable.ColumnInfo;

public class LatencyStats extends SiteStatsSource {

    public static final int INTERVAL_SECONDS = 5;

    private AtomicReference<AbstractHistogram> m_diffHistProvider = new AtomicReference<AbstractHistogram>();
    private ScheduledExecutorService m_updater = Executors.newScheduledThreadPool(1);

    private class UpdaterJob implements Runnable {

        private AbstractHistogram m_previousHist = LatencyHistogramStats.constructHistogram(false);

        @Override
        public void run() {
            AbstractHistogram currentHist = LatencyHistogramStats.constructHistogram(false);
            ClientInterface ci = VoltDB.instance().getClientInterface();
            if (ci != null) {
                List<AbstractHistogram> thisci = ci.getLatencyStats();
                for (AbstractHistogram info : thisci) {
                    currentHist.add(info);
                }
            }
            AbstractHistogram diffHist = currentHist.copy();
            diffHist.subtract(m_previousHist);
            diffHist.setEndTimeStamp(System.currentTimeMillis());
            m_previousHist = currentHist;
            m_diffHistProvider.set(diffHist);
        }
    }

    /** A dummy iterator that lets getStatsRowKeyIterator() access a single row. */
    private static class SingleRowIterator implements Iterator<Object> {
        boolean rowProvided = false;

        @Override
        public boolean hasNext() {
            if (!rowProvided) {
                rowProvided = true;
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

    public LatencyStats(long siteId) {
        super(siteId, false);
        m_diffHistProvider.set(LatencyHistogramStats.constructHistogram(false));
        final int initialDelay = 0;
        m_updater.scheduleAtFixedRate(new UpdaterJob(), initialDelay, INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new SingleRowIterator();
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo("INTERVAL",  VoltType.INTEGER)); // seconds
        columns.add(new ColumnInfo("COUNT",     VoltType.INTEGER)); // # samples
        columns.add(new ColumnInfo("P50",       VoltType.BIGINT));  // milliseconds
        columns.add(new ColumnInfo("P95",       VoltType.BIGINT));  // milliseconds
        columns.add(new ColumnInfo("P99",       VoltType.BIGINT));  // milliseconds
        columns.add(new ColumnInfo("P99.9",     VoltType.BIGINT));  // milliseconds
        columns.add(new ColumnInfo("P99.99",    VoltType.BIGINT));  // milliseconds
        columns.add(new ColumnInfo("P99.999",   VoltType.BIGINT));  // milliseconds
        columns.add(new ColumnInfo("MAX",       VoltType.BIGINT));  // milliseconds
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        super.updateStatsRow(rowKey, rowValues);
        AbstractHistogram diffHist = m_diffHistProvider.get();

        // Override timestamp from the procedure call with the one from when the data was fetched.
        rowValues[columnNameToIndex.get("TIMESTAMP")] = diffHist.getEndTimeStamp();
        rowValues[columnNameToIndex.get("INTERVAL")]  = INTERVAL_SECONDS;
        rowValues[columnNameToIndex.get("COUNT")]     = diffHist.getTotalCount();
        rowValues[columnNameToIndex.get("P50")]       = diffHist.getValueAtPercentile(0.50);
        rowValues[columnNameToIndex.get("P95")]       = diffHist.getValueAtPercentile(0.95);
        rowValues[columnNameToIndex.get("P99")]       = diffHist.getValueAtPercentile(0.99);
        rowValues[columnNameToIndex.get("P99.9")]     = diffHist.getValueAtPercentile(0.999);
        rowValues[columnNameToIndex.get("P99.99")]    = diffHist.getValueAtPercentile(0.9999);
        rowValues[columnNameToIndex.get("P99.999")]   = diffHist.getValueAtPercentile(0.99999);
        rowValues[columnNameToIndex.get("MAX")]       = diffHist.getMaxValue();
    }
}
