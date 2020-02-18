/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
import org.voltdb.ClientInterface;
import org.voltdb.StatsSource;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;

/** Source of @Statistics LATENCY, which provides key latency metrics from the past 5 seconds of transactions.
 * This is intended to be used as part of a manual or automatic monitoring solution,
 * where plotting latency fluctuations over time with minimal post-processing is desired.
 *
 * Each call returns latency percentiles from the most recent complete window,
 * along with the timestamp associated with that window.
 * Samples are calculated by using a differential histogram,
 * subtracting the previous window's histogram from the current one.
 * Tables with the same HOST_ID and TIMESTAMP represent the same data.
 *
 * Statistics are returned with one row for each node.
 *
 * To get a complete latency curve which includes all data since VoltDB started,
 * get the whole histogram via @Statistics LATENCY_COMPRESSED or @Statistics LATENCY_HISTOGRAM (both undocumented).
 */
public class LatencyStats extends StatsSource {

    public static final int INTERVAL_MS = Integer.getInteger("LATENCY_STATS_WINDOW_MS", (int) TimeUnit.SECONDS.toMillis(5));

    private AtomicReference<AbstractHistogram> m_diffHistProvider = new AtomicReference<AbstractHistogram>();
    private ScheduledExecutorService m_updater = Executors.newScheduledThreadPool(1);

    private class UpdaterJob implements Runnable {

        private AbstractHistogram m_previousHist = LatencyHistogramStats.constructHistogram(false);

        @Override
        public void run() {
            AbstractHistogram currentHist = LatencyHistogramStats.constructHistogram(false);
            ClientInterface clientInterface = VoltDB.instance().getClientInterface();
            if (clientInterface != null) {
                List<AbstractHistogram> statsHists = clientInterface.getLatencyStats();
                for (AbstractHistogram hist : statsHists) {
                    currentHist.add(hist);
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

    public LatencyStats() {
        super(false);
        m_diffHistProvider.set(LatencyHistogramStats.constructHistogram(false));
        final int initialDelay = 0;
        m_updater.scheduleAtFixedRate(new UpdaterJob(), initialDelay, INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new SingleRowIterator();
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);                       // timestamp is milliseconds
        columns.add(new ColumnInfo("INTERVAL", VoltType.INTEGER)); // milliseconds
        columns.add(new ColumnInfo("COUNT",    VoltType.INTEGER)); // samples
        columns.add(new ColumnInfo("TPS",      VoltType.INTEGER)); // samples per second
        columns.add(new ColumnInfo("P50",      VoltType.BIGINT));  // microseconds
        columns.add(new ColumnInfo("P95",      VoltType.BIGINT));  // microseconds
        columns.add(new ColumnInfo("P99",      VoltType.BIGINT));  // microseconds
        columns.add(new ColumnInfo("P99.9",    VoltType.BIGINT));  // microseconds
        columns.add(new ColumnInfo("P99.99",   VoltType.BIGINT));  // microseconds
        columns.add(new ColumnInfo("P99.999",  VoltType.BIGINT));  // microseconds
        columns.add(new ColumnInfo("MAX",      VoltType.BIGINT));  // microseconds
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        super.updateStatsRow(rowKey, rowValues);
        AbstractHistogram diffHist = m_diffHistProvider.get();

        // Override timestamp from the procedure call with the one from when the data was fetched.
        rowValues[columnNameToIndex.get("TIMESTAMP")] = diffHist.getEndTimeStamp();
        rowValues[columnNameToIndex.get("INTERVAL")]  = INTERVAL_MS;
        rowValues[columnNameToIndex.get("COUNT")]     = diffHist.getTotalCount();
        rowValues[columnNameToIndex.get("TPS")]       = (int) (TimeUnit.SECONDS.toMillis(diffHist.getTotalCount()) / INTERVAL_MS);
        rowValues[columnNameToIndex.get("P50")]       = diffHist.getValueAtPercentile(50D);
        rowValues[columnNameToIndex.get("P95")]       = diffHist.getValueAtPercentile(95D);
        rowValues[columnNameToIndex.get("P99")]       = diffHist.getValueAtPercentile(99D);
        rowValues[columnNameToIndex.get("P99.9")]     = diffHist.getValueAtPercentile(99.9D);
        rowValues[columnNameToIndex.get("P99.99")]    = diffHist.getValueAtPercentile(99.99D);
        rowValues[columnNameToIndex.get("P99.999")]   = diffHist.getValueAtPercentile(99.999D);
        rowValues[columnNameToIndex.get("MAX")]       = diffHist.getMaxValue();
    }
}
