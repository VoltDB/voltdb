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
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

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

    public enum Latency {
        INTERVAL                    ("INTERVAL", VoltType.INTEGER),
        COUNT                       ("COUNT", VoltType.INTEGER),
        TPS                         ("TPS", VoltType.INTEGER),
        P50                         ("P50", VoltType.BIGINT),
        P95                         ("P95", VoltType.BIGINT),
        P99                         ("P99", VoltType.BIGINT),
        // Those three columns in statistics are "P99.9", "P99.99" and "P99.999", which cannot be the enum names.
        // Use the alias name as workaround.
        P99_9                       ("P99.9", VoltType.BIGINT),
        P99_99                      ("P99.99", VoltType.BIGINT),
        P99_999                     ("P99.999", VoltType.BIGINT),
        MAX                         ("MAX", VoltType.BIGINT);

        public final VoltType m_type;
        public final String m_alias;
        Latency(String name, VoltType type)
        {
            m_alias = name;
            m_type = type;
        }

        public String alias() {
            return m_alias;
        }
    }

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
        for (Latency col : Latency.values()) {
            columns.add(new VoltTable.ColumnInfo(col.alias(), col.m_type));
        }
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        AbstractHistogram diffHist = m_diffHistProvider.get();

        // Override timestamp from the procedure call with the one from when the data was fetched.
        rowValues[StatsCommon.TIMESTAMP.ordinal()] = diffHist.getEndTimeStamp();

        rowValues[offset + Latency.INTERVAL.ordinal()]  = INTERVAL_MS;
        rowValues[offset + Latency.COUNT.ordinal()]     = diffHist.getTotalCount();
        rowValues[offset + Latency.TPS.ordinal()]       = (int) (TimeUnit.SECONDS.toMillis(diffHist.getTotalCount()) / INTERVAL_MS);
        rowValues[offset + Latency.P50.ordinal()]       = diffHist.getValueAtPercentile(50D);
        rowValues[offset + Latency.P95.ordinal()]       = diffHist.getValueAtPercentile(95D);
        rowValues[offset + Latency.P99.ordinal()]       = diffHist.getValueAtPercentile(99D);
        rowValues[offset + Latency.P99_9.ordinal()]     = diffHist.getValueAtPercentile(99.9D);
        rowValues[offset + Latency.P99_99.ordinal()]    = diffHist.getValueAtPercentile(99.99D);
        rowValues[offset + Latency.P99_999.ordinal()]   = diffHist.getValueAtPercentile(99.999D);
        rowValues[offset + Latency.MAX.ordinal()]       = diffHist.getMaxValue();
        return offset + Latency.values().length;
    }
}
