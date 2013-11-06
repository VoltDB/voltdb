/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.join;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.VoltLogger;
import org.voltdb.StatsSource;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import com.google.common.collect.Maps;

public class BalancePartitionsStatistics extends StatsSource {
    private static final VoltLogger log = new VoltLogger("JOIN");

    long start;
    long totalRangeSize;

    int count = 0;
    long rangeSizeMoved = 0;
    long lastReportTime;
    long totalBalanceTime = 0;
    long lastBalanceDuration = 0;
    long balanceStart = 0;


    // Bytes transferred in each @BalancePartitions call in the past second. Keyed by timestamp.
    TreeMap<Long, Long> bytesTransferredInLastSec = Maps.newTreeMap();
    long throughput = 0;
    long lastTransferTimeMS = 0;

    AtomicReference<StatsPoint> statsPoint = new AtomicReference<>(new StatsPoint());

    public BalancePartitionsStatistics()
    {
        this(0L);
    }

    public BalancePartitionsStatistics(long totalRangeSize)
    {
        super(false);
        initialize(totalRangeSize);
    }

    public void initialize(long totalRangeSize)
    {
        this.start = System.currentTimeMillis();
        this.totalRangeSize = totalRangeSize;
        this.lastReportTime = start;
        this.rangeSizeMoved = 0;
        this.totalBalanceTime = 0;
        this.lastBalanceDuration = 0;
        this.balanceStart = 0;
        this.count = 0;

        this.statsPoint.set(new StatsPoint((int)totalRangeSize));
        this.bytesTransferredInLastSec.clear();
    }

    public void logBalanceStarts()
    {
        balanceStart = System.nanoTime();
    }

    public void logBalanceEnds(long rangeSizeMoved, long bytesTransferred, long transferTimeMS)
    {
        final long balanceEnd = System.nanoTime();
        if (balanceEnd > balanceStart) {
            lastBalanceDuration = balanceEnd - balanceStart;
        }
        totalBalanceTime += lastBalanceDuration;
        count++;

        final long now = System.currentTimeMillis();
        this.rangeSizeMoved += rangeSizeMoved;
        bytesTransferredInLastSec.put(now, bytesTransferred);
        throughput += bytesTransferred;


        // remove entries older than a second
        while (bytesTransferredInLastSec.firstKey() < now - 1000) {
            throughput -= bytesTransferredInLastSec.pollFirstEntry().getValue();
        }
        lastTransferTimeMS = transferTimeMS;

        markStatsPoint();

        if (now - lastReportTime > 120000 && now != lastReportTime) {
            lastReportTime = now;
            printLog();
        }

    }

    public long getThroughput()
    {
        return throughput;
    }

    public void printLog()
    {
        if (bytesTransferredInLastSec.isEmpty()) {
            log.info("No range is migrated so far.");
        } else {
            double rangesPerSecond = (rangeSizeMoved /
                    (double) (bytesTransferredInLastSec.lastKey() - start)) * 1000.0;
            double avgBalanceTime = totalBalanceTime / (double)count;
            //Convert to floating point millis
            avgBalanceTime /= 1000000.0;
            log.info(String.format("Migrated %.2f%% of the range at %.2f ranges/sec or %.2f MB/sec with average " +
                                   "@BalancePartition round-trip latency of %.2f milliseconds.",
                                   (rangeSizeMoved * 100.0) / totalRangeSize,
                                   rangesPerSecond,
                                   throughput / (1024.0 * 1024.0),
                                   avgBalanceTime));
        }
    }

    private void markStatsPoint()
    {
        if (bytesTransferredInLastSec.isEmpty()) return;

        double durationInSecs = (bytesTransferredInLastSec.lastKey() - start) / (double)1000.0;
        double rangesPerSecond = rangeSizeMoved / durationInSecs;
        double avgBalanceTime = totalBalanceTime / (double)count;
        //Convert to floating point millis
        avgBalanceTime /= 1000000.0;

        StatsPoint sp = new StatsPoint(
                (int)totalRangeSize,
                (int)rangeSizeMoved,
                rangesPerSecond,
                avgBalanceTime,
                throughput / (1024.0 * 1024.0)
                );
        statsPoint.set(sp);
    }

    public static interface Constants
    {
        public final static String TOTAL_RANGES = "TOTAL_RANGES";
        public final static String MOVED_RANGES = "MOVED_RANGES";
        public final static String RANGES_PER_SECOND = "RANGES_PER_SECOND";
        public final static String BALANCE_TX_LATENCY = "BALANCE_TX_LATENCY";
        public final static String BALANCE_THROUGHPUT = "BALANCE_THROUGHPUT";
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns)
    {
        columns.add(new ColumnInfo(Constants.TOTAL_RANGES, VoltType.INTEGER));
        columns.add(new ColumnInfo(Constants.MOVED_RANGES, VoltType.INTEGER));
        columns.add(new ColumnInfo(Constants.RANGES_PER_SECOND, VoltType.FLOAT));

        columns.add(new ColumnInfo(Constants.BALANCE_TX_LATENCY, VoltType.FLOAT));
        columns.add(new ColumnInfo(Constants.BALANCE_THROUGHPUT, VoltType.FLOAT));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues)
    {
        StatsPoint point = statsPoint.get();

        rowValues[columnNameToIndex.get(Constants.TOTAL_RANGES)] = point.totalRanges;
        rowValues[columnNameToIndex.get(Constants.MOVED_RANGES)] = point.movedRanges;
        rowValues[columnNameToIndex.get(Constants.RANGES_PER_SECOND)] = point.rangesPerSecond;
        rowValues[columnNameToIndex.get(Constants.BALANCE_TX_LATENCY)] = point.balanceTxLatency;
        rowValues[columnNameToIndex.get(Constants.BALANCE_THROUGHPUT)] = point.balanceThroughput;
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval)
    {
        if (totalRangeSize > 0)
        {
            return Arrays.asList(Object.class.cast(new Long(1))).iterator();
        }
        else
        {
            return Collections.emptyList().iterator();
        }
    }

    public static class StatsPoint implements Serializable
    {

        private static final long serialVersionUID = 2635982992941464809L;

        public final int totalRanges;
        public final int movedRanges;
        public final double rangesPerSecond;
        public final double balanceTxLatency;
        public final double balanceThroughput;

        public StatsPoint()
        {
            this(0,0,0.0,0.0,0.0);
        }

        public StatsPoint(int totalRanges)
        {
            this(totalRanges,0,0.0,0.0,0.0);
        }

        public StatsPoint(int totalRanges, int movedRanges,
                double rangesPerSecond, double balanceOpLatency,
                double balanceOpThroughput)
        {
            this.totalRanges = totalRanges;
            this.movedRanges = movedRanges;
            this.rangesPerSecond = rangesPerSecond;
            this.balanceTxLatency = balanceOpLatency;
            this.balanceThroughput = balanceOpThroughput;
        }

        @Override
        public String toString()
        {
            return "StatsPoint [totalRanges=" + totalRanges + ", movedRanges="
                    + movedRanges + ", rangesPerSecond=" + rangesPerSecond
                    + ", balanceTcLatency=" + balanceTxLatency
                    + ", balanceThroughput=" + balanceThroughput + "]";
        }
    }
}
