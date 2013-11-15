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

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.TreeMap;

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
    long rowCount;


    // Bytes transferred in each @BalancePartitions call in the past second. Keyed by timestamp.
    TreeMap<Long, Long> bytesTransferredInLastSec = Maps.newTreeMap();
    long throughput = 0;
    long lastTransferTimeMS = 0;

    private volatile StatsPoint statsPoint = new StatsPoint();

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
        this.rowCount = 0;

        this.statsPoint = new StatsPoint((int)totalRangeSize);
        this.bytesTransferredInLastSec.clear();
    }

    public void logBalanceStarts()
    {
        balanceStart = System.nanoTime();
    }

    public void logBalanceEnds(long rangeSizeMoved, long bytesTransferred, long transferTimeMS, long rowsTranfered)
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
        rowCount += rowsTranfered;
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

        long durationInMillis = bytesTransferredInLastSec.lastKey() - start;

        StatsPoint sp = new StatsPoint(
                totalRangeSize,
                rangeSizeMoved,
                rowCount,
                durationInMillis,
                count,
                totalBalanceTime,
                throughput);

        statsPoint = sp;
    }

    public static interface Constants
    {
        public final static String TOTAL_RANGES = "TOTAL_RANGES";
        public final static String PERCENTAGE_MOVED = "PERCENTAGE_MOVED";
        public final static String MOVED_ROWS = "MOVED_ROWS";
        public final static String ROWS_PER_SECOND = "ROWS_PER_SECOND";
        public final static String ESTIMATED_REMAINING = "ESTIMATED_REMAINING";
        public final static String MEGABYTES_PER_SECOND = "MEGABYTES_PER_SECOND";
        public final static String CALLS_PER_SECOND = "CALLS_PER_SECOND";
        public final static String CALLS_LATENCY = "CALLS_LATENCY";
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns)
    {
        columns.add(new ColumnInfo(Constants.TOTAL_RANGES, VoltType.BIGINT));
        columns.add(new ColumnInfo(Constants.PERCENTAGE_MOVED, VoltType.FLOAT));
        columns.add(new ColumnInfo(Constants.MOVED_ROWS, VoltType.BIGINT));
        columns.add(new ColumnInfo(Constants.ROWS_PER_SECOND, VoltType.FLOAT));
        columns.add(new ColumnInfo(Constants.ESTIMATED_REMAINING, VoltType.BIGINT));
        columns.add(new ColumnInfo(Constants.MEGABYTES_PER_SECOND, VoltType.FLOAT));
        columns.add(new ColumnInfo(Constants.CALLS_PER_SECOND, VoltType.FLOAT));
        columns.add(new ColumnInfo(Constants.CALLS_LATENCY, VoltType.FLOAT));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues)
    {
        final StatsPoint point = statsPoint;

        rowValues[columnNameToIndex.get(Constants.TOTAL_RANGES)] = point.getTotalRanges();
        rowValues[columnNameToIndex.get(Constants.PERCENTAGE_MOVED)] = point.getPercentageMoved();
        rowValues[columnNameToIndex.get(Constants.MOVED_ROWS)] = point.getMovedRows();
        rowValues[columnNameToIndex.get(Constants.ROWS_PER_SECOND)] = point.getRowsPerSecond();
        rowValues[columnNameToIndex.get(Constants.ESTIMATED_REMAINING)] = point.getEstimatedRemaining();
        rowValues[columnNameToIndex.get(Constants.MEGABYTES_PER_SECOND)] = point.getMegabytesPerSecond();
        rowValues[columnNameToIndex.get(Constants.CALLS_PER_SECOND)] = point.getInvocationsPerSecond();
        rowValues[columnNameToIndex.get(Constants.CALLS_LATENCY)] = point.getAverageInvocationTime();
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

        private final long   totalRanges;
        private final long   movedRanges;
        private final long   movedRows;
        private final long   durationMillis;
        private final long   invocationCount;
        private final long   invocationTime;
        private final long   throughput;

        public StatsPoint()
        {
            this(0,0,0,0,0,0,0);
        }

        public StatsPoint(int totalRanges)
        {
            this(totalRanges,0,0,0,0,0,0);
        }

        public StatsPoint(long totalRanges, long movedRanges,
                long movedRows, long durationMillis,
                long invocationCount, long invocationTime,
                long throughput)
        {
            this.totalRanges = totalRanges;
            this.movedRanges = movedRanges;
            this.movedRows = movedRows;
            this.durationMillis = durationMillis;
            this.invocationCount = invocationCount;
            this.invocationTime = invocationTime;
            this.throughput = throughput;
        }

        long getTotalRanges()
        {
            return totalRanges;
        }

        long getMovedRanges()
        {
            return movedRanges;
        }

        long getMovedRows()
        {
            return movedRows;
        }

        long getDurationMillis()
        {
            return durationMillis;
        }

        long getInvocationCount()
        {
            return invocationCount;
        }

        long getInvocationTime()
        {
            return invocationTime;
        }

        long getThroughput()
        {
            return throughput;
        }

        public double getPercentageMoved()
        {
            return (movedRanges / (double)totalRanges) * 100.0;
        }

        public double getRowsPerSecond()
        {
            final double durationInSecs = durationMillis / (double)1000.0;
            return movedRows / durationInSecs;
        }

        public String getFormattedEstimatedRemaining()
        {
            return formatTimeInterval(getEstimatedRemaining());
        }

        public long getEstimatedRemaining() {
            long estimatedRemaining = -1L;
            if (movedRanges > 0) {
                estimatedRemaining = (totalRanges * durationMillis) / movedRanges;
            }
            return estimatedRemaining;
        }

        public double getMegabytesPerSecond()
        {
            return (double)throughput / (1024.0 * 1024.0);
        }

        public double getInvocationsPerSecond()
        {
            final double durationInSecs = durationMillis / (double)1000.0;
            return invocationCount / durationInSecs;
        }

        public double getAverageInvocationTime()
        {
            double avgBalanceTime = invocationTime / (double)invocationCount;
            //Convert to floating point millis
            return avgBalanceTime / 1000000.0;
        }

        public final static String formatTimeInterval(long l)
        {
            if (l < 0) return null;
            final long day = MILLISECONDS.toDays(l);
            if (day > 100) return null;
            final long hr  = MILLISECONDS.toHours(l   - DAYS.toMillis(day));
            final long min = MILLISECONDS.toMinutes(l - DAYS.toMillis(day)  - HOURS.toMillis(hr));
            final long sec = MILLISECONDS.toSeconds(l - DAYS.toMillis(day)  - HOURS.toMillis(hr) - MINUTES.toMillis(min));
            final long ms  = MILLISECONDS.toMillis(l  - DAYS.toMillis(day)  - HOURS.toMillis(hr) - MINUTES.toMillis(min) - SECONDS.toMillis(sec));
            return String.format("%4d %02d:%02d:%02d.%03d", day, hr, min, sec, ms);
        }

        @Override
        public String toString()
        {
            return "StatsPoint [totalRanges=" + totalRanges + ", movedRanges="
                    + movedRanges + ", movedRows=" + movedRows
                    + ", durationMillis=" + durationMillis
                    + ", invocationCount=" + invocationCount
                    + ", invocationTime=" + invocationTime + ", throughput="
                    + throughput + "]";
        }
    }
}
