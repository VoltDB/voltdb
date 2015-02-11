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
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltdb.StatsSource;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.Maps;

public class BalancePartitionsStatistics extends StatsSource {
    private static final VoltLogger log = new VoltLogger("JOIN");

    private static long logIntervalNanos = TimeUnit.SECONDS.toNanos(120);

    long totalRangeSize;

    long lastReportTime;
    long lastBalanceDuration = 0;
    long balanceStart = 0;

    // Bytes transferred in each @BalancePartitions call in the past second.
    // Keyed by nanosecond timestamp.
    TreeMap<Long, Long> bytesTransferredInLastSec = Maps.newTreeMap();
    long throughput = 0;
    long lastTransferTimeNanos = 0;

    private volatile StatsPoint statsPoint;
    private StatsPoint intervalStats;
    private StatsPoint overallStats;

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
        this.overallStats = new StatsPoint("Overall", totalRangeSize);

        this.totalRangeSize = totalRangeSize;
        this.lastReportTime = overallStats.getStartTimeNanos();
        this.lastBalanceDuration = 0;

        startInterval();

        this.statsPoint = new StatsPoint("Point", totalRangeSize);

        this.bytesTransferredInLastSec.clear();
    }

    public void logBalanceStarts()
    {
        balanceStart = System.nanoTime();
    }

    public void logBalanceEnds(long rangeSizeMoved, long bytesTransferred, long callTimeNanos, long transferTimeNanos, long rowsTransferred)
    {
        final long balanceEnd = System.nanoTime();
        lastBalanceDuration = balanceEnd - balanceStart;

        final long now = System.nanoTime();
        final long aSecondAgo = now - TimeUnit.SECONDS.toNanos(1);
        bytesTransferredInLastSec.put(now, bytesTransferred);

        // remove entries older than a second
        throughput += bytesTransferred;
        while (bytesTransferredInLastSec.firstKey() < aSecondAgo) {
            throughput -= bytesTransferredInLastSec.pollFirstEntry().getValue();
        }

        lastTransferTimeNanos = transferTimeNanos;

        overallStats = overallStats.update(balanceEnd,
                                           lastBalanceDuration,
                                           callTimeNanos,
                                           transferTimeNanos,
                                           rangeSizeMoved,
                                           rowsTransferred,
                                           bytesTransferred,
                                           1);
        intervalStats = intervalStats.update(balanceEnd,
                                             lastBalanceDuration,
                                             callTimeNanos,
                                             transferTimeNanos,
                                             rangeSizeMoved,
                                             rowsTransferred,
                                             bytesTransferred,
                                             1);

        markStatsPoint();

        // Close out the interval and log statistics every logIntervalSeconds seconds.
        if (now - lastReportTime > logIntervalNanos && now != lastReportTime) {
            lastReportTime = now;
            endInterval();
        }
    }

    public long getThroughput()
    {
        return throughput;
    }

    private void startInterval()
    {
        this.intervalStats = new StatsPoint("Interval", totalRangeSize);
    }

    private void endInterval()
    {
        printLog();
    }

    public void printLog()
    {
        if (this.bytesTransferredInLastSec.isEmpty()) {
            log.info("No data has been migrated yet.");
        }
        else {
            log.info(String.format("JOIN PROGRESS SUMMARY: "
                                   + "time elapsed: %s  "
                                   + "amount completed: %.2f%%  "
                                   + "est. time remaining: %s",
                                   this.overallStats.getFormattedDuration(),
                                   this.overallStats.getCompletedFraction() * 100.0,
                                   this.overallStats.getFormattedEstimatedRemaining()));
            log.info(String.format("JOIN DIAGNOSTICS: %s", intervalStats.toString()));
            log.info(String.format("JOIN DIAGNOSTICS: %s", overallStats.toString()));
        }
        // Immediately start the next interval.
        this.startInterval();
    }

    // Mainly for testing.
    public StatsPoint getOverallStats()
    {
        return this.overallStats;
    }

    // Mainly for testing.
    public StatsPoint getIntervalStats()
    {
        return this.intervalStats;
    }

    public StatsPoint getLastStatsPoint()
    {
        return this.statsPoint;
    }

    private void markStatsPoint()
    {
        if (!bytesTransferredInLastSec.isEmpty()) {
            statsPoint = overallStats.capture(
                    "Point",
                    bytesTransferredInLastSec.lastKey());
        }
    }

    public static interface Constants
    {
        public final static String TIMESTAMP = "TIMESTAMP";
        public final static String PERCENTAGE_MOVED = "PERCENTAGE_MOVED";
        public final static String MOVED_ROWS = "MOVED_ROWS";
        public final static String ROWS_PER_SECOND = "ROWS_PER_SECOND";
        public final static String ESTIMATED_REMAINING = "ESTIMATED_REMAINING";
        public final static String MEGABYTES_PER_SECOND = "MEGABYTES_PER_SECOND";
        public final static String CALLS_PER_SECOND = "CALLS_PER_SECOND";
        public final static String CALLS_LATENCY = "CALLS_LATENCY";
        public final static String CALLS_TIME = "CALLS_TIME";
        public final static String CALLS_TRANSFER_TIME = "CALLS_TRANSFER_TIME";
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns)
    {
        columns.add(new ColumnInfo(Constants.TIMESTAMP, VoltType.BIGINT));
        columns.add(new ColumnInfo(Constants.PERCENTAGE_MOVED, VoltType.FLOAT));
        columns.add(new ColumnInfo(Constants.MOVED_ROWS, VoltType.BIGINT));
        columns.add(new ColumnInfo(Constants.ROWS_PER_SECOND, VoltType.FLOAT));
        columns.add(new ColumnInfo(Constants.ESTIMATED_REMAINING, VoltType.BIGINT));
        columns.add(new ColumnInfo(Constants.MEGABYTES_PER_SECOND, VoltType.FLOAT));
        columns.add(new ColumnInfo(Constants.CALLS_PER_SECOND, VoltType.FLOAT));
        columns.add(new ColumnInfo(Constants.CALLS_LATENCY, VoltType.FLOAT));
        columns.add(new ColumnInfo(Constants.CALLS_TIME, VoltType.FLOAT));
        columns.add(new ColumnInfo(Constants.CALLS_TRANSFER_TIME, VoltType.FLOAT));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues)
    {
        final StatsPoint point = statsPoint;

        rowValues[columnNameToIndex.get(Constants.TIMESTAMP)] = System.currentTimeMillis();
        rowValues[columnNameToIndex.get(Constants.PERCENTAGE_MOVED)] = point.getPercentageMoved();
        rowValues[columnNameToIndex.get(Constants.MOVED_ROWS)] = point.getMovedRows();
        rowValues[columnNameToIndex.get(Constants.ROWS_PER_SECOND)] = point.getRowsPerSecond();
        rowValues[columnNameToIndex.get(Constants.ESTIMATED_REMAINING)] = point.getEstimatedRemaining();
        rowValues[columnNameToIndex.get(Constants.MEGABYTES_PER_SECOND)] = point.getMegabytesPerSecond();
        rowValues[columnNameToIndex.get(Constants.CALLS_PER_SECOND)] = point.getInvocationsPerSecond();
        rowValues[columnNameToIndex.get(Constants.CALLS_LATENCY)] = point.getAverageInvocationLatency();
        rowValues[columnNameToIndex.get(Constants.CALLS_TIME)] = point.getAverageInvocationTime();
        rowValues[columnNameToIndex.get(Constants.CALLS_TRANSFER_TIME)] = point.getAverageInvocationTransferTime();
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

        /// Name for logging, etc..
        private final String name;
        /// Start time in nanoseconds.
        private final long startTimeNanos;
        /// # of ranges to move.
        private final long totalRanges;
        /// End time in nanoseconds.
        private final long endTimeNanos;
        /// # of ranges transferred.
        private final long movedRanges;
        /// # of rows transferred.
        private final long movedRows;
        /// # of bytes transferred.
        private final long movedBytes;
        /// # of calls.
        private final long invocationCount;
        // Nanoseconds waiting on sysproc call
        private final long invocationLatencyNanos;
        // Nanoseconds spent executing sysproc
        private final long invocationTimeNanos;
        // Nanosecond spent transferring data in sysproc
        private final long invocationTransferTimeNanos;

        /**
         * Scratch constructor.
         * Default to the current time for start/end. Clear raw statistics.
         * @param name          stats point name
         * @param totalRanges   total ranges to move
         */
        public StatsPoint(String name, long totalRanges)
        {
            this(name, null, null, totalRanges, 0, 0, 0, 0, 0, 0, 0);
        }

        /**
         * Full constructor.
         * @param name                 stat point name
         * @param startTimeNanos       start time in nanoseconds
         * @param endTimeNanos         end time in nanoseconds
         * @param totalRanges          total ranges to move
         * @param movedRanges          moved range count
         * @param movedRows            moved row count
         * @param movedBytes           moved byte count
         * @param invocationCount      invocation count
         * @param invocationLatencyNanos  time spent waiting on sysproc
         */
        public StatsPoint(
                String name,
                Long startTimeNanos,    // can be null
                Long endTimeNanos,      // can be null
                long totalRanges,
                long movedRanges,
                long movedRows,
                long movedBytes,
                long invocationCount,
                long invocationLatencyNanos,
                long invocationTimeNanos,
                long invocationTransferTimeNanos)
        {
            // Substitute the current time for null start or end times
            long nowNanos = System.nanoTime();
            this.name = name;
            this.startTimeNanos = startTimeNanos != null ? startTimeNanos : nowNanos;
            this.endTimeNanos = endTimeNanos != null ? endTimeNanos : nowNanos;
            this.totalRanges = totalRanges;
            this.movedRanges = movedRanges;
            this.movedRows = movedRows;
            this.movedBytes = movedBytes;
            this.invocationCount = invocationCount;
            this.invocationLatencyNanos = invocationLatencyNanos;
            this.invocationTimeNanos = invocationTimeNanos;
            this.invocationTransferTimeNanos = invocationTransferTimeNanos;
        }

        long getStartTimeMillis()
        {
            return startTimeNanos / TimeUnit.MILLISECONDS.toNanos(1);
        }

        long getStartTimeNanos()
        {
            return startTimeNanos;
        }

        long getEndTimeMillis()
        {
            return endTimeNanos / TimeUnit.MILLISECONDS.toNanos(1);
        }

        long getMovedRows()
        {
            return movedRows;
        }

        // Derive duration from start/end times.
        long getDurationMillis()
        {
            return getEndTimeMillis() - getStartTimeMillis();
        }

        public String getFormattedDuration()
        {
            return formatTimeInterval(getDurationMillis());
        }

        long getInvocationCount()
        {
            return invocationCount;
        }

        double getInvocationLatencyMillis()
        {
            return invocationLatencyNanos / (double)TimeUnit.MILLISECONDS.toNanos(1);
        }

        double getThroughput()
        {
            return getDurationMillis() == 0 ? 0.0 : movedBytes / getDurationMillis();
        }

        double getCompletedFraction()
        {
            return totalRanges == 0 ? 0.0 : (double)movedRanges / totalRanges;
        }

        public double getPercentageMoved()
        {
            return totalRanges == 0 ? 0.0 : (movedRanges / (double)totalRanges) * 100.0;
        }

        public String getFormattedPercentageMovedRate()
        {
            double nanos = getDurationMillis() * MILLISECONDS.toNanos(1);
            return MiscUtils.HumanTime.formatRate(getPercentageMoved(), nanos, "%");
        }

        public double getRowsPerSecond()
        {
            final double durationInSecs = getDurationMillis() / 1000.0;
            return getDurationMillis() == 0 ? 0.0 : movedRows / durationInSecs;
        }

        public String getFormattedEstimatedRemaining()
        {
            return formatTimeInterval(getEstimatedRemaining());
        }

        public double getEstimatedRemaining() {
            double estimatedRemaining = -1.0;
            if (movedRanges > 0) {
                estimatedRemaining = ((totalRanges * getDurationMillis()) / movedRanges) - getDurationMillis();
            }
            return estimatedRemaining;
        }

        public double getRangesPerSecond()
        {
            final double durationInSecs = getDurationMillis() / 1000.0;
            return getDurationMillis() == 0 ? 0.0 : movedRanges / durationInSecs;
        }

        public double getMegabytesPerSecond()
        {
            return getDurationMillis() == 0 ?
                    0.0 : (movedBytes / (1024.0 * 1024.0)) / (getDurationMillis() / 1000.0);
        }

        public double getInvocationsPerSecond()
        {
            final double durationInSecs = getDurationMillis() / 1000.0;
            return getDurationMillis() == 0 ? 0.0 : invocationCount / durationInSecs;
        }

        public double getAverageInvocationLatency()
        {
            //Convert to floating point millis
            return invocationCount == 0 ? 0.0 : getInvocationLatencyMillis() / (double)invocationCount;
        }

        public double getAverageInvocationTime() {
            return invocationCount == 0 ? 0.0 : TimeUnit.NANOSECONDS.toMillis(invocationTimeNanos) / (double)invocationCount;
        }

        public double getAverageInvocationTransferTime() {
            return invocationCount == 0 ? 0.0 : TimeUnit.NANOSECONDS.toMillis(invocationTransferTimeNanos) / (double)invocationCount;
        }

        public final static String formatTimeInterval(double dms)
        {
            long ldms = (long)dms;
            if (ldms < 0) ldms = 0;

            final long day = MILLISECONDS.toDays(ldms);
            final long hr  = MILLISECONDS.toHours(  ldms
                                                  - DAYS.toMillis(day));
            final long min = MILLISECONDS.toMinutes(  ldms
                                                    - DAYS.toMillis(day)
                                                    - HOURS.toMillis(hr));
            final long sec = MILLISECONDS.toSeconds(  ldms
                                                    - DAYS.toMillis(day)
                                                    - HOURS.toMillis(hr)
                                                    - MINUTES.toMillis(min));
            final long ms  = MILLISECONDS.toMillis(  ldms
                                                    - DAYS.toMillis(day)
                                                    - HOURS.toMillis(hr)
                                                    - MINUTES.toMillis(min)
                                                    - SECONDS.toMillis(sec));
            return String.format("%d %02d:%02d:%02d.%03d", day, hr, min, sec, ms);
        }

        /**
         * Update statistics.
         * @param lastTimeNanos           time in nanoseconds
         * @param lastInvocationLatencyNanos time spent while invoking the sysproc
         * @param lastMovedRanges         moved range count
         * @param lastMovedRows           moved row count
         * @param lastMovedBytes          moved byte count
         * @param lastInvocationCount     invocation count
         */
        public StatsPoint update(
                Long lastTimeNanos,
                long lastInvocationLatencyNanos,
                long lastInvocationTimeNanos,
                long lastInvocationTransferTimeNanos,
                long lastMovedRanges,
                long lastMovedRows,
                long lastMovedBytes,
                long lastInvocationCount)
        {
            return new StatsPoint(
                    this.name,
                    this.startTimeNanos,
                    lastTimeNanos != null ? lastTimeNanos : System.nanoTime(),
                    this.totalRanges,
                    this.movedRanges + lastMovedRanges,
                    this.movedRows + lastMovedRows,
                    this.movedBytes + lastMovedBytes,
                    this.invocationCount + lastInvocationCount,
                    this.invocationLatencyNanos + lastInvocationLatencyNanos,
                    this.invocationTimeNanos + lastInvocationTimeNanos,
                    this.invocationTransferTimeNanos + lastInvocationTransferTimeNanos);
        }

        /**
         * Capture a copy of the current stats plus an end time and a recent throughput.
         * @param name          stats point name
         * @param endTimeNanos  end time in nanoseconds
         * @return  immutable snapshot of stats point
         */
        public StatsPoint capture(String name, long endTimeNanos)
        {
            return new StatsPoint(
                    name,
                    startTimeNanos,
                    endTimeNanos,
                    totalRanges,
                    movedRanges,
                    movedRows,
                    movedBytes,
                    invocationCount,
                    invocationLatencyNanos,
                    invocationTimeNanos,
                    invocationTransferTimeNanos);

        }

        @Override
        public String toString()
        {
            return String.format("StatsPoint(%s): "
                    +   "duration=%.2f s"
                    + ", percent=%.2f%% (%s)"
                    + ", rows=%d @ %.2f rows/second"
                    + ", bytes=%d @ %.2f MB/second"
                    + ", invocation=%.2f ms (%d @ %.2f ms latency %.2f ms execution time %.2f ms transfer time)",
                    name,
                    getDurationMillis() / 1000.0,
                    getPercentageMoved(), getFormattedPercentageMovedRate(),
                    movedRows, getRowsPerSecond(),
                    movedBytes, getMegabytesPerSecond(),
                    getInvocationLatencyMillis(), invocationCount,
                    getAverageInvocationLatency(), getAverageInvocationTime(), getAverageInvocationTransferTime());
        }
    }
}
