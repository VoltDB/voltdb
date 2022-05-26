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

package org.voltdb.elastic;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltdb.StatsSource;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.utils.MiscUtils;

public class BalancePartitionsStatistics extends StatsSource {
    private static final VoltLogger log = new VoltLogger("ELASTIC");

    private static final long s_logIntervalNanos = TimeUnit.SECONDS.toNanos(120);

    long m_totalRangeSize;

    long m_lastReportTime;
    long m_lastBalanceDuration = 0;
    long m_balanceStart = 0;

    // Bytes transferred in each @BalancePartitions call in the past second.
    ArrayDeque<DataTransfered> m_bytesTransferredInLastSec = new ArrayDeque<>();
    long m_throughput = 0;
    long m_lastTransferTimeNanos = 0;

    private volatile StatsPoint m_statsPoint;
    private StatsPoint m_intervalStats;
    private StatsPoint m_overallStats;

    public enum Rebalance {
        TIMESTAMP               (VoltType.BIGINT),
        PERCENTAGE_MOVED        (VoltType.FLOAT),
        MOVED_ROWS              (VoltType.BIGINT),
        ROWS_PER_SECOND         (VoltType.FLOAT),
        ESTIMATED_REMAINING     (VoltType.BIGINT),
        MEGABYTES_PER_SECOND    (VoltType.FLOAT),
        CALLS_PER_SECOND        (VoltType.FLOAT),
        CALLS_LATENCY           (VoltType.FLOAT),
        CALLS_TIME              (VoltType.FLOAT),
        CALLS_TRANSFER_TIME     (VoltType.FLOAT);

        public final VoltType m_type;
        Rebalance(VoltType type) { m_type = type; }
    }

    public BalancePartitionsStatistics()
    {
        this(0L);
    }

    public BalancePartitionsStatistics(long totalRangeSize) {
        super(false);
        initialize(totalRangeSize);
    }

    public void initialize(long totalRangeSize) {
        m_overallStats = new StatsPoint("Overall", totalRangeSize);

        m_totalRangeSize = totalRangeSize;
        m_lastReportTime = m_overallStats.getStartTimeNanos();
        m_lastBalanceDuration = 0;
        m_throughput = 0;

        startInterval();

        m_statsPoint = new StatsPoint("Point", totalRangeSize);

        m_bytesTransferredInLastSec.clear();
    }

    public void logBalanceStarts()
    {
        m_balanceStart = System.nanoTime();
    }

    public void logBalanceEnds(long rangeSizeMoved, long bytesTransferred,
                               long callTimeNanos, long transferTimeNanos, long rowsTransferred) {
        final long balanceEnd = System.nanoTime();
        m_lastBalanceDuration = balanceEnd - m_balanceStart;

        final long aSecondAgo = balanceEnd - TimeUnit.SECONDS.toNanos(1);
        m_bytesTransferredInLastSec.add(new DataTransfered(balanceEnd, bytesTransferred));

        // remove entries older than a second
        m_throughput += bytesTransferred;
        while (m_bytesTransferredInLastSec.peekFirst().m_timestamp < aSecondAgo) {
            m_throughput -= m_bytesTransferredInLastSec.pollFirst().m_bytesTransferred;
        }

        m_lastTransferTimeNanos = transferTimeNanos;

        m_overallStats = m_overallStats.update(balanceEnd,
                m_lastBalanceDuration,
                callTimeNanos,
                transferTimeNanos,
                rangeSizeMoved,
                rowsTransferred,
                bytesTransferred,
                1);
        m_intervalStats = m_intervalStats.update(balanceEnd,
                m_lastBalanceDuration,
                callTimeNanos,
                transferTimeNanos,
                rangeSizeMoved,
                rowsTransferred,
                bytesTransferred,
                1);

        markStatsPoint();

        // Close out the interval and log statistics every logIntervalSeconds seconds.
        if (balanceEnd - m_lastReportTime > s_logIntervalNanos && balanceEnd != m_lastReportTime) {
            m_lastReportTime = balanceEnd;
            endInterval();
        }
    }

    public long getThroughput()
    {
        return m_throughput;
    }

    private void startInterval()
    {
        this.m_intervalStats = new StatsPoint("Interval", m_totalRangeSize);
    }

    private void endInterval()
    {
        printLog();
    }

    public void printLog() {
        if (m_bytesTransferredInLastSec.isEmpty()) {
            log.info("No data has been migrated yet.");
        } else {
            log.info(String.format("JOIN PROGRESS SUMMARY: "
                                   + "time elapsed: %s  "
                                   + "amount completed: %.2f%%  "
                                   + "est. time remaining: %s",
                                   m_overallStats.getFormattedDuration(),
                                   m_overallStats.getCompletedFraction() * 100.0,
                                   m_overallStats.getFormattedEstimatedRemaining()));
            log.info(String.format("JOIN DIAGNOSTICS: %s", m_intervalStats.toString()));
            log.info(String.format("JOIN DIAGNOSTICS: %s", m_overallStats.toString()));
        }
        // Immediately start the next interval.
        this.startInterval();
    }

    // Mainly for testing.
    public StatsPoint getOverallStats()
    {
        return m_overallStats;
    }

    // Mainly for testing.
    public StatsPoint getIntervalStats()
    {
        return m_intervalStats;
    }

    public StatsPoint getLastStatsPoint()
    {
        return m_statsPoint;
    }

    private void markStatsPoint() {
        if (! m_bytesTransferredInLastSec.isEmpty()) {
            m_statsPoint = m_overallStats.capture(
                    "Point",
                    m_bytesTransferredInLastSec.peekLast().m_timestamp);
        }
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        for (Rebalance col : Rebalance.values()) {
            columns.add(new VoltTable.ColumnInfo(col.name(), col.m_type));
        }
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        final StatsPoint point = m_statsPoint;

        rowValues[Rebalance.TIMESTAMP.ordinal()] = System.currentTimeMillis();
        rowValues[Rebalance.PERCENTAGE_MOVED.ordinal()] = point.getPercentageMoved();
        rowValues[Rebalance.MOVED_ROWS.ordinal()] = point.getMovedRows();
        rowValues[Rebalance.ROWS_PER_SECOND.ordinal()] = point.getRowsPerSecond();
        rowValues[Rebalance.ESTIMATED_REMAINING.ordinal()] = point.getEstimatedRemaining();
        rowValues[Rebalance.MEGABYTES_PER_SECOND.ordinal()] = point.getMegabytesPerSecond();
        rowValues[Rebalance.CALLS_PER_SECOND.ordinal()] = point.getInvocationsPerSecond();
        rowValues[Rebalance.CALLS_LATENCY.ordinal()] = point.getAverageInvocationLatency();
        rowValues[Rebalance.CALLS_TIME.ordinal()] = point.getAverageInvocationTime();
        rowValues[Rebalance.CALLS_TRANSFER_TIME.ordinal()] = point.getAverageInvocationTransferTime();
        return Rebalance.values().length;
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        if (m_totalRangeSize > 0) {
            return Collections.singletonList((Object) 1L).iterator();
        } else {
            return Collections.emptyIterator();
        }
    }

    public static class StatsPoint implements Serializable {
        private static final long serialVersionUID = 2635982992941464809L;

        /// Name for logging, etc..
        private final String m_name;
        /// Start time in nanoseconds.
        private final long m_startTimeNanos;
        /// # of ranges to move.
        private final long m_totalRanges;
        /// End time in nanoseconds.
        private final long m_endTimeNanos;
        /// # of ranges transferred.
        private final long m_movedRanges;
        /// # of rows transferred.
        private final long m_movedRows;
        /// # of bytes transferred.
        private final long m_movedBytes;
        /// # of calls.
        private final long m_invocationCount;
        // Nanoseconds waiting on sysproc call
        private final long m_invocationLatencyNanos;
        // Nanoseconds spent executing sysproc
        private final long m_invocationTimeNanos;
        // Nanosecond spent transferring data in sysproc
        private final long m_invocationTransferTimeNanos;

        /**
         * Scratch constructor.
         * Default to the current time for start/end. Clear raw statistics.
         * @param name          stats point name
         * @param totalRanges   total ranges to move
         */
        public StatsPoint(String name, long totalRanges) {
            this(name, null, null, totalRanges,
                    0, 0, 0, 0,
                    0, 0, 0);
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
                long invocationTransferTimeNanos) {
            // Substitute the current time for null start or end times
            long nowNanos = System.nanoTime();
            m_name = name;
            m_startTimeNanos = startTimeNanos != null ? startTimeNanos : nowNanos;
            m_endTimeNanos = endTimeNanos != null ? endTimeNanos : nowNanos;
            m_totalRanges = totalRanges;
            m_movedRanges = movedRanges;
            m_movedRows = movedRows;
            m_movedBytes = movedBytes;
            m_invocationCount = invocationCount;
            m_invocationLatencyNanos = invocationLatencyNanos;
            m_invocationTimeNanos = invocationTimeNanos;
            m_invocationTransferTimeNanos = invocationTransferTimeNanos;
        }

        public long getStartTimeMillis()
        {
            return m_startTimeNanos / TimeUnit.MILLISECONDS.toNanos(1);
        }

        long getStartTimeNanos()
        {
            return m_startTimeNanos;
        }

        public long getEndTimeMillis()
        {
            return m_endTimeNanos / TimeUnit.MILLISECONDS.toNanos(1);
        }

        long getMovedRows()
        {
            return m_movedRows;
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
            return m_invocationCount;
        }

        double getInvocationLatencyMillis() {
            return m_invocationLatencyNanos / (double)TimeUnit.MILLISECONDS.toNanos(1);
        }

        double getThroughput()
        {
            return getDurationMillis() == 0 ? 0.0 : (float) m_movedBytes / getDurationMillis();
        }

        double getCompletedFraction()
        {
            return m_totalRanges == 0 ? 0.0 : (double) m_movedRanges / m_totalRanges;
        }

        public double getPercentageMoved() {
            return m_totalRanges == 0 ? 0.0 : (m_movedRanges / (double) m_totalRanges) * 100.0;
        }

        public String getFormattedPercentageMovedRate() {
            double nanos = getDurationMillis() * MILLISECONDS.toNanos(1);
            return MiscUtils.HumanTime.formatRate(getPercentageMoved(), nanos, "%");
        }

        public double getRowsPerSecond() {
            final double durationInSecs = getDurationMillis() / 1000.0;
            return getDurationMillis() == 0 ? 0.0 : m_movedRows / durationInSecs;
        }

        public String getFormattedEstimatedRemaining()
        {
            return formatTimeInterval(getEstimatedRemaining());
        }

        public double getEstimatedRemaining() {
            double estimatedRemaining = -1.0;
            if (m_movedRanges > 0) {
                estimatedRemaining = ((m_totalRanges * getDurationMillis()) / (float) m_movedRanges) - getDurationMillis();
            }
            return estimatedRemaining;
        }

        public double getRangesPerSecond() {
            final double durationInSecs = getDurationMillis() / 1000.0;
            return getDurationMillis() == 0 ? 0.0 : m_movedRanges / durationInSecs;
        }

        public double getMegabytesPerSecond() {
            return getDurationMillis() == 0 ?
                    0.0 : (m_movedBytes / (1024.0 * 1024.0)) / (getDurationMillis() / 1000.0);
        }

        public double getInvocationsPerSecond() {
            final double durationInSecs = getDurationMillis() / 1000.0;
            return getDurationMillis() == 0 ? 0.0 : m_invocationCount / durationInSecs;
        }

        public double getAverageInvocationLatency() {
            //Convert to floating point millis
            return m_invocationCount == 0 ? 0.0 : getInvocationLatencyMillis() / m_invocationCount;
        }

        public double getAverageInvocationTime() {
            return m_invocationCount == 0 ? 0.0 : TimeUnit.NANOSECONDS.toMillis(m_invocationTimeNanos) / (double) m_invocationCount;
        }

        public double getAverageInvocationTransferTime() {
            return m_invocationCount == 0 ? 0.0 : TimeUnit.NANOSECONDS.toMillis(m_invocationTransferTimeNanos) / (double) m_invocationCount;
        }

        public static String formatTimeInterval(double dms) {
            long ldms = (long)dms;
            if (ldms < 0) {
                ldms = 0;
            }

            final long day = MILLISECONDS.toDays(ldms);
            final long hr  = MILLISECONDS.toHours(ldms
                    - DAYS.toMillis(day));
            final long min = MILLISECONDS.toMinutes(ldms
                    - DAYS.toMillis(day)
                    - HOURS.toMillis(hr));
            final long sec = MILLISECONDS.toSeconds(ldms
                    - DAYS.toMillis(day)
                    - HOURS.toMillis(hr)
                    - MINUTES.toMillis(min));
            final long ms  = MILLISECONDS.toMillis(ldms
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
                long lastInvocationCount) {
            return new StatsPoint(
                    m_name,
                    m_startTimeNanos,
                    lastTimeNanos != null ? lastTimeNanos : System.nanoTime(),
                    m_totalRanges,
                    m_movedRanges + lastMovedRanges,
                    m_movedRows + lastMovedRows,
                    m_movedBytes + lastMovedBytes,
                    m_invocationCount + lastInvocationCount,
                    m_invocationLatencyNanos + lastInvocationLatencyNanos,
                    m_invocationTimeNanos + lastInvocationTimeNanos,
                    m_invocationTransferTimeNanos + lastInvocationTransferTimeNanos);
        }

        /**
         * Capture a copy of the current stats plus an end time and a recent throughput.
         * @param name          stats point name
         * @param endTimeNanos  end time in nanoseconds
         * @return  immutable snapshot of stats point
         */
        public StatsPoint capture(String name, long endTimeNanos) {
            return new StatsPoint(
                    name,
                    m_startTimeNanos,
                    endTimeNanos,
                    m_totalRanges,
                    m_movedRanges,
                    m_movedRows,
                    m_movedBytes,
                    m_invocationCount,
                    m_invocationLatencyNanos,
                    m_invocationTimeNanos,
                    m_invocationTransferTimeNanos);

        }

        @Override
        public String toString() {
            return String.format("StatsPoint(%s): "
                    +   "duration=%.2f s"
                    + ", percent=%.2f%% (%s)"
                    + ", rows=%d @ %.2f rows/second"
                    + ", bytes=%d @ %.2f MB/second"
                    + ", invocation=%.2f ms (%d @ %.2f ms latency %.2f ms execution time %.2f ms transfer time)",
                    m_name,
                    getDurationMillis() / 1000.0,
                    getPercentageMoved(), getFormattedPercentageMovedRate(),
                    m_movedRows, getRowsPerSecond(),
                    m_movedBytes, getMegabytesPerSecond(),
                    getInvocationLatencyMillis(), m_invocationCount,
                    getAverageInvocationLatency(), getAverageInvocationTime(), getAverageInvocationTransferTime());
        }
    }

    private static final class DataTransfered {
        final long m_timestamp;
        final long m_bytesTransferred;

        public DataTransfered(long m_timestamp, long m_bytesTransferred) {
            super();
            this.m_timestamp = m_timestamp;
            this.m_bytesTransferred = m_bytesTransferred;
        }
    }
}
