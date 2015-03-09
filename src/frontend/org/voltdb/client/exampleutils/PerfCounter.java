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
package org.voltdb.client.exampleutils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.voltdb.client.ClientResponse;

/**
 * A thread-safe performance counter used to track procedure/statement execution statistics.
 *
 * @author Seb Coursol
 * @since 2.0
 */
public class PerfCounter implements Cloneable
{
    private static final Lock lock = new ReentrantLock();
    private long StartTime = Long.MAX_VALUE;
    private long EndTime   = Long.MIN_VALUE;
    private long min   = 999999999l;
    private long max   = -1l;
    private long tot   = 0l;
    private long cnt   = 0l;
    private long err   = 0l;

    /*
     * The buckets cover 0 - 500ms+.
     *
     * The first 100 buckets cover 0 - 100ms, 1ms each (e.g. (0, 1ms]). The next
     * 8 buckets cover 100 - 500ms, 50ms each. The last one covers 500ms+.
     */
    private long[] lat = new long[109];

    /**
     * Creates a new performance counter and immediately starts tracking time (for rate/second calculations).
     */
    public PerfCounter() { this(true); }

    /**
     * Creates a new performance counter, optionally starting time tracking.
     *
     * @param start the flag indicating whether time tracking should be started immediately.  When false time tracking is started on the first counter update.
     */
    public PerfCounter(boolean start)
    {
        if (start)
            StartTime = System.currentTimeMillis();
    }

    /**
     * Gets the time (in milliseconds since 1/1/1970 00:00 UTC) at which this counter's time tracking began (the time at which this counter was created - if created with the start parameter set to true - or the time of the first update).
     */
    public long getStartTime()
    {
        return StartTime;
    }

    /**
     * Gets the time (in milliseconds since 1/1/1970 00:00 UTC) at which this counter's time tracking ended (the time if the last update).
     */
    public long getEndTime()
    {
        return EndTime;
    }

    /**
     * Gets the minimum execution latency tracked by this counter.
     */
    public long getMinLatency()
    {
        return min;
    }

    /**
     * Gets the maximum execution latency tracked by this counter.
     */
    public long getMaxLatency()
    {
        return max;
    }

    /**
     * Gets the total execution duration tracked by this counter (the sum of execution durations of all calls tracked by this counter).
     */
    public long getTotalExecutionDuration()
    {
        return tot;
    }

    /**
     * Gets the number of execution calls tracked by this counter.
     */
    public long getExecutionCount()
    {
        return cnt;
    }

    /**
     * Gets the number of execution errors tracked by this counter.
     */
    public long getErrorCount()
    {
        return err;
    }

    /**
     * Gets the latency distribution buckets of execution calls tracked by this counter.
     */
    public long[] getLatencyBuckets()
    {
        return lat;
    }

    /**
     * Gets the elapsed duration between the start and end time of this counter.
     */
    public long getElapsedDuration()
    {
        return this.EndTime-this.StartTime;
    }

    /**
     * Gets the average number of execution calls per second for all execution calls tracked by this counter.
     */
    public double getTransactionRatePerSecond()
    {
        return getExecutionCount()*1000d/getElapsedDuration();
    }

    /**
     * Gets the average execution latency for calls tracked by this counter.
     */
    public double getAverageLatency()
    {
        return (double)getTotalExecutionDuration()/(double)getExecutionCount();
    }

    /**
     * Tracks a call execution by processing the ClientResponse sent back by the VoltDB server.
     *
     * @param response the response sent by the VoltDB server, containing details about the procedure/statement execution.
     */
    public void update(ClientResponse response)
    {
        this.update(response.getClientRoundtrip(), response.getStatus() == ClientResponse.SUCCESS);
    }

    /**
     * Tracks a generic call execution by reporting the execution duration.  This method should be used for successful calls only.
     *
     * @param executionDuration the duration of the execution call to track in this counter.
     * @see #update(long executionDuration, boolean success)
     */
    public void update(long executionDuration)
    {
        this.update(executionDuration, true);
    }

    /**
     * Tracks a generic call execution by reporting the execution duration.  This method should be used for successful calls only.
     *
     * @param executionDuration the duration of the execution call to track in this counter.
     * @param success the flag indicating whether the execution call was successful.
     */
    public void update(long executionDuration, boolean success)
    {
        lock.lock();
        try
        {
            EndTime = System.currentTimeMillis();
            if (StartTime == Long.MAX_VALUE)
                StartTime = EndTime-executionDuration;
            cnt++;
            tot+= executionDuration;
            if (min > executionDuration)
                min = executionDuration;
            if (max < executionDuration)
                max = executionDuration;
            int bucket = (int) executionDuration;
            if (executionDuration > 100)
                bucket = Math.min((int)((executionDuration-100l)/50l),8) + 100;
            lat[bucket]++;
            if (!success)
                err++;
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Gets a representation of this counter as a single-line short-format string detailing the statistics tracked by this counter.
     *
     * @return the string representation of this counter.
     * @see #toString(boolean useSimpleFormat)
     */
    @Override
    public String toString()
    {
        return toString(true);
    }

    /**
     * Gets a representation of this counter as a string detailing the statistics tracked by this counter.
     *
     * @param useSimpleFormat the flag indicating whether to use a short one-line format, or detailed statistics including latency bucketing.
     * @return the string representation of this counter.
     */
    public String toString(boolean useSimpleFormat)
    {
        SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        long elapsedDuration = (this.StartTime == Long.MAX_VALUE) ? 1 : this.EndTime-this.StartTime;
        if (useSimpleFormat)
            return String.format(
                                  "%8s | Txn.: %,11d%s @ %,11.1f TPS | Lat. = %7d <  %7.2f < %7d\n"
                                , dateFormat.format(new Date(Math.round(elapsedDuration/1000d)*1000l))
                                , this.cnt
                                , this.err > 0 ? String.format(" [!%,11d]", this.err) : ""
                                , (this.cnt*1000d / (double)elapsedDuration)
                                , this.min == 999999999l ? 0l : this.min
                                , (double)this.tot/(double)this.cnt
                                , this.max == -1l ? 0l : this.max
                                );
        else {
            long[] coarseLat = new long[9];
            // Roll up the latencies below 100ms
            for (int i = 0; i < 4; i++) {
                for (int j = 0; j < 25; j++) {
                    coarseLat[i] += this.lat[i * 25 + j];
                }
            }
            // Roll up the rest
            for (int i = 4; i < 8; i++) {
                coarseLat[i] = this.lat[100 + i - 4];
            }
            for (int j = 104; j < this.lat.length; j++) {
                coarseLat[8] += this.lat[j];
            }

            return String.format(
                                 "-------------------------------------------------------------------------------------\n" +
                                 "Final:   | Txn.: %,11d%s @ %,11.1f TPS | Lat. = %7d <  %7.2f < %7d\n" +
                                 "-------------------------------------------------------------------------------------\n" +
                                 "Lat.:     25 <     50 <     75 <    100 <    150 <    200 <    250 <    300 <    300+\n" +
                                 "-------------------------------------------------------------------------------------\n" +
                                 "%%     %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f\n"
                                , this.cnt
                                , this.err > 0 ? String.format(" [!%,11d]", this.err) : ""
                                , (this.cnt*1000d / elapsedDuration)
                                , this.min == 999999999l ? 0l : this.min
                                , (double)this.tot/(double)this.cnt
                                , this.max == -1l ? 0l : this.max
                                , 100*(double)coarseLat[0]/this.cnt
                                , 100*(double)coarseLat[1]/this.cnt
                                , 100*(double)coarseLat[2]/this.cnt
                                , 100*(double)coarseLat[3]/this.cnt
                                , 100*(double)coarseLat[4]/this.cnt
                                , 100*(double)coarseLat[5]/this.cnt
                                , 100*(double)coarseLat[6]/this.cnt
                                , 100*(double)coarseLat[7]/this.cnt
                                , 100*(double)coarseLat[8]/this.cnt
                                );
        }
    }

    /**
     * Format the statistics into a delimiter separated string.
     *
     * Currently, the format is
     * "start (ms), end (ms), total proc calls, min lat., max lat., lat. buckets..."
     *
     * @param delimiter Delimiter to separate values, e.g. ',' or '\t'
     * @return
     */
    public String toRawString(char delimiter)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(getStartTime())
          .append(delimiter)
          .append(getEndTime())
          .append(delimiter)
          .append(getExecutionCount())
          .append(delimiter)
          .append(getMinLatency())
          .append(delimiter)
          .append(getMaxLatency());
        // There are 109 buckets
        for (long latency : getLatencyBuckets()) {
            sb.append(delimiter).append(latency);
        }

        return sb.toString();
    }

    /**
     * Clones this counter to provide a snapshot copy of statistics.
     *
     * @return the snapshot clone of this counter.
     */
    @Override
    public PerfCounter clone()
    {
        PerfCounter counter = new PerfCounter(false);
        counter.StartTime = this.StartTime;
        counter.EndTime = this.EndTime;
        counter.min = this.min;
        counter.max = this.max;
        counter.tot = this.tot;
        counter.cnt = this.cnt;
        counter.err = this.err;
        for(int i=0;i<9;i++)
            counter.lat[i] = this.lat[i];
        return counter;
    }

    /**
     * Merges the statistics of this counter with another counter.
     *
     * @param other the counter to merge statistics from.
     * @return the reference to the current counter after modification.  Useful for command-chaining.
     */
    public PerfCounter merge(PerfCounter other)
    {
        lock.lock();
        try
        {
            this.StartTime = Math.min(this.StartTime, other.StartTime);
            this.EndTime = Math.max(this.EndTime, other.EndTime);
            this.min = Math.min(this.min,other.min);
            this.max = Math.max(this.max,other.max);
            this.tot = this.tot+other.tot;
            this.cnt = this.cnt+other.cnt;
            this.err = this.err+other.err;
            for(int i=0;i<109;i++)
                this.lat[i] = this.lat[i]+other.lat[i];
        }
        finally
        {
            lock.unlock();
        }
        return this;
    }

    /**
     * Merges the statistics of a list of counters.
     *
     * @param counters the list of counters to merge statistics from.
     * @return the new counter containing aggregated statistics from all the provided counters.
     */
    public static PerfCounter merge(PerfCounter[] counters)
    {
        PerfCounter counter = counters[0].clone();
        for(int i=1;i<counters.length;i++)
            counter.merge(counters[i]);
        return counter;
    }

    /**
     * Performs a difference between the statistics of this counter and those of a previously cloned counter, for incremental statistics tracking.
     *
     * @param previous the counter to compare statistics with to generate a difference.
     * @return the new counter containing the incremental statistics between the two counters.  The original counter is left unchanged.
     */
    public PerfCounter difference(PerfCounter previous)
    {
        PerfCounter diff = this.clone();
        if (previous != null)
        {
            diff.StartTime = previous.EndTime;
            diff.tot = this.tot-previous.tot;
            diff.cnt = this.cnt-previous.cnt;
            diff.err = this.err-previous.err;
            for(int i=0;i<109;i++)
                diff.lat[i] = this.lat[i]-previous.lat[i];
        }
        return diff;
    }
}

