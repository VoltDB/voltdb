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
package org.voltdb.client.exampleutils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.voltdb.client.ClientResponse;

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
    private long[] lat = new long[9];

    public PerfCounter() { this(true); }

    public long getStartTime()
    {
        return StartTime;
    }
    public long getEndTime()
    {
        return EndTime;
    }
    public long getMinLatency()
    {
        return min;
    }
    public long getMaxLatency()
    {
        return max;
    }
    public long getTotalExecutionDuration()
    {
        return tot;
    }
    public long getExecutionCount()
    {
        return cnt;
    }
    public long getErrorCount()
    {
        return cnt;
    }
    public long[] getLatencyBuckets()
    {
        return lat;
    }
    public long getElapsedDuration()
    {
        return this.EndTime-this.StartTime;
    }

    public double getTransactionRatePerSecond()
    {
        return getExecutionCount()*1000d/getElapsedDuration();
    }

    public double getAverageLatency()
    {
        return (double)getTotalExecutionDuration()/(double)getExecutionCount();
    }

    public PerfCounter(boolean autoStart)
    {
        if (autoStart)
            StartTime = System.currentTimeMillis();
    }
    public void update(ClientResponse response)
    {
        this.update(response.getClientRoundtrip(), response.getStatus() == ClientResponse.SUCCESS);
    }
    public void update(long executionDuration)
    {
        this.update(executionDuration, true);
    }
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
            lat[Math.min((int)(executionDuration/25l),8)]++;
            if (!success)
                err++;
        }
        finally
        {
            lock.unlock();
        }
    }
    @Override
    public String toString()
    {
        return toString(true);
    }
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
        else
            return String.format(
                                   "-------------------------------------------------------------------------------------\nFinal:   | Txn.: %,11d%s @ %,11.1f TPS | Lat. = %7d <  %7.2f < %7d\n-------------------------------------------------------------------------------------\nLat.:     25 <     50 <     75 <    100 <    125 <    150 <    175 <    200 <    200+\n-------------------------------------------------------------------------------------\n%%     %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f\n"
                                , this.cnt
                                , this.err > 0 ? String.format(" [!%,11d]", this.err) : ""
                                , (this.cnt*1000d / elapsedDuration)
                                , this.min == 999999999l ? 0l : this.min
                                , (double)this.tot/(double)this.cnt
                                , this.max == -1l ? 0l : this.max
                                , 100*(double)this.lat[0]/this.cnt
                                , 100*(double)this.lat[1]/this.cnt
                                , 100*(double)this.lat[2]/this.cnt
                                , 100*(double)this.lat[3]/this.cnt
                                , 100*(double)this.lat[4]/this.cnt
                                , 100*(double)this.lat[5]/this.cnt
                                , 100*(double)this.lat[6]/this.cnt
                                , 100*(double)this.lat[7]/this.cnt
                                , 100*(double)this.lat[8]/this.cnt
                                );
    }

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
            for(int i=0;i<9;i++)
                this.lat[i] = this.lat[i]+other.lat[i];
        }
        finally
        {
            lock.unlock();
        }
        return this;
    }

    public static PerfCounter merge(PerfCounter[] counters)
    {
        PerfCounter counter = counters[0].clone();
        for(int i=1;i<counters.length;i++)
            counter.merge(counters[i]);
        return counter;
    }
    public PerfCounter difference(PerfCounter previous)
    {
        PerfCounter diff = this.clone();
        if (previous != null)
        {
            diff.StartTime = previous.EndTime;
            diff.tot = this.tot-previous.tot;
            diff.cnt = this.cnt-previous.cnt;
            diff.err = this.err-previous.err;
            for(int i=0;i<9;i++)
                diff.lat[i] = this.lat[i]-previous.lat[i];
        }
        return diff;
    }
}

