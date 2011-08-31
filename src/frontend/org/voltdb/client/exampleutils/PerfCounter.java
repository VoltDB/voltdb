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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltdb.client.ClientResponse;

public class PerfCounter implements Cloneable
{
    private final ThreadLocal<SimpleDateFormat> DateFormat = new ThreadLocal<SimpleDateFormat>()
    {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
            return format;
        }
    };

    private AtomicLong StartTime = new AtomicLong(0);
    private AtomicLong EndTime   = new AtomicLong(0);
    private AtomicLong min   = new AtomicLong(999999999l);
    private AtomicLong max   = new AtomicLong(-1l);
    private AtomicLong tot   = new AtomicLong(0);
    private AtomicLong cnt   = new AtomicLong(0);
    private AtomicLong err   = new AtomicLong(0);
    private AtomicLongArray lat = new AtomicLongArray(9);

    public PerfCounter() { this(true); }

    public long getStartTime()
    {
        return StartTime.get();
    }
    public long getEndTime()
    {
        return EndTime.get();
    }
    public long getMinLatency()
    {
        return min.get();
    }
    public long getMaxLatency()
    {
        return max.get();
    }
    public long getTotalExecutionDuration()
    {
        return tot.get();
    }
    public long getExecutionCount()
    {
        return cnt.get();
    }
    public long getErrorCount()
    {
        return cnt.get();
    }
    public long[] getLatencyBuckets()
    {
        return new long[]
        {
          this.lat.get(0)
        , this.lat.get(1)
        , this.lat.get(2)
        , this.lat.get(3)
        , this.lat.get(4)
        , this.lat.get(5)
        , this.lat.get(6)
        , this.lat.get(7)
        , this.lat.get(8)
        };
    }
    public long getElapsedDuration()
    {
        return this.EndTime.get()-this.StartTime.get();
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
            StartTime.set(System.currentTimeMillis());
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
        EndTime.set(System.currentTimeMillis());
        if (StartTime.get() == 0)
            StartTime.set(EndTime.get()-executionDuration);
        cnt.incrementAndGet();
        tot.addAndGet(executionDuration);
        if (min.get() > executionDuration)
            min.set(executionDuration);
        if (max.get() < executionDuration)
            max.set(executionDuration);
        lat.incrementAndGet(Math.min((int) (executionDuration / 25l),8));
        if (!success)
            err.incrementAndGet();
    }
    @Override
    public String toString()
    {
        return toString(true);
    }
    public String toString(boolean useSimpleFormat)
    {
        long elapsedDuration = this.EndTime.get()-this.StartTime.get();
        if (useSimpleFormat)
            return String.format(
                                  "%8s | Txn.: %,11d%s @ %,11.1f TPS | Lat. = %7d <  %7.2f < %7d\n"
                                , this.DateFormat.get().format(new Date(Math.round(elapsedDuration/1000d)*1000l))
                                , this.cnt.get()
                                , this.err.get() > 0 ? String.format(" [!%,11d]", this.err.get()) : ""
                                , (this.cnt.get()*1000d / (double)elapsedDuration)
                                , this.min.get() == 999999999l ? 0l : this.min.get()
                                , (double)this.tot.get()/(double)this.cnt.get()
                                , this.max.get() == -1l ? 0l : this.max.get()
                                );
        else
            return String.format(
                                   "-------------------------------------------------------------------------------------\nFinal:   | Txn.: %,11d%s @ %,11.1f TPS | Lat. = %7d <  %7.2f < %7d\n-------------------------------------------------------------------------------------\nLat.:     25 <     50 <     75 <    100 <    125 <    150 <    175 <    200 <    200+\n-------------------------------------------------------------------------------------\n%%     %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f\n"
                                , this.cnt.get()
                                , this.err.get() > 0 ? String.format(" [!%,11d]", this.err.get()) : ""
                                , (this.cnt.get()*1000d / elapsedDuration)
                                , this.min.get() == 999999999l ? 0l : this.min.get()
                                , (double)this.tot.get()/(double)this.cnt.get()
                                , this.max.get() == -1l ? 0l : this.max.get()
                                , 100*(double)this.lat.get(0)/this.cnt.get()
                                , 100*(double)this.lat.get(1)/this.cnt.get()
                                , 100*(double)this.lat.get(2)/this.cnt.get()
                                , 100*(double)this.lat.get(3)/this.cnt.get()
                                , 100*(double)this.lat.get(4)/this.cnt.get()
                                , 100*(double)this.lat.get(5)/this.cnt.get()
                                , 100*(double)this.lat.get(6)/this.cnt.get()
                                , 100*(double)this.lat.get(7)/this.cnt.get()
                                , 100*(double)this.lat.get(8)/this.cnt.get()
                                );
    }

    @Override
    public PerfCounter clone()
    {
        PerfCounter counter = new PerfCounter(false);
        counter.StartTime.set(this.StartTime.get());
        counter.EndTime.set(this.EndTime.get());
        counter.min.set(this.min.get());
        counter.max.set(this.max.get());
        counter.tot.set(this.tot.get());
        counter.cnt.set(this.cnt.get());
        counter.err.set(this.err.get());
        for(int i=0;i<9;i++)
            counter.lat.set(i, this.lat.get(i));
        return counter;
    }

    public PerfCounter merge(PerfCounter other)
    {
        this.StartTime.set(Math.min(this.StartTime.get(), other.StartTime.get()));
        this.EndTime.set(Math.max(this.EndTime.get(), other.EndTime.get()));
        this.min.set(Math.min(this.min.get(),other.min.get()));
        this.max.set(Math.max(this.max.get(),other.max.get()));
        this.tot.set(this.tot.get()+other.tot.get());
        this.cnt.set(this.cnt.get()+other.cnt.get());
        this.err.set(this.err.get()+other.err.get());
        for(int i=0;i<9;i++)
            this.lat.set(i, this.lat.get(i)+other.lat.get(i));
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
            diff.StartTime.set(previous.EndTime.get());
            diff.tot.set(this.tot.get()-previous.tot.get());
            diff.cnt.set(this.cnt.get()-previous.cnt.get());
            diff.err.set(this.err.get()-previous.err.get());
            for(int i=0;i<9;i++)
                diff.lat.set(i, this.lat.get(i)-previous.lat.get(i));
        }
        return diff;
    }
}

