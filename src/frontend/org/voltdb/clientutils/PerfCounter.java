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
package org.voltdb.clientutils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class PerfCounter
{
    private static final SimpleDateFormat DateFormat;
    static
    {
        DateFormat = new SimpleDateFormat("HH:mm:ss");
        DateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    public AtomicLong StartTime = new AtomicLong(0);
    public AtomicLong EndTime   = new AtomicLong(0);
    public AtomicLong min   = new AtomicLong(999999999l);
    public AtomicLong max   = new AtomicLong(-1l);
    public AtomicLong tot   = new AtomicLong(0);
    public AtomicLong cnt   = new AtomicLong(0);
    public AtomicLong alat  = new AtomicLong(0);
    public AtomicLongArray lat = new AtomicLongArray(9);
    public PerfCounter() { this(true); }
    public PerfCounter(boolean autoStart)
    {
        if (autoStart)
            StartTime.set(System.currentTimeMillis());
    }
    public void update(long executionDuration)
    {
        EndTime.set(System.currentTimeMillis());
        if (StartTime.get() == 0)
            StartTime.set(EndTime.get());
        cnt.incrementAndGet();
        alat.incrementAndGet();
        tot.addAndGet(executionDuration);
        if (min.get() > executionDuration)
            min.set(executionDuration);
        if (max.get() < executionDuration)
            max.set(executionDuration);
        lat.incrementAndGet(Math.min((int) (executionDuration / 25l),8));
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
                                  "%8s | Txn.: %,11d @ %,11.1f TPS | Lat. = %7d <  %7.2f < %7d\n"
                                , DateFormat.format(new Date(Math.round(elapsedDuration/1000d)*1000l))
                                , this.cnt.get()
                                , (this.cnt.get()*1000f / (double)elapsedDuration)
                                , this.min.get()
                                , (double)this.tot.get()/(double)this.alat.get()
                                , this.max.get()
                                );
        else
            return String.format(
                                   "-------------------------------------------------------------------------------------\nFinal:   | Txn.: %,11d @ %,11.1f TPS | Lat. = %7d <  %7.2f < %7d\n-------------------------------------------------------------------------------------\nLat.:     25 <     50 <     75 <    100 <    125 <    150 <    175 <    200 <    200+\n-------------------------------------------------------------------------------------\n%%     %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f\n"
                                , this.cnt.get()
                                , (this.cnt.get()*1000f / elapsedDuration)
                                , this.min.get()
                                , (double)this.tot.get()/(double)this.alat.get()
                                , this.max.get()
                                , 100*(double)this.lat.get(0)/this.alat.get()
                                , 100*(double)this.lat.get(1)/this.alat.get()
                                , 100*(double)this.lat.get(2)/this.alat.get()
                                , 100*(double)this.lat.get(3)/this.alat.get()
                                , 100*(double)this.lat.get(4)/this.alat.get()
                                , 100*(double)this.lat.get(5)/this.alat.get()
                                , 100*(double)this.lat.get(6)/this.alat.get()
                                , 100*(double)this.lat.get(7)/this.alat.get()
                                , 100*(double)this.lat.get(8)/this.alat.get()
                                );
    }
}

