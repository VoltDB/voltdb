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

public class LatencyLimiter implements IRateLimiter
{
    private final ClientConnection Connection;
    private final double TargetLatency;
    private final String Procedure;
    private PerfCounter Start;
    private PerfCounter End;
    private long Rate;
    private final RateLimiter Limiter;
    private long LastCheck;
    private final SimpleDateFormat DateFormat;
    private final long StartTime;
    public LatencyLimiter(ClientConnection connection, String procedure, double targetLatency, long initialMaxProcessPerSecond)
    {
        this.Connection = connection;
        this.TargetLatency = targetLatency;
        this.Procedure = procedure;
        this.Start = (PerfCounter)ClientConnectionPool.getStatistics(this.Connection).get(this.Procedure).clone();
        this.End = ClientConnectionPool.getStatistics(this.Connection).get(this.Procedure);
        this.Rate = initialMaxProcessPerSecond;
        this.Limiter = new RateLimiter(this.Rate);
        this.LastCheck = System.currentTimeMillis();
        this.DateFormat = new SimpleDateFormat("HH:mm:ss");
        this.DateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        this.StartTime = System.currentTimeMillis();
    }

    public void throttle()
    {
        this.throttle(true);
    }
    public void throttle(boolean showTuningMessages)
    {
        // Observe latency to adjust the rate to reach target latency
        if (System.currentTimeMillis() - this.LastCheck > 5000l)
        {
            if (this.End.getExecutionCount()-this.Start.getExecutionCount() > 0)
            {
                double observedLatency = (double)(this.End.getTotalExecutionDuration()-this.Start.getTotalExecutionDuration())/(double)(this.End.getExecutionCount()-this.Start.getExecutionCount());
                double tuningLatency = observedLatency;
                long[] el = this.End.getLatencyBuckets();
                long[] sl = this.Start.getLatencyBuckets();
                long ec = this.End.getExecutionCount()-this.Start.getExecutionCount();

                // If most (97%) requests are in the fastest latency bucket, fudge out observed latency to remove accidental outliers that would cause too much oscillation in latency targetting
                if (((double)(el[0]-sl[0])/(double)ec) > 0.97)
                {
                    long outlierExecutionDuration = 0;
                    long outlierExecutionCount = 0;
                    for(int i=1;i<9;i++)
                    {
                        outlierExecutionCount += (el[i]-sl[i]);
                        outlierExecutionDuration += (el[i]-sl[i])*25l;
                    }
                    tuningLatency = (double)(this.End.getTotalExecutionDuration()-this.Start.getTotalExecutionDuration()-outlierExecutionDuration)/(double)(this.End.getExecutionCount()-this.Start.getExecutionCount()-outlierExecutionCount);
                }

                long oldRate = this.Rate;
                if (tuningLatency > this.TargetLatency*2.0)
                    this.Rate = (long)(this.Rate*0.8);
                else if (tuningLatency > this.TargetLatency*1.25)
                    this.Rate = (long)(this.Rate*0.95);
                else if (tuningLatency > this.TargetLatency*1.1)
                    this.Rate = (long)(this.Rate*0.999);
                else if (tuningLatency < this.TargetLatency*0.5)
                    this.Rate = (long)(this.Rate*1.1);
                else if (tuningLatency < this.TargetLatency*0.75)
                    this.Rate = (long)(this.Rate*1.01);
                else if (tuningLatency < this.TargetLatency*0.9)
                    this.Rate = (long)(this.Rate*1.001);

                if (showTuningMessages && oldRate != this.Rate)
                    System.out.printf(
                      "%8s | Adjusting %s to:  %,11.1f TPS | Recent Latency :  %7.2f\n"
                    , this.DateFormat.format(new Date(Math.round((System.currentTimeMillis()-this.StartTime)/1000d)*1000l))
                    , (oldRate < this.Rate ? " UP " : "DOWN")
                    , (double)this.Rate
                    , tuningLatency
                    );
            }
            this.Start = (PerfCounter)this.End.clone();
            this.End = ClientConnectionPool.getStatistics(this.Connection).get(this.Procedure);
            this.LastCheck = System.currentTimeMillis();
        }
        this.Limiter.throttle(this.Rate);
    }
}

