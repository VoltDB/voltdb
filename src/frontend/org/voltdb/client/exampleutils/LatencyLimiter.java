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
package org.voltdb.client.exampleutils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * A rate limiter that monitors execution of a specific procedure to bind the execution rate with the purpose of achieving a specific target latency.
 *
 * @author Seb Coursol
 * @since 2.0
 */
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

    /**
     * Creates a new latency-based rate limiter.
     *
     * @param connection the connection for which execution will be monitored.  The limiter will actually monitor execution accross all existing connections with the same parameters by querying performance data from the connection pool (not just the provided connection), thus preventing the limiter to fall apart should a specific connection be closed and ensuring performance at a global level within the calling application.
     * @param procedure the name of the procedure to monitor.
     * @param targetLatency the desired latency to target (in milliseconds).
     * @param initialMaxProcessPerSecond the initial maximum number of requests the limiter should allow per second to gather a first batch of monitoring data to act against.
     */
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

    /**
     * {@inheritDoc}
     */
    public void throttle()
    {
        this.throttle(true);
    }

    /**
     * Throttle the execution process and re-adjust the rate requirement on the fly.  The limiter will automatically re-adjust the rate internally by using a basic {@link RateLimiter} after analysis of the latency data gathered from the performance tracking.
     *
     * @param verbose the flag indicating whether rate adjustment messages should be displayed.
     */
    public void throttle(boolean verbose)
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

                long elsum = 0;
                for (int i = 0; i < 25; i++) {
                    elsum += el[i];
                }

                long slsum = 0;
                for (int i = 0; i < 25; i++) {
                    slsum += sl[i];
                }

                // If most (97%) requests are below 25ms, fudge out observed latency to remove accidental outliers that would cause too much oscillation in latency targetting
                if (((double)(elsum-slsum)/(double)ec) > 0.97)
                {
                    long outlierExecutionDuration = 0;
                    long outlierExecutionCount = 0;
                    for(int i=25;i<109;i++)
                    {
                        outlierExecutionCount += (el[i]-sl[i]);
                        // buckets over 99 cover 50ms each
                        if (i >= 100)
                            outlierExecutionDuration += (el[i]-sl[i])*50l;
                        else
                            outlierExecutionDuration += (el[i]-sl[i]);
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

                if (verbose && oldRate != this.Rate)
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

