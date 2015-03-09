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

import java.util.ArrayList;

/**
 * A simple rate limiter that can be used to bind an asynchronous application/benchmark to a maximum transactional rate, preventing firehosing or providing a fixed expected throughput for performance validation (latency review at specific rates).
 *
 * @author Seb Coursol
 * @since 2.0
 */
public class RateLimiter implements IRateLimiter
{
    private long SleepTime;
    private int StepCount;
    private Long[] CycleStepDuration;
    private Long[] CycleStepMaxCount;
    private Long[] CycleStepCount;
    private Long[] CycleStepEndTime;
    private long MaxProcessPerSecond = 0l;
    private long CycleAdjustment = Long.MIN_VALUE;
    private long LastCycleCount = Long.MAX_VALUE;

    /**
     * Creates a new rate limiter.
     *
     * @param maxProcessPerSecond the maximum number of requests the limiter should allow per second.
     */
    public RateLimiter(long maxProcessPerSecond)
    {
        this.set(maxProcessPerSecond, 0l, false);
    }

    /**
     * {@inheritDoc}
     */
    public void throttle()
    {
        this.throttle(this.MaxProcessPerSecond);
    }

    /**
     * Throttle the execution process and re-adjust the rate requirement on the fly.  This call allows the calling application to dynamically adjust the rate over time, for instance as a result of some external changes or analysis.  For an example of such an application, see {@link LatencyLimiter}.
     *
     * @param maxProcessPerSecond the maximum number of requests the limiter should allow per second.
     */
    public void throttle(long maxProcessPerSecond)
    {
        long adjustment = 0l;
        boolean forceSet = false;
        if (this.CycleAdjustment <= System.currentTimeMillis())
        {
            if ((this.MaxProcessPerSecond == maxProcessPerSecond) && (this.LastCycleCount != Long.MAX_VALUE))
            {
                    if (this.MaxProcessPerSecond > this.LastCycleCount)
                    {
                        adjustment = this.MaxProcessPerSecond - this.LastCycleCount;
                    }
                    else
                        forceSet = true;
            }
            this.CycleAdjustment = System.currentTimeMillis() + 1000l;
            this.LastCycleCount = 0l;
        }
        this.set(maxProcessPerSecond, adjustment, forceSet);
        try
        {
                // For rates below 1/ms a pre-empting sleep is the best way to approach the desired rate without
                // impacting latency (see note in loop below)
                if (this.SleepTime > 0)
                    Thread.sleep(this.SleepTime);

                for(int i=0;i<this.StepCount;i++)
                {
                    this.CycleStepCount[i]++;
                    if (this.CycleStepCount[i] >= this.CycleStepMaxCount[i])
                    {
                        // If the Cycle duration is more than 1 ms, we might have to wait for a considerable time.
                        // - sleeping is more CPU-efficient than spinning, so we'll sleep just a bit less so we're
                        // considerate without blowing our window.
                        // Because of Java's threading model, and the fact the callbacks are executed in the calling
                        // thread, low execution rates will experience artificial latency (to the order of a few
                        // milliseconds).
                        long sleepDuration = this.CycleStepEndTime[i]-System.currentTimeMillis()-1;
                        if (sleepDuration > 0)
                            Thread.sleep(sleepDuration);

                        // 1ms or less to wait - Spin until the time changes
                        while (System.currentTimeMillis() < this.CycleStepEndTime[i]);

                        // Reset counters
                        this.CycleStepEndTime[i] = System.currentTimeMillis() + this.CycleStepDuration[i];
                        this.CycleStepCount[i] = 0l;
                    }
                }

                this.LastCycleCount++;
        }
        catch(Exception tie) {}
    }

    /**
     * Adjusts the rate limit, whether through automatic adjustment based on monitoring of executed versus requested rates, or as a consequence of an external request to modify the expected rate.
     *
     * @param maxProcessPerSecond the requested maximum number of requests per second.
     * @param adjustment the adjustment calculated internally by the limiter by comparing actual executions versus the requested rate.
     * @param forceSet the flag indicating this call is a conseuqnce to an external rate-change request and the distribution of executions over time should be recalculated.
     */
    private void set(long maxProcessPerSecond, long adjustment, boolean forceSet)
    {
        if ((this.MaxProcessPerSecond != maxProcessPerSecond) || (adjustment != 0l) || forceSet)
        {
            this.MaxProcessPerSecond = maxProcessPerSecond;
            maxProcessPerSecond += adjustment;

            // Ruth's tests fail with a divide by zero (ENG-2426).
            // So the quick fix is to set maxProcessPerSecond to 1000 if it is rate limited to zero by the latency limiter.
            // Basically set a floor.
            if (0 == maxProcessPerSecond)
                maxProcessPerSecond=1000;

            // For rates below 1/ms we can sleep a while between each execution, which is the most efficient approach.
            this.SleepTime = (1000l/maxProcessPerSecond)-1l;

            // Calculate the largest cycle duration based on requested rate
            long gcd = MathEx.gcd(maxProcessPerSecond,1000l);

            // Calculate incremental sub-cycles that will be used to approach the desired rate (1s, 100ms, 10ms, 1ms (or a
            // similar variation if the largest cycle duration is less than 1s))
            double cycleDuration = (1000l/gcd);
            double cycleMaxCount = (maxProcessPerSecond/gcd);
            ArrayList<Long> cycleStepDuration = new ArrayList<Long>();
            ArrayList<Long> cycleStepMaxCount = new ArrayList<Long>();
            ArrayList<Long> cycleStepCount    = new ArrayList<Long>();
            while((cycleDuration >= 1d) && (cycleMaxCount > 0d))
            {
                cycleStepDuration.add((long)cycleDuration);
                cycleStepMaxCount.add((long)cycleMaxCount);
                cycleStepCount.add(0l);
                cycleDuration = cycleDuration/10d;
                cycleMaxCount = Math.ceil(cycleMaxCount/10d);
            }

            // Convert ArrayLists to arrays (easier/faster to work with)
            this.StepCount = cycleStepCount.size();
            this.CycleStepDuration = cycleStepDuration.toArray(new Long[this.StepCount]);
            this.CycleStepMaxCount = cycleStepMaxCount.toArray(new Long[this.StepCount]);
            this.CycleStepCount = cycleStepCount.toArray(new Long[this.StepCount]);
            this.CycleStepEndTime = cycleStepCount.toArray(new Long[this.StepCount]);

            // Initialize cycle end times
            for(int i=0;i<this.StepCount;i++)
                this.CycleStepEndTime[i] = System.currentTimeMillis() + this.CycleStepDuration[i];

        }
    }

}

