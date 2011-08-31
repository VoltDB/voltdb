/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package com;

import java.util.ArrayList;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.exampleutils.MathEx;

public abstract class ClientThread implements Runnable
{
    private static int IdGenerator = 1;
    private final int Id;

    private final AppMonitor Monitor;
    protected Client Client;
    protected Random Rand = new Random();

    private final long SleepTime;
    private final int StepCount;
    private final Long[] CycleStepDuration;
    private final Long[] CycleStepMaxCount;
    private final Long[] CycleStepCount;
    private final Long[] CycleStepEndTime;
    public ClientThread(AppMonitor monitor, Client client, long maxProcessPerSecond)
    {
        this.Monitor = monitor;
        this.Client = client;
        this.Id = IdGenerator++;

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
    }

    protected void updateCounter(long executionDuration)
    {
        this.Monitor.updateCounter(executionDuration);
    }

    @Override
    public void run()
    {
        try
        {
            // Initialize run
            this.initialize();

            // Initialize cycle end times
            for(int i=0;i<this.StepCount;i++)
                this.CycleStepEndTime[i] = System.currentTimeMillis() + this.CycleStepDuration[i];

            // Process One item at a time in an infinite loop until the monitor declares the application should end.
            while (this.Monitor.isRunning())
            {
                // Perform one process call
                this.processOne();

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
            }

            // Finalize run
            this.finalize();
        }
        catch(Exception e1)
        {
            System.out.printf("Thread %d Failed",this.Id);
            e1.printStackTrace();
        }
        finally
        {
            try { this.Client.drain(); } catch(Exception e2) { }
            try { this.Client.close(); } catch(Exception e2) { }
        }

    }

    // Virtual method: Initialize run
    public void initialize() throws Exception {}
    // Abstract method: Process One iteration
    public abstract void processOne() throws Exception;
    // Virtual method: Finalize run
    @Override
    public void finalize() throws Exception {}
}

