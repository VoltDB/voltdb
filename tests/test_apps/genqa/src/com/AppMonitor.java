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

import java.util.Timer;
import java.util.TimerTask;
import org.voltdb.client.exampleutils.PerfCounter;

public class AppMonitor
{
    static class DisplayTask extends TimerTask
    {
        private final PerfCounter Counter;
        public DisplayTask(PerfCounter counter)
        {
            this.Counter = counter;
        }
        @Override
        public void run()
        {
            System.out.print(this.Counter);
        }
    }
    private final long RunDuration;
    private final long DisplayInterval;
    private final double StartTime;
    private double EndTime;

    private final PerfCounter Counter;
    private final Timer Timer;
    public AppMonitor(long runDuration, long displayInterval)
    {
        this.RunDuration = runDuration;
        this.DisplayInterval = displayInterval;
        this.StartTime = System.currentTimeMillis();
        this.EndTime = this.StartTime+this.RunDuration;
        this.Counter = new PerfCounter();
        this.Timer = new Timer();
        this.Timer.scheduleAtFixedRate(new DisplayTask(this.Counter), this.DisplayInterval, this.DisplayInterval);
    }
    public boolean updateCounter(long executionDuration)
    {
        this.Counter.update(executionDuration);
        return this.isRunning();
    }
    public boolean isRunning()
    {
        return System.currentTimeMillis() < this.EndTime;
    }
    public void stop()
    {
        this.Timer.cancel();
        this.EndTime = System.currentTimeMillis();
        System.out.print(this.Counter.toString(false));
    }
}

