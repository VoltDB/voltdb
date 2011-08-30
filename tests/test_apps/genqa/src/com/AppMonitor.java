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

