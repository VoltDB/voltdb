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

import java.util.concurrent.ConcurrentHashMap;

import org.voltdb.client.ClientResponse;

public class PerfCounterMap
{
    private final ConcurrentHashMap<String,PerfCounter> Counters = new ConcurrentHashMap<String,PerfCounter>();

    public PerfCounter get(String counter)
    {
        // Admited: could get a little race condition at the very beginning, but all that'll happen is that we'll lose a handful of tracking event, a loss far outweighed by overall reduced contention.
        if (!this.Counters.containsKey(counter))
            this.Counters.put(counter, new PerfCounter(false));
        return this.Counters.get(counter);
    }

    public void update(String counter, ClientResponse response)
    {
        this.get(counter).update(response);
    }
    public void update(String counter, long executionDuration)
    {
        this.get(counter).update(executionDuration);
    }
    public void update(String counter, long executionDuration, boolean success)
    {
        this.get(counter).update(executionDuration, success);
    }

    @Override
    public String toString()
    {
        return toString(true);
    }
    public String toString(boolean useSimpleFormat)
    {
        StringBuilder result = new StringBuilder();
        for(String counter : this.Counters.keySet())
        {
            if (useSimpleFormat)
                result.append(String.format("%1$-24s:", counter));
            else
                result.append(String.format("---- %1$-24s -------------------------------------------------------\n", counter));
            result.append(this.Counters.get(counter).toString(useSimpleFormat));
            result.append("\n\n");
        }
        return result.toString();
    }
}

