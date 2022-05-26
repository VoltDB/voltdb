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

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.voltdb.client.ClientResponse;

/**
 * A thread-safe map of performance counters used to track procedure/statement execution statistics.
 *
 * @author Seb Coursol
 * @since 2.0
 */
public class PerfCounterMap
{
    private final ConcurrentHashMap<String,PerfCounter> Counters = new ConcurrentHashMap<String,PerfCounter>();

    /**
     * Gets a performance counter.
     *
     * @param counter the name of the counter to retrieve (typically the procedure name).  A new counter is created on the fly if none previously existed in the map.
     */
    public PerfCounter get(String counter)
    {
        // Admited: could get a little race condition at the very beginning, but all that'll happen is that we'll lose a handful of tracking event, a loss far outweighed by overall reduced contention.
        if (!this.Counters.containsKey(counter))
            this.Counters.put(counter, new PerfCounter(false));
        return this.Counters.get(counter);
    }

    /**
     * Tracks a call execution by processing the ClientResponse sent back by the VoltDB server.
     *
     * @param counter the name of the counter to update.
     * @param response the response sent by the VoltDB server, containing details about the procedure/statement execution.
     */
    public void update(String counter, ClientResponse response)
    {
        this.get(counter).update(response);
    }

    /**
     * Tracks a generic call execution by reporting the execution duration.  This method should be used for successful calls only.
     *
     * @param counter the name of the counter to update.
     * @param executionDuration the duration of the execution call to track in this counter.
     * @see #update(String counter, long executionDuration, boolean success)
     */
    public void update(String counter, long executionDuration)
    {
        this.get(counter).update(executionDuration);
    }

    /**
     * Tracks a generic call execution by reporting the execution duration.  This method should be used for successful calls only.
     *
     * @param counter the name of the counter to update.
     * @param executionDuration the duration of the execution call to track in this counter.
     * @param success the flag indicating whether the execution call was successful.
     */
    public void update(String counter, long executionDuration, boolean success)
    {
        this.get(counter).update(executionDuration, success);
    }

    /**
     * Gets a representation of this counter map as a list of single-line short-format strings detailing the statistics tracked by each counter available in this map.
     *
     * @return the string representation of this counter map.
     * @see #toString(boolean useSimpleFormat)
     */
    @Override
    public String toString()
    {
        return toString(true);
    }

    /**
     * Gets a representation of this counter map as a list of strings detailing the statistics tracked by each counter available in this map.
     *
     * @param useSimpleFormat the flag indicating whether to use a short one-line format, or detailed statistics including latency bucketing.
     * @return the string representation of this counter map.
     */
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

    /**
     * Gets the statistics as delimiter separated strings. Each line contains
     * statistics for a single procedure. There might be multiple lines.
     *
     * @param delimiter Delimiter, e.g. ',' or '\t'
     * @return
     */
    public String toRawString(char delimiter)
    {
        StringBuilder result = new StringBuilder();
        for (Entry<String, PerfCounter> e : Counters.entrySet()) {
            result.append(e.getKey())
                  .append(delimiter)
                  .append(e.getValue().toRawString(delimiter))
                  .append('\n');
        }
        return result.toString();
    }
}

