/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package callcenter;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Queue-like object that allows you to add items with a wall-clock
 * schedule for making them available to de-queue.
 *
 * This is often used to schedule some kind of event handling in the future.
 *
 * @param <T>
 */
public class DelayedQueue<T> {

    /** Map of ms timestamps to list of objects schedule for that timestamp */
    final NavigableMap<Long, Object[]> delayed = new TreeMap<>();
    /** size maintained separately for quick response */
    protected int m_size = 0;

    /**
     * Schedule an object to be available for polling at a given timestamp (ms).
     *
     * @param readyTs The ms timestamp when the event is safe to deliver.
     * @param value Object to deliver.
     */
    public void add(long readyTs, T value) {
        Object[] values = delayed.get(readyTs);

        // if one or more objects is already scheduled for given time
        if (values != null) {
            // make a list
            Object[] values2 = new Object[values.length + 1];
            values2[0] = value;
            for (int i = 0; i < values.length; i++) {
                values2[i + 1] = values[i];
            }
            values = values2;
        }
        else {
            values = new Object[] { value };
        }
        delayed.put(readyTs, values);
        m_size++;
    }

    /**
     * Return the next object that is safe for delivery or null
     * if there are no safe objects to deliver.
     *
     * Null response could mean empty, or could mean all objects
     * are scheduled for the future.
     *
     * @param systemCurrentTimeMillis The current time.
     * @return Object of type T.
     */
    public T nextReady(long systemCurrentTimeMillis) {
        if (delayed.size() == 0) {
            return null;
        }

        // no ready objects
        if (delayed.firstKey() > systemCurrentTimeMillis) {
            return null;
        }

        Entry<Long, Object[]> entry = delayed.pollFirstEntry();
        Object[] values = entry.getValue();

        @SuppressWarnings("unchecked")
        T value = (T) values[0];

        // if this map entry had multiple values, put all but one
        // of them back
        if (values.length > 1) {
            int prevLength = values.length;
            values = Arrays.copyOfRange(values, 1, values.length);
            assert(values.length == prevLength - 1);
            delayed.put(entry.getKey(), values);
        }

        m_size--;
        return value;
    }

    /**
     * Ignore any scheduled delays and return events in
     * schedule order until empty.
     */
    public T drain() {
        // just pretend it's the future. Woooooo.
        return nextReady(Long.MAX_VALUE);
    }

    /**
     * @return The number of events waiting to be delivered.
     */
    public int size() {
        // slow but correct code used for debugging
        /*int delayedCount = 0;
        for (Entry<Long, Object[]> entry : delayed.entrySet()) {
            delayedCount += entry.getValue().length;
        }
        return delayedCount;*/

        return m_size;
    }


}
