/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltcore.logging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.voltcore.utils.EstTime;

/*
 * This class provides rate-limiting support for
 * VoltLogger. It is not intended for independent
 * use.
 *
 * An instance of the class can answer a single
 * type of  question for VoltLogger: should I
 * log this string now?
 *
 * There is a fixed limit on the size of the
 * cache; if adding a new entry causes the limit
 * to be exceeded, we evict entries (based on
 * time since last logged) until below the limit.
 */
class LogRateLimiter {
    static final int MAXSIZE = 1000;

    // Basically a mutable integer
    private static final class Timestamp {
        volatile long value;
    }

    // Tracks last-logged times, in a map for fast lookup, and
    // in a queue ordered by time of logging ('age') for use in
    // cache eviction. Note it's possible for a given key to be
    // in the map but not in the queue for a brief period; the
    // reverse is not true.
    private final ConcurrentHashMap<String, Timestamp> whenLogged = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<String> ageOrder = new ConcurrentLinkedQueue<>();

    // Should we log this message now?
    // Updates the last-logged-time tracker appropriately.
    //
    // Concurrency:
    // - The map is safe for concurrent updates and inserts; newly-inserted
    //   keys will be immediately eligible for logging because the apparent
    //   time of last logging will be zero, guaranteed to be very long ago.
    // - The test for being able to log now is made outside of explicit
    //   synchronization, thus multiple threads can decide it is time to
    //   log the same message. But we then sync on the timestamp and test
    //   again, thus resolving the race (in arbitrary fashion).
    // - The update to the timestamp is made while synchronized on the
    //   timestamp, so is obviously safe.
    // - The update to the age-ordered list is also protected by timestamp
    //   synchronization; this is safe because there is a one-to-one relation
    //   between the string key and the timestamp, so no other thread can
    //   be modifying this list entry. The list itself permits concurrent
    //   modifications.
    // - There is a race between this routine moving the entry in the
    //   age-ordered list, and the cache eviction code, if the entry has
    //   the bad luck to be the oldest entry just as we decide to log it.
    //   This is structurally safe, but may result in erroneously permitting
    //   the next attempt to log the same string. This is not very different
    //   from aging out of the cache before the suppression interval expired;
    //   it is a symptom of a too-small cache.
    boolean shouldLog(String str, long interval) {
        boolean decision = false;
        try {
            long now = EstTime.currentTimeMillis();
            Timestamp ts = whenLogged.computeIfAbsent(str, k -> new Timestamp());
            if (now - ts.value > interval) {
                boolean newTs = false;
                synchronized (ts) {
                    if (now - ts.value > interval) {
                        newTs = (ts.value == 0);
                        if (newTs || ageOrder.remove(str)) {
                            ageOrder.add(str);
                        }
                        ts.value = now;
                        decision = true;
                    }
                }
                if (newTs) {
                    shrinkToLimit();
                }
            }
        }
        catch (Exception ex) {
            // Unexpected; but let's err on the side of logging it
            decision = true;
        }
        return decision;
    }

    // Ensure cache is within size limits, by removing entries that
    // have been the longest since doing any logging. The practical
    // result of this is, the next time we see that particular string,
    // it will be logged (and added back to the cache).
    //
    // Concurrency:
    // - The test for exceeding MAXSIZE is loose, since the map may
    //   be changing. We can exceed MAXSIZE by something like the
    //   number of threads that use rate-limited logging.
    // - It is possible that multiple threads will be executing this
    //   in parallel, thus we can decide to remove more elements than
    //   needed to get down to MAXSIZE.
    // - It is possible that we could remove the current oldest entry
    //   from the ageOrder queue just as another thread is about to
    //   move it to the end of the queue, having decided to log. The
    //   net result is that the entry won't be moved (shouldLog will
    //   not find it) and we will remove the map entry. This may allow
    //   the next logging attempt to succeed where it would otherwise
    //   be suppressed.
    // These effects are considered acceptable; MAXSIZE is a goal and
    // not a strict limit, and in any case is large relative to the
    // likely concurrency level.
    private void shrinkToLimit() {
        while (whenLogged.size() > MAXSIZE) {
            String key = ageOrder.poll();
            if (key == null) break; // empty list? unlikely race
            whenLogged.remove(key);
        }
    }
}
