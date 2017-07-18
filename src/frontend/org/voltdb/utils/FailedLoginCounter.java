/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.utils;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class FailedLoginCounter {
    // key is username, value is number of attempts
    private Map<String,Integer> userFailedAttempts;
    // key is time bucket, value is user attempts
    private Map<Long,Map<String,Integer>> buckets;

    final long ONE_MINUTE_IN_MILLIS = 60000;//millisecs

    public FailedLoginCounter() {
        buckets = new HashMap<Long, Map<String,Integer>>();
        userFailedAttempts = new HashMap<String,Integer>();
    }

    public Map<String, Integer> getUserFailedAttempts() {
        return userFailedAttempts;
    }

    public Map<Long, Map<String, Integer>> getTimeBuckets() {
        return buckets;
    }

    public int getCount(String user) {
        if (userFailedAttempts.containsKey(user)) {
            return userFailedAttempts.get(user);
        }
        return 0;
    }

    public void logMessage(long timestamp, String user) {
        System.out.println(Thread.currentThread().getId());
        if (buckets.containsKey(timestamp)) {
            Map<String,Integer> bucket = buckets.get(timestamp);
            int bucketCount = bucket.getOrDefault(user,0) + 1;
              bucket.put(user,bucketCount);
        } else {
            buckets.put(timestamp, new HashMap<String,Integer>());
            buckets.get(timestamp).put(user, 1);
        }
        int totalCount = userFailedAttempts.getOrDefault(user,0) + 1;
        userFailedAttempts.put(user,totalCount);
    }

    public void checkCounter(Long timestamp) throws ParseException {
        java.util.Iterator<Entry<Long, Map<String, Integer>>> it = buckets.entrySet().iterator();
        while(it.hasNext()) {
            Entry<Long, Map<String, Integer>> entry = it.next();
            long previousTimestamp = entry.getKey();
            if (previousTimestamp <= timestamp - ONE_MINUTE_IN_MILLIS) {
                Map<String,Integer> map = buckets.get(previousTimestamp);
                for (String user: map.keySet()) {
                    userFailedAttempts.put(user, userFailedAttempts.get(user) - map.get(user));
                }
                it.remove();
            } else {
                break;
            }
        }
    }
}
