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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class FailedLoginCounter {
    // key is username, value is number of failed logging attempts count
    private Map<String,Integer> m_userFailedAttempts;
    // key is time bucket, value is username/failed logging attempts count pair
    private Map<Integer,Map<String,Integer>> m_buckets;

    final long ONE_MINUTE_IN_MILLIS = 60000;//millisecs

    public FailedLoginCounter() {
        m_buckets = new HashMap<Integer, Map<String,Integer>>();
        m_userFailedAttempts = new HashMap<String,Integer>();
    }

    public Map<String, Integer> getUserFailedAttempts() {
        return m_userFailedAttempts;
    }

    public Map<Integer, Map<String, Integer>> getTimeBuckets() {
        return m_buckets;
    }

    public int getCount(String user) {
        if (m_userFailedAttempts.containsKey(user)) {
            return m_userFailedAttempts.get(user);
        }
        return 0;
    }

    public void logMessage(int timestamp, String user) {
    	//checkCounter(timestamp);
        if (m_buckets.containsKey(timestamp)) {
            Map<String,Integer> bucket = m_buckets.get(timestamp);
            int bucketCount = bucket.getOrDefault(user,0) + 1;
              bucket.put(user,bucketCount);
        } else {
            m_buckets.put(timestamp, new HashMap<String,Integer>());
            m_buckets.get(timestamp).put(user, 1);
        }
        int totalCount = m_userFailedAttempts.getOrDefault(user,0) + 1;
        m_userFailedAttempts.put(user,totalCount);
    }

    // time is in seconds now
    public void checkCounter(int timestamp) {
        java.util.Iterator<Entry<Integer, Map<String, Integer>>> it = m_buckets.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, Map<String, Integer>> entry = it.next();
            long previousTimestamp = entry.getKey();
            if (previousTimestamp <= timestamp - ONE_MINUTE_IN_MILLIS) {
                Map<String,Integer> map = m_buckets.get(previousTimestamp);
                for (String user: map.keySet()) {
                    m_userFailedAttempts.put(user, m_userFailedAttempts.get(user) - map.get(user));
                }
                it.remove();
            } else {
                break;
            }
        }
    }
}
