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

import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.RateLimitedLogger;

public class FailedLoginCounter {
    public class TimeBucket {
        int m_totalFailedAttempts;
        int m_ts;
        Map<String, Integer> m_userFailedAttempts;
        public TimeBucket () {
            m_userFailedAttempts = new HashMap<String, Integer>();
        }
    }

    // key is username, value is number of failed logging attempts count
    private Map<String,Integer> m_userFailedAttempts;
    Deque<TimeBucket> m_timeBucketQueue;

    int totalFailedAttempts;
    final long ONE_MINUTE_IN_MILLIS = 60000;//millisecs
    private static final VoltLogger authLog = new VoltLogger("AUTH");

    public FailedLoginCounter() {
        m_timeBucketQueue = new LinkedList<TimeBucket>();
        m_userFailedAttempts = new HashMap<String,Integer>();
    }

    public Map<String, Integer> getUserFailedAttempts() {
        return m_userFailedAttempts;
    }

    public int getCount(String user) {
        if (m_userFailedAttempts.containsKey(user)) {
            return m_userFailedAttempts.get(user);
        }
        return 0;
    }

    public void logMessage(long timestampMilis, String user) {
        checkCounter((int)timestampMilis/1000);
        int timestampSeconds = (int)(timestampMilis/1000);
        if (m_timeBucketQueue.peekLast().m_ts == timestampSeconds) {
            TimeBucket bucket = m_timeBucketQueue.peekLast();
            int bucketCount = bucket.m_userFailedAttempts.getOrDefault(user,0) + 1;
            bucket.m_userFailedAttempts.put(user,bucketCount);
        } else {
            TimeBucket bucket = new TimeBucket();
            int bucketCount = bucket.m_userFailedAttempts.getOrDefault(user,0) + 1;
            bucket.m_userFailedAttempts.put(user,bucketCount);
        }
        int totalCount = m_userFailedAttempts.getOrDefault(user,0) + 1;
        String messageFormat = "user %s failed to authenticate %d times in last minute";
        RateLimitedLogger.tryLogForMessage(timestampMilis,
                ONE_MINUTE_IN_MILLIS,
                TimeUnit.MILLISECONDS,
                authLog,
                Level.WARN,
                messageFormat,
                user,
                totalCount);
        m_userFailedAttempts.put(user,totalCount);
    }

    public void checkCounter(int timestamp) {
        while (!m_timeBucketQueue.isEmpty() && m_timeBucketQueue.peek().m_ts < timestamp) {
            TimeBucket tb = m_timeBucketQueue.poll();
            totalFailedAttempts -= tb.m_totalFailedAttempts;
            Iterator<Entry<String, Integer>> it = tb.m_userFailedAttempts.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, Integer> entry = it.next();
                String user = entry.getKey();
                m_userFailedAttempts.put(user, m_userFailedAttempts.get(user) - tb.m_userFailedAttempts.get(user));
            }
        }
    }
}
