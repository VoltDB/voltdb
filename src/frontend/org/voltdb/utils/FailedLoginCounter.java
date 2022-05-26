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

public class FailedLoginCounter {
    public class TimeBucket {
        int m_totalFailedAttempts;
        long m_ts;
        Map<String, Integer> m_userFailedAttempts;
        Map<String, Integer> m_ipFailedAttempts;

        public TimeBucket () {
            m_userFailedAttempts = new HashMap<String, Integer>();
            m_ipFailedAttempts = new HashMap<String, Integer>();
        }
    }

    // key is username, value is number of failed logging attempts count
    private Map<String,Integer> m_userFailedAttempts;
    private Map<String,Integer> m_ipFailedAttempts;
    Deque<TimeBucket> m_timeBucketQueue;

    int m_totalFailedAttempts;
    final long ONE_MINUTE_IN_MILLIS = 60000;//millisecs
    private static final VoltLogger authLog = new VoltLogger("AUTH");

    public FailedLoginCounter() {
        m_timeBucketQueue = new LinkedList<TimeBucket>();
        m_userFailedAttempts = new HashMap<String,Integer>();
        m_ipFailedAttempts = new HashMap<String,Integer>();
    }

    public void logMessage(long timestampMilis, String user, String ip) {
        checkCounter(timestampMilis);
        long timestampSeconds = timestampMilis / 1000;
        if (!m_timeBucketQueue.isEmpty() && m_timeBucketQueue.peekLast().m_ts == timestampSeconds) {
            TimeBucket bucket = m_timeBucketQueue.peekLast();
            int bucketUserFailedCount = bucket.m_userFailedAttempts.getOrDefault(user, 0) + 1;
            bucket.m_userFailedAttempts.put(user,bucketUserFailedCount);
            int bucketIPFailedCount = bucket.m_ipFailedAttempts.getOrDefault(ip, 0) + 1;
            bucket.m_ipFailedAttempts.put(ip, bucketIPFailedCount);
            bucket.m_totalFailedAttempts++;
        } else {
            TimeBucket bucket = new TimeBucket();
            bucket.m_userFailedAttempts.put(user, 1);
            bucket.m_ipFailedAttempts.put(ip, 1);
            bucket.m_ts = timestampSeconds;
            bucket.m_totalFailedAttempts = 1;
            m_timeBucketQueue.offer(bucket);
        }
        int userFailedCount = m_userFailedAttempts.getOrDefault(user,0) + 1;
        String messageFormat = "User "+ user +" failed to authenticate %d times in last minute";
        authLog.rateLimitedInfo(10, messageFormat, userFailedCount);
        m_userFailedAttempts.put(user, userFailedCount);

        int ipFailedCount = m_ipFailedAttempts.getOrDefault(ip, 0) + 1;
        messageFormat = "IP address "+ ip +" failed to authenticate %d times in last minute";
        authLog.rateLimitedInfo(10, messageFormat, ipFailedCount);
        m_ipFailedAttempts.put(ip, ipFailedCount);

        m_totalFailedAttempts++;
        messageFormat = "Total failed logins: %d in last minute";
        authLog.rateLimitedInfo(60, messageFormat, m_totalFailedAttempts);
    }

    public void checkCounter(long timestamp) {
        while (!m_timeBucketQueue.isEmpty() && m_timeBucketQueue.peek().m_ts < (timestamp - ONE_MINUTE_IN_MILLIS)/1000) {
            TimeBucket tb = m_timeBucketQueue.poll();
            m_totalFailedAttempts -= tb.m_totalFailedAttempts;
            Iterator<Entry<String, Integer>> userIter = tb.m_userFailedAttempts.entrySet().iterator();
            while (userIter.hasNext()) {
                Entry<String, Integer> entry = userIter.next();
                String user = entry.getKey();
                int currentUserFailedAttempts = m_userFailedAttempts.get(user) - tb.m_userFailedAttempts.get(user);
                m_userFailedAttempts.put(user, currentUserFailedAttempts);
            }
            Iterator<Entry<String, Integer>> ipIter = tb.m_ipFailedAttempts.entrySet().iterator();
            while (ipIter.hasNext()) {
                Entry<String, Integer> entry = ipIter.next();
                String ip = entry.getKey();
                int currentIPFailedAttempts = m_ipFailedAttempts.get(ip) - tb.m_ipFailedAttempts.get(ip);
                m_ipFailedAttempts.put(ip, currentIPFailedAttempts);
            }
        }
    }

    public Map<String, Integer> getUserFailedAttempts() {
        return m_userFailedAttempts;
    }

    public Map<String, Integer> getIPFailedAttempts() {
        return m_ipFailedAttempts;
    }
}
