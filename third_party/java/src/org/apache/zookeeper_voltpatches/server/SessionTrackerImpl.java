/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper_voltpatches.server;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper_voltpatches.server.SessionTracker;
import org.apache.zookeeper_voltpatches.server.SessionTrackerImpl;
import org.apache.zookeeper_voltpatches.server.ZooTrace;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.SessionExpiredException;
import org.voltcore.logging.VoltLogger;

/**
 * This is a full featured SessionTracker. It tracks session in grouped by tick
 * interval. It always rounds up the tick interval to provide a sort of grace
 * period. Sessions are thus expired in batches made up of sessions that expire
 * in a given interval.
 */
public class SessionTrackerImpl implements SessionTracker {
    private static final VoltLogger LOG = new VoltLogger(SessionTrackerImpl.class.getSimpleName());

    ConcurrentHashMap<Long, Long> sessionsById = new ConcurrentHashMap<Long, Long>();

    SessionExpirer m_expirer;

    public SessionTrackerImpl(SessionExpirer expirer,
            ConcurrentHashMap<Long, Long> sessionsAndOwners)
    {
        m_expirer = expirer;
        sessionsById = sessionsAndOwners;
    }

    @Override
    public void dumpSessions(PrintWriter pwriter) {
        pwriter.print("Session Sets (");
        pwriter.println("):");
        ArrayList<Long> keys = new ArrayList<Long>(sessionsById.keySet());
        Collections.sort(keys);
        for (long sid : keys) {
            pwriter.print("Session ");
            pwriter.print("0x");
            pwriter.print(sid);
            pwriter.print(" owner ");
            pwriter.println(sessionsById.get(sid));
        }
    }

    @Override
    synchronized public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpSessions(pwriter);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }

    @Override
    public void removeSession(long sessionId) {
        if (!sessionsById.containsKey(sessionId)) {
            return;
        }
        long owner = sessionsById.remove(sessionId);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                    "SessionTrackerImpl --- Removing session 0x"
                    + Long.toHexString(sessionId) + " owner " + owner);
        }
    }

    @Override
    public void addSession(long id, long owner) {
        if (sessionsById.get(id) == null) {
            sessionsById.put(id, owner);
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                        "SessionTrackerImpl --- Adding session 0x"
                        + Long.toHexString(id) + " owner " + owner);
            }
        } else {
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                        "SessionTrackerImpl --- Existing session 0x"
                        + Long.toHexString(id) + " owner " + sessionsById.get(id));
            }
        }
    }

    @Override
    public void expireSessionsWithOwner(long owner) {
        Iterator<Map.Entry<Long,Long>> iter = sessionsById.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, Long> entry = iter.next();
            if (entry.getValue() == owner) {
                iter.remove();
                m_expirer.expire(entry.getKey());
            }
        }

    }
}
