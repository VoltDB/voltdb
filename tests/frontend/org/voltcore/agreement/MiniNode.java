/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltcore.agreement;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.agreement.FakeMesh.Message;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.FailureSiteUpdateMessage;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;

class MiniNode extends Thread
{
    final VoltLogger m_nodeLog;
    final static long TIMEOUT = 30 * 1000; // 30 seconds for now

    static class DeadHostTracker
    {
        final private long m_timeout;
        private Map<Long, Long> m_lastTimeForHosts = new HashMap<Long, Long>();

        DeadHostTracker(long timeout)
        {
            m_timeout = timeout;
        }

        void startTracking(long HSId)
        {
            m_lastTimeForHosts.put(HSId, System.currentTimeMillis());
        }

        void updateHSId(long HSId)
        {
            m_lastTimeForHosts.put(HSId, System.currentTimeMillis());
        }

        void stopTracking(long HSId)
        {
            m_lastTimeForHosts.remove(HSId);
        }

        Set<Long> checkTimeouts()
        {
            Set<Long> results = new HashSet<Long>();
            long now = System.currentTimeMillis();
            for (Entry<Long, Long> e : m_lastTimeForHosts.entrySet()) {
                long delta = now - e.getValue();
                if (delta > m_timeout) {
                    results.add(e.getKey());
                }
            }

            return results;
        }
    }

    final private long m_HSId;
    final private Set<Long> m_HSIds = new HashSet<Long>();
    final private FakeMesh m_mesh;
    final private MiniMailbox m_mailbox;
    MiniSite m_miniSite;
    DeadHostTracker m_deadTracker;

    private final Queue<Message> m_sendQ = new ConcurrentLinkedQueue<Message>();
    private final Queue<Message> m_recvQ = new ConcurrentLinkedQueue<Message>();
    AtomicBoolean m_shouldContinue = new AtomicBoolean(true);

    MiniNode(long HSId, Set<Long> HSIds, FakeMesh mesh)
    {
        m_nodeLog = new VoltLogger("MININODE-" + CoreUtils.hsIdToString(HSId));
        m_nodeLog.info("Constructing MiniNode for HSID: " + CoreUtils.hsIdToString(HSId));
        m_HSId = HSId;
        m_HSIds.addAll(HSIds);
        m_mesh = mesh;
        m_deadTracker = new DeadHostTracker(TIMEOUT);
        mesh.registerNode(m_HSId, m_sendQ, m_recvQ);
        m_mailbox = new MiniMailbox(m_HSId, m_sendQ);
        m_miniSite = new MiniSite(m_mailbox, HSIds, m_nodeLog);
    }

    void shutdown()
    {
        m_nodeLog.info("Shutting down...");
        m_miniSite.shutdown();
        try {
            m_miniSite.join();
        }
        catch (InterruptedException ie) {}
        m_mesh.unregisterNode(m_HSId);
        m_shouldContinue.set(false);
    }

    @Override
    public void run()
    {
        m_miniSite.start();
        for (long HSId : m_HSIds) {
            m_deadTracker.startTracking(HSId);
        }
        while (m_shouldContinue.get())
        {
            Message msg = m_recvQ.poll();
            if (msg != null) {
                m_deadTracker.updateHSId(msg.m_src);
                // inject actual message into mailbox
                VoltMessage message = msg.m_msg;
                m_mailbox.deliver(message);
                // snoop for FailureSiteUpdateMessages, inject into MiniSite's mailbox
                if (message instanceof FailureSiteUpdateMessage)
                {
                    for (long failedHostId : ((FailureSiteUpdateMessage)message).m_failedHSIds) {
                        long agreementHSId = CoreUtils.getHSIdFromHostAndSite((int)failedHostId,
                                HostMessenger.AGREEMENT_SITE_ID);
                        m_miniSite.reportFault(agreementHSId, false);
                    }
                }
            }
            // Do dead host detection.  Need to keep track of receive gaps from the remaining set
            // of live hosts.
            Set<Long> deadHosts = m_deadTracker.checkTimeouts();
            for (long HSId : deadHosts) {
                int failedHostId = CoreUtils.getHostIdFromHSId(HSId);
                long agreementHSId = CoreUtils.getHSIdFromHostAndSite(failedHostId,
                        HostMessenger.AGREEMENT_SITE_ID);
                m_miniSite.reportFault(agreementHSId, true);
                m_deadTracker.stopTracking(HSId);
            }
        }
    }
}
