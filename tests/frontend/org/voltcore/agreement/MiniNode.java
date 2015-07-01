/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.agreement.FakeMesh.Message;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.DisconnectFailedHostsCallback;
import org.voltcore.messaging.FaultMessage;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.SiteFailureForwardMessage;
import org.voltcore.messaging.SiteFailureMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;

class MiniNode extends Thread implements DisconnectFailedHostsCallback
{
    final VoltLogger m_nodeLog;
    final static long TIMEOUT = 10 * 1000; // 30 seconds for now

    public enum NodeState {
        START,
        RUN,
        RESOLVE,
        STOP
    }

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
            if (m_lastTimeForHosts.containsKey(HSId)) {
                m_lastTimeForHosts.put(HSId, System.currentTimeMillis());
            }
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
    private AtomicReference<NodeState> m_nodeState = new AtomicReference<NodeState>(NodeState.START);
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
        m_miniSite = new MiniSite(m_mailbox, HSIds, this, m_nodeLog);
    }

    synchronized void stopTracking(long HSId) {
        m_deadTracker.stopTracking(HSId);
        m_mesh.failLink(m_HSId, HSId);
        m_mesh.failLink(HSId, m_HSId);
        m_mailbox.deliver(m_miniSite.createSitePruneMessage(HSId));
    }

    synchronized void joinWith(long HSId) {
        m_HSIds.add(HSId);
        m_mailbox.deliver(m_miniSite.createSiteJoinMessage(HSId));
        m_deadTracker.startTracking(HSId);
    }

    void shutdown()
    {
        m_nodeLog.info("Shutting down...");
        m_nodeState.set(NodeState.STOP);
        m_miniSite.shutdown();
        try {
            m_miniSite.join();
        }
        catch (InterruptedException ie) {}
        m_mesh.unregisterNode(m_HSId);
        m_shouldContinue.set(false);
    }

    public NodeState getNodeState()
    {
        NodeState state = m_nodeState.get();
        if (state == NodeState.START || state == NodeState.STOP) {
            return state;
        }
        if (m_miniSite.isInArbitration()) {
            m_nodeState.set(NodeState.RESOLVE);
        }
        else {
            m_nodeState.set(NodeState.RUN);
        }
        return m_nodeState.get();
    }

    synchronized public Set<Long> getConnectedNodes()
    {
        Set<Long> HSIds = new HashSet<Long>();
        HSIds.addAll(m_HSIds);
        return HSIds;
    }

    @Override
    public void start() {
        setName("MiniNode-" + CoreUtils.hsIdToString(m_HSId));
        super.start();
    }

    @Override
    public void run()
    {
        m_miniSite.start();
        for (long HSId : m_HSIds) {
            // Don't track your own death
            if (HSId != m_HSId) {
                m_deadTracker.startTracking(HSId);
            }
        }
        m_nodeState.set(NodeState.RUN);
        while (m_shouldContinue.get())
        {
            Message msg = m_recvQ.poll();
            synchronized(this) {
                if (msg != null) {
                    if (msg.m_close) {
                        int failedHostId = CoreUtils.getHostIdFromHSId(msg.m_src);
                        long agreementHSId = CoreUtils.getHSIdFromHostAndSite(failedHostId,
                                HostMessenger.AGREEMENT_SITE_ID);
                        m_miniSite.reportFault(agreementHSId);
                        m_deadTracker.stopTracking(msg.m_src);
                    } else {
                        m_deadTracker.updateHSId(msg.m_src);
                        // inject actual message into mailbox
                        VoltMessage message = msg.m_msg;

                        // snoop for SiteFailureMessage, inject into MiniSite's mailbox
                        if (   message instanceof SiteFailureMessage
                                && !(message instanceof SiteFailureForwardMessage)) {
                            SiteFailureMessage sfm = (SiteFailureMessage)message;

                            for (FaultMessage fm: sfm.asFaultMessages()) {
                                m_miniSite.reportFault(fm);
                            }
                        }
                        m_mailbox.deliver(message);
                    }
                }
                // Do dead host detection.  Need to keep track of receive gaps from the remaining set
                // of live hosts.
                Set<Long> deadHosts = m_deadTracker.checkTimeouts();
                for (long HSId : deadHosts) {
                    int failedHostId = CoreUtils.getHostIdFromHSId(HSId);
                    long agreementHSId = CoreUtils.getHSIdFromHostAndSite(failedHostId,
                            HostMessenger.AGREEMENT_SITE_ID);
                    m_miniSite.reportFault(agreementHSId);
                    m_deadTracker.stopTracking(HSId);
                }
            }
        }
    }

    @Override
    public void disconnect(Set<Integer> failedHostIds) {
        synchronized(this) {
            for (int hostId : failedHostIds) {
                long HSId = CoreUtils.getHSIdFromHostAndSite(hostId, HostMessenger.AGREEMENT_SITE_ID);
                m_HSIds.remove(HSId);
                m_deadTracker.stopTracking(HSId);
                // Ghetto way to disconnect ourselves from someone we've decided is dead
                m_mesh.closeLink(m_HSId, HSId);
            }
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Node: " + CoreUtils.hsIdToString(m_HSId));
        sb.append("\n\tState: " + getNodeState());
        if (getNodeState() != NodeState.STOP) {
            sb.append("\n\tConnected to: " +
                    CoreUtils.hsIdCollectionToString(getConnectedNodes()));
        }

        return sb.toString();
    }
}
