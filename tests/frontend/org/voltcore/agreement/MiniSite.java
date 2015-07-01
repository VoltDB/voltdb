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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.DisconnectFailedHostsCallback;
import org.voltcore.messaging.FaultMessage;
import org.voltcore.messaging.HeartbeatMessage;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.collect.ImmutableSet;

class MiniSite extends Thread implements MeshAide
{
    final VoltLogger m_siteLog;
    MeshArbiter m_arbiter;
    Mailbox m_mailbox;
    AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    private final DisconnectFailedHostsCallback m_failedHosts;
    Set<Long> m_initialHSIds = new HashSet<Long>();
    Set<Long> m_currentHSIds = new HashSet<Long>();
    Set<Long> m_failedHSIds = new HashSet<Long>();

    MiniSite(Mailbox mbox, Set<Long> HSIds, DisconnectFailedHostsCallback callback,
            VoltLogger logger)
    {
        m_siteLog = logger;
        m_initialHSIds.addAll(HSIds);
        m_currentHSIds.addAll(HSIds);
        m_mailbox = mbox;
        m_arbiter = new MeshArbiter(mbox.getHSId(), mbox, this);
        m_failedHosts = callback;
    }

    void shutdown()
    {
        m_siteLog.info("MiniSite shutting down");
        m_shouldContinue.set(false);
    }

    @Override
    public Long getNewestSafeTransactionForInitiator(Long initiatorId) {
        return 1L;
    }

    @Override
    public void sendHeartbeats(Set<Long> hsIds) {
        for (long initiatorId : hsIds) {
            HeartbeatMessage heartbeat =
                new HeartbeatMessage(m_mailbox.getHSId(), 1, 1);
            m_mailbox.send( initiatorId, heartbeat);
        }
    }

    public void reportFault(long faultingSite) {
        if (m_siteLog.isDebugEnabled()) {
            m_siteLog.debug("Reported fault: " + faultingSite + ", witnessed?: true" );
        }
        FaultMessage fm = new FaultMessage(m_mailbox.getHSId(), faultingSite);
        fm.m_sourceHSId = m_mailbox.getHSId();
        m_mailbox.deliver(fm);
    }

    public void reportFault(FaultMessage fm) {
        fm.m_sourceHSId = m_mailbox.getHSId();
        if (m_siteLog.isDebugEnabled()) {
            m_siteLog.debug("Reporting fault: " + fm);
        }
        m_mailbox.deliver(fm);
    }

    @Override
    public void start() {
        setName("MiniSite-" + CoreUtils.hsIdToString(m_mailbox.getHSId()));
        super.start();
    }

    public LocalObjectMessage createSiteJoinMessage(final long hSId) {
        Runnable siteJoiner = new Runnable() {
            @Override
            public void run() {
                m_currentHSIds.add(hSId);
            }
        };
        return new LocalObjectMessage(siteJoiner);
    }

    public LocalObjectMessage createSitePruneMessage(final long hSId) {
        Runnable sitePruner = new Runnable() {
            @Override
            public void run() {
                m_currentHSIds.remove(hSId);
            }
        };
        return new LocalObjectMessage(sitePruner);
    }

    @Override
    public void run() {
        long lastHeartbeatTime = System.currentTimeMillis();
        while (m_shouldContinue.get()) {
            VoltMessage msg = m_mailbox.recvBlocking(5);
            if (msg != null) {
                if (msg instanceof LocalObjectMessage) {
                    LocalObjectMessage lomsg = (LocalObjectMessage)msg;
                    ((Runnable)lomsg.payload).run();
                } else {
                    processMessage(msg);
                }
            }
            long now = System.currentTimeMillis();
            if (now - lastHeartbeatTime > 5) {
                sendHeartbeats(m_currentHSIds);
                lastHeartbeatTime = now;
            }
        }
    }

    public boolean isInArbitration() {
        return m_arbiter.isInArbitration();
    }

    public int getFailedSitesCount() {
        return m_arbiter.getFailedSitesCount();
    }

    private void processMessage(VoltMessage msg)
    {
        if (!m_currentHSIds.contains(msg.m_sourceHSId)) {
            if (m_siteLog.isDebugEnabled()) {
                m_siteLog.debug("Dropping message " + msg + " because it is not from a known up site");
            }
            return;
        }
        // need heartbeat something in here?
        if (msg instanceof FaultMessage) {
            FaultMessage fm = (FaultMessage)msg;
            discoverGlobalFaultData(fm);
        }
    }

    private void discoverGlobalFaultData(FaultMessage faultMessage)
    {
        if (m_siteLog.isDebugEnabled()) {
            m_siteLog.debug("Saw fault: " + faultMessage);
        }
        Map<Long, Long> results = m_arbiter.reconfigureOnFault(m_currentHSIds, faultMessage);
        if (results.isEmpty()) {
            return;
        }
        m_failedHSIds.addAll(results.keySet());
        m_currentHSIds.removeAll(results.keySet());
        ImmutableSet.Builder<Integer> failedHosts = ImmutableSet.builder();
        for (long HSId : results.keySet()) {
            failedHosts.add(CoreUtils.getHostIdFromHSId(HSId));
        }
        m_failedHosts.disconnect(failedHosts.build());
        // need to "disconnect" these failed guys somehow?
    }
}
