/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltcore.agreement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.voltcore.agreement.AgreementSite.FaultMessage;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.FailureSiteUpdateMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;

public class MeshArbiter {

    protected static final VoltLogger m_recoveryLog = new VoltLogger("REJOIN");

    protected final Map<Long, Boolean> m_inTrouble = new TreeMap<Long, Boolean>();
    protected final long m_hsId;
    protected final Mailbox m_mailbox;
    protected final MeshAide m_meshAide;
    protected final HashMap<Pair<Long, Long>, Long> m_failureSiteUpdateLedger =
            new HashMap<Pair<Long, Long>, Long>();

    public MeshArbiter(final long hsId, final Mailbox mailbox,
            final MeshAide meshAide) {

        m_hsId  = hsId;
        m_mailbox = mailbox;
        m_meshAide = meshAide;
    }

    public Map<Long,Long> reconfigureOnFault(Set<Long> hsIds, AgreementSite.FaultMessage fm) {

        if (m_inTrouble.containsKey(fm.failedSite)) {

            if (fm.witnessed && !m_inTrouble.get(fm.failedSite)) {
                m_inTrouble.put(fm.failedSite, fm.witnessed);
            }

            m_recoveryLog.info("Received fault message for failed site " +
                    CoreUtils.hsIdToString(fm.failedSite) + " ignoring");

            return ImmutableMap.of();
        }
        m_inTrouble.put(fm.failedSite,fm.witnessed);

        HashSet<Long> survivorSet = new HashSet<Long>(hsIds);
        survivorSet.removeAll(m_inTrouble.keySet());
        long survivors[] = Longs.toArray(survivorSet);
        m_recoveryLog.info("Agreement, Sending fault data "
                + CoreUtils.hsIdCollectionToString(m_inTrouble.keySet())
                + " to "
                + CoreUtils.hsIdCollectionToString(survivorSet) + " survivors");

        discoverGlobalFaultData_send(survivors);

        if (discoverGlobalFaultData_rcv(hsIds,survivors)) {
            return extractGlobalFaultData(hsIds,survivors);
        } else {
            return ImmutableMap.of();
        }

    }

    /**
     * Send one message to each surviving execution site providing this site's
     * multi-partition commit point and this site's safe txnid
     * (the receiver will filter the later for its
     * own partition). Do this once for each failed initiator that we know about.
     * Sends all data all the time to avoid a need for request/response.
     */
    private int discoverGlobalFaultData_send(long [] survivors) {

        m_recoveryLog.info("Agreement, Sending fault data " + CoreUtils.hsIdCollectionToString(m_inTrouble.keySet())
                + " to "
                + CoreUtils.hsIdCollectionToString(Longs.asList(survivors)) + " survivors");
        for (Long site : m_inTrouble.keySet()) {
            /*
             * Check the queue for the data and get it from the ledger if necessary.\
             * It might not even be in the ledger if the site has been failed
             * since recovery of this node began.
             */
            Long txnId = m_meshAide.getNewestSafeTransactionForInitiator(site);
            FailureSiteUpdateMessage srcmsg =
                new FailureSiteUpdateMessage(m_inTrouble.keySet(),
                        site,
                        txnId != null ? txnId : Long.MIN_VALUE,
                        site);

            m_mailbox.send(survivors, srcmsg);
        }
        m_recoveryLog.info("Agreement, Sent fault data. Expecting " + (survivors.length * m_inTrouble.size()) + " responses.");
        return (survivors.length * m_inTrouble.size());
    }

    /**
     * Collect the failure site update messages from all sites This site sent
     * its own mailbox the above broadcast the maximum is local to this site.
     * This also ensures at least one response.
     *
     * Concurrent failures can be detected by additional reports from the FaultDistributor
     * or a mismatch in the set of failed hosts reported in a message from another site
     */
    private boolean discoverGlobalFaultData_rcv(Set<Long> hsIds, long [] survivors) {

        long blockedOnReceiveStart = System.currentTimeMillis();
        long lastReportTime = 0;
        do {
            VoltMessage m = m_mailbox.recvBlocking(new Subject[] { Subject.FAILURE, Subject.FAILURE_SITE_UPDATE }, 5);

            /*
             * If fault resolution takes longer then 10 seconds start logging
             */
            final long now = System.currentTimeMillis();
            if (now - blockedOnReceiveStart > 10000) {
                if (now - lastReportTime > 60000) {
                    lastReportTime = System.currentTimeMillis();
                    haveNecessaryFaultInfo(Longs.toArray(m_inTrouble.keySet()), true);
                }
            }

            if (m == null) {
                // Send a heartbeat to keep the dead host timeout active.  Needed because IV2 doesn't
                // generate its own heartbeats to keep this running.
                m_meshAide.sendHeartbeats(hsIds);
                continue;
            }
            if (!hsIds.contains(m.m_sourceHSId)) continue;

            FailureSiteUpdateMessage fsum = null;

            if (m.getSubject() == Subject.FAILURE_SITE_UPDATE.getId()) {
                fsum = (FailureSiteUpdateMessage)m;
                m_failureSiteUpdateLedger.put(
                        Pair.of(fsum.m_sourceHSId, fsum.m_initiatorForSafeTxnId),
                        fsum.m_safeTxnId);
            } else if (m.getSubject() == Subject.FAILURE.getId()) {
                /*
                 * If the fault distributor reports a new fault, ignore it if it is known , otherwise
                 * re-deliver the message to ourself and then abort so that the process can restart.
                 */
                FaultMessage fm = (FaultMessage)m;

                Boolean alreadyWitnessed = m_inTrouble.get(fm.failedSite);
                if (alreadyWitnessed == null) {
                    m_mailbox.deliverFront(m);
                    m_recoveryLog.info("Agreement, Detected a concurrent failure from FaultDistributor, new failed site "
                            + CoreUtils.hsIdToString(fm.failedSite));
                    return false;
                }

                if (!alreadyWitnessed && fm.witnessed) {
                    m_inTrouble.put(fm.failedSite, fm.witnessed);
                }

            }

            m_recoveryLog.info("Agreement, Received failure message from " +
                    CoreUtils.hsIdToString(fsum.m_sourceHSId) + " for failed sites " +
                    CoreUtils.hsIdCollectionToString(fsum.m_failedHSIds) +
                    " safe txn id " + fsum.m_safeTxnId + " failed site " +
                    CoreUtils.hsIdToString(fsum.m_initiatorForSafeTxnId));
        } while(!haveNecessaryFaultInfo(survivors, false));

        return true;
    }

    private boolean haveNecessaryFaultInfo( long [] survivors, boolean log) {
        List<Pair<Long, Long>> missingMessages = new ArrayList<Pair<Long, Long>>();
        for (long survivingSite : survivors) {
            for (Long failingSite : m_inTrouble.keySet()) {
                Pair<Long, Long> key = Pair.of( survivingSite, failingSite);
                if (!m_failureSiteUpdateLedger.containsKey(key)) {
                    missingMessages.add(key);
                }
            }
        }
        if (log) {
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            boolean first = true;
            for (Pair<Long, Long> p : missingMessages) {
                if (!first) sb.append(", ");
                first = false;
                sb.append(CoreUtils.hsIdToString(p.getFirst()));
                sb.append('-');
                sb.append(CoreUtils.hsIdToString(p.getSecond()));
            }
            sb.append(']');

            m_recoveryLog.warn("Failure resolution stalled waiting for ( ExecutionSite, Initiator ) " +
                                "information: " + sb.toString());
        }
        return missingMessages.isEmpty();
    }

    private Map<Long,Long> extractGlobalFaultData(Set<Long> hsIds, long[] survivors) {

        if (!haveNecessaryFaultInfo(survivors, false)) {
            VoltDB.crashLocalVoltDB("Error extracting fault data", true, null);
        }

        Map<Long, Long> initiatorSafeInitPoint = new HashMap<Long, Long>();

        Iterator<Map.Entry<Pair<Long, Long>, Long>> iter =
            m_failureSiteUpdateLedger.entrySet().iterator();

        while (iter.hasNext()) {
            final Map.Entry<Pair<Long, Long>, Long> entry = iter.next();
            final Pair<Long, Long> key = entry.getKey();
            final Long safeTxnId = entry.getValue();

            if (!hsIds.contains(key.getFirst())) {
                continue;
            }

            Long initiatorId = key.getSecond();
            if (!initiatorSafeInitPoint.containsKey(initiatorId)) {
                initiatorSafeInitPoint.put( initiatorId, Long.MIN_VALUE);
            }

            initiatorSafeInitPoint.put( initiatorId,
                    Math.max(initiatorSafeInitPoint.get(initiatorId), safeTxnId));
        }
        assert(!initiatorSafeInitPoint.containsValue(Long.MIN_VALUE));

        return ImmutableMap.copyOf(initiatorSafeInitPoint);
    }
}
