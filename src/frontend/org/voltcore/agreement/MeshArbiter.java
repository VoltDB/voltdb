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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.SiteFailureForwardMessage;
import org.voltcore.messaging.FaultMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.SiteFailureMessage;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

public class MeshArbiter {

    protected static final VoltLogger m_recoveryLog = new VoltLogger("REJOIN");

    protected final Map<Long, Boolean> m_inTrouble = Maps.newTreeMap();
    protected final long m_hsId;
    protected final Mailbox m_mailbox;
    protected final MeshAide m_meshAide;
    protected final HashMap<Pair<Long, Long>, Long> m_failureSiteUpdateLedger =
            Maps.newHashMap();
    protected final Set<Long> m_failedSites = Sets.newTreeSet();
    protected final Set<Long> m_staleUnwitnessed = Sets.newTreeSet();
    protected final AgreementSeeker m_seeker;

    public MeshArbiter(final long hsId, final Mailbox mailbox,
            final MeshAide meshAide) {

        m_hsId  = hsId;
        m_mailbox = mailbox;
        m_meshAide = meshAide;
        m_seeker = new AgreementSeeker(ArbitrationStrategy.MATCHING_CARDINALITY, m_hsId);
    }

    protected Predicate<Long> in(final Set<Long> hsids) {
        return new Predicate<Long>() {
            @Override
            public boolean apply(Long l) {
                return hsids.contains(l);
            }
        };
    }

    protected boolean mayIgnore(FaultMessage fm) {
        Boolean alreadyWitnessed = m_inTrouble.get(fm.failedSite);

               // implausible suicide
        return (fm.failedSite == m_hsId)
               // stale failed site
            || m_failedSites.contains(fm.failedSite)
               // already witnessed
            || (   alreadyWitnessed != null
                && (alreadyWitnessed || alreadyWitnessed == fm.witnessed))
               // stale unwitnessed
            || (   !fm.witnessed && m_inTrouble.isEmpty()
                && m_staleUnwitnessed.contains(fm.failedSite));
    }

    public Map<Long,Long> reconfigureOnFault(Set<Long> hsIds, FaultMessage fm) {
        final Subject [] justFailures = new Subject [] { Subject.FAILURE };
        boolean proceed = false;
        do {
            if (mayIgnore(fm)) {
                m_recoveryLog.info("Received stale fault message for site " +
                        CoreUtils.hsIdToString(fm.failedSite) + " ignoring");
            } else {
                m_inTrouble.put(fm.failedSite,fm.witnessed);
                proceed = true;
            }

            fm = (FaultMessage)m_mailbox.recv(justFailures);
        } while (fm != null);

        if (!proceed) {
            return ImmutableMap.of();
        }

        // we are here if failed site was not previously recorded
        // or it was previously recorded but it became witnessed from unwitnessed
        m_seeker.startSeekingFor(Sets.difference(hsIds, m_failedSites), m_inTrouble);

        discoverGlobalFaultData_send();

        if (discoverGlobalFaultData_rcv(hsIds)) {
            Map<Long,Long> lastTxnIdByFailedSite = extractGlobalFaultData(hsIds);
            if (lastTxnIdByFailedSite.isEmpty()) {
                return ImmutableMap.of();
            }

            m_failedSites.addAll( lastTxnIdByFailedSite.keySet());

            m_recoveryLog.info(
                    "Adding "
                  + CoreUtils.hsIdCollectionToString(lastTxnIdByFailedSite.keySet())
                  + " to failed sites history");

            clearInTrouble();
            m_seeker.clear();

            return lastTxnIdByFailedSite;
        } else {
            return ImmutableMap.of();
        }
    }

    protected void clearInTrouble() {
        m_staleUnwitnessed.clear();

        TreeSet<Long> ledgerStales = Sets.newTreeSet();
        Iterator<Map.Entry<Long, Boolean>> itr = m_inTrouble.entrySet().iterator();

        while (itr.hasNext()) {
            Map.Entry<Long, Boolean> e = itr.next();
            if (!e.getValue() && !m_failedSites.contains(e.getKey())) {
                m_staleUnwitnessed.add(e.getKey());
                ledgerStales.add(e.getKey());
            }
            itr.remove();
        }

        Iterator<Map.Entry<Pair<Long,Long>, Long>> ltr =
                m_failureSiteUpdateLedger.entrySet().iterator();

        while (ltr.hasNext()) {
            Map.Entry<Pair<Long,Long>, Long> e = ltr.next();
            Pair<Long,Long> p = e.getKey();
            if (   ledgerStales.contains(p.getSecond())
                || p.getFirst().equals(p.getSecond())) {
                ltr.remove();
            }
        }
    }

    /**
     * Send one message to each surviving execution site providing this site's
     * multi-partition commit point and this site's safe txnid
     * (the receiver will filter the later for its
     * own partition). Do this once for each failed initiator that we know about.
     * Sends all data all the time to avoid a need for request/response.
     */
    private int discoverGlobalFaultData_send() {
        long [] survivors = Longs.toArray(m_seeker.getSurvivors());

        m_recoveryLog.info("Agreement, Sending survivor set "
                + CoreUtils.hsIdCollectionToString(m_seeker.getSurvivors()));

        SiteFailureMessage.Builder msgBuilder = SiteFailureMessage.builder();
        msgBuilder.addSurvivors(m_seeker.getSurvivors());
        for (long troubled: m_inTrouble.keySet()) {
            if (troubled == m_hsId) continue;
            /*
             * Check the queue for the data and get it from the ledger if necessary.\
             * It might not even be in the ledger if the site has been failed
             * since recovery of this node began.
             */
            Long txnId = m_meshAide.getNewestSafeTransactionForInitiator(troubled);
            msgBuilder.addSafeTxnId(troubled, txnId != null ? txnId : Long.MIN_VALUE);
        }

        m_mailbox.send(survivors, msgBuilder.build());

        m_recoveryLog.info("Agreement, Sent fault data. Expecting " + survivors.length + " responses.");
        return (survivors.length);
    }

    /**
     * Collect the failure site update messages from all sites This site sent
     * its own mailbox the above broadcast the maximum is local to this site.
     * This also ensures at least one response.
     *
     * Concurrent failures can be detected by additional reports from the FaultDistributor
     * or a mismatch in the set of failed hosts reported in a message from another site
     */
    private boolean discoverGlobalFaultData_rcv(Set<Long> hsIds) {

        long blockedOnReceiveStart = System.currentTimeMillis();
        long lastReportTime = 0;
        final Subject [] receiveSubjects = new Subject [] {
                Subject.FAILURE,
                Subject.SITE_FAILURE_UPDATE,
                Subject.SITE_FAILURE_FORWARD
        };
        boolean haveEnough = false;
        Map<Long,SiteFailureForwardMessage> forwardCandidates = Maps.newHashMap();

        do {
            VoltMessage m = m_mailbox.recvBlocking(receiveSubjects, 5);

            /*
             * If fault resolution takes longer then 10 seconds start logging
             */
            final long now = System.currentTimeMillis();
            if (now - blockedOnReceiveStart > 10000) {
                if (now - lastReportTime > 60000) {
                    lastReportTime = System.currentTimeMillis();
                    haveNecessaryFaultInfo(m_inTrouble.keySet(), true);
                }
            }

            if (m == null) {
                // Send a heartbeat to keep the dead host timeout active.  Needed because IV2 doesn't
                // generate its own heartbeats to keep this running.
                m_meshAide.sendHeartbeats(hsIds);
                continue;
            }

            if (m.getSubject() == Subject.SITE_FAILURE_UPDATE.getId()) {
                if (!hsIds.contains(m.m_sourceHSId)) continue;

                SiteFailureMessage sfm = (SiteFailureMessage)m;

                for (Map.Entry<Long, Long> e: sfm.m_safeTxnIds.entrySet()) {

                    if(!hsIds.contains(e.getKey()) || m_hsId == e.getKey()) continue;

                    m_failureSiteUpdateLedger.put(
                            Pair.of(sfm.m_sourceHSId, e.getKey()),
                            e.getValue());
                }

                m_seeker.add(sfm);
                forwardCandidates.put(sfm.m_sourceHSId, new SiteFailureForwardMessage(sfm));

                m_recoveryLog.info("Agreement, Received failure message: " + sfm);

            } else if (m.getSubject() == Subject.SITE_FAILURE_FORWARD.getId()) {
                SiteFailureForwardMessage fsfm = (SiteFailureForwardMessage)m;

                forwardCandidates.put(fsfm.m_reportingHSId, fsfm);

                if (   !hsIds.contains(fsfm.m_sourceHSId)
                    || m_seeker.getSurvivors().contains(fsfm.m_reportingHSId)) continue;

                m_seeker.add(fsfm);

                m_recoveryLog.info("Agreement, Received forwarded failure message: " + fsfm);

            } else if (m.getSubject() == Subject.FAILURE.getId()) {
                /*
                 * If the fault distributor reports a new fault, ignore it if it is known , otherwise
                 * re-deliver the message to ourself and then abort so that the process can restart.
                 */
                FaultMessage fm = (FaultMessage)m;

                if (!mayIgnore(fm)) {
                    m_mailbox.deliverFront(m);
                    m_recoveryLog.info("Agreement, Detected a concurrent failure from FaultDistributor, new failed site "
                            + CoreUtils.hsIdToString(fm.failedSite));
                    return false;
                }
            }

            haveEnough = haveEnough || haveNecessaryFaultInfo(m_seeker.getSurvivors(), false);
            if (haveEnough) {

                Iterator<Map.Entry<Long, SiteFailureForwardMessage>> itr =
                        forwardCandidates.entrySet().iterator();

                while (itr.hasNext()) {
                    Map.Entry<Long, SiteFailureForwardMessage> e = itr.next();
                    long [] fhsids = Longs.toArray(m_seeker.forWhomSiteIsDead(e.getKey()));
                    if (fhsids.length > 0) {
                        m_mailbox.send(fhsids,e.getValue());
                    }
                    itr.remove();
                }
            }

        } while (!haveEnough || m_seeker.needForward(m_hsId));

        return true;
    }

    private boolean haveNecessaryFaultInfo( Set<Long> survivors, boolean log) {
        List<Pair<Long, Long>> missingMessages = new ArrayList<Pair<Long, Long>>();
        for (long survivingSite : survivors) {
            for (Long failingSite : m_inTrouble.keySet()) {
                Pair<Long, Long> key = Pair.of( survivingSite, failingSite);
                if (   survivingSite != failingSite
                    && !m_failureSiteUpdateLedger.containsKey(key)) {
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

    private Map<Long,Long> extractGlobalFaultData(Set<Long> hsIds) {

        if (!haveNecessaryFaultInfo(m_seeker.getSurvivors(), false)) {
            VoltDB.crashLocalVoltDB("Error extracting fault data", true, null);
        }

        Set<Long> toBeKilled = m_seeker.nextKill();
        Map<Long, Long> initiatorSafeInitPoint = new HashMap<Long, Long>();

        Iterator<Map.Entry<Pair<Long, Long>, Long>> iter =
            m_failureSiteUpdateLedger.entrySet().iterator();

        while (iter.hasNext()) {
            final Map.Entry<Pair<Long, Long>, Long> entry = iter.next();
            final Pair<Long, Long> key = entry.getKey();
            final Long safeTxnId = entry.getValue();

            if (!hsIds.contains(key.getFirst()) || !toBeKilled.contains(key.getSecond())) {
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

        initiatorSafeInitPoint.remove(m_hsId);

        return ImmutableMap.copyOf(initiatorSafeInitPoint);
    }
}
