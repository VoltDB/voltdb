/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.agreement;

import org.voltdb.TransactionIdManager;
import org.voltdb.VoltDB;
import org.voltdb.dtxn.ExecutorTxnIdSafetyState;
import org.voltdb.dtxn.OrderableTransaction;
import org.voltdb.dtxn.RestrictedPriorityQueue;
import org.voltdb.dtxn.RestrictedPriorityQueue.QueueState;
import org.voltdb.fault.FaultDistributorInterface;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.fault.VoltFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.*;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.voltdb.utils.DBBPool;
import org.voltdb.utils.MiscUtils;

import java.io.*;
import java.util.concurrent.Semaphore;
import java.util.zip.*;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper_voltpatches.ZooDefs.OpCode;
import org.apache.zookeeper_voltpatches.server.*;

/*
 * A wrapper around a single node ZK server. The server is a modified version of ZK that speaks the ZK
 * wire protocol and data model, but has no durability. Agreement is provided
 * by the AgreementSite wrapper which contains a restricted priority queue like an execution site,
 * but also has a transaciton id manager and a unique initiator id. The intiator ID and site id are the same
 * as the id of the regular txn initiator on this node. The mailbox used has a different ID so messages
 * for agreement are routed here.
 *
 * Recovery is implemented by shipping a complete snapshot at a txnid to the recovering node, then every node
 * ships all the agreement txns they know about to the recovering node.
 */
public class AgreementSite implements org.apache.zookeeper_voltpatches.server.ZooKeeperServer.Callout {

    private final ZooKeeperServer m_server;
    private final NIOServerCnxn.Factory m_cnxnFactory;
    private final Mailbox m_mailbox;
    private final TransactionIdManager m_idManager;
    private final RestrictedPriorityQueue m_txnQueue;
    private final int m_siteId;
    /*
     * All sites
     */
    private final Set<Integer> m_allSiteIds = new HashSet<Integer>();
    /*
     * Not failed sites
     */
    private final TreeSet<Integer> m_siteIds = new TreeSet<Integer>();
    private final
        HashMap<Long, AgreementTransactionState> m_transactionsById = new HashMap<Long, AgreementTransactionState>();
    final ExecutorTxnIdSafetyState m_safetyState;
    private volatile boolean m_shouldContinue = true;
    private volatile boolean m_recovering = false;
    private static final VoltLogger m_recoveryLog = new VoltLogger("RECOVERY");
    private static final VoltLogger m_agreementLog = new VoltLogger("AGREEMENT");
    private final FaultHandler m_faultHandler = new FaultHandler();
    private final FaultDistributorInterface m_faultDistributor;
    private long m_minTxnIdAfterRecovery = Long.MIN_VALUE;
    private final Semaphore m_shutdownComplete = new Semaphore(0);

    private final HashSet<Integer> m_sitesCompletedRecoveryShipping = new HashSet<Integer>();

    /**
     * The list of failed sites we know about. Included with all failure messages
     * to identify what the information was used to generate commit points
     */
    private final HashSet<Integer> m_knownFailedSites = new HashSet<Integer>();

    /**
     * Failed sites for which agreement has been reached.
     */
    private final HashSet<Integer> m_handledFailedSites = new HashSet<Integer>();

    /**
     * Store values from older failed nodes. They are repeated with every failure message
     */
    private final HashMap<Integer, Long> m_newestSafeTransactionForInitiatorLedger =
        new HashMap<Integer, Long>();

    public static final class FaultMessage extends VoltMessage {

        public final Set<NodeFailureFault> nodeFaults;
        public final boolean cleared;

        public FaultMessage(Set<NodeFailureFault> faults, boolean cleared) {
            this.nodeFaults = faults;
            this.cleared = cleared;
        }

        @Override
        protected void initFromBuffer() {
            // TODO Auto-generated method stub

        }

        @Override
        protected void flattenToBuffer(DBBPool pool) throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public byte getSubject() {
            return Subject.FAILURE.getId();
        }

    }

    private class FaultHandler implements org.voltdb.fault.FaultHandler {

        @Override
        public void faultOccured(Set<VoltFault> faults) {
            if (m_shouldContinue == false) {
                return;
            }
            Set<NodeFailureFault> faultedNodes = new HashSet<NodeFailureFault>();
            for (VoltFault fault : faults) {
                if (fault instanceof NodeFailureFault) {
                    NodeFailureFault nodeFault = (NodeFailureFault)fault;
                    faultedNodes.add(nodeFault);
                }
            }
            if (!faultedNodes.isEmpty()) {
                m_mailbox.deliver(new FaultMessage(faultedNodes, false));
            }
        }

        @Override
        public void faultCleared(Set<VoltFault> faults) {
            if (m_shouldContinue == false) {
                return;
            }
            Set<NodeFailureFault> faultedNodes = new HashSet<NodeFailureFault>();
            for (VoltFault fault : faults) {
                if (fault instanceof NodeFailureFault) {
                    NodeFailureFault nodeFault = (NodeFailureFault)fault;
                    faultedNodes.add(nodeFault);
                }
            }
            if (!faultedNodes.isEmpty()) {
                m_mailbox.deliver(new FaultMessage(faultedNodes, true));
            }
        }

    }

    public AgreementSite(
            int myAgreementSiteId,
            Set<Integer> agreementSiteIds,
            int initiatorId,
            Set<Integer> failedSiteIds,
            Mailbox mailbox,
            InetSocketAddress address,
            FaultDistributorInterface faultDistributor,
            boolean recovering) throws IOException {
        m_faultDistributor = faultDistributor;
        m_mailbox = mailbox;
        m_siteId = myAgreementSiteId;
        m_siteIds.addAll(agreementSiteIds);
        m_allSiteIds.addAll(agreementSiteIds);
        m_siteIds.removeAll(failedSiteIds);
        m_sitesCompletedRecoveryShipping.addAll(agreementSiteIds);
        m_sitesCompletedRecoveryShipping.removeAll(failedSiteIds);
        m_sitesCompletedRecoveryShipping.remove(m_siteId);
        m_knownFailedSites.addAll(failedSiteIds);
        m_handledFailedSites.addAll(m_knownFailedSites);
        m_idManager = new TransactionIdManager( initiatorId, 0);
        m_txnQueue =
            new RestrictedPriorityQueue(
                    MiscUtils.toArray(agreementSiteIds),
                    myAgreementSiteId, mailbox,
                    VoltDB.AGREEMENT_MAILBOX_ID);
        m_safetyState = new ExecutorTxnIdSafetyState(myAgreementSiteId, MiscUtils.toArray(agreementSiteIds));
        for (Integer sid : failedSiteIds) {
            m_txnQueue.gotFaultForInitiator(sid);
            m_safetyState.removeState(sid);
        }
        m_cnxnFactory =
            new NIOServerCnxn.Factory( address, 10);
        m_server = new ZooKeeperServer(this);
        m_handledFailedSites.addAll(m_knownFailedSites);
        if (faultDistributor != null) {
            faultDistributor.registerFaultHandler(
                    NodeFailureFault.NODE_FAILURE_INITIATOR,
                    m_faultHandler,
                    FaultType.NODE_FAILURE);
        }
        m_recovering = recovering;
    }

    public void start() throws InterruptedException, IOException {
        m_cnxnFactory.startup(m_server);
    }

    public void shutdown() throws InterruptedException {
        m_shouldContinue = false;
        m_cnxnFactory.shutdown();
        m_shutdownComplete.acquire();
    }

    public void shutdownInternal() {
        m_cnxnFactory.shutdown();
    }


    @Override
    public void run() {
        long lastHeartbeatTime = System.currentTimeMillis();
        try {
            while (m_shouldContinue) {
                VoltMessage message = m_mailbox.recvBlocking(5);
                if (message != null) {
                    processMessage(message);
                }

                final long now = System.currentTimeMillis();
                if (now - lastHeartbeatTime > 5) {
                    lastHeartbeatTime = now;
                    sendHeartbeats();
                }

                if (m_recovering) {
                    continue;
                }

                AgreementTransactionState txnState = (AgreementTransactionState)m_txnQueue.poll();
                if (txnState != null) {
                    if (txnState.txnId <= m_minTxnIdAfterRecovery) {
                        m_recoveryLog.fatal("Transaction queue released a transaction from before this " +
                                " node was recovered was complete");
                        VoltDB.crashVoltDB();
                    }
                    m_transactionsById.remove(txnState.txnId);
                    //Owner is what associates the session with a specific initiator
                    //only used for createSession
                    txnState.m_request.setOwner(txnState.initiatorSiteId);
                    m_server.prepRequest(txnState.m_request, txnState.txnId);
                }
            }
        } finally {
            try {
                shutdownInternal();
            } finally {
                m_shutdownComplete.release();
            }
        }
    }

    private void sendHeartbeats() {
        long txnId = m_idManager.getNextUniqueTransactionId();
        for (int initiatorId : m_siteIds) {
            HeartbeatMessage heartbeat =
                new HeartbeatMessage( m_siteId, txnId, m_safetyState.getNewestSafeTxnIdForExecutorBySiteId(initiatorId));
            try {
                m_mailbox.send( initiatorId, VoltDB.AGREEMENT_MAILBOX_ID, heartbeat);
            } catch (MessagingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void processMessage(VoltMessage message) {
        if (message instanceof TransactionInfoBaseMessage) {
            TransactionInfoBaseMessage info = (TransactionInfoBaseMessage)message;

            // Special case heartbeats which only update RPQ
            if (info instanceof HeartbeatMessage) {
                // use the heartbeat to unclog the priority queue if clogged
                long lastSeenTxnFromInitiator = m_txnQueue.noteTransactionRecievedAndReturnLastSeen(
                        info.getInitiatorSiteId(), info.getTxnId(),
                        true, ((HeartbeatMessage) info).getLastSafeTxnId());

                // respond to the initiator with the last seen transaction
                HeartbeatResponseMessage response = new HeartbeatResponseMessage(
                        m_siteId, lastSeenTxnFromInitiator,
                        m_txnQueue.getQueueState() == QueueState.BLOCKED_SAFETY);
                try {
                    m_mailbox.send(info.getInitiatorSiteId(), VoltDB.AGREEMENT_MAILBOX_ID, response);
                } catch (MessagingException e) {
                    // hope this never happens... it doesn't right?
                    throw new RuntimeException(e);
                }
                // we're done here (in the case of heartbeats)
                return;
            }
            assert(false);
        } else if (message instanceof HeartbeatResponseMessage) {
            HeartbeatResponseMessage hrm = (HeartbeatResponseMessage)message;
            m_safetyState.updateLastSeenTxnIdFromExecutorBySiteId(
                    hrm.getExecSiteId(),
                    hrm.getLastReceivedTxnId(),
                    hrm.isBlocked());
        } else if (message instanceof LocalObjectMessage) {
            LocalObjectMessage lom = (LocalObjectMessage)message;
            if (lom.payload instanceof Runnable) {
                ((Runnable)lom.payload).run();
            } else if (lom.payload instanceof Request) {
                Request r = (Request)lom.payload;
                long txnId = 0;
                if (r.type == OpCode.createSession) {
                    txnId = r.sessionId;
                } else {
                    txnId = m_idManager.getNextUniqueTransactionId();
                }
                for (int initiatorId : m_siteIds) {
                    if (initiatorId == m_siteId) continue;
                    AgreementTaskMessage atm =
                        new AgreementTaskMessage(
                                r,
                                txnId,
                                m_siteId,
                                m_safetyState.getNewestSafeTxnIdForExecutorBySiteId(initiatorId));
                    try {
                        m_mailbox.send( initiatorId, VoltDB.AGREEMENT_MAILBOX_ID, atm);
                    } catch (MessagingException e) {
                        throw new RuntimeException(e);
                    }
                }
                //Process the ATM eagerly locally to aid
                //in having a complete set of stuff to ship
                //to a recovering agreement site
                AgreementTaskMessage atm =
                    new AgreementTaskMessage(
                            r,
                            txnId,
                            m_siteId,
                            m_safetyState.getNewestSafeTxnIdForExecutorBySiteId(m_siteId));
                processMessage(atm);
            }
        } else if (message instanceof AgreementTaskMessage) {
            AgreementTaskMessage atm = (AgreementTaskMessage)message;
            if (!m_transactionsById.containsKey(atm.m_txnId) && atm.m_txnId > m_minTxnIdAfterRecovery) {
                m_txnQueue.noteTransactionRecievedAndReturnLastSeen(atm.m_initiatorId,
                        atm.m_txnId,
                        false,
                        atm.m_lastSafeTxnId);
                AgreementTransactionState transactionState =
                    new AgreementTransactionState(atm.m_txnId, atm.m_initiatorId, atm.m_request);
                m_txnQueue.add(transactionState);
                m_transactionsById.put(transactionState.txnId, transactionState);
            } else {
                m_recoveryLog.info("Agreement, discarding duplicate txn during recovery, txnid is " + atm.m_txnId +
                        " this should only occur during recovery.");
            }
        } else if (message instanceof BinaryPayloadMessage) {
            BinaryPayloadMessage bpm = (BinaryPayloadMessage)message;

            /*
             * Binary payload can be the ZK snapshot OR... it can be the end of recovery
             * message from that initiator. If the payload is null then it is end of recovery and the metadata
             * contains the id of the initiator
             */
            if (bpm.m_payload != null) {
                processZKSnapshot(bpm);
            } else {
                ByteBuffer buf = ByteBuffer.wrap(bpm.m_metadata);
                int initiatorId = buf.getInt();
                if (!m_sitesCompletedRecoveryShipping.remove(initiatorId)) {
                    m_recoveryLog.fatal("Received a notice that recovery shipping is complete from " + initiatorId +
                            " but didn't not expect to receive one from that site");
                    VoltDB.crashVoltDB();
                }
                if (m_sitesCompletedRecoveryShipping.isEmpty()) {
                    m_recovering = false;
                }
            }
        } else if (message instanceof FaultMessage) {
            FaultMessage fm = (FaultMessage)message;

            for (NodeFailureFault fault : fm.nodeFaults){
                for (Integer faultedInitiator : fault.getFailedNonExecSites()) {
                    if (fm.cleared) {
                        m_safetyState.addRejoinedState(faultedInitiator);
                        m_txnQueue.ensureInitiatorIsKnown(faultedInitiator);
                        try {
                            shipRecoveryData(faultedInitiator);
                        } catch (IOException e) {
                            m_agreementLog.fatal("Unable to ship recovery data", e);
                            VoltDB.crashVoltDB();
                        }
                        m_siteIds.add(faultedInitiator);
                    } else {
                        m_safetyState.removeState(faultedInitiator);
                    }
                }
            }

            if (!fm.cleared){
                discoverGlobalFaultData(fm);
            }
        }
    }

    private void processZKSnapshot(BinaryPayloadMessage bpm) {
        m_minTxnIdAfterRecovery = ByteBuffer.wrap(bpm.m_metadata).getLong();
        Iterator<Map.Entry<Long, AgreementTransactionState>> iter = m_transactionsById.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, AgreementTransactionState> entry = iter.next();
            if (entry.getKey() <= m_minTxnIdAfterRecovery) {
                iter.remove();
                m_txnQueue.faultTransaction(entry.getValue());
            }
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(bpm.m_payload);
        try {
            GZIPInputStream gis = new GZIPInputStream(bais);
            DataInputStream dis = new DataInputStream(gis);
            BinaryInputArchive bia = new BinaryInputArchive(dis);
            m_server.getZKDatabase().deserializeSnapshot(bia);
            m_server.createSessionTracker();
        } catch (Exception e) {
            m_recoveryLog.fatal("Error loading agreement database", e);
            VoltDB.crashVoltDB();
        }
    }

    private void shipRecoveryData(Integer faultedInitiator) throws IOException {
        if (m_siteIds.first().equals(m_siteId)) {
            m_recoveryLog.info("Shipping ZK snapshot from " + m_siteId + " to " + faultedInitiator);
            shipZKDatabaseSnapshot(faultedInitiator);
        }

        TreeMap<Long, AgreementTransactionState> transactions =
            new TreeMap<Long, AgreementTransactionState>(m_transactionsById);
        for(AgreementTransactionState entry : transactions.values()) {
            AgreementTaskMessage task =
                new AgreementTaskMessage(
                        entry.m_request,
                        entry.txnId,
                        entry.initiatorSiteId,
                        0);
            try {
                m_mailbox.send( faultedInitiator, VoltDB.AGREEMENT_MAILBOX_ID, task);
            } catch (MessagingException e) {
                throw new IOException(e);
            }
        }
        ByteBuffer idBuffer = ByteBuffer.allocate(16);
        idBuffer.putInt(m_siteId);
        try {
            m_mailbox.send(
                    faultedInitiator,
                    VoltDB.AGREEMENT_MAILBOX_ID,
                    new BinaryPayloadMessage(idBuffer.array(), null));
        } catch (MessagingException e) {
            throw new IOException(e);
        }
    }

    private void shipZKDatabaseSnapshot(int faultedInitiator) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gos = new GZIPOutputStream(baos);
        DataOutputStream dos = new DataOutputStream(gos);
        BinaryOutputArchive boa = new BinaryOutputArchive(dos);
        m_server.getZKDatabase().serializeSnapshot(boa);
        dos.flush();
        gos.finish();
        byte databaseBytes[] = baos.toByteArray();
        ByteBuffer metadata = ByteBuffer.allocate(16);
        metadata.putLong(m_server.getZKDatabase().getDataTreeLastProcessedZxid());
        BinaryPayloadMessage bpm = new BinaryPayloadMessage( metadata.array(), databaseBytes);
        try {
            m_mailbox.send( faultedInitiator, VoltDB.AGREEMENT_MAILBOX_ID, bpm);
        } catch (MessagingException e) {
            throw new IOException(e);
        }
    }

    Set<Integer> getFaultingSites(FaultMessage fm) {
        HashSet<Integer> faultingSites = new HashSet<Integer>();
        for (NodeFailureFault fault : fm.nodeFaults) {
            faultingSites.addAll(fault.getFailedNonExecSites());
        }
        return faultingSites;
    }

    private void discoverGlobalFaultData(FaultMessage faultMessage) {
        //Keep it simple and don't try to recover on the recovering node.
        if (m_recovering) {
            m_recoveryLog.fatal("Aborting recovery due to a remote node failure. Retry again.");
            VoltDB.crashVoltDB();
        }
        Set<NodeFailureFault> failures = faultMessage.nodeFaults;

        Set<Integer> failedSiteIds = getFaultingSites(faultMessage);
        m_knownFailedSites.addAll(failedSiteIds);
        m_siteIds.removeAll(failedSiteIds);
        int expectedResponses = discoverGlobalFaultData_send();
        Long safeInitPoint = discoverGlobalFaultData_rcv(expectedResponses);

        if (safeInitPoint == null) {
            return;
        }


        // Agreed on a fault set.

        // Do the work of patching up the execution site.
        // Do a little work to identify the newly failed site ids and only handle those

        HashSet<Integer> newFailedSiteIds = new HashSet<Integer>(failedSiteIds);
        newFailedSiteIds.removeAll(m_handledFailedSites);

        handleSiteFaults(newFailedSiteIds, safeInitPoint);

        m_handledFailedSites.addAll(failedSiteIds);
        for (NodeFailureFault fault : failures) {
            if (newFailedSiteIds.containsAll(fault.getFailedNonExecSites())) {
                m_faultDistributor.
                    reportFaultHandled(m_faultHandler, fault);
            }
        }
    }

    private void handleSiteFaults(HashSet<Integer> newFailedSiteIds,
            Long safeInitPoint) {
        m_recoveryLog.info("Agreement, handling site faults for newly failed sites " +
                newFailedSiteIds + " safeInitPoint " + safeInitPoint);
        // Fix safe transaction scoreboard in transaction queue
        for (Integer siteId : newFailedSiteIds) {
            m_txnQueue.gotFaultForInitiator(siteId);
            m_server.closeSessions(siteId);
        }

        // Remove affected transactions from RPQ and txnId hash
        // that are not globally initiated
        Iterator<Long> it = m_transactionsById.keySet().iterator();
        while (it.hasNext())
        {
            final long tid = it.next();
            AgreementTransactionState ts = m_transactionsById.get(tid);

            // Fault a transaction that was not globally initiated
            if (ts.txnId > safeInitPoint &&
                    newFailedSiteIds.contains(ts.initiatorSiteId))
            {
                m_recoveryLog.info("Faulting non-globally initiated transaction " + ts.txnId);
                m_txnQueue.faultTransaction(ts);
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
    private int discoverGlobalFaultData_send()
    {
        HashSet<Integer> survivorSet = new HashSet<Integer>(m_siteIds);
        survivorSet.removeAll(m_knownFailedSites);
        int survivors[] = MiscUtils.toArray(survivorSet);
        m_recoveryLog.info("Agreement, Sending fault data " + m_knownFailedSites.toString() + " to "
                + survivorSet.toString() + " survivors");
        try {
            for (Integer site : m_knownFailedSites) {
                /*
                 * Check the queue for the data and get it from the ledger if necessary.\
                 * It might not even be in the ledger if the site has been failed
                 * since recovery of this node began.
                 */
                Long txnId = m_txnQueue.getNewestSafeTransactionForInitiator(site);
                if (txnId == null) {
                    txnId = m_newestSafeTransactionForInitiatorLedger.get(site);
                    //assert(txnId != null);
                } else {
                    m_newestSafeTransactionForInitiatorLedger.put(site, txnId);
                }

                FailureSiteUpdateMessage srcmsg =
                    new FailureSiteUpdateMessage(m_siteId,
                                                 m_knownFailedSites,
                                                 txnId != null ? txnId : Long.MIN_VALUE,
                                                 0);

                m_mailbox.send(survivors, VoltDB.AGREEMENT_MAILBOX_ID, srcmsg);
            }
        }
        catch (MessagingException e) {
            // TODO: unsure what to do with this. maybe it implies concurrent failure?
            e.printStackTrace();
            VoltDB.crashVoltDB();
        }
        m_recoveryLog.info("Agreement, Sent fault data. Expecting " + survivors.length + " responses.");
        return survivors.length;
    }

    /**
     * Collect the failure site update messages from all sites This site sent
     * its own mailbox the above broadcast the maximum is local to this site.
     * This also ensures at least one response.
     *
     * Concurrent failures can be detected by additional reports from the FaultDistributor
     * or a mismatch in the set of failed hosts reported in a message from another site
     */
    private Long discoverGlobalFaultData_rcv(int expectedResponses)
    {
        int responses = 0;
        long safeInitPoint = Long.MIN_VALUE;
        java.util.ArrayList<FailureSiteUpdateMessage> messages = new java.util.ArrayList<FailureSiteUpdateMessage>();

        do {
            VoltMessage m = m_mailbox.recvBlocking(new Subject[] { Subject.FAILURE, Subject.FAILURE_SITE_UPDATE }, 5);
            if (m == null) {
                //Don't need to do anything here?
                continue;
            }

            FailureSiteUpdateMessage fm = null;

            if (m.getSubject() == Subject.FAILURE_SITE_UPDATE.getId()) {
                fm = (FailureSiteUpdateMessage)m;
                messages.add(fm);
            } else if (m.getSubject() == Subject.FAILURE.getId()) {
                /*
                 * If the fault distributor reports a new fault, assert that the fault currently
                 * being handled is included, redeliver the message to ourself and then abort so
                 * that the process can restart.
                 */
                Set<NodeFailureFault> faults = ((FaultMessage)m).nodeFaults;
                HashSet<Integer> newFailedSiteIds = new HashSet<Integer>();
                for (NodeFailureFault fault : faults) {
                    newFailedSiteIds.addAll(fault.getFailedNonExecSites());
                }
                m_mailbox.deliverFront(m);
                m_recoveryLog.info("Agreement, Detected a concurrent failure from FaultDistributor, new failed sites "
                        + newFailedSiteIds);
                return null;
            }
            try {
            /*
             * If the other surviving host saw a different set of failures
             */
            if (!m_knownFailedSites.equals(fm.m_failedSiteIds)) {
                if (!m_knownFailedSites.containsAll(fm.m_failedSiteIds)) {
                    /*
                     * In this case there is a new failed host we didn't know about. Time to
                     * start the process again from square 1 with knowledge of the new failed hosts
                     * There is no need to do additional work because the execution sites
                     * will take care of it. We will pick up again when the fault distributor passes
                     * it to us. We do redeliver the message so we can pick it up again
                     * once failure discovery restarts
                     */
                    m_mailbox.deliver(fm);
                    return null;
                } else {
                    /*
                     * In this instance they are not equal because the message is missing some
                     * failed sites. Drop the message. The sender will detect the fault and resend
                     * the message later with the correct information.
                     */
                    HashSet<Integer> difference = new HashSet<Integer>(m_knownFailedSites);
                    difference.removeAll(fm.m_failedSiteIds);
                    m_recoveryLog.info("Agreement, Discarding failure message from " +
                            fm.m_sourceSiteId + " because it was missing failed sites " + difference.toString());
                    continue;
                }
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
            ++responses;
            m_recoveryLog.info("Agreement, Received failure message " + responses + " of " + expectedResponses
                    + " from " + fm.m_sourceSiteId + " for failed sites " + fm.m_failedSiteIds +
                    " safe txn id " + fm.m_safeTxnId);
            safeInitPoint =
                Math.max(safeInitPoint, fm.m_safeTxnId);
        } while(responses < expectedResponses);
        assert(safeInitPoint != Long.MIN_VALUE);
        return safeInitPoint;
    }

    @Override
    public void request(Request r) {
        m_mailbox.deliver(new LocalObjectMessage(r));
    }

    private static final class AgreementTransactionState extends OrderableTransaction {
        private final Request m_request;
        public AgreementTransactionState(long txnId, int initiatorSiteId, Request r) {
            super(txnId, initiatorSiteId);
            m_request = r;
        }

        @Override
        public boolean isDurable() {
            return true;
        }
    }

    @Override
    public Semaphore createSession(final ServerCnxn cnxn, final byte[] passwd, final int timeout) {
        final Semaphore sem = new Semaphore(0);
        final Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    long sessionId = m_idManager.getNextUniqueTransactionId();
                    Random r = new Random(sessionId ^ ZooKeeperServer.superSecret);
                    r.nextBytes(passwd);
                    ByteBuffer to = ByteBuffer.allocate(4);
                    to.putInt(timeout);
                    to.flip();
                    cnxn.setSessionId(sessionId);
                    Request si = new Request(cnxn, sessionId, 0, OpCode.createSession, to, null);
                    processMessage(new LocalObjectMessage(si));
                } finally {
                    sem.release();
                }
            }
        };
        m_mailbox.deliverFront(new LocalObjectMessage(r));
        return sem;
    }
}
