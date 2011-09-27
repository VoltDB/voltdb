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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.jute_voltpatches.BinaryInputArchive;
import org.apache.jute_voltpatches.BinaryOutputArchive;
import org.apache.zookeeper_voltpatches.ZooDefs.OpCode;
import org.apache.zookeeper_voltpatches.server.NIOServerCnxn;
import org.apache.zookeeper_voltpatches.server.Request;
import org.apache.zookeeper_voltpatches.server.ServerCnxn;
import org.apache.zookeeper_voltpatches.server.ZooKeeperServer;
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
import org.voltdb.messaging.AgreementTaskMessage;
import org.voltdb.messaging.BinaryPayloadMessage;
import org.voltdb.messaging.FailureSiteUpdateMessage;
import org.voltdb.messaging.HeartbeatMessage;
import org.voltdb.messaging.HeartbeatResponseMessage;
import org.voltdb.messaging.LocalObjectMessage;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.RecoveryMessage;
import org.voltdb.messaging.Subject;
import org.voltdb.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.MiscUtils;

/*
 * A wrapper around a single node ZK server. The server is a modified version of ZK that speaks the ZK
 * wire protocol and data model, but has no durability. Agreement is provided
 * by the AgreementSite wrapper which contains a restricted priority queue like an execution site,
 * but also has a transaction id manager and a unique initiator id. The intiator ID and site id are the same
 * as the id of the regular txn initiator on this node. The mailbox used has a different ID so messages
 * for agreement are routed here.
 *
 * Recovery is implemented by shipping a complete snapshot at a txnid to the recovering node, then every node
 * ships all the agreement txns they know about to the recovering node.
 */
public class AgreementSite implements org.apache.zookeeper_voltpatches.server.ZooKeeperServer.Callout {

    private static enum RecoveryStage {
        WAITING_FOR_SAFETY,
        SENT_PROPOSAL,
        RECEIVED_SNAPSHOT,
        RECOVERED
    }

    private RecoveryStage m_recoveryStage = RecoveryStage.RECOVERED;
    private final CountDownLatch m_recoveryComplete = new CountDownLatch(1);
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
    private final CountDownLatch m_shutdownComplete = new CountDownLatch(1);
    private byte m_recoverySnapshot[] = null;
    private Long m_recoverBeforeTxn = null;
    private Integer m_siteRequestingRecovery = null;

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

        public FaultMessage(Set<NodeFailureFault> faults) {
            this.nodeFaults = faults;
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
            Set<Integer> faultedNonExecSites = new HashSet<Integer>();
            for (VoltFault fault : faults) {
                if (fault instanceof NodeFailureFault) {
                    NodeFailureFault nodeFault = (NodeFailureFault)fault;
                    faultedNodes.add(nodeFault);
                    faultedNonExecSites.addAll(nodeFault.getFailedNonExecSites());
                }
            }
            if (!faultedNodes.isEmpty()) {
                m_recoveryLog.info("Delivering fault message with failed non-exec sites " + faultedNonExecSites);
                m_mailbox.deliver(new FaultMessage(faultedNodes));
            }
        }

        @Override
        public void faultCleared(Set<VoltFault> faults) {
            final HashSet<VoltFault> copy = new HashSet<VoltFault>(faults);
            m_mailbox.deliver(new LocalObjectMessage(new Runnable() {
                @Override
                public void run() {
                    for (VoltFault fault : copy) {
                        if (fault instanceof NodeFailureFault) {
                            NodeFailureFault nff = (NodeFailureFault)fault;
                            for (Integer site : nff.getFailedNonExecSites()) {
                                processRejoin(site);
                            }
                        }
                    }
                }
            }));
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
        if (recovering) {
            m_recoveryStage = RecoveryStage.WAITING_FOR_SAFETY;
        } else {
            m_recoveryComplete.countDown();
        }
    }

    public void start() throws InterruptedException, IOException {
        m_cnxnFactory.startup(m_server);
    }

    public void shutdown() throws InterruptedException {
        m_shouldContinue = false;
        m_shutdownComplete.await();
    }

    private void shutdownInternal() {
        m_cnxnFactory.shutdown();
    }


    public void recoveryRunLoop() throws Exception {
        long lastHeartbeatTime = System.currentTimeMillis();
        while (m_recovering && m_shouldContinue) {
            if (m_recoveryStage == RecoveryStage.WAITING_FOR_SAFETY) {
                Long safeTxnId = m_txnQueue.safeToRecover();
                if (safeTxnId != null) {
                    m_recoveryStage = RecoveryStage.SENT_PROPOSAL;
                    m_recoverBeforeTxn = safeTxnId;
                    int sourceSiteId = 0;
                    for (Integer siteId : m_siteIds) {
                        if (siteId != m_siteId) {
                            sourceSiteId = siteId;
                            break;
                        }
                    }
                    ByteBuffer buf = ByteBuffer.allocate(2048);
                    BBContainer container = DBBPool.wrapBB(buf);
                    RecoveryMessage recoveryMessage =
                        new RecoveryMessage(
                                container,
                                m_siteId,
                                safeTxnId,
                                new byte[4], -1);
                    m_mailbox.send( sourceSiteId, VoltDB.AGREEMENT_MAILBOX_ID, recoveryMessage);
                }
            }

            VoltMessage message = m_mailbox.recvBlocking(5);
            if (message != null) {
                processMessage(message);
            }

            final long now = System.currentTimeMillis();
            if (now - lastHeartbeatTime > 5) {
                lastHeartbeatTime = now;
                sendHeartbeats();
            }

            if (m_recoverBeforeTxn == null) {
                continue;
            }

            if (m_txnQueue.peek() != null && m_txnQueue.peek().txnId < m_recoverBeforeTxn.longValue()) {
                m_transactionsById.remove(m_txnQueue.poll().txnId);
            } else if (m_recoveryStage == RecoveryStage.RECEIVED_SNAPSHOT) {
                processZKSnapshot();
                return;
            }
        }
    }
    @Override
    public void run() {
        try {
            if (m_recovering) {
                recoveryRunLoop();
            }
            long lastHeartbeatTime = System.currentTimeMillis();
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

                OrderableTransaction ot = m_txnQueue.poll();
                if (ot != null) {
                    if (m_recoverBeforeTxn != null) {
                        assert(m_recoveryStage == RecoveryStage.RECOVERED);
                        assert(m_recovering == false);
                        assert(m_siteRequestingRecovery != null);
                        if (ot.txnId >= m_recoverBeforeTxn) {
                            shipZKDatabaseSnapshot(m_siteRequestingRecovery, ot.txnId);
                        }
                    }

                    if (ot.txnId <= m_minTxnIdAfterRecovery) {
                        String errMsg = "Transaction queue released a transaction from before this " +
                                " node was recovered was complete";
                        VoltDB.crashLocalVoltDB(errMsg, false, null);
                    }
                    m_transactionsById.remove(ot.txnId);

                    if (ot instanceof AgreementRejoinTransactionState) {
                        AgreementRejoinTransactionState txnState = (AgreementRejoinTransactionState)ot;
                    } else if (ot instanceof AgreementTransactionState) {
                        AgreementTransactionState txnState = (AgreementTransactionState)ot;
                        //Owner is what associates the session with a specific initiator
                        //only used for createSession
                        txnState.m_request.setOwner(txnState.initiatorSiteId);
                        m_server.prepRequest(txnState.m_request, txnState.txnId);
                    }
                } else if (m_recoverBeforeTxn != null) {
                    assert(m_recoveryStage == RecoveryStage.RECOVERED);
                    assert(m_recovering == false);
                    assert(m_siteRequestingRecovery != null);
                    Long foo = m_txnQueue.safeToRecover();
                    if (foo != null && foo.longValue() >= m_recoverBeforeTxn.longValue()) {
                        shipZKDatabaseSnapshot(m_siteRequestingRecovery, foo);
                    }
                }
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error in agreement site", false, e);
        } finally {
            try {
                shutdownInternal();
            } finally {
                m_shutdownComplete.countDown();
            }
        }
    }

    private void processRejoin(int rejoiningAgreementSite) {
        m_knownFailedSites.remove(rejoiningAgreementSite);
        m_handledFailedSites.remove(rejoiningAgreementSite);
        m_safetyState.addRejoinedState(rejoiningAgreementSite);
        m_txnQueue.ensureInitiatorIsKnown(rejoiningAgreementSite);
        m_siteIds.add(rejoiningAgreementSite);
        m_recoveryLog.info("Unfaulting site " + rejoiningAgreementSite + " known failed sites "
                + m_knownFailedSites + " handled failed sites " + m_handledFailedSites + " active sites " + m_siteIds);
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
                if (m_txnQueue.add(transactionState)) {
                    m_transactionsById.put(transactionState.txnId, transactionState);
                } else {
                    m_agreementLog.info(
                            "Dropping txn " + transactionState.txnId +
                            " data from failed initiatorSiteId: " + transactionState.initiatorSiteId);
                }
            } else {
                m_recoveryLog.info("Agreement, discarding duplicate txn during recovery, txnid is " + atm.m_txnId +
                        " this should only occur during recovery.");
            }
        } else if (message instanceof BinaryPayloadMessage) {
            BinaryPayloadMessage bpm = (BinaryPayloadMessage)message;
            assert(m_recovering);
            assert(m_recoveryStage == RecoveryStage.SENT_PROPOSAL);
            if (m_recoveryStage != RecoveryStage.SENT_PROPOSAL) {
                VoltDB.crashLocalVoltDB("Received a recovery snapshot in stage " + m_recoveryStage.toString(), true, null);
            }
            long selectedRecoverBeforeTxn = ByteBuffer.wrap(bpm.m_metadata).getLong();
            if (selectedRecoverBeforeTxn < m_recoverBeforeTxn) {
                VoltDB.crashLocalVoltDB("Selected recover before txn was earlier than the  proposed recover before txn", true, null);
            }
            m_recoverBeforeTxn = selectedRecoverBeforeTxn;
            m_recoverySnapshot = bpm.m_payload;
            m_recoveryStage = RecoveryStage.RECEIVED_SNAPSHOT;
        } else if (message instanceof FaultMessage) {
            FaultMessage fm = (FaultMessage)message;

            for (NodeFailureFault fault : fm.nodeFaults){
                for (Integer faultedInitiator : fault.getFailedNonExecSites()) {
                    m_safetyState.removeState(faultedInitiator);
                }
            }
            discoverGlobalFaultData(fm);
        } else if (message instanceof RecoveryMessage) {
            RecoveryMessage rm = (RecoveryMessage)message;
            assert(m_recoverBeforeTxn == null);
            assert(m_siteRequestingRecovery == null);
            assert(m_recovering == false);
            assert(m_recoveryStage == RecoveryStage.RECOVERED);
            m_recoverBeforeTxn = rm.txnId();
            m_siteRequestingRecovery = rm.sourceSite();
        }
    }

    private void processZKSnapshot() {
        ByteArrayInputStream bais = new ByteArrayInputStream(m_recoverySnapshot);
        try {
            InflaterInputStream iis = new InflaterInputStream(bais);
            DataInputStream dis = new DataInputStream(iis);
            BinaryInputArchive bia = new BinaryInputArchive(dis);
            m_server.getZKDatabase().deserializeSnapshot(bia);
            m_server.createSessionTracker();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error loading agreement database", false, e);
        }
        m_recoverySnapshot = null;
        m_recoveryStage = RecoveryStage.RECOVERED;
        m_recovering = false;
        m_recoverBeforeTxn = null;
        m_recoveryComplete.countDown();
        VoltDB.instance().onAgreementSiteRecoveryCompletion();
        m_agreementLog.info("Loaded ZK snapshot");
    }

    private void shipZKDatabaseSnapshot(int faultedInitiator, long txnId) throws IOException {
        m_recoveryLog.info("Shipping ZK snapshot from " + m_siteId + " to " + faultedInitiator);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
        DeflaterOutputStream defos = new DeflaterOutputStream(baos, deflater);
        DataOutputStream dos = new DataOutputStream(defos);
        BinaryOutputArchive boa = new BinaryOutputArchive(dos);
        m_server.getZKDatabase().serializeSnapshot(boa);
        dos.flush();
        defos.finish();
        byte databaseBytes[] = baos.toByteArray();
        ByteBuffer metadata = ByteBuffer.allocate(16);
        metadata.putLong(txnId);
        BinaryPayloadMessage bpm = new BinaryPayloadMessage( metadata.array(), databaseBytes);
        try {
            m_mailbox.send( faultedInitiator, VoltDB.AGREEMENT_MAILBOX_ID, bpm);
        } catch (MessagingException e) {
            throw new IOException(e);
        }
        m_siteRequestingRecovery = null;
        m_recoverBeforeTxn = null;
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
            VoltDB.crashLocalVoltDB("Aborting recovery due to a remote node failure. Retry again.", true, null);
        }
        Set<NodeFailureFault> failures = faultMessage.nodeFaults;

        Set<Integer> failedSiteIds = getFaultingSites(faultMessage);
        m_knownFailedSites.addAll(failedSiteIds);
        m_siteIds.removeAll(m_knownFailedSites);
        HashMap<Integer, Integer> expectedResponseCounts = new HashMap<Integer, Integer>();
        int expectedResponses = discoverGlobalFaultData_send(expectedResponseCounts);
        HashMap<Integer, Long> initiatorSafeInitPoint =
            discoverGlobalFaultData_rcv(expectedResponses, expectedResponseCounts);

        if (initiatorSafeInitPoint == null) {
            return;
        }


        // Agreed on a fault set.

        // Do the work of patching up the execution site.
        // Do a little work to identify the newly failed site ids and only handle those

        HashSet<Integer> newFailedSiteIds = new HashSet<Integer>(failedSiteIds);
        newFailedSiteIds.removeAll(m_handledFailedSites);

        handleSiteFaults(newFailedSiteIds, initiatorSafeInitPoint);

        m_handledFailedSites.addAll(failedSiteIds);
        for (NodeFailureFault fault : failures) {
            if (newFailedSiteIds.containsAll(fault.getFailedNonExecSites())) {
                m_faultDistributor.
                    reportFaultHandled(m_faultHandler, fault);
            }
        }
    }

    private void handleSiteFaults(HashSet<Integer> newFailedSiteIds,
            HashMap<Integer, Long> initiatorSafeInitPoint) {
        m_recoveryLog.info("Agreement, handling site faults for newly failed sites " +
                newFailedSiteIds + " initiatorSafeInitPoints " + initiatorSafeInitPoint);
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
            if (!initiatorSafeInitPoint.containsKey(ts.initiatorSiteId)){
                //Not from a failed initiator, no need to inspect and potentially discard
                continue;
            }
            // Fault a transaction that was not globally initiated
            if (ts.txnId > initiatorSafeInitPoint.get(ts.initiatorSiteId) &&
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
    private int discoverGlobalFaultData_send(HashMap<Integer, Integer> messagesPerSite)
    {
        HashSet<Integer> survivorSet = new HashSet<Integer>(m_siteIds);
        survivorSet.removeAll(m_knownFailedSites);
        int survivors[] = MiscUtils.toArray(survivorSet);
        m_recoveryLog.info("Agreement, Sending fault data " + m_knownFailedSites.toString() + " to "
                + survivorSet.toString() + " survivors");
        for (Integer survivor : survivors) {
            messagesPerSite.put(survivor, m_knownFailedSites.size());
        }
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
                                                 site,
                                                 txnId != null ? txnId : Long.MIN_VALUE,
                                                 site);

                m_mailbox.send(survivors, VoltDB.AGREEMENT_MAILBOX_ID, srcmsg);
            }
        }
        catch (MessagingException e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), false, e);
        }
        m_recoveryLog.info("Agreement, Sent fault data. Expecting " + (survivors.length * m_knownFailedSites.size()) + " responses.");
        return (survivors.length * m_knownFailedSites.size());
    }

    /**
     * Collect the failure site update messages from all sites This site sent
     * its own mailbox the above broadcast the maximum is local to this site.
     * This also ensures at least one response.
     *
     * Concurrent failures can be detected by additional reports from the FaultDistributor
     * or a mismatch in the set of failed hosts reported in a message from another site
     */
    private HashMap<Integer, Long> discoverGlobalFaultData_rcv(
            int expectedResponses,
            HashMap<Integer, Integer> expectedResponseCount)
    {
        int responses = 0;
        java.util.ArrayList<FailureSiteUpdateMessage> messages = new java.util.ArrayList<FailureSiteUpdateMessage>();
        HashMap<Integer, Long> initiatorSafeInitPoint = new HashMap<Integer, Long>();
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
                    HashSet<Integer> difference = new HashSet<Integer>(fm.m_failedSiteIds);
                    difference.removeAll(m_knownFailedSites);
                    Set<Integer> differenceHosts = new HashSet<Integer>();
                    for (Integer siteId : difference) {
                        differenceHosts.add(VoltDB.instance().getCatalogContext().siteTracker.getHostForSite(siteId));
                    }
                    for (Integer hostId : differenceHosts) {
                        String hostname = String.valueOf(hostId);
                        if (VoltDB.instance() != null) {
                            if (VoltDB.instance().getHostMessenger() != null) {
                                String hostnameTemp = VoltDB.instance().getHostMessenger().getHostnameForHostID(hostId);
                                if (hostnameTemp != null) hostname = hostnameTemp;
                            }
                        }
                        VoltDB.instance().getFaultDistributor().
                            reportFault(new NodeFailureFault(
                                    hostId,
                                    VoltDB.instance().getCatalogContext().siteTracker.getNonExecSitesForHost(hostId),
                                    hostname));
                    }
                    m_recoveryLog.info("Detected a concurrent failure from " +
                            fm.m_sourceSiteId + " with new failed sites " + difference.toString());
                    m_mailbox.deliver(m);
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

            expectedResponseCount.put( fm.m_sourceSiteId, expectedResponseCount.get(fm.m_sourceSiteId) - 1);
            ++responses;
            m_recoveryLog.info("Agreement, Received failure message " + responses + " of " + expectedResponses
                    + " from " + fm.m_sourceSiteId + " for failed sites " + fm.m_failedSiteIds +
                    " safe txn id " + fm.m_safeTxnId + " failed site " + fm.m_committedTxnId);
            m_recoveryLog.info("Agreement, expecting failures messages " + expectedResponseCount);
            if (!initiatorSafeInitPoint.containsKey(fm.m_initiatorForSafeTxnId)) {
                initiatorSafeInitPoint.put(fm.m_initiatorForSafeTxnId, Long.MIN_VALUE);
            }
            initiatorSafeInitPoint.put(
                    fm.m_initiatorForSafeTxnId,
                    Math.max(initiatorSafeInitPoint.get(fm.m_initiatorForSafeTxnId), fm.m_safeTxnId));
        } while(responses < expectedResponses);
        assert(!initiatorSafeInitPoint.containsValue(Long.MIN_VALUE));
        return initiatorSafeInitPoint;
    }

    @Override
    public void request(Request r) {
        m_mailbox.deliver(new LocalObjectMessage(r));
    }

    private static class AgreementTransactionState extends OrderableTransaction {
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

    private static final class AgreementRejoinTransactionState extends AgreementTransactionState {
        private final int m_rejoiningSite;

        public AgreementRejoinTransactionState(long txnId, int initiatorSiteId, int rejoiningSite, CountDownLatch onCompletion) {
            super(txnId, initiatorSiteId, null);
            m_rejoiningSite = rejoiningSite;
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

    public void clearFault(final int rejoiningSite) throws InterruptedException {
        final CountDownLatch onCompletion = new CountDownLatch(1);
        final Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    processRejoin(rejoiningSite);
                } finally {
                    onCompletion.countDown();
                }
            }
        };
        m_mailbox.deliver(new LocalObjectMessage(r));
        onCompletion.await();
    }

    public void waitForRecovery() throws InterruptedException {
        if (!m_recovering) {
            return;
        }
        // this timeout is totally arbitrary
        // 30s is pretty long in general, but sometimes localcluster may need this long :-(
        if (!m_recoveryComplete.await(30, TimeUnit.SECONDS)) {
            m_recoveryLog.fatal("Timed out waiting for the agreement site to recover");
            VoltDB.crashVoltDB();
        }
    }
}
