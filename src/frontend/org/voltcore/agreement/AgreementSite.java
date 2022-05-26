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
package org.voltcore.agreement;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.jute_voltpatches.BinaryInputArchive;
import org.apache.jute_voltpatches.BinaryOutputArchive;
import org.apache.zookeeper_voltpatches.ZooDefs.OpCode;
import org.apache.zookeeper_voltpatches.server.NIOServerCnxn;
import org.apache.zookeeper_voltpatches.server.Request;
import org.apache.zookeeper_voltpatches.server.ServerCnxn;
import org.apache.zookeeper_voltpatches.server.ZooKeeperServer;
import org.json_voltpatches.JSONObject;
import org.voltcore.TransactionIdManager;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.AgreementTaskMessage;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.DisconnectFailedHostsCallback;
import org.voltcore.messaging.FaultMessage;
import org.voltcore.messaging.HeartbeatMessage;
import org.voltcore.messaging.HeartbeatResponseMessage;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.RecoveryMessage;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.network.VoltPort;
import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.collect.ImmutableSet;

/*
 * A wrapper around a single node ZK server. The server is a modified version of ZK that speaks the ZK
 * wire protocol and data model, but has no durability. Agreement is provided
 * by the AgreementSite wrapper which contains a restricted priority queue like an execution site,
 * but also has a transaction id manager and a unique initiator id. The initiator ID and site id are the same
 * as the id of the regular txn initiator on this node. The mailbox used has a different ID so messages
 * for agreement are routed here.
 *
 * Recovery is implemented by shipping a complete snapshot at a txnid to the recovering node, then every node
 * ships all the agreement txns they know about to the recovering node.
 */
public class AgreementSite implements org.apache.zookeeper_voltpatches.server.ZooKeeperServer.Callout {

    private static final byte BINARY_PAYLOAD_SNAPSHOT = 0;
    private static final byte BINARY_PAYLOAD_JOIN_REQUEST = 1;

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
    private final long m_hsId;

    /*
     * Not failed sites
     */
    private final TreeSet<Long> m_hsIds = new TreeSet<Long>();
    private final
    HashMap<Long, OrderableTransaction> m_transactionsById = new HashMap<Long, OrderableTransaction>();
    final AgreementTxnIdSafetyState m_safetyState;
    private volatile boolean m_shouldContinue = true;
    private volatile boolean m_recovering = false;
    private static final VoltLogger m_recoveryLog = new VoltLogger("REJOIN");
    private static final VoltLogger m_agreementLog = new VoltLogger("AGREEMENT");
    private long m_minTxnIdAfterRecovery = Long.MIN_VALUE;
    private final CountDownLatch m_shutdownComplete = new CountDownLatch(1);
    private Map<Long, ZkSnapshot> m_zkSnapshotByTxnId = new HashMap<>();
    private byte m_recoverySnapshot[] = null;
    private Long m_recoverBeforeTxn = null;
    private Long m_siteRequestingRecovery = null;
    private final DisconnectFailedHostsCallback m_failedHostsCallback;
    private final MeshArbiter m_meshArbiter;

    // Max payload length, leave 1k as meta data headspace.
    public static final int MAX_PAYLOAD_MESSAGE_LENGTH = VoltPort.MAX_MESSAGE_LENGTH - 1024;

    private static class ZkSnapshot {
        private byte m_zkSnapshotFrags[][];
        private int m_total = 0;
        private int m_count = 0;
        private int m_compressedSize = 0;

        public ZkSnapshot(int totalFragments) {
            m_total = totalFragments;
            m_zkSnapshotFrags = new byte[totalFragments][];
        }

        public void addFragment(byte[] fragment, int fragmentIndex) {
            m_zkSnapshotFrags[fragmentIndex] = fragment;
            m_compressedSize += fragment.length;
            m_count++;
        }

        public boolean isCompleted() {
            return m_count == m_total;
        }

        public byte[] assemble() {
            // some special cases
            if (m_compressedSize == 0) {
                return null;
            }
            if (m_total == 1) {
                return m_zkSnapshotFrags[0];
            }
            // copy fragments into one big compressed snapshot
            byte[] compressedBytes = new byte[m_compressedSize];
            int destPos = 0;
            for (int i = 0; i < m_total; i++) {
                System.arraycopy(m_zkSnapshotFrags[i], 0, compressedBytes, destPos, m_zkSnapshotFrags[i].length);
                destPos += m_zkSnapshotFrags[i].length;
            }
            return compressedBytes;
        }
    }


    public AgreementSite(
            long myAgreementHSId,
            Set<Long> agreementHSIds,
            int initiatorId,
            Mailbox mailbox,
            InetSocketAddress address,
            long backwardsTimeForgiveness,
            DisconnectFailedHostsCallback failedHostsCallback
            ) throws IOException {
        m_mailbox = mailbox;
        m_hsId = myAgreementHSId;
        m_hsIds.addAll(agreementHSIds);
        m_failedHostsCallback = failedHostsCallback;

        m_idManager = new TransactionIdManager( initiatorId, 0, backwardsTimeForgiveness );
        // note, the agreement site always uses the safety dance, even
        // if it could skip it if there was one node
        m_txnQueue =
            new RestrictedPriorityQueue(
                    myAgreementHSId, mailbox, true);
        m_safetyState = new AgreementTxnIdSafetyState(myAgreementHSId);
        for (Long hsId : m_hsIds) {
            m_txnQueue.ensureInitiatorIsKnown(hsId);
            m_safetyState.addState(hsId);
        }

        m_meshArbiter = new MeshArbiter(m_hsId, mailbox, m_meshAide);
        m_cnxnFactory = new NIOServerCnxn.Factory( address, 10);
        m_server = new ZooKeeperServer(this);
        if (agreementHSIds.size() > 1) {
            m_recovering = true;
        }
        if (m_recovering) {
            m_recoveryStage = RecoveryStage.WAITING_FOR_SAFETY;
        } else {
            m_recoveryComplete.countDown();
        }
    }

    private Set<Long> m_threadIds;
    public void start() throws InterruptedException, IOException {
        m_threadIds = ImmutableSet.<Long>copyOf(m_cnxnFactory.startup(m_server));
    }

    public Set<Long> getThreadIds() {
        return m_threadIds;
    }

    public void shutdown() throws InterruptedException {
        m_shouldContinue = false;
        m_shutdownComplete.await();
    }

    private void shutdownInternal() {
        // note that shutdown will join the thread
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
                    long sourceHSId = 0;
                    for (Long hsId : m_hsIds) {
                        if (hsId != m_hsId) {
                            sourceHSId = hsId;
                            break;
                        }
                    }
                    RecoveryMessage recoveryMessage =
                        new RecoveryMessage(
                                m_hsId,
                                safeTxnId,
                                -1);
                    m_mailbox.send( sourceHSId, recoveryMessage);
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

    private long m_lastUsedTxnId = 0;

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

                    if (ot.txnId < m_minTxnIdAfterRecovery) {
                        String errMsg = "Transaction queue released a transaction from before this " +
                        " node was recovered was complete";
                        org.voltdb.VoltDB.crashLocalVoltDB(errMsg, false, null);
                    }
                    m_transactionsById.remove(ot.txnId);

                    if (ot instanceof AgreementRejoinTransactionState) {
                        AgreementRejoinTransactionState txnState = (AgreementRejoinTransactionState)ot;
                        try {
                            processJoin(txnState.m_rejoiningSite);
                        } finally {
                            if (txnState.m_onCompletion != null) {
                                txnState.m_onCompletion.countDown();
                            }
                        }
                    } else if (ot instanceof AgreementTransactionState) {
                        AgreementTransactionState txnState = (AgreementTransactionState)ot;
                        //Owner is what associates the session with a specific initiator
                        //only used for createSession
                        txnState.m_request.setOwner(txnState.initiatorHSId);

                        /*
                         * We may pull reads out of the priority queue outside the global
                         * order. This means the txnid might be wrong so just sub
                         * the last used txnid from a write that is guaranteed to have been globally
                         * ordered properly
                         *
                         * It doesn't matter for the most part, but the ZK code we give the ID to expects to
                         * it to always increase and if we pull reads in early that will not always be true.
                         */
                        long txnIdToUse = txnState.txnId;
                        switch (txnState.m_request.type) {
                            case OpCode.exists:
                            case OpCode.getChildren:
                            case OpCode.getChildren2:
                            case OpCode.getData:
                                //Don't use the txnid generated for the read since
                                //it may not be globally ordered with writes
                                txnIdToUse = m_lastUsedTxnId;
                                break;
                            default:
                                //This is a write, stash away the txnid for use
                                //for future reads
                                m_lastUsedTxnId = txnState.txnId;
                                break;
                        }

                        m_server.prepRequest(txnState.m_request, txnIdToUse);
                    }
                }
                else if (m_recoverBeforeTxn != null) {
                    assert(m_recoveryStage == RecoveryStage.RECOVERED);
                    assert(m_recovering == false);
                    assert(m_siteRequestingRecovery != null);
                    Long foo = m_txnQueue.safeToRecover();
                    if (foo != null && foo.longValue() >= m_recoverBeforeTxn.longValue()) {
                        shipZKDatabaseSnapshot(m_siteRequestingRecovery, foo);
                    }
                }
            }
        } catch (Throwable e) {
            org.voltdb.VoltDB.crashLocalVoltDB("Error in agreement site", true, e);
        } finally {
            try {
                shutdownInternal();
            }
            catch (Exception e) {
                m_agreementLog.warn("Exception during agreement internal shutdown.", e);
            }
            finally {
                m_shutdownComplete.countDown();
            }
        }
    }

    private void processJoin(long joiningAgreementSite) {
        m_safetyState.addState(joiningAgreementSite);
        m_txnQueue.ensureInitiatorIsKnown(joiningAgreementSite);
        m_hsIds.add(joiningAgreementSite);
        m_recoveryLog.info("Joining site " + CoreUtils.hsIdToString(joiningAgreementSite) +
                " known  active sites " +  CoreUtils.hsIdCollectionToString(m_hsIds));
    }

    private void sendHeartbeats() {
        sendHeartbeats(m_hsIds);
    }

    private void sendHeartbeats(Set<Long> hsIds) {
        long txnId = m_idManager.getNextUniqueTransactionId();
        for (long initiatorId : hsIds) {
            HeartbeatMessage heartbeat =
                new HeartbeatMessage( m_hsId, txnId, m_safetyState.getNewestGloballySafeTxnId());
            m_mailbox.send( initiatorId, heartbeat);
        }
    }

    private long m_lastHeartbeatTime = System.nanoTime();

    private void processMessage(VoltMessage message) throws Exception {
        if (!m_hsIds.contains(message.m_sourceHSId)) {
            String messageFormat = "Dropping message %s because it is not from a known up site";
            m_agreementLog.rateLimitedInfo(10, messageFormat, message);
            return;
        }
        if (message instanceof TransactionInfoBaseMessage) {
            TransactionInfoBaseMessage info = (TransactionInfoBaseMessage)message;

            // Special case heartbeats which only update RPQ
            if (info instanceof HeartbeatMessage) {
                // use the heartbeat to unclog the priority queue if clogged
                long lastSeenTxnFromInitiator = m_txnQueue.noteTransactionRecievedAndReturnLastSeen(
                        info.getInitiatorHSId(), info.getTxnId(),
                        ((HeartbeatMessage) info).getLastSafeTxnId());

                // respond to the initiator with the last seen transaction
                HeartbeatResponseMessage response = new HeartbeatResponseMessage(
                        m_hsId, lastSeenTxnFromInitiator,
                        m_txnQueue.getQueueState() == RestrictedPriorityQueue.QueueState.BLOCKED_SAFETY);
                m_mailbox.send(info.getInitiatorHSId(), response);
                // we're done here (in the case of heartbeats)
                return;
            }
            assert(false);
        } else if (message instanceof HeartbeatResponseMessage) {
            HeartbeatResponseMessage hrm = (HeartbeatResponseMessage)message;
            m_safetyState.updateLastSeenTxnIdFromExecutorBySiteId(
                    hrm.getExecHSId(),
                    hrm.getLastReceivedTxnId());
        } else if (message instanceof LocalObjectMessage) {
            LocalObjectMessage lom = (LocalObjectMessage)message;
            if (lom.payload instanceof Runnable) {
                ((Runnable)lom.payload).run();
            } else if (lom.payload instanceof Request) {
                Request r = (Request)lom.payload;
                long txnId = 0;
                boolean isRead = false;
                switch(r.type) {
                    case OpCode.createSession:
                        txnId = r.sessionId;
                        break;
                    //For reads see if we can skip global agreement and just do the read
                    case OpCode.exists:
                    case OpCode.getChildren:
                    case OpCode.getChildren2:
                    case OpCode.getData:
                        //If there are writes they can go in the queue (and some reads), don't short circuit
                        //in this case because ordering of reads and writes matters
                        if (m_txnQueue.isEmpty()) {
                            r.setOwner(m_hsId);
                            m_server.prepRequest(new Request(r), m_lastUsedTxnId);
                            return;
                        }
                        isRead = true;
                        //Fall through is intentional, going with the default of putting
                        //it in the global order
                    default:
                        txnId = m_idManager.getNextUniqueTransactionId();
                        break;
                }

                /*
                 * Don't send the whole request if this is a read blocked on a write
                 * We may send a heartbeat instead of propagating a useless read transaction
                 * at the end of this block
                 */
                if (!isRead) {
                    for (long initiatorHSId : m_hsIds) {
                        if (initiatorHSId == m_hsId) continue;
                        AgreementTaskMessage atm =
                            new AgreementTaskMessage(
                                    r,
                                    txnId,
                                    m_hsId,
                                    m_safetyState.getNewestGloballySafeTxnId());
                        m_mailbox.send( initiatorHSId, atm);
                    }
                }

                //Process the ATM eagerly locally to aid
                //in having a complete set of stuff to ship
                //to a recovering agreement site
                AgreementTaskMessage atm =
                    new AgreementTaskMessage(
                            new Request(r),
                            txnId,
                            m_hsId,
                            m_safetyState.getNewestGloballySafeTxnId());
                atm.m_sourceHSId = m_hsId;
                processMessage(atm);

                /*
                 * Don't send a heartbeat out for ever single blocked read that occurs
                 * Try and limit to 2000 a second which is a lot and should be pretty
                 * close to the previous behavior of propagating all reads. My measurements
                 * don't show the old behavior is better than none at all, but I fear
                 * change.
                 */
                if (isRead) {
                    final long now = System.nanoTime();
                    if (TimeUnit.NANOSECONDS.toMicros(now - m_lastHeartbeatTime) > 500) {
                        m_lastHeartbeatTime = now;
                        sendHeartbeats();
                    }
                }
            }
        } else if (message instanceof AgreementTaskMessage) {
            AgreementTaskMessage atm = (AgreementTaskMessage)message;
            if (!m_transactionsById.containsKey(atm.m_txnId) && atm.m_txnId >= m_minTxnIdAfterRecovery) {
                m_txnQueue.noteTransactionRecievedAndReturnLastSeen(atm.m_initiatorHSId,
                        atm.m_txnId,
                        atm.m_lastSafeTxnId);

                AgreementTransactionState transactionState =
                    new AgreementTransactionState(atm.m_txnId, atm.m_initiatorHSId, atm.m_request);
                if (m_txnQueue.add(transactionState)) {
                    m_transactionsById.put(transactionState.txnId, transactionState);
                } else {
                    m_agreementLog.info(
                            "Dropping txn " + transactionState.txnId +
                            " data from failed initiatorSiteId: " + transactionState.initiatorHSId);
                }
            } else {
                m_recoveryLog.info("Agreement, discarding duplicate txn during recovery, txnid is " + atm.m_txnId +
                        " this should only occur during recovery. minTxnIdAfterRecovery " +
                        m_minTxnIdAfterRecovery + " and  dup is " + m_transactionsById.containsKey(atm.m_txnId));
            }
        } else if (message instanceof BinaryPayloadMessage) {
            BinaryPayloadMessage bpm = (BinaryPayloadMessage)message;
            ByteBuffer metadata = ByteBuffer.wrap(bpm.m_metadata);
            final byte type = metadata.get();
            if (type == BINARY_PAYLOAD_SNAPSHOT) {
                assert(m_recovering);
                assert(m_recoveryStage == RecoveryStage.SENT_PROPOSAL);
                if (m_recoveryStage != RecoveryStage.SENT_PROPOSAL) {
                    org.voltdb.VoltDB.crashLocalVoltDB(
                            "Received a recovery snapshot in stage " + m_recoveryStage.toString(), true, null);
                }
                final int payloadIndex = metadata.getInt();
                final int totalPayloads = metadata.getInt();
                long selectedRecoverBeforeTxn = metadata.getLong();
                if (selectedRecoverBeforeTxn < m_recoverBeforeTxn) {
                    org.voltdb.VoltDB.crashLocalVoltDB(
                            "Selected recover before txn was earlier than the  proposed recover before txn", true, null);
                }
                m_recoverBeforeTxn = selectedRecoverBeforeTxn;
                // Anything before this precedes the snapshot
                m_minTxnIdAfterRecovery = m_recoverBeforeTxn;
                // Though map is used here I only expect at most one entry in the map.
                // Because zk snapshot is one at a time.
                // If I am proved to be wrong, I may need to revisit the whole path. -ylu
                assert(m_zkSnapshotByTxnId.size() <= 1);
                ZkSnapshot snapshot = m_zkSnapshotByTxnId.get(selectedRecoverBeforeTxn);
                if (snapshot == null) {
                    snapshot = new ZkSnapshot(totalPayloads);
                    m_zkSnapshotByTxnId.put(selectedRecoverBeforeTxn, snapshot);
                }
                snapshot.addFragment(bpm.m_payload, payloadIndex);

                if (!snapshot.isCompleted()) {
                    return;
                }
                // Copy all parts into one big array
                byte[] compressedRecoverySnapshot = snapshot.assemble();
                // snapshot data transfer to m_recoverySnapshot, no longer need this
                snapshot = null;
                m_zkSnapshotByTxnId.clear();
                try {
                    m_recoverySnapshot = org.xerial.snappy.Snappy.uncompress(compressedRecoverySnapshot);
                } catch (IOException e) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Unable to decompress ZK snapshot", true, e);
                }
                m_recoveryStage = RecoveryStage.RECEIVED_SNAPSHOT;

                /*
                 * Clean out all txns from before the snapshot
                 */
                Iterator<Map.Entry< Long, OrderableTransaction>> iter = m_transactionsById.entrySet().iterator();
                while (iter.hasNext()) {
                    final Map.Entry< Long, OrderableTransaction> entry = iter.next();
                    if (entry.getKey() < m_minTxnIdAfterRecovery) {
                        m_txnQueue.faultTransaction(entry.getValue());
                        iter.remove();
                    }
                }
            } else if (type == BINARY_PAYLOAD_JOIN_REQUEST) {
                JSONObject jsObj = new JSONObject(new String(bpm.m_payload, "UTF-8"));
                final long initiatorHSId = jsObj.getLong("initiatorHSId");
                final long txnId = jsObj.getLong("txnId");
                final long lastSafeTxnId = jsObj.getLong("lastSafeTxnId");
                final long joiningHSId = jsObj.getLong("joiningHSId");
                if (m_recovering) {
                    org.voltdb.VoltDB.crashLocalVoltDB(
                            "Received a join request during recovery for " +
                            CoreUtils.hsIdToString(joiningHSId)  +
                            " from " + CoreUtils.hsIdToString(initiatorHSId), true, null);
                }
                m_txnQueue.noteTransactionRecievedAndReturnLastSeen(initiatorHSId,
                        txnId,
                        lastSafeTxnId);
                AgreementRejoinTransactionState transactionState =
                    new AgreementRejoinTransactionState(txnId, initiatorHSId, joiningHSId, null);
                if (m_txnQueue.add(transactionState)) {
                    m_transactionsById.put(transactionState.txnId, transactionState);
                } else {
                    m_agreementLog.info(
                            "Dropping txn " + transactionState.txnId +
                            " data from failed initiatorSiteId: " + transactionState.initiatorHSId);
                }
            }

        } else if (message instanceof FaultMessage) {
            FaultMessage fm = (FaultMessage)message;
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
            DataInputStream dis = new DataInputStream(bais);
            BinaryInputArchive bia = new BinaryInputArchive(dis);
            m_server.getZKDatabase().deserializeSnapshot(bia);
            m_server.createSessionTracker();
        } catch (Exception e) {
            org.voltdb.VoltDB.crashLocalVoltDB("Error loading agreement database", false, e);
        }
        m_recoverySnapshot = null;
        m_recoveryStage = RecoveryStage.RECOVERED;
        m_recovering = false;
        m_recoverBeforeTxn = null;
        m_recoveryComplete.countDown();
        m_agreementLog.info("Loaded ZK snapshot");
    }

    private void shipZKDatabaseSnapshot(long joiningAgreementSite, long txnId) throws IOException {
        m_recoveryLog.info("Shipping ZK snapshot from " + CoreUtils.hsIdToString(m_hsId) +
                " to " + CoreUtils.hsIdToString(joiningAgreementSite));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        BinaryOutputArchive boa = new BinaryOutputArchive(dos);
        m_server.getZKDatabase().serializeSnapshot(boa);
        dos.flush();
        byte databaseBytes[] = org.xerial.snappy.Snappy.compress(baos.toByteArray());

        int startPos = 0;
        int snapshotFragmentIndex = 0;
        int remaining;
        // if payload is larger than max payload size, send it by chunks
        while ((remaining = databaseBytes.length - startPos) > 0) {
            ByteBuffer metadata = ByteBuffer.allocate(17);
            metadata.put(BINARY_PAYLOAD_SNAPSHOT);
            metadata.putInt(snapshotFragmentIndex++);
            metadata.putInt(databaseBytes.length / MAX_PAYLOAD_MESSAGE_LENGTH + 1);
            metadata.putLong(txnId);
            BinaryPayloadMessage bpm =
                    new BinaryPayloadMessage( metadata.array(), databaseBytes, startPos,
                            Math.min(remaining, MAX_PAYLOAD_MESSAGE_LENGTH));
            m_mailbox.send( joiningAgreementSite, bpm);
            if (remaining > MAX_PAYLOAD_MESSAGE_LENGTH) {
                startPos += MAX_PAYLOAD_MESSAGE_LENGTH;
            } else {
                startPos += remaining;
            }
        }
        m_siteRequestingRecovery = null;
        m_recoverBeforeTxn = null;
    }

    private final MeshAide m_meshAide = new MeshAide() {
        @Override
        public void sendHeartbeats(Set<Long> hsIds) {
            AgreementSite.this.sendHeartbeats(hsIds);
        }
        @Override
        public Long getNewestSafeTransactionForInitiator(Long initiatorId) {
            return m_txnQueue.getNewestSafeTransactionForInitiator(initiatorId);
        }
    };

    private void discoverGlobalFaultData(FaultMessage faultMessage) {
        //Keep it simple and don't try to recover on the recovering node.
        if (m_recovering) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Aborting recovery due to a remote node (" + CoreUtils.hsIdToString(faultMessage.failedSite) +
                    ") failure. Retry again.",
                    false,
                    null);
        }
        m_failedHostsCallback.disconnectWithoutMeshDetermination();
        Set<Long> unknownFaultedHosts = new TreeSet<>();

        // This one line is a biggie. Gets agreement on what the post-fault cluster will be.
        Map<Long, Long> initiatorSafeInitPoint = m_meshArbiter.reconfigureOnFault(m_hsIds, faultMessage, unknownFaultedHosts);

        ImmutableSet<Long> failedSites = ImmutableSet.copyOf(initiatorSafeInitPoint.keySet());

        // check if nothing actually happened
        if (initiatorSafeInitPoint.isEmpty() && unknownFaultedHosts.isEmpty()) {
            return;
        }

        ImmutableSet.Builder<Integer> failedHosts = ImmutableSet.builder();
        for (long hsId: failedSites) {
            failedHosts.add(CoreUtils.getHostIdFromHSId(hsId));
        }
        // Remove any hosts associated with failed sites that we don't know
        // about, as could be the case with a failure early in a rejoin
        for (long hsId : unknownFaultedHosts) {
            failedHosts.add(CoreUtils.getHostIdFromHSId(hsId));
        }
        m_failedHostsCallback.disconnect(failedHosts.build());

        // Handle the failed sites after the failedHostsCallback to ensure
        // that partition detection is run first -- as this might release
        // work back to a client waiting on a failure notice. That's unsafe
        // if we partitioned.
        if (!initiatorSafeInitPoint.isEmpty()) {
            handleSiteFaults(failedSites, initiatorSafeInitPoint);
        }

        m_hsIds.removeAll(failedSites);
    }

    private void handleSiteFaults(
            Set<Long> newFailedSiteIds,
            Map<Long, Long> initiatorSafeInitPoint) {

        m_recoveryLog.info("Agreement, handling site faults for newly failed sites " +
                CoreUtils.hsIdCollectionToString(newFailedSiteIds) +
                " initiatorSafeInitPoints " + CoreUtils.hsIdKeyMapToString(initiatorSafeInitPoint));

        // Fix safe transaction scoreboard in transaction queue
        for (Long siteId : newFailedSiteIds) {
            m_safetyState.removeState(siteId);
            m_txnQueue.gotFaultForInitiator(siteId);
            m_server.closeSessions(siteId);
        }

        // Remove affected transactions from RPQ and txnId hash
        // that are not globally initiated
        Iterator<Long> it = m_transactionsById.keySet().iterator();
        while (it.hasNext())
        {
            final long tid = it.next();
            OrderableTransaction ts = m_transactionsById.get(tid);
            if (!initiatorSafeInitPoint.containsKey(ts.initiatorHSId)){
                //Not from a failed initiator, no need to inspect and potentially discard
                continue;
            }
            // Fault a transaction that was not globally initiated
            if (ts.txnId > initiatorSafeInitPoint.get(ts.initiatorHSId) &&
                    newFailedSiteIds.contains(ts.initiatorHSId))
            {
                m_recoveryLog.info("Faulting non-globally initiated transaction " + ts.txnId);
                it.remove();
                m_txnQueue.faultTransaction(ts);
            }
        }
    }

    @Override
    public void request(Request r) {
        LocalObjectMessage lom = new LocalObjectMessage(r);
        lom.m_sourceHSId = m_hsId;
        m_mailbox.deliver(lom);
    }

    static class AgreementTransactionState extends OrderableTransaction {
        public final Request m_request;
        public AgreementTransactionState(long txnId, long initiatorHSId, Request r) {
            super(txnId, initiatorHSId);
            m_request = r;
        }
    }

    /*
     * Txn state associated with rejoining a node
     */
    private static final class AgreementRejoinTransactionState extends OrderableTransaction {
        private final long m_rejoiningSite;
        private final CountDownLatch m_onCompletion;
        public AgreementRejoinTransactionState(
                long txnId,
                long initiatorSiteId,
                long rejoiningSite,
                CountDownLatch onCompletion) {
            super(txnId, initiatorSiteId);
            m_rejoiningSite = rejoiningSite;
            m_onCompletion = onCompletion;
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
                    try {
                        LocalObjectMessage lom = new LocalObjectMessage(si);
                        lom.m_sourceHSId = m_hsId;
                        processMessage(lom);
                    } catch (Exception e) {
                        org.voltdb.VoltDB.crashLocalVoltDB(
                                "Unexpected exception processing AgreementSite message", true, e);
                    }
                } finally {
                    sem.release();
                }
            }
        };
        LocalObjectMessage lom = new LocalObjectMessage(r);
        lom.m_sourceHSId = m_hsId;
        m_mailbox.deliverFront(lom);
        return sem;
    }

    public void reportFault(long faultingSite) {
        FaultMessage fm = new FaultMessage(m_hsId,faultingSite);
        fm.m_sourceHSId = m_hsId;
        m_mailbox.deliver(fm);
    }

    public void reportFault(FaultMessage fm) {
        fm.m_sourceHSId = m_hsId;
        m_mailbox.deliver(fm);
    }

    public void waitForRecovery() throws InterruptedException {
        if (!m_recovering) {
            return;
        }
        // this timeout is totally arbitrary
        // 30s is pretty long in general, but sometimes localcluster may need this long :-(
        if (!m_recoveryComplete.await(30, TimeUnit.SECONDS)) {
            org.voltdb.VoltDB.crashLocalVoltDB("Timed out waiting for the agreement site to recover", false, null);
        }
    }

    /*
     * Construct a ZK transaction that will add the initiator to the cluster
     */
    public CountDownLatch requestJoin(final long joiningSite) throws Exception {
        final CountDownLatch cdl = new CountDownLatch(1);
        final Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    final long txnId = m_idManager.getNextUniqueTransactionId();

                    for (long initiatorHSId : m_hsIds) {
                        if (initiatorHSId == m_hsId) continue;
                        JSONObject jsObj = new JSONObject();
                        jsObj.put("txnId", txnId);
                        jsObj.put("initiatorHSId", m_hsId);
                        jsObj.put("joiningHSId", joiningSite);
                        jsObj.put("lastSafeTxnId", m_safetyState.getNewestSafeTxnIdForExecutorBySiteId(initiatorHSId));
                        byte payload[] = jsObj.toString(4).getBytes("UTF-8");
                        ByteBuffer metadata = ByteBuffer.allocate(1);
                        metadata.put(BINARY_PAYLOAD_JOIN_REQUEST);
                        BinaryPayloadMessage bpm = new BinaryPayloadMessage(metadata.array(), payload);
                        m_mailbox.send( initiatorHSId, bpm);
                    }

                    m_txnQueue.noteTransactionRecievedAndReturnLastSeen(m_hsId,
                            txnId,
                            m_safetyState.getNewestGloballySafeTxnId());

                    AgreementRejoinTransactionState arts =
                        new AgreementRejoinTransactionState( txnId, m_hsId, joiningSite, cdl );

                    if (!m_txnQueue.add(arts)) {
                        org.voltdb.VoltDB.crashLocalVoltDB("Shouldn't have failed to add txn", true, null);
                    }
                    m_transactionsById.put(arts.txnId, arts);
                } catch (Throwable e) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Error constructing JSON", false, e);
                }
            }
        };
        LocalObjectMessage lom = new LocalObjectMessage(r);
        lom.m_sourceHSId = m_hsId;
        m_mailbox.deliver(lom);
        return cdl;
    }

    public int getFailedSiteCount() {
        return m_meshArbiter.getFailedSitesCount();
    }
}
