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

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.PartitionDRGateway;
import org.voltdb.PartitionDRGateway.DRRecordType;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.ReplicatedTableException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.exceptions.TransactionRestartException;
import org.voltdb.exceptions.TransactionTerminationException;
import org.voltdb.messaging.BorrowTaskMessage;
import org.voltdb.messaging.DumpMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTableUtil;
import org.voltdb.utils.VoltTrace;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.collect.Maps;

public class MpTransactionState extends TransactionState
{
    static VoltLogger tmLog = new VoltLogger("TM");

    public static final int DR_MAX_AGGREGATE_BUFFERSIZE = Integer.getInteger("DR_MAX_AGGREGATE_BUFFERSIZE", (45 * 1024 * 1024) + 4096);
    public static final long MP_MAX_TOTAL_RESP_SIZE = calculateMpMaxTotalResponse();

    static final long MP_MAX_TOTAL_RESP_SIZE_MIN = 50 * 1024 * 1024; // 50MB
    static final String MP_MAX_TOTAL_RESP_SIZE_KEY = "MP_MAX_TOTAL_RESP_SIZE";

    private static final String dr_max_consumer_partitionCount_str = "DR_MAX_CONSUMER_PARTITIONCOUNT";
    private static final String dr_max_consumer_messageheader_room_str = "DR_MAX_CONSUMER_MESSAGEHEADER_ROOM";
    private static final String volt_output_buffer_overflow = "V0001";
    private static final int DR_BEGINTXN_MSG_LEN = PartitionDRGateway.getMessageTypeLength(DRRecordType.BEGIN_TXN);
    private static final int DR_ENDTXN_MSG_LEN = PartitionDRGateway.getMessageTypeLength(DRRecordType.END_TXN);

    /**
     *  This is thrown by the TransactionState instance when something
     *  goes wrong mid-fragment, and execution needs to back all the way
     *  out to the stored procedure call.
     */
    public static class FragmentFailureException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    final Iv2InitiateTaskMessage m_initiationMsg;

    final LinkedBlockingDeque<FragmentResponseMessage> m_newDeps =
        new LinkedBlockingDeque<FragmentResponseMessage>();
    Map<Integer, Set<Long>> m_remoteDeps;
    Map<Integer, List<VoltTable>> m_remoteDepTables =
        new HashMap<Integer, List<VoltTable>>();
    private int m_drBufferChangedAgg = 0;
    private int m_localPartitionCount;
    private final boolean m_drProducerActive = VoltDB.instance().getNodeDRGateway() != null && VoltDB.instance().getNodeDRGateway().isActive();

    final List<Long> m_useHSIds = new ArrayList<Long>();
    final Map<Integer, Long> m_masterHSIds = Maps.newHashMap();
    long m_buddyHSId = -1;
    FragmentTaskMessage m_remoteWork = null;
    FragmentTaskMessage m_localWork = null;
    boolean m_haveDistributedInitTask = false;
    boolean m_isRestart = false;
    boolean m_fragmentRestarted = false;
    int m_fragmentIndex = 0;
    final boolean m_nPartTxn;
    boolean m_haveSentfragment = false;
    private Supplier<Long> m_buddySupplier;

    //Master change from MigratePartitionLeader. The remote dependencies are built before MigratePartitionLeader. After
    //fragment restart, the FragmentResponseMessage will come from the new partition master. The map is used to remove
    //the remote dependency which is built with the old partition master.
    final Map<Long, Long> m_masterMapForFragmentRestart = Maps.newHashMap();

    //The timeout value for fragment response in minute. default: 5 min
    private static long PULL_TIMEOUT = Long.valueOf(System.getProperty("MP_TXN_RESPONSE_TIMEOUT", "5")) * 60L;

    MpTransactionState(Mailbox mailbox,
                       TransactionInfoBaseMessage notice,
                       List<Long> useHSIds, Map<Integer, Long> partitionMasters,
                       Supplier<Long> buddySupplier, boolean isRestart, boolean nPartTxn)
    {
        super(mailbox, notice);
        m_initiationMsg = (Iv2InitiateTaskMessage)notice;
        m_useHSIds.addAll(useHSIds);
        m_masterHSIds.putAll(partitionMasters);
        m_buddySupplier = buddySupplier;
        m_isRestart = isRestart;
        m_nPartTxn = nPartTxn;
        m_localPartitionCount = m_masterHSIds.size();
    }

    static final int StoreProcedureInvocationHeaderSize;
    static final int InitiateTaskMessageHeaderSize;
    static final int FragmentTaskMessageHeaderSize;
    static final int MessageAdditionalSize;

    static {
        StoredProcedureInvocation mpSpi = new StoredProcedureInvocation();
        mpSpi.setProcName("@ApplyBinaryLogMP");
        StoreProcedureInvocationHeaderSize = mpSpi.getFixedHeaderSize();
        InitiateTaskMessageHeaderSize = (new Iv2InitiateTaskMessage()).getFixedHeaderSize();
        FragmentTaskMessageHeaderSize = (new FragmentTaskMessage()).getFixedHeaderSize();
        // We estimate the message size for the consumer cluster via the message size on producer cluster.
        // However, this could be underestimation if consumer cluster's message is bigger. (usually true for newer volt version).
        // We add MessageAdditionSize for capturing max additional size the consumer side can have over producer side.
        // For volt version < 7.0, it can only connect to 7.0, the message size didn't change.
        // For volt 7.0 < version <= 7.5 , it can connect to 7.6 and up, the message size at lease increased by 11 bytes. (10 from fragmentTaskMessage and 1 from Iv2InitiateTaskMessage).
        // For forward compatibility, reserve 100 bytes.
        // TODO: We could report an more accurate consumer side message header size via DR protocol change.
        MessageAdditionalSize = Integer.getInteger(dr_max_consumer_messageheader_room_str, 100);
    }

    private int getDRMessageSizeEstimation() {
        // we could get accurate remote cluster(s) partition count through query dr producer (which get through sync snapshot request)
        // however that is too heavy weight for the txn path
        // default assume homogenous dr, i.e remote partition count equals to local partition count
        // user can also override this via system property
        int remotePartitionCount = Integer.getInteger(dr_max_consumer_partitionCount_str, m_localPartitionCount);

        // assume still has replicated stream for upper bound estimation avoid check the dr system for
        // performance concern. The streamCount could be m_localPartitionCount if DR ProtocolVersion is 8 or more.
        int streamCount = m_localPartitionCount + 1;
        int concatLogSize = m_drBufferChangedAgg + (DR_BEGINTXN_MSG_LEN + DR_ENDTXN_MSG_LEN) * streamCount; // adding BEGIN_Transaction and END_Transaction Size

        // estimate of ParametersSet Size of @ApplyBinaryLogMP on the consumer side
        int serializedParamSize = getSerializedParamSizeForApplyBinaryLog(streamCount, remotePartitionCount, concatLogSize);

        return serializedParamSize + StoreProcedureInvocationHeaderSize + InitiateTaskMessageHeaderSize + FragmentTaskMessageHeaderSize + MessageAdditionalSize;
    }

    // calculate based on BinaryLogHelper and ParameterSet.fromArrayNoCopy
    private static int getSerializedParamSizeForApplyBinaryLog(int streamCount, int remotePartitionCount, int concatLogSize) {
        int serializedParamSize = 2
                + 1 + 4                                                             // placeholder byte[0]
                + 1 + 4                                                             // producerClusterId Integer
                + 1 + 4 + 4 +  (4 + 8 * remotePartitionCount) * streamCount         // concatLogIds byte[]
                + 1 + 4 + 4  + (4 + 8 + 8 + 4 + 4 + 16) * streamCount               // concatTrackerBufs (DRConsumerDrIdTracker) byte[]
                + 1 + 4 + 4 + 4 * streamCount + concatLogSize                       // concatLogs byte[]
                + 1 + 1                                                             // extraOption Byte
                + 1 + 4;                                                            // extraParameters byte[0]

        return serializedParamSize;
    }

    static long calculateMpMaxTotalResponse() {
        String key = MP_MAX_TOTAL_RESP_SIZE_KEY;
        String mpHeapString = System.getProperty(key, System.getenv(key));
        long maxTotalResponse = 0;
        if (mpHeapString != null) {
            try {
                if (mpHeapString.endsWith("%")) {
                    double percent = Double.parseDouble(mpHeapString.substring(0, mpHeapString.length() - 1));
                    if (percent > 0) {
                        maxTotalResponse = percentOfTotalHeap(percent);
                    }
                } else {
                    maxTotalResponse = Long.parseLong(mpHeapString);
                }

                if (maxTotalResponse < MP_MAX_TOTAL_RESP_SIZE_MIN) {
                    tmLog.warn(String.format(
                            "Invalid value, '%s', provided for config  %s. Value must result in a size greater than %d : %d",
                            mpHeapString, key, MP_MAX_TOTAL_RESP_SIZE, maxTotalResponse));
                    return MP_MAX_TOTAL_RESP_SIZE_MIN;
                }
                tmLog.info(String.format("Config %s set to %s evaluated to max mp response total of %d", key,
                        mpHeapString, maxTotalResponse));
                return maxTotalResponse;
            } catch (NumberFormatException e) {
                tmLog.warn(String.format("Could not parse value for config %s: '%s'", key, mpHeapString));
            }
        }

        maxTotalResponse = percentOfTotalHeap(65.0);
        if (tmLog.isDebugEnabled()) {
            tmLog.debug("Using default mp max total response of " + maxTotalResponse);
        }
        return maxTotalResponse;
    }

    private static long percentOfTotalHeap(double percent) {
        double percentOfMemory = Runtime.getRuntime().maxMemory() * percent / 100;
        return (long) Math.min(percentOfMemory, Long.MAX_VALUE);
    }

    public void updateMasters(List<Long> masters, Map<Integer, Long> partitionMasters)
    {
        // TODO separate NPTransactionState from MPTransactionState when work on concurrent np transaction
        if (m_nPartTxn) {
            partitionMasters = trimPartitionMasters(partitionMasters);
            masters = Lists.newArrayList(partitionMasters.values());
        }
        if (tmLog.isDebugEnabled()) {
            tmLog.debug("[MpTransactionState] TXN ID: " + TxnEgo.txnIdSeqToString(txnId) + " update masters from " +  CoreUtils.hsIdCollectionToString(m_useHSIds)
            + " to "+ CoreUtils.hsIdCollectionToString(masters));
        }
        m_useHSIds.clear();
        m_useHSIds.addAll(masters);
        m_masterHSIds.clear();
        m_masterHSIds.putAll(partitionMasters);
        m_localPartitionCount = m_masterHSIds.size();
    }

    private HashMap<Integer, Long> trimPartitionMasters(Map<Integer, Long> partitionMasters)
    {
        HashMap<Integer,Long> partitionMastersCopy = Maps.newHashMap(partitionMasters);

        // For n-partition transaction, only care about the partitions involved in the transaction.
        partitionMastersCopy.keySet().retainAll(m_masterHSIds.keySet());
        return partitionMastersCopy;
    }

    /**
     * Used to reset the internal state of this transaction so it can be successfully restarted
     */
    void restart()
    {
        // The poisoning path will, unfortunately, set this to true.  Need to undo that.
        setNeedsRollback(false);
        // Also need to make sure that we get the original invocation in the first fragment
        // since some masters may not have seen it.
        m_haveDistributedInitTask = false;
        m_isRestart = true;
        m_fragmentIndex = 0;
        m_haveSentfragment = false;
        m_drBufferChangedAgg = 0;
    }

    @Override
    public boolean isSinglePartition()
    {
        return false;
    }

    @Override
    public StoredProcedureInvocation getInvocation()
    {
        return m_initiationMsg.getStoredProcedureInvocation();
    }

    // Overrides needed by MpProcedureRunner
    @Override
    public void setupProcedureResume(int[] dependencies)
    {
        // Reset state so we can run this batch cleanly
        m_localWork = null;
        m_remoteWork = null;
        m_remoteDeps = null;
        m_remoteDepTables.clear();
    }

    // I met this List at bandcamp...
    public void setupProcedureResume(List<Integer> deps)
    {
        setupProcedureResume(com.google_voltpatches.common.primitives.Ints.toArray(deps));
    }

    @Override
    public void createLocalFragmentWork(FragmentTaskMessage task, boolean nonTransactional)
    {
        m_localWork = task;
        m_localWork.setTruncationHandle(m_initiationMsg.getTruncationHandle());
    }

    @Override
    public void createAllParticipatingFragmentWork(FragmentTaskMessage task)
    {
        // Don't generate remote work or dependency tracking or anything if
        // there are no fragments to be done in this message
        // At some point maybe ProcedureRunner.slowPath() can get smarter
        if (task.getFragmentCount() > 0) {
            // Distribute the initiate task for command log replay.
            // Command log must log the initiate task;
            // Only send the fragment once.
            if (!m_haveDistributedInitTask && !isForReplay() && !isReadOnly()) {
                m_haveDistributedInitTask = true;
                task.setStateForDurability((Iv2InitiateTaskMessage) getNotice(), m_masterHSIds.keySet());
            }

            if (m_isRestart) {
                task.setTimestamp(m_restartTimestamp);
            }
            m_remoteWork = task;
            m_remoteWork.setTruncationHandle(m_initiationMsg.getTruncationHandle());
            m_haveSentfragment = true;
            // Distribute fragments to remote destinations.
            long[] non_local_hsids = new long[m_useHSIds.size()];
            for (int i = 0; i < m_useHSIds.size(); i++) {
                non_local_hsids[i] = m_useHSIds.get(i);

                int finalI = i;
                final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.MPSITE);
                if (traceLog != null) {
                    traceLog.add(() -> VoltTrace.beginAsync("sendfragment",
                                                            MiscUtils.hsIdPairTxnIdToString(m_mbox.getHSId(), non_local_hsids[finalI], txnId, task.getCurrentBatchIndex()),
                                                            "txnId", TxnEgo.txnIdToString(txnId),
                                                            "dest", CoreUtils.hsIdToString(non_local_hsids[finalI])));
                }
            }
            // send to all non-local sites
            if (non_local_hsids.length > 0) {
                m_mbox.send(non_local_hsids, m_remoteWork);
            }
        }
        else {
            m_remoteWork = null;
        }
    }

    private static Map<Integer, Set<Long>>
        createTrackedDependenciesFromTask(FragmentTaskMessage task, List<Long> expectedHSIds)
    {
        Map<Integer, Set<Long>> depMap = new HashMap<Integer, Set<Long>>();
        for (int i = 0; i < task.getFragmentCount(); i++) {
            int dep = task.getOutputDepId(i);
            Set<Long> scoreboard = new HashSet<Long>();
            depMap.put(dep, scoreboard);
            for (long hsid : expectedHSIds) {
                scoreboard.add(hsid);
            }
        }
        return depMap;
    }

    @Override
    public Map<Integer, List<VoltTable>> recursableRun(SiteProcedureConnection siteConnection)
    {
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.MPSITE);

        // if we're restarting this transaction, and we only have local work, add some dummy
        // remote work so that we can avoid injecting a borrow task into the local buddy site
        // before the CompleteTransactionMessage with the restart flag reaches it.
        // Right now, any read on a replicated table which has no distributed work will
        // generate these null fragments in the restarted transaction.
        boolean usedNullFragment = false;
        if (m_isRestart && m_remoteWork == null) {
            usedNullFragment = true;
            m_remoteWork = new FragmentTaskMessage(m_localWork.getInitiatorHSId(),
                    m_localWork.getCoordinatorHSId(),
                    m_localWork.getTxnId(),
                    m_localWork.getUniqueId(),
                    m_localWork.isReadOnly(),
                    false,
                    false,
                    m_nPartTxn,
                    m_restartTimestamp);
            m_remoteWork.setEmptyForRestart(getNextDependencyId());
            if (!m_haveDistributedInitTask && !isForReplay() && !isReadOnly()) {
                m_haveDistributedInitTask = true;
                m_remoteWork.setStateForDurability((Iv2InitiateTaskMessage) getNotice(),
                        m_masterHSIds.keySet());
            }
            m_haveSentfragment = true;
            // Distribute fragments to remote destinations.
            if (!m_useHSIds.isEmpty()) {
                m_mbox.send(com.google_voltpatches.common.primitives.Longs.toArray(m_useHSIds), m_remoteWork);
            }
        }
        // Do distributed fragments, if any
        if (m_remoteWork != null) {
            // Create some record of expected dependencies for tracking
            m_remoteDeps = createTrackedDependenciesFromTask(m_remoteWork,
                                                             m_useHSIds);
            // clear up DR buffer size tracker
            m_drBufferChangedAgg = 0;
            // if there are remote deps, block on them
            // FragmentResponses indicating failure will throw an exception
            // which will propagate out of handleReceivedFragResponse and
            // cause ProcedureRunner to do the right thing and cause rollback.
            while (!checkDoneReceivingFragResponses()) {
                FragmentResponseMessage msg = pollForResponses();
                if (traceLog != null) {
                    final int batchIdx = m_remoteWork.getCurrentBatchIndex();
                    traceLog.add(() -> VoltTrace.endAsync("sendfragment",
                                                          MiscUtils.hsIdPairTxnIdToString(m_mbox.getHSId(), msg.m_sourceHSId, txnId, batchIdx),
                                                          "status", Byte.toString(msg.getStatusCode())));
                }

                boolean expectedMsg = handleReceivedFragResponse(msg);
                if (expectedMsg) {
                    // Will roll-back and throw if this message has an exception
                    checkForException(msg);
                }
            }
            checkForDRBufferLimit();
        }
        // satisfied. Clear this defensively. Procedure runner is sloppy with
        // cleaning up if it decides new work is necessary that is local-only.
        m_remoteWork = null;
        if (m_buddyHSId == -1) {
            m_buddyHSId = m_buddySupplier.get();
        }
        BorrowTaskMessage borrowmsg = new BorrowTaskMessage(m_localWork);
        m_localWork.setCoordinatorTask(true);
        m_localWork.m_sourceHSId = m_mbox.getHSId();
        // if we created a bogus fragment to distribute to serialize restart and borrow tasks,
        // don't include the empty dependencies we got back in the borrow fragment.
        if (!usedNullFragment) {
            borrowmsg.addInputDepMap(m_remoteDepTables);
        }
        if (traceLog != null) {
            final int batchIdx = m_localWork.getCurrentBatchIndex();
            traceLog.add(() -> VoltTrace.beginAsync("sendborrow",
                                                    MiscUtils.hsIdPairTxnIdToString(m_mbox.getHSId(), m_buddyHSId, txnId, batchIdx),
                                                    "txnId", TxnEgo.txnIdToString(txnId),
                                                    "dest", CoreUtils.hsIdToString(m_buddyHSId)));
        }
        m_mbox.send(m_buddyHSId, borrowmsg);

        FragmentResponseMessage msg;
        while (true){
            msg = pollForResponses();
            final FragmentResponseMessage finalMsg = msg;
            if (traceLog != null) {
                final int batchIdx = m_localWork.getCurrentBatchIndex();
                traceLog.add(() -> VoltTrace.endAsync("sendborrow",
                                                      MiscUtils.hsIdPairTxnIdToString(m_mbox.getHSId(), m_buddyHSId, txnId, batchIdx),
                                                      "status", Byte.toString(finalMsg.getStatusCode())));
            }

            assert(msg.getTableCount() > 0);
            // If this is a restarted TXN, verify that this is not a stale message from a different Dependency
            if (msg.getStatusCode()== FragmentResponseMessage.TERMINATION || !m_isRestart || (msg.m_sourceHSId == m_buddyHSId &&
                    msg.getTableDependencyIdAtIndex(0) == m_localWork.getOutputDepId(0))) {
                // Will roll-back and throw if this message has an exception
                checkForException(msg);
                break;
            } else {
                // It's possible to receive stale responses from remote sites on restart,
                // ignore those
                assert m_isRestart;
            }
        }
        m_localWork = null;

        // Build results from the FragmentResponseMessage
        // This is similar to dependency tracking...maybe some
        // sane way to merge it
        Map<Integer, List<VoltTable>> results =
            new HashMap<Integer, List<VoltTable>>();
        for (int i = 0; i < msg.getTableCount(); i++) {
            int this_depId = msg.getTableDependencyIdAtIndex(i);
            VoltTable this_dep = msg.getTableAtIndex(i);
            List<VoltTable> tables = results.get(this_depId);
            if (tables == null) {
                tables = new ArrayList<VoltTable>();
                results.put(this_depId, tables);
            }
            tables.add(this_dep);
        }

        // Need some sanity check that we got all of the expected output dependencies?
        return results;
    }

    private FragmentResponseMessage pollForResponses()
    {
        FragmentResponseMessage msg = null;
        try {
            final String snapShotRestoreProcName = "@SnapshotRestore";
            while (msg == null) {
                msg = m_newDeps.poll(PULL_TIMEOUT, TimeUnit.SECONDS);
                if (msg == null && !snapShotRestoreProcName.equals(m_initiationMsg.getStoredProcedureName())) {
                    StringBuilder deadlockMsg = new StringBuilder();
                    deadlockMsg.append("Possible multipartition transaction deadlock detected for: ").append(m_initiationMsg);
                    if (m_remoteWork == null) {
                        deadlockMsg.append("\nWaiting on local BorrowTask response from site: ").append(CoreUtils.hsIdToString(m_buddyHSId));
                    }
                    else {
                        deadlockMsg.append("\nWaiting on remote dependencies for message:\n").append(m_remoteWork).append("\n");
                        for (Entry<Integer, Set<Long>> e : m_remoteDeps.entrySet()) {
                            deadlockMsg.append("Dep ID: " + e.getKey() + " waiting on: ").append(CoreUtils.hsIdCollectionToString(e.getValue()));
                        }
                        if (!m_masterMapForFragmentRestart.isEmpty()) {
                            deadlockMsg.append("\nOne or more fragments misrouted and resubmitted to:\n");
                            deadlockMsg.append(CoreUtils.hsIdCollectionToString(m_masterMapForFragmentRestart.keySet()));
                        }
                    }
                    tmLog.warn(deadlockMsg.toString());
                    m_mbox.send(com.google_voltpatches.common.primitives.Longs.toArray(m_useHSIds), new DumpMessage(txnId));
                    m_mbox.send(m_mbox.getHSId(), new DumpMessage(txnId));
                }

                if (msg != null) {
                    SerializableException se = msg.getException();
                    if (se instanceof TransactionRestartException) {
                        if (tmLog.isDebugEnabled()) {
                            tmLog.debug("Transaction exception, txnid: " + TxnEgo.txnIdToString(msg.getTxnId()) + " status:" + msg.getStatusCode()  + " isMisrouted:"+ ((TransactionRestartException) se).isMisrouted()
                                    + " msg: " + msg);
                        }

                        // If this is a restart exception from the inject poison pill, we don't need to match up the DependencyId
                        // Don't rely on the restartTimeStamp check since it's not reliable for poison
                        if (!((TransactionRestartException) se).isMisrouted()) {
                            setNeedsRollback(true);
                            throw se;
                        }
                    }

                    // Filter out stale responses due to the transaction restart, normally the timestamp is Long.MIN_VALUE
                    if (m_restartTimestamp != msg.getRestartTimestamp()) {
                        if (tmLog.isDebugEnabled()) {
                            tmLog.debug("Receives unmatched fragment response, expect timestamp " + MpRestartSequenceGenerator.restartSeqIdToString(m_restartTimestamp) +
                                    " actually receives: " + msg);
                        }
                        msg = null;
                    }
                    if (msg != null) {
                        if (se instanceof TransactionRestartException) {
                            // If this is an misrouted exception, rerouted only this fragment
                            if (((TransactionRestartException) se).isMisrouted()) {
                                restartFragment(msg, ((TransactionRestartException) se).getMasterList(), ((TransactionRestartException) se).getPartitionMasterMap());
                                msg = null;
                            }
                        }
                    }
                }
            }
        }
        catch (InterruptedException e) {
            // can't leave yet - the transaction is inconsistent.
            // could retry; but this is unexpected. Crash.
            throw new RuntimeException(e);
        }

        return msg;
    }

    private void checkForException(FragmentResponseMessage msg)
    {
        if (msg.getStatusCode() != FragmentResponseMessage.SUCCESS) {
            if (msg.getException() instanceof ReplicatedTableException) {
                // There should be a real response from the thread that actually threw the real exception
                // Ignore this exception in lieu of the real one.
                return;
            }
            setNeedsRollback(true);
            if (msg.getException() != null) {
                throw msg.getException();
            } else {
                throw new FragmentFailureException();
            }
        }
    }

    private void checkForDRBufferLimit() {
        if (!m_drProducerActive) {
            return;
        }
        if (tmLog.isTraceEnabled()) {
            tmLog.trace("Total DR buffer allocate for this txn: " + m_drBufferChangedAgg + " limit:" + DR_MAX_AGGREGATE_BUFFERSIZE);
        }
        if (getDRMessageSizeEstimation() >= DR_MAX_AGGREGATE_BUFFERSIZE) {
            if (tmLog.isDebugEnabled()) {
                tmLog.debug("Transaction txnid: " + TxnEgo.txnIdToString(txnId) + " exceeding DR Buffer Limit, need rollback.");
            }
            setNeedsRollback(true);
            throw new SQLException(volt_output_buffer_overflow,
                    "Aggregate MP Transaction requiring " + m_drBufferChangedAgg + " bytes exceeds max DR Buffer size of " + DR_MAX_AGGREGATE_BUFFERSIZE + " bytes.");
        }
    }

    private boolean trackDependency(long hsid, int depId, VoltTable table)
    {
        // Remove the distributed fragment for this site from remoteDeps
        // for the dependency Id depId.
        Set<Long> localRemotes = m_remoteDeps.get(depId);
        if (localRemotes == null && m_isRestart) {
            // Tolerate weird deps showing up on restart
            // After Ariel separates unique ID from transaction ID, rewrite restart to restart with
            // a new transaction ID and make this and the fake distributed fragment stuff go away.
            return false;
        }
        boolean needed = localRemotes.remove(hsid);
        if (!needed) {
            //m_remoteDeps may be built before MigratePartitionLeader. The dependency should be then removed with new partition master
            Long newHsid = m_masterMapForFragmentRestart.get(hsid);
            if (newHsid != null) {
                needed = localRemotes.remove(newHsid);
                if (tmLog.isDebugEnabled()){
                    tmLog.debug("[trackDependency]: remote dependency was built before MigratePartitionLeader. current leader:" + CoreUtils.hsIdToString(hsid)
                    + " prior leader:" + CoreUtils.hsIdToString(newHsid));
                }
            }
        }
        if (needed) {
            // add table to storage
            List<VoltTable> tables = m_remoteDepTables.get(depId);
            if (tables == null) {
                tables = new ArrayList<VoltTable>();
                m_remoteDepTables.put(depId, tables);
            }
            // null dependency table is from a joining node, has no content, drop it
            if (table.getStatusCode() != VoltTableUtil.NULL_DEPENDENCY_STATUS) {
                tables.add(table);
            }
        }
        else if (tmLog.isDebugEnabled()){
            tmLog.debug("No remote dependency for local site: " + hsid);
        }
        return needed;
    }

    private boolean handleReceivedFragResponse(FragmentResponseMessage msg)
    {
        boolean expectedMsg = false;
        final long src_hsid = msg.getExecutorSiteId();
        for (int i = 0; i < msg.getTableCount(); i++)
        {
            int this_depId = msg.getTableDependencyIdAtIndex(i);
            VoltTable this_dep = msg.getTableAtIndex(i);
            expectedMsg |= trackDependency(src_hsid, this_depId, this_dep);
        }
        if (msg.getTableCount() > 0) {
            int drBufferChanged = msg.getDRBufferSize();
            // sum the dr buffer change size among all partitions (already aggregate all previous fragments buffer size)
            m_drBufferChangedAgg += drBufferChanged;
            if (tmLog.isDebugEnabled()) {
                tmLog.debug("[trackDependency]:  drBufferSize added :" + drBufferChanged +
                    " aggregated drBufferSize: " + m_drBufferChangedAgg +
                    " for transaction: " + TxnEgo.txnIdToString(txnId) +
                    " for partition: " + CoreUtils.hsIdToString(src_hsid));
            }
        }

        return expectedMsg;
    }

    private boolean checkDoneReceivingFragResponses()
    {
        for (Set<Long> depid : m_remoteDeps.values()) {
            if (depid.size() != 0) {
                return false;
            }
        }
        return true;
    }

    // Runs from Mailbox's network thread
    public void offerReceivedFragmentResponse(FragmentResponseMessage message)
    {
        // push into threadsafe queue
        m_newDeps.offer(message);
    }

    public boolean drTxnDataCanBeRolledBack() {
        if (tmLog.isTraceEnabled()) {
            tmLog.trace("DR Txn can be rolled back=" + (m_drBufferChangedAgg == 0));
        }
        return m_drBufferChangedAgg == 0;
    }

    /**
     * Restart this fragment after the fragment is mis-routed from MigratePartitionLeader
     * If the masters have been updated, the fragment will be routed to its new master. The fragment will be routed to the old master.
     * until new master is updated.
     * @param message The mis-routed response message
     * @param partitionMastersMap The current partition masters
     */
    public void restartFragment(FragmentResponseMessage message, List<Long> masters, Map<Integer, Long> partitionMastersMap) {
        final int partionId = message.getPartitionId();
        Long restartHsid = partitionMastersMap.get(partionId);
        Long hsid = message.getExecutorSiteId();
        if (!hsid.equals(restartHsid)) {
            m_masterMapForFragmentRestart.clear();
            m_masterMapForFragmentRestart.put(restartHsid, hsid);
            //The very first fragment is to be rerouted to the new leader, then all the follow-up fragments are routed
            //to new leaders.
            updateMasters(masters, partitionMastersMap);
        }

        if (restartHsid == null) {
            restartHsid = hsid;
        }
        if (tmLog.isDebugEnabled()) {
            tmLog.debug("Rerouted fragment from " + CoreUtils.hsIdToString(hsid) + " to " + CoreUtils.hsIdToString(restartHsid) + "\n" + m_remoteWork);
        }
        m_fragmentRestarted = true;
        m_mbox.send(restartHsid, m_remoteWork);
    }

    public boolean isFragmentRestarted() {
        return m_fragmentRestarted;
    }

    public List<Long> getMasterHSIDs() {
        return m_useHSIds;
    }

    /**
     * Kill a transaction - maybe shutdown mid-transaction? Or a timeout
     * collecting fragments? This is a don't-know-what-to-do-yet
     * stub.
     * TODO: fix this.
     */
    @Override
    public void terminateTransaction()
    {
        if (tmLog.isDebugEnabled()) {
            tmLog.debug("Aborting transaction: " + TxnEgo.txnIdToString(txnId));
        }
        FragmentTaskMessage dummy = new FragmentTaskMessage(0L, 0L, 0L, 0L, false, false, false,
                m_nPartTxn, m_restartTimestamp);
        FragmentResponseMessage poison = new FragmentResponseMessage(dummy, 0L);
        TransactionTerminationException termination = new TransactionTerminationException(
                "Transaction interrupted.", txnId);
        poison.setStatus(FragmentResponseMessage.TERMINATION, termination);
        offerReceivedFragmentResponse(poison);
     }

    /**
     * For @BalancePartitions, get the master HSID for the given partition so that the MPI can plan who to send data
     * to whom.
     */
    public Long getMasterHSId(int partition)
    {
        Preconditions.checkArgument(m_masterHSIds.values().containsAll(m_useHSIds) &&
                                    m_useHSIds.containsAll(m_masterHSIds.values()));
        return m_masterHSIds.get(partition);
    }

    public boolean isNPartTxn() {
        return m_nPartTxn;
    }

    public int getNextFragmentIndex() {
        return m_fragmentIndex++;
    }

    // Have MPI sent out at least one round of fragment to leaders?
    // When MP txn is restarted, the flag is reset to false.
    public boolean haveSentFragment() {
        return m_haveSentfragment;
    }

    public boolean isRestart() {
        return m_isRestart;
    }

    // Check dependencies on failed host for rerouted transactions upon leader migration.
    public boolean checkFailedHostDependancies(List<Long> masters) {
        if (!isFragmentRestarted()) {
            return false;
        }

        Set<Integer> hostIds = masters.stream().map(CoreUtils::getHostIdFromHSId).collect(Collectors.toSet());
        for (Set<Long> hsids : m_remoteDeps.values()) {
            for (Long hsid : hsids) {
                if (!hostIds.contains(CoreUtils.getHostIdFromHSId(hsid))) {
                    return true;
                }
            }
        }
        return false;
    }
}

