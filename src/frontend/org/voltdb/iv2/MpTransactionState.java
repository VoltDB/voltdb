/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.SQLException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.exceptions.TransactionRestartException;
import org.voltdb.messaging.BorrowTaskMessage;
import org.voltdb.messaging.DumpMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.utils.VoltTableUtil;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.Maps;

public class MpTransactionState extends TransactionState
{
    static VoltLogger tmLog = new VoltLogger("TM");

    private static final int DR_MAX_AGGREGATE_BUFFERSIZE = Integer.getInteger("DR_MAX_AGGREGATE_BUFFERSIZE", (45 * 1024 * 1024) + 4096);
    private static final String dr_max_consumer_partitionCount_str = "DR_MAX_CONSUMER_PARTITIONCOUNT";
    private static final String dr_max_consumer_messageheader_room_str = "DR_MAX_CONSUMER_MESSAGEHEADER_ROOM";
    private static final String volt_output_buffer_overflow = "V0001";

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
    long m_buddyHSId;
    FragmentTaskMessage m_remoteWork = null;
    FragmentTaskMessage m_localWork = null;
    boolean m_haveDistributedInitTask = false;
    boolean m_isRestart = false;

    MpTransactionState(Mailbox mailbox,
                       TransactionInfoBaseMessage notice,
                       List<Long> useHSIds, Map<Integer, Long> partitionMasters,
                       long buddyHSId, boolean isRestart)
    {
        super(mailbox, notice);
        m_initiationMsg = (Iv2InitiateTaskMessage)notice;
        m_useHSIds.addAll(useHSIds);
        m_masterHSIds.putAll(partitionMasters);
        m_buddyHSId = buddyHSId;
        m_isRestart = isRestart;
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

        // assume still has replicated stream for upper bound estimation
        // avoid check the dr system for performance concern
        int streamCount = m_localPartitionCount + 1;
        int concatLogSize = m_drBufferChangedAgg + 13 * streamCount; // adding END_Transaction Size

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

    public void updateMasters(List<Long> masters, Map<Integer, Long> partitionMasters)
    {
        m_useHSIds.clear();
        m_useHSIds.addAll(masters);

        m_masterHSIds.clear();
        m_masterHSIds.putAll(partitionMasters);
        m_localPartitionCount = m_masterHSIds.size();
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
    public void setupProcedureResume(boolean isFinal, int[] dependencies)
    {
        // Reset state so we can run this batch cleanly
        m_localWork = null;
        m_remoteWork = null;
        m_remoteDeps = null;
        m_remoteDepTables.clear();
        m_drBufferChangedAgg = 0;
    }

    // I met this List at bandcamp...
    public void setupProcedureResume(boolean isFinal, List<Integer> deps)
    {
        setupProcedureResume(isFinal,
                com.google_voltpatches.common.primitives.Ints.toArray(deps));
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

            m_remoteWork = task;
            m_remoteWork.setTruncationHandle(m_initiationMsg.getTruncationHandle());
            // Distribute fragments to remote destinations.
            long[] non_local_hsids = new long[m_useHSIds.size()];
            for (int i = 0; i < m_useHSIds.size(); i++) {
                non_local_hsids[i] = m_useHSIds.get(i);
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
    createTrackedDependenciesFromTask(FragmentTaskMessage task,
                                      List<Long> expectedHSIds)
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
                    false);
            m_remoteWork.setEmptyForRestart(getNextDependencyId());
            if (!m_haveDistributedInitTask && !isForReplay() && !isReadOnly()) {
                m_haveDistributedInitTask = true;
                m_remoteWork.setStateForDurability((Iv2InitiateTaskMessage) getNotice(),
                        m_masterHSIds.keySet());
            }
            // Distribute fragments to remote destinations.
            long[] non_local_hsids = new long[m_useHSIds.size()];
            for (int i = 0; i < m_useHSIds.size(); i++) {
                non_local_hsids[i] = m_useHSIds.get(i);
            }
            // send to all non-local sites
            if (non_local_hsids.length > 0) {
                m_mbox.send(non_local_hsids, m_remoteWork);
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
                boolean expectedMsg = handleReceivedFragResponse(msg);
                if (expectedMsg) {
                    // Will roll-back and throw if this message has an exception
                    checkForException(msg);
                }
            }
            checkForDRBufferLimit();
        }
        // satisified. Clear this defensively. Procedure runner is sloppy with
        // cleaning up if it decides new work is necessary that is local-only.
        m_remoteWork = null;

        BorrowTaskMessage borrowmsg = new BorrowTaskMessage(m_localWork);
        m_localWork.m_sourceHSId = m_mbox.getHSId();
        // if we created a bogus fragment to distribute to serialize restart and borrow tasks,
        // don't include the empty dependencies we got back in the borrow fragment.
        if (!usedNullFragment) {
            borrowmsg.addInputDepMap(m_remoteDepTables);
        }
        m_mbox.send(m_buddyHSId, borrowmsg);

        FragmentResponseMessage msg;
        while (true){
            msg = pollForResponses();
            assert(msg.getTableCount() > 0);
            // If this is a restarted TXN, verify that this is not a stale message from a different Dependency
            if (!m_isRestart || (msg.m_sourceHSId == m_buddyHSId &&
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
                msg = m_newDeps.poll(60L * 5, TimeUnit.SECONDS);
                if (msg == null && !snapShotRestoreProcName.equals(m_initiationMsg.getStoredProcedureName())) {
                    tmLog.warn("Possible multipartition transaction deadlock detected for: " + m_initiationMsg);
                    if (m_remoteWork == null) {
                        tmLog.warn("Waiting on local BorrowTask response from site: " +
                                CoreUtils.hsIdToString(m_buddyHSId));
                    }
                    else {
                        tmLog.warn("Waiting on remote dependencies: ");
                        for (Entry<Integer, Set<Long>> e : m_remoteDeps.entrySet()) {
                            tmLog.warn("Dep ID: " + e.getKey() + " waiting on: " +
                                    CoreUtils.hsIdCollectionToString(e.getValue()));
                        }
                    }
                    m_mbox.send(com.google_voltpatches.common.primitives.Longs.toArray(m_useHSIds), new DumpMessage());
                }
            }
        }
        catch (InterruptedException e) {
            // can't leave yet - the transaction is inconsistent.
            // could retry; but this is unexpected. Crash.
            throw new RuntimeException(e);
        }
        SerializableException se = msg.getException();
        if (se != null && se instanceof TransactionRestartException) {
            // If this is a restart exception, we don't need to match up the DependencyId
            setNeedsRollback(true);
            throw se;
        }
        return msg;
    }

    private void checkForException(FragmentResponseMessage msg)
    {
        if (msg.getStatusCode() != FragmentResponseMessage.SUCCESS) {
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
        else {
            System.out.println("No remote dep for local site: " + hsid);
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
        boolean done = true;
        for (Set<Long> depid : m_remoteDeps.values()) {
            if (depid.size() != 0) {
                done = false;
            }
        }
        return done;
    }

    // Runs from Mailbox's network thread
    public void offerReceivedFragmentResponse(FragmentResponseMessage message)
    {
        // push into threadsafe queue
        m_newDeps.offer(message);
    }

    /**
     * Kill a transaction - maybe shutdown mid-transaction? Or a timeout
     * collecting fragments? This is a don't-know-what-to-do-yet
     * stub.
     * TODO: fix this.
     */
    void terminateTransaction()
    {
        throw new RuntimeException("terminateTransaction is not yet implemented.");
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
}
