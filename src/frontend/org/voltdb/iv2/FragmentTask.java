/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.Level;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.InterruptException;
import org.voltdb.exceptions.ReplicatedTableException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.planner.ActivePlanRepository;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTableUtil;
import org.voltdb.utils.VoltTrace;

public class FragmentTask extends FragmentTaskBase
{
    final Mailbox m_initiator;
    final FragmentTaskMessage m_fragmentMsg;
    final Map<Integer, List<VoltTable>> m_inputDeps;
    boolean m_respBufferable = true;
    static final byte[] m_rawDummyResponse;

    static {
        VoltTable dummyResponse = new VoltTable(new ColumnInfo("STATUS", VoltType.TINYINT));
        dummyResponse.setStatusCode(VoltTableUtil.NULL_DEPENDENCY_STATUS);
        m_rawDummyResponse = dummyResponse.buildReusableDependenyResult();
    }

    // This constructor is used during live rejoin log replay.
    FragmentTask(Mailbox mailbox,
                 FragmentTaskMessage message,
                 ParticipantTransactionState txnState)
    {
        this(mailbox,
             txnState,
             null,
             message,
             null);
    }

    // This constructor is used during normal operation.
    FragmentTask(Mailbox mailbox,
                 ParticipantTransactionState txnState,
                 TransactionTaskQueue queue,
                 FragmentTaskMessage message,
                 Map<Integer, List<VoltTable>> inputDeps)
    {
        super(txnState, queue);
        m_initiator = mailbox;
        m_fragmentMsg = message;
        m_inputDeps = inputDeps;

        if (txnState != null && !txnState.isReadOnly()) {
            m_respBufferable = false;
        }
    }

    public void setResponseNotBufferable() {
        m_respBufferable = false;
    }

    private void deliverResponse(FragmentResponseMessage response) {
        response.m_sourceHSId = m_initiator.getHSId();
        response.setRespBufferable(m_respBufferable);
        m_initiator.deliver(response);
    }

    @Override
    protected void durabilityTraceEnd() {
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.endAsync("durability",
                                                  MiscUtils.hsIdTxnIdToString(m_initiator.getHSId(), m_fragmentMsg.getSpHandle())));
        }
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        waitOnDurabilityBackpressureFuture();
        if (hostLog.isDebugEnabled()) {
            hostLog.debug("STARTING: " + this);
        }
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPSITE);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.beginDuration("runfragmenttask",
                                                       "txnId", TxnEgo.txnIdToString(getTxnId()),
                                                       "partition", Integer.toString(siteConnection.getCorrespondingPartitionId())));
        }

        // if this has a procedure name from the initiation bundled,
        // inform the site connection here
        String procName = m_fragmentMsg.getProcedureName();
        if (procName != null) {
            siteConnection.setProcedureName(procName);
        }

        // Set the begin undo token if we haven't already
        // In the future we could record a token per batch
        // and do partial rollback
        if (!m_txnState.isReadOnly()) {
            if (m_txnState.getBeginUndoToken() == Site.kInvalidUndoToken) {
                m_txnState.setBeginUndoToken(siteConnection.getLatestUndoToken());
            }
        }

        int originalTimeout = siteConnection.getBatchTimeout();
        int individualTimeout = m_fragmentMsg.getBatchTimeout();
        try {
            if (BatchTimeoutOverrideType.isUserSetTimeout(individualTimeout)) {
                siteConnection.setBatchTimeout(individualTimeout);
            }

            // execute the procedure
            final FragmentResponseMessage response = processFragmentTask(siteConnection);

            //The fragment is not misrouted and the site may have been marked as non-leader via @MigratePartitionLeader
            //but it should be processed by the same site, act like a leader.
            response.setForOldLeader(m_fragmentMsg.isForOldLeader());
            deliverResponse(response);
        } finally {
            if (BatchTimeoutOverrideType.isUserSetTimeout(individualTimeout)) {
                siteConnection.setBatchTimeout(originalTimeout);
            }
        }

        completeFragment();

        if (hostLog.isDebugEnabled()) {
            hostLog.debug("COMPLETE: " + this);
        }
        if (traceLog != null) {
            traceLog.add(VoltTrace::endDuration);
        }
    }

    @Override
    public long getSpHandle()
    {
        return m_fragmentMsg.getSpHandle();
    }

    /**
     * Produce a rejoining response.
     */
    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
        if (!m_txnState.isReadOnly()) {
            taskLog.logTask(m_fragmentMsg);
        }

        final FragmentResponseMessage response =
            new FragmentResponseMessage(m_fragmentMsg, m_initiator.getHSId());
        response.setRecovering(true);
        response.setStatus(FragmentResponseMessage.SUCCESS, null);

        // Set the dependencies even if this is a dummy response. This site could be the master
        // on elastic join, so the fragment response message is actually going to the MPI.
        for (int frag = 0; frag < m_fragmentMsg.getFragmentCount(); frag++) {
            final int outputDepId = m_fragmentMsg.getOutputDepId(frag);
            response.addDependency(new DependencyPair.BufferDependencyPair(outputDepId,
                    m_rawDummyResponse, 0, m_rawDummyResponse.length));
        }

        deliverResponse(response);
        completeFragment();
    }

    /**
     * Run for replay after a live rejoin snapshot transfer.
     */
    @Override
    public void runFromTaskLog(SiteProcedureConnection siteConnection)
    {
        if (hostLog.isDebugEnabled()) {
            hostLog.debug("START replaying txn: " + this);
        }
        // Set the begin undo token if we haven't already
        // In the future we could record a token per batch
        // and do partial rollback
        if (!m_txnState.isReadOnly()) {
            if (m_txnState.getBeginUndoToken() == Site.kInvalidUndoToken) {
                m_txnState.setBeginUndoToken(siteConnection.getLatestUndoToken());
            }
        }

        // ignore response.
        processFragmentTask(siteConnection);
        completeFragment();
        if (hostLog.isDebugEnabled()) {
            hostLog.debug("COMPLETE replaying txn: " + this);
        }
    }

    private void completeFragment()
    {
        // Check and see if we can flush early
        // right now, this is just read-only and final task
        // This
        if (m_fragmentMsg.isFinalTask() && m_txnState.isReadOnly())
        {
            doCommonSPICompleteActions();
        }
    }

    // Cut and pasted from ExecutionSite processFragmentTask(), then
    // modifed to work in the new world
    public FragmentResponseMessage processFragmentTask(SiteProcedureConnection siteConnection)
    {
        // Ensure default procs loaded here
        // Also used for LoadMultipartitionTable
        String procNameToLoad = m_fragmentMsg.getProcNameToLoad();
        if (procNameToLoad != null) {
            // this will ensure proc is loaded
            ProcedureRunner runner = siteConnection.getProcedureRunner(procNameToLoad);
            assert(runner != null);
        }

        // IZZY: actually need the "executor" HSId these days?
        final FragmentResponseMessage currentFragResponse =
                new FragmentResponseMessage(m_fragmentMsg, m_initiator.getHSId());
        currentFragResponse.setStatus(FragmentResponseMessage.SUCCESS, null);

        if (m_inputDeps != null) {
            siteConnection.stashWorkUnitDependencies(m_inputDeps);
        }

        if (m_fragmentMsg.isEmptyForRestart()) {
            int outputDepId = m_fragmentMsg.getOutputDepId(0);
            currentFragResponse.addDependency(new DependencyPair.BufferDependencyPair(outputDepId,
                    m_rawDummyResponse, 0, m_rawDummyResponse.length));
            return currentFragResponse;
        }

        ProcedureRunner currRunner = siteConnection.getProcedureRunner(m_fragmentMsg.getProcedureName());
        long[] executionTimes = null;
        int succeededFragmentsCount = 0;
        if (currRunner != null) {
            currRunner.getExecutionEngine().setPerFragmentTimingEnabled(m_fragmentMsg.isPerFragmentStatsRecording());
            if (m_fragmentMsg.isPerFragmentStatsRecording()) {
                // At this point, we will execute the fragments one by one.
                executionTimes = new long[1];
            }
        }

        for (int frag = 0; frag < m_fragmentMsg.getFragmentCount(); frag++)
        {
            byte[] planHash = m_fragmentMsg.getPlanHash(frag);
            final int outputDepId = m_fragmentMsg.getOutputDepId(frag);

            ParameterSet params = m_fragmentMsg.getParameterSetForFragment(frag);
            final int inputDepId = m_fragmentMsg.getOnlyInputDepId(frag);

            long fragmentId = 0;
            byte[] fragmentPlan = null;

            /*
             * Currently the error path when executing plan fragments
             * does not adequately distinguish between fatal errors and
             * abort type errors that should result in a roll back.
             * Assume that it is ninja: succeeds or doesn't return.
             * No roll back support.
             *
             * AW in 2012, the preceding comment might be wrong,
             * I am pretty sure what we don't support is partial rollback.
             * The entire procedure will roll back successfully on failure
             */
            VoltTable dependency = null;
            try {
                FastDeserializer fragResult;
                fragmentPlan = m_fragmentMsg.getFragmentPlan(frag);
                String stmtText = null;

                // if custom fragment, load the plan and get local fragment id
                if (fragmentPlan != null) {
                    // statement text for unplanned fragments are pulled from the message,
                    // to ensure that we get the correct constants from the most recent
                    // invocation.
                    stmtText = m_fragmentMsg.getStmtText(frag);
                    fragmentId = ActivePlanRepository.loadOrAddRefPlanFragment(planHash, fragmentPlan, null);
                }
                // otherwise ask the plan source for a local fragment id
                else {
                    fragmentId = ActivePlanRepository.getFragmentIdForPlanHash(planHash);
                    stmtText = ActivePlanRepository.getStmtTextForPlanHash(planHash);
                }

                // set up the batch context for the fragment set
                siteConnection.setBatch(m_fragmentMsg.getCurrentBatchIndex());

                fragResult = siteConnection.executePlanFragments(
                        1,
                        new long[] { fragmentId },
                        new long [] { inputDepId },
                        new ParameterSet[] { params },
                        null,
                        stmtText == null ? null : new String[] { stmtText }, // for long-running queries
                        new boolean[] { false },    // FragmentTasks don't generate statement hashes,
                        null,
                        m_txnState.txnId,
                        m_txnState.m_spHandle,
                        m_txnState.uniqueId,
                        m_txnState.isReadOnly(),
                        VoltTrace.log(VoltTrace.Category.EE) != null);

                // get a copy of the result buffers from the cache buffer so we can post the
                // fragment response to the network
                final int tableSize;
                final byte fullBacking[];
                try {
                    // read the complete size of the buffer used
                    fragResult.readInt();
                    // read number of dependencies (1)
                    fragResult.readInt();
                    // read the dependencyId() -1;
                    fragResult.readInt();
                    tableSize = fragResult.readInt();
                    fullBacking = new byte[tableSize];
                    // get a copy of the buffer
                    fragResult.readFully(fullBacking);
                } catch (final IOException ex) {
                    hostLog.error("Failed to deserialze result table" + ex);
                    throw new EEException(ExecutionEngine.ERRORCODE_WRONG_SERIALIZED_BYTES);
                }

                if (hostLog.isTraceEnabled()) {
                    hostLog.l7dlog(Level.TRACE,
                       LogKeys.org_voltdb_ExecutionSite_SendingDependency.name(),
                       new Object[] { outputDepId }, null);
                }
                currentFragResponse.addDependency(new DependencyPair.BufferDependencyPair(outputDepId, fullBacking, 0, tableSize));
            } catch (final EEException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { Encoder.hexEncode(planHash) }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                if (currentFragResponse.getTableCount() == 0) {
                    // Make sure the response has at least 1 result with a valid DependencyId
                    currentFragResponse.addDependency(new DependencyPair.BufferDependencyPair(outputDepId,
                            m_rawDummyResult, 0, m_rawDummyResult.length));
                }
                break;
            } catch (final SQLException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { Encoder.hexEncode(planHash) }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                if (currentFragResponse.getTableCount() == 0) {
                    // Make sure the response has at least 1 result with a valid DependencyId
                    currentFragResponse.addDependency(new DependencyPair.BufferDependencyPair(outputDepId,
                            m_rawDummyResult, 0, m_rawDummyResult.length));
                }
                break;
            } catch (final ReplicatedTableException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { Encoder.hexEncode(planHash) }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                if (currentFragResponse.getTableCount() == 0) {
                    // Make sure the response has at least 1 result with a valid DependencyId
                    currentFragResponse.addDependency(new DependencyPair.BufferDependencyPair(outputDepId,
                            m_rawDummyResult, 0, m_rawDummyResult.length));
                }
                break;
            }
            catch (final InterruptException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { Encoder.hexEncode(planHash) }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                if (currentFragResponse.getTableCount() == 0) {
                    // Make sure the response has at least 1 result with a valid DependencyId
                    currentFragResponse.addDependency(new DependencyPair.BufferDependencyPair(outputDepId,
                            m_rawDummyResult, 0, m_rawDummyResult.length));
                }
                break;
            }
            finally {
                // ensure adhoc plans are unloaded
                if (fragmentPlan != null) {
                    ActivePlanRepository.decrefPlanFragmentById(fragmentId);
                }
                // If the executed fragment comes from a stored procedure, we need to update the per-fragment stats for it.
                // Notice that this code path is used to handle multi-partition stored procedures.
                // The single-partition stored procedure handler is in the ProcedureRunner.
                if (currRunner != null) {
                    succeededFragmentsCount = currRunner.getExecutionEngine().extractPerFragmentStats(1, executionTimes);

                    long stmtDuration = 0;
                    int stmtResultSize = 0;
                    int stmtParameterSetSize = 0;
                    if (m_fragmentMsg.isPerFragmentStatsRecording()) {
                        stmtDuration = executionTimes == null ? 0 : executionTimes[0];
                        stmtResultSize = dependency == null ? 0 : dependency.getSerializedSize();
                        stmtParameterSetSize = params == null ? 0 : params.getSerializedSize();
                    }

                    currRunner.getStatsCollector().endFragment(m_fragmentMsg.getStmtName(frag),
                                                               m_fragmentMsg.isCoordinatorTask(),
                                                               succeededFragmentsCount == 0,
                                                               m_fragmentMsg.isPerFragmentStatsRecording(),
                                                               stmtDuration,
                                                               stmtResultSize,
                                                               stmtParameterSetSize);
                }
            }
        }
        return currentFragResponse;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("FragmentTask:");
        sb.append("  TXN ID: ").append(TxnEgo.txnIdToString(getTxnId()));
        sb.append("  SP HANDLE ID: ").append(TxnEgo.txnIdToString(getSpHandle()));
        sb.append("  ON HSID: ").append(CoreUtils.hsIdToString(m_initiator.getHSId()));
        sb.append("  TIMESTAMP: ");
        MpRestartSequenceGenerator.restartSeqIdToString(getTimestamp(), sb);
        return sb.toString();
    }

    public boolean needCoordination() {
        return !(m_txnState.isReadOnly() || isBorrowedTask() || m_isNPartition);
    }

    public boolean isBorrowedTask() {
        return false;
    }

    @Override
    public long getTimestamp() {
        return m_fragmentMsg.getTimestamp();
    }
}
