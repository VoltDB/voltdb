/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import org.voltdb.ParameterSet;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.InterruptException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.planner.ActivePlanRepository;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltTableUtil;

public class FragmentTask extends TransactionTask
{
    final Mailbox m_initiator;
    final FragmentTaskMessage m_fragmentMsg;
    final Map<Integer, List<VoltTable>> m_inputDeps;

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
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        waitOnDurabilityBackpressureFuture();
        if (hostLog.isDebugEnabled()) {
            hostLog.debug("STARTING: " + this);
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
        final FragmentResponseMessage response = processFragmentTask(siteConnection);
        // completion?
        response.m_sourceHSId = m_initiator.getHSId();
        m_initiator.deliver(response);
        completeFragment();

        if (hostLog.isDebugEnabled()) {
            hostLog.debug("COMPLETE: " + this);
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
        response.m_sourceHSId = m_initiator.getHSId();
        response.setRecovering(true);
        response.setStatus(FragmentResponseMessage.SUCCESS, null);

        // Set the dependencies even if this is a dummy response. This site could be the master
        // on elastic join, so the fragment response message is actually going to the MPI.
        VoltTable depTable = new VoltTable(new ColumnInfo("STATUS", VoltType.TINYINT));
        depTable.setStatusCode(VoltTableUtil.NULL_DEPENDENCY_STATUS);
        for (int frag = 0; frag < m_fragmentMsg.getFragmentCount(); frag++) {
            final int outputDepId = m_fragmentMsg.getOutputDepId(frag);
            response.addDependency(outputDepId, depTable);
        }

        m_initiator.deliver(response);
        completeFragment();
    }

    /**
     * Run for replay after a live rejoin snapshot transfer.
     */
    @Override
    public void runFromTaskLog(SiteProcedureConnection siteConnection)
    {
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
            currentFragResponse.addDependency(outputDepId,
                    new VoltTable(new ColumnInfo[] {new ColumnInfo("UNUSED", VoltType.INTEGER)}, 1));
            return currentFragResponse;
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
            try {
                VoltTable dependency;
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

                dependency = siteConnection.executePlanFragments(
                        1,
                        new long[] { fragmentId },
                        new long [] { inputDepId },
                        new ParameterSet[] { params },
                        stmtText == null ? null : new String[] { stmtText },
                        m_txnState.txnId,
                        m_txnState.m_spHandle,
                        m_txnState.uniqueId,
                        m_txnState.isReadOnly())[0];

                if (hostLog.isTraceEnabled()) {
                    hostLog.l7dlog(Level.TRACE,
                       LogKeys.org_voltdb_ExecutionSite_SendingDependency.name(),
                       new Object[] { outputDepId }, null);
                }
                currentFragResponse.addDependency(outputDepId, dependency);
            } catch (final EEException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { Encoder.hexEncode(planHash) }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                break;
            } catch (final SQLException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { Encoder.hexEncode(planHash) }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                break;
            }
            catch (final InterruptException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { Encoder.hexEncode(planHash) }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                break;
            }
            finally {
                // ensure adhoc plans are unloaded
                if (fragmentPlan != null) {
                    ActivePlanRepository.decrefPlanFragmentById(fragmentId);
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
        return sb.toString();
    }
}
