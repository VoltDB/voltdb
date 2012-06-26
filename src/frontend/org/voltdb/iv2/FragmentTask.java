/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.List;
import java.util.Map;

import org.voltcore.logging.Level;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ParameterSet;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.utils.LogKeys;

public class FragmentTask extends TransactionTask
{
    final Mailbox m_initiator;
    final FragmentTaskMessage m_task;
    final Map<Integer, List<VoltTable>> m_inputDeps;

    FragmentTask(Mailbox mailbox,
                 ParticipantTransactionState txn,
                 TransactionTaskQueue queue,
                 FragmentTaskMessage message,
                 Map<Integer, List<VoltTable>> inputDeps)
    {
        super(txn, queue);
        m_initiator = mailbox;
        m_task = message;
        m_inputDeps = inputDeps;
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        hostLog.debug("STARTING: " + this);
        // Set the begin undo token if we haven't already
        // In the future we could record a token per batch
        // and do partial rollback
        if (!m_txn.isReadOnly()) {
            if (m_txn.getBeginUndoToken() == Site.kInvalidUndoToken) {
                m_txn.setBeginUndoToken(siteConnection.getLatestUndoToken());
            }
        }
        final FragmentResponseMessage response = processFragmentTask(siteConnection);
        // completion?
        response.m_sourceHSId = m_initiator.getHSId();
        m_initiator.deliver(response);
        hostLog.debug("COMPLETE: " + this);
    }

    // Cut and pasted from ExecutionSite processFragmentTask(), then
    // modifed to work in the new world
    public FragmentResponseMessage processFragmentTask(SiteProcedureConnection siteConnection)
    {
        // IZZY: actually need the "executor" HSId these days?
        final FragmentResponseMessage currentFragResponse =
            new FragmentResponseMessage(m_task, m_initiator.getHSId());
        currentFragResponse.setStatus(FragmentResponseMessage.SUCCESS, null);

        if (m_inputDeps != null) {
            siteConnection.stashWorkUnitDependencies(m_inputDeps);
        }

        for (int frag = 0; frag < m_task.getFragmentCount(); frag++)
        {
            final long fragmentId = m_task.getFragmentId(frag);
            final int outputDepId = m_task.getOutputDepId(frag);

            ParameterSet params = m_task.getParameterSetForFragment(frag);
            final int inputDepId = m_task.getOnlyInputDepId(frag);

            /*
             * Currently the error path when executing plan fragments
             * does not adequately distinguish between fatal errors and
             * abort type errors that should result in a roll back.
             * Assume that it is ninja: succeeds or doesn't return.
             * No roll back support.
             */
            try {
                VoltTable dependency;
                if (m_task.getFragmentPlan(frag) != null) {
                    // make dependency ids available to the execution engine
                    if ((m_inputDeps != null) && (m_inputDeps.size() > 0)) {
                        siteConnection.stashWorkUnitDependencies(m_inputDeps);
                    }
                    dependency = siteConnection.executeCustomPlanFragment(
                            m_task.getFragmentPlan(frag),
                            inputDepId,
                            m_txn.txnId,
                            params,
                            m_txn.isReadOnly());
                }
                else {
                    dependency =
                        siteConnection.executePlanFragment(fragmentId,
                                inputDepId,
                                params,
                                m_txn.txnId,
                                m_txn.isReadOnly());
                }

                if (hostLog.isTraceEnabled()) {
                    hostLog.l7dlog(Level.TRACE,
                       LogKeys.org_voltdb_ExecutionSite_SendingDependency.name(),
                       new Object[] { outputDepId }, null);
                }
                currentFragResponse.addDependency(outputDepId, dependency);
            } catch (final EEException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                break;
            } catch (final SQLException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                break;
            }
        }
        return currentFragResponse;
    }

    @Override
    public long getMpTxnId()
    {
        return m_task.getTxnId();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("FragmentTask:");
        sb.append("  MP TXN ID: ").append(getMpTxnId());
        sb.append("  LOCAL TXN ID: ").append(getLocalTxnId());
        sb.append("  ON HSID: ").append(CoreUtils.hsIdToString(m_initiator.getHSId()));
        return sb.toString();
    }
}
