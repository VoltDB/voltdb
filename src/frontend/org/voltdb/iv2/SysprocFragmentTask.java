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

package org.voltdb.iv2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.Level;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.sysprocs.SysProcFragmentId;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;

public class SysprocFragmentTask extends TransactionTask
{
    final Mailbox m_initiator;
    final FragmentTaskMessage m_fragmentMsg;
    Map<Integer, List<VoltTable>> m_inputDeps;

    SysprocFragmentTask(Mailbox mailbox,
                 ParticipantTransactionState txnState,
                 TransactionTaskQueue queue,
                 FragmentTaskMessage message,
                 Map<Integer, List<VoltTable>> inputDeps)
    {
        super(txnState, queue);
        m_initiator = mailbox;
        m_fragmentMsg = message;
        m_inputDeps = inputDeps;
        if (m_inputDeps == null) {
            m_inputDeps = new HashMap<Integer, List<VoltTable>>();
        }
        assert(m_fragmentMsg.isSysProcTask());
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        if (!m_txnState.isReadOnly()) {
            if (m_txnState.getBeginUndoToken() == Site.kInvalidUndoToken) {
                m_txnState.setBeginUndoToken(siteConnection.getLatestUndoToken());
            }
        }

        // HACK HACK HACK
        // We take the coward's way out to prevent rejoining sites from doing
        // snapshot work by finding every snapshot fragment and responding with the
        // recovering status instead of running the fragment.
        // rejoinDataPending() is VoltDB state which will be flipped to false by
        // the rejoin code once all of the site data is synchronized.  This will then
        // allow truncation snapshots necessary to make the node officially rejoined
        // to take place.
        if (m_fragmentMsg.isSysProcTask() &&
            SysProcFragmentId.isSnapshotSaveFragment(m_fragmentMsg.getPlanHash(0)) &&
            VoltDB.instance().rejoinDataPending()) {
            final FragmentResponseMessage response =
                new FragmentResponseMessage(m_fragmentMsg, m_initiator.getHSId());
            response.setRecovering(true);
            response.setStatus(FragmentResponseMessage.SUCCESS, null);
            m_initiator.deliver(response);
            return;
        }

        final FragmentResponseMessage response = processFragmentTask(siteConnection);
        response.m_sourceHSId = m_initiator.getHSId();
        m_initiator.deliver(response);
    }

    /**
     * Produce a rejoining response.
     */
    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
        taskLog.logTask(m_fragmentMsg);

        final FragmentResponseMessage response =
            new FragmentResponseMessage(m_fragmentMsg, m_initiator.getHSId());
        response.setRecovering(true);
        response.setStatus(FragmentResponseMessage.SUCCESS, null);

        // Set the dependencies even if this is a dummy response. This site could be the master
        // on elastic join, so the fragment response message is actually going to the MPI.
        for (int frag = 0; frag < m_fragmentMsg.getFragmentCount(); frag++) {
            final int outputDepId = m_fragmentMsg.getOutputDepId(frag);
            response.addDependency(outputDepId, null);
        }

        m_initiator.deliver(response);
    }

    @Override
    public void runFromTaskLog(SiteProcedureConnection siteConnection)
    {
        throw new RuntimeException("Not implemented: replay sysproc during live rejoin.");
    }


    // Extracted the sysproc portion of ExecutionSite processFragmentTask(), then
    // modifed to work in the new world
    public FragmentResponseMessage processFragmentTask(SiteProcedureConnection siteConnection)
    {
        final FragmentResponseMessage currentFragResponse =
            new FragmentResponseMessage(m_fragmentMsg, m_initiator.getHSId());
        currentFragResponse.setStatus(FragmentResponseMessage.SUCCESS, null);

        for (int frag = 0; frag < m_fragmentMsg.getFragmentCount(); frag++)
        {
            final long fragmentId = VoltSystemProcedure.hashToFragId(m_fragmentMsg.getPlanHash(frag));
            // equivalent to dep.depId:
            // final int outputDepId = m_fragmentMsg.getOutputDepId(frag);

            ParameterSet params = m_fragmentMsg.getParameterSetForFragment(frag);

            try {
                // run the overloaded sysproc planfragment. pass an empty dependency
                // set since remote (non-aggregator) fragments don't receive dependencies.
                final DependencyPair dep
                    = siteConnection.executeSysProcPlanFragment(m_txnState,
                                                         m_inputDeps,
                                                         fragmentId,
                                                         params);
                // @Shutdown returns null, handle it here
                if (dep != null) {
                    currentFragResponse.addDependency(dep.depId, dep.dependency);
                }
            } catch (final EEException e) {
                hostLog.l7dlog(Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(),
                        new Object[] { Encoder.hexEncode(m_fragmentMsg.getFragmentPlan(frag)) }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                break;
            } catch (final SQLException e) {
                hostLog.l7dlog(Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(),
                        new Object[] { Encoder.hexEncode(m_fragmentMsg.getFragmentPlan(frag)) }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                break;
            }
        }
        return currentFragResponse;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SysprocFragmentTask:");
        sb.append("  TXN ID: ").append(TxnEgo.txnIdToString(getTxnId()));
        sb.append("  SP HANDLE ID: ").append(TxnEgo.txnIdToString(getSpHandle()));
        sb.append("  ON HSID: ").append(CoreUtils.hsIdToString(m_initiator.getHSId()));
        return sb.toString();
    }
}
