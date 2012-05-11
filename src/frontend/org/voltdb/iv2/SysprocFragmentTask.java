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

import java.util.HashMap;
import java.util.List;

import org.voltcore.logging.Level;
import org.voltcore.messaging.Mailbox;

import org.voltdb.DependencyPair;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.ParameterSet;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.utils.LogKeys;
import org.voltdb.VoltTable;

public class SysprocFragmentTask extends TransactionTask
{
    final Mailbox m_initiator;
    final FragmentTaskMessage m_task;

    SysprocFragmentTask(Mailbox mailbox,
                 ParticipantTransactionState txn,
                 FragmentTaskMessage message)
    {
        super(txn);
        m_initiator = mailbox;
        m_task = message;
        assert(m_task.isSysProcTask());
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        if (!m_txn.isReadOnly()) {
            if (m_txn.getBeginUndoToken() == Site.kInvalidUndoToken) {
                m_txn.setBeginUndoToken(siteConnection.getLatestUndoToken());
            }
        }

        final FragmentResponseMessage response = processFragmentTask(siteConnection);
        m_initiator.deliver(response);
    }

    // Extracted the sysproc portion of ExecutionSite processFragmentTask(), then
    // modifed to work in the new world
    public FragmentResponseMessage processFragmentTask(SiteProcedureConnection siteConnection)
    {
        final FragmentResponseMessage currentFragResponse =
            new FragmentResponseMessage(m_task, m_initiator.getHSId());
        currentFragResponse.setStatus(FragmentResponseMessage.SUCCESS, null);

        for (int frag = 0; frag < m_task.getFragmentCount(); frag++)
        {
            final long fragmentId = m_task.getFragmentId(frag);
            // equivalent to dep.depId:
            // final int outputDepId = m_task.getOutputDepId(frag);

            ParameterSet params = m_task.getParameterSetForFragment(frag);

            try {
                // run the overloaded sysproc planfragment. pass an empty dependency
                // set since remote (non-aggregator) fragments don't receive dependencies.
                final DependencyPair dep
                    = siteConnection.executePlanFragment(m_txn,
                            new HashMap<Integer, List<VoltTable>>(),
                            fragmentId,
                            params);
                currentFragResponse.addDependency(dep.depId, dep.dependency);
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
}
