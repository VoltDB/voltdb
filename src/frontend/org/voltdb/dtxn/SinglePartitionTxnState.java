/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.dtxn;

import org.voltdb.ExecutionSite;
import org.voltdb.debugstate.ExecutorContext.ExecutorTxnState;
import org.voltdb.messages.*;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;

class SinglePartitionTxnState extends TransactionState {

    InitiateTask m_task = null;

    public SinglePartitionTxnState(Mailbox mbox, SimpleDtxnConnection conn, ExecutionSite site, InitiateTask task) {
        super(mbox, conn, site, task);
        m_task = task;
    }

    @Override
    public boolean doWork() {
        if (m_done)
            return true;

        m_site.beginNewTxn(m_task.getTxnId(), isReadOnly);
        InitiateResponse response = m_site.processInitiateTask(m_task);
        if (response.shouldCommit() == false) {
            // check if we need to undo
            if (!isReadOnly) {
                m_site.rollbackTransaction(isReadOnly);
            }
        }

        try {
            m_mbox.send(initiatorSiteId, 0, response);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
        m_done = true;
        return true;
    }

    @Override
    public void getDumpContents(StringBuilder sb) {
        sb.append("  Single Partition Txn State with id ").append(txnId);
    }

    @Override
    public ExecutorTxnState getDumpContents() {
        ExecutorTxnState retval = new ExecutorTxnState();
        retval.txnId = txnId;
        retval.coordinatorSiteId = coordinatorSiteId;
        retval.initiatorSiteId = initiatorSiteId;
        retval.isReadOnly = isReadOnly;
        retval.nonCoordinatingSites = null;
        retval.procedureIsAborting = false;
        return retval;
    }
}
