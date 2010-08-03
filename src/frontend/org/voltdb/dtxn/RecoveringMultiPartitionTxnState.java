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

import java.util.HashMap;
import java.util.List;

import org.voltdb.ExecutionSite;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltMessage;


/**
 * FOR PARTICIPANTS ONLY, NOT FOR COORDINATORS
 *
 */
public class RecoveringMultiPartitionTxnState extends MultiPartitionParticipantTxnState {

    public RecoveringMultiPartitionTxnState(Mailbox mbox, ExecutionSite site,
                                             TransactionInfoBaseMessage notice)
    {
        super(mbox, site, notice);
        // this txn state shouldn't be used if coordinating
        // during recovery
        assert(m_isCoordinator == false);
    }

    @Override
    public String toString() {
        return "RecoveringMultiPartitionTxnState initiator: " + initiatorSiteId +
            " in-progress: " + m_hasStartedWork +
            " txnId: " + TransactionIdManager.toString(txnId);
    }

    @Override
    public boolean doWork() {
        if (!m_hasStartedWork) {
            m_site.beginNewTxn(this);
            m_hasStartedWork = true;
        }

        if (m_done) {
            return true;
        }

        WorkUnit wu = m_readyWorkUnits.poll();
        while (wu != null) {

            VoltMessage payload = wu.getPayload();

            if (payload instanceof InitiateTaskMessage) {
                // this txn state shouldn't be used if coordinating
                // during recovery
                assert(false);
            }
            else if (payload instanceof FragmentTaskMessage) {
                processFragmentWork((FragmentTaskMessage) payload, wu.getDependencies());
            }

            // get the next workunit from the ready list
            wu = m_readyWorkUnits.poll();
        }

        return m_done;
    }

    @Override
    void processFragmentWork(FragmentTaskMessage ftask, HashMap<Integer, List<VoltTable>> dependencies) {
        assert(ftask.getFragmentCount() > 0);

        FragmentResponseMessage response = new FragmentResponseMessage(ftask, m_siteId);
        response.setRecovering(true);

        // add a dummy table for all of the expected dependency ids
        for (int i = 0; i < ftask.getFragmentCount(); i++) {
            response.addDependency(ftask.getOutputDepId(i),
                    new VoltTable(new VoltTable.ColumnInfo("DUMMY", VoltType.BIGINT)));
        }

        try {
            m_mbox.send(response.getDestinationSiteId(), 0, response);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }
}
