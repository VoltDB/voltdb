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

import java.io.IOException;

import org.voltcore.messaging.Mailbox;
import org.voltdb.DRConsumerDrIdTracker;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.messaging.Iv2GetDrTrackerRequestMessage;
import org.voltdb.messaging.Iv2GetDrTrackerResponseMessage;
import org.voltdb.rejoin.TaskLog;

public class GetDrTrackerTask extends TransactionTask
{
    final private Iv2GetDrTrackerRequestMessage m_requestMsg;
    final private Mailbox m_initiator;

    public GetDrTrackerTask(Mailbox initiator, TransactionTaskQueue queue, Iv2GetDrTrackerRequestMessage request)
    {
        super(new SpTransactionState(request), queue);
        m_initiator = initiator;
        m_requestMsg = request;
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        DRConsumerDrIdTracker tracker = siteConnection.getCorrespondingDrAppliedTxns(m_requestMsg.getProducerClusterId(),
                m_requestMsg.getProducerPartitionId());
        Iv2GetDrTrackerResponseMessage responseMsg = new Iv2GetDrTrackerResponseMessage(m_requestMsg.getRequestId(), tracker);
        responseMsg.m_sourceHSId = m_initiator.getHSId();
        m_initiator.send(m_requestMsg.m_sourceHSId, responseMsg);
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
            throws IOException
    {
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("GetDrTrackerTask:");
        sb.append(" REQID: ");
        sb.append(m_requestMsg.getRequestId());
        sb.append(" CLUSTERID: ");
        sb.append(m_requestMsg.getProducerClusterId());
        sb.append(" PARTITIONID: ");
        sb.append(m_requestMsg.getProducerPartitionId());
        return sb.toString();
    }

    @Override
    public void runFromTaskLog(SiteProcedureConnection siteConnection) {
        //  Auto-generated method stub

    }
}
