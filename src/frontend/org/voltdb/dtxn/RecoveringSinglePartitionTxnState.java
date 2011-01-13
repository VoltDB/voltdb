/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import org.voltdb.ClientResponseImpl;
import org.voltdb.ExecutionSite;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.TransactionInfoBaseMessage;

public class RecoveringSinglePartitionTxnState extends SinglePartitionTxnState {

    private RecoveringSinglePartitionTxnState(Mailbox mbox, ExecutionSite site, TransactionInfoBaseMessage task) {
        super(mbox, site, task);
        // TODO Auto-generated constructor stub
    }

    @Override
    public boolean doWork(boolean recovering) {
        if (!m_done) {
            InitiateResponseMessage response = new InitiateResponseMessage(m_task);

            // add an empty dummy response
            response.setResults(new ClientResponseImpl(
                    ClientResponse.SUCCESS,
                    new VoltTable[0],
                    null));

            // this tells the initiator that the response is a dummy
            response.setRecovering(true);

            try {
                m_mbox.send(initiatorSiteId, 0, response);
            } catch (MessagingException e) {
                throw new RuntimeException(e);
            }

            m_done = true;
        }
        return m_done;
    }

}
