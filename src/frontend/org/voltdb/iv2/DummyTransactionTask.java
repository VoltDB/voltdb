/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import org.voltdb.SiteProcedureConnection;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.DummyTransactionResponseMessage;
import org.voltdb.rejoin.TaskLog;

/**
 * This dummy transaction task is a SP read only transaction that will be put into transaction task queue.
 * And it will be duplicated to followers as well. This task acts as a synchronization transaction on all
 * partition replicas like normal SP write transaction.
 */
public class DummyTransactionTask extends TransactionTask {
    final Mailbox m_initiator;

    public DummyTransactionTask(Mailbox initiator,
            TransactionState txnState, TransactionTaskQueue queue) {
        super(txnState, queue);
        m_initiator = initiator;
    }

    private void generateDummyResponse() {
        doCommonSPICompleteActions();
        DummyTransactionResponseMessage response = new DummyTransactionResponseMessage(this);
        response.m_sourceHSId = m_initiator.getHSId();
        m_initiator.deliver(response);
    }

    @Override
    public void run(SiteProcedureConnection siteConnection) {
        generateDummyResponse();
    }

    @Override
    public void runFromTaskLog(SiteProcedureConnection siteConnection) {
        generateDummyResponse();
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog rejoinTaskLog) throws IOException {
        generateDummyResponse();
    }

    public long getSPIHSId() {
        return m_txnState.initiatorHSId;
    }

    public boolean needCoordination() {
        return false;
    }
}
