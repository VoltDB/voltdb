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

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.rejoin.TaskLog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class JoinProducer extends JoinProducerBase implements TaskLog {
    private static final VoltLogger log = new VoltLogger("HOST");

    private final AtomicBoolean m_currentlyJoining = new AtomicBoolean(true);
    private long m_joinCoordinatorHSId = Long.MIN_VALUE;
    private boolean m_receivedFirstFragment = false;

    private static class CompletionAction extends JoinCompletionAction {
        CompletionAction(long snapshotTxnId)
        {
            super(snapshotTxnId);
        }

        @Override
        public void run()
        {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    JoinProducer(int partitionId, SiteTaskerQueue taskQueue)
    {
        super(partitionId, taskQueue);
    }

    @Override
    public boolean acceptPromotion()
    {
        return true;
    }

    @Override
    public void deliver(RejoinMessage message)
    {
        if (message.getType() == RejoinMessage.Type.INITIATION) {
            m_joinCoordinatorHSId = message.m_sourceHSId;
            m_taskQueue.offer(this);
            log.info("P" + m_partitionId + " received initiation");
        }
    }

    @Override
    public TaskLog getTaskLog()
    {
        return this;
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        throw new RuntimeException("Unexpected execution of run method in rejoin producer");
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog rejoinTaskLog) throws IOException
    {
        // TODO: setting join as complete now because the MPI doesn't know how to handle dummy
        // responses yet
        setJoinComplete(siteConnection);
        log.info("P" + m_partitionId + " finished join");

//        if (m_receivedFirstFragment) {
//            log.info("P" + m_partitionId + " run for rejoin is complete");
//            // TODO: now set join as completed. Should wait for replicated table snapshot
//            // transfer to finish.
//            setJoinComplete(siteConnection);
//        } else {
//            m_taskQueue.offer(this);
//        }
    }

    @Override
    public void logTask(TransactionInfoBaseMessage message) throws IOException
    {
        if (message instanceof FragmentTaskMessage) {
            log.info("P" + m_partitionId + " received first fragment, " +
                    "sending to mailbox " + CoreUtils.hsIdToString(m_joinCoordinatorHSId));
            if (!m_receivedFirstFragment) {
                RejoinMessage msg = new RejoinMessage(m_mailbox.getHSId(),
                        RejoinMessage.Type.FIRST_FRAGMENT_RECEIVED);
                m_mailbox.send(m_joinCoordinatorHSId, msg);
                m_receivedFirstFragment = true;
            }
        }
    }

    @Override
    public TransactionInfoBaseMessage getNextMessage() throws IOException
    {
        return null;
    }

    @Override
    public void setEarliestTxnId(long txnId)
    {

    }

    @Override
    public boolean isEmpty() throws IOException
    {
        return true;
    }

    @Override
    public void close() throws IOException
    {

    }

    private void setJoinComplete(SiteProcedureConnection siteConnection)
    {
        m_currentlyJoining.set(false);
        // TODO: need to provide a rejoin complete action and export sequence numbers
        siteConnection.setRejoinComplete(new CompletionAction(0), new HashMap<String, Map<Integer, Pair<Long, Long>>>());
    }
}
