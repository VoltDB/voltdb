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

import org.voltcore.messaging.MessagingException;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.messaging.BorrowTaskMessage;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class SpInitiatorMessageHandler extends InitiatorMessageHandler
{
    long[] m_replicaHSIds = new long[] {};

    public SpInitiatorMessageHandler(Scheduler scheduler)
    {
        super(scheduler);
    }

    @Override
    public void updateReplicas(long[] hsids)
    {
        for (long l : m_replicaHSIds) hostLog.info("Before Updating replicas: " + l);
        m_replicaHSIds = hsids;
        for (long l : m_replicaHSIds) hostLog.info("After Updating replicas: " + l);
    }

    // SpInitiators will see every message type.  The Responses currently come
    // from local work, but will come from replicas when replication is
    // implemented
    public void deliver(VoltMessage message)
    {
        if (message instanceof Iv2InitiateTaskMessage) {
            handleIv2InitiateTaskMessage((Iv2InitiateTaskMessage)message);
        }
        else if (message instanceof InitiateResponseMessage) {
            handleInitiateResponseMessage((InitiateResponseMessage)message);
        }
        else if (message instanceof FragmentTaskMessage) {
            handleFragmentTaskMessage((FragmentTaskMessage)message);
        }
        else if (message instanceof FragmentResponseMessage) {
            handleFragmentResponseMessage((FragmentResponseMessage)message);
        }
        else if (message instanceof CompleteTransactionMessage) {
            handleCompleteTransactionMessage((CompleteTransactionMessage)message);
        }
        else if (message instanceof BorrowTaskMessage) {
            handleBorrowTaskMessage((BorrowTaskMessage)message);
        }
        else {
            throw new RuntimeException("UNKNOWN MESSAGE TYPE, BOOM!");
        }
    }

    // InitiateTaskMessages for the primary initiator always get
    // forwarded to the local scheduler for handling
    private void handleIv2InitiateTaskMessage(Iv2InitiateTaskMessage message)
    {
        if (m_replicaHSIds.length > 0) {
            try {
                Iv2InitiateTaskMessage replmsg =
                    new Iv2InitiateTaskMessage(m_mailbox.getHSId(),
                            m_mailbox.getHSId(),
                            message.getTxnId(),
                            message.isReadOnly(),
                            message.isSinglePartition(),
                            message.getStoredProcedureInvocation(),
                            message.getClientInterfaceHandle()) ;
                for (long i : m_replicaHSIds) {
                    hostLog.info("replicationg " + message.getTxnId() + " to " + i);
                }
                m_mailbox.send(m_replicaHSIds, replmsg);
            } catch (MessagingException e) {
                hostLog.error("Failed to deliver response from execution site.", e);
            }
            DuplicateCounter counter = new DuplicateCounter(
                    message.getInitiatorHSId(), m_replicaHSIds.length + 1,
                    message.getTxnId());
        }
        m_scheduler.handleIv2InitiateTaskMessage(message);
    }

    // InitiateResponses come from the local Site and just get forwarded
    // on to either the client interface (for single-part work) or the MPI
    // (for every-partition sysprocs).  Deduping for replicas will change this
    // pattern.
    private void handleInitiateResponseMessage(InitiateResponseMessage message)
    {
        if (message.getInitiatorHSId() != m_mailbox.getHSId()) {
            try {
                // the initiatorHSId is the ClientInterface mailbox. Yeah. I know.
                m_mailbox.send(message.getInitiatorHSId(), message);
            }
            catch (MessagingException e) {
                // hostLog.error("Failed to deliver response from execution site.", e);
            }
        }
    }

    // FragmentTaskMessages for the primary initiator always get
    // forwarded to the local scheduler for handling
    private void handleFragmentTaskMessage(FragmentTaskMessage message)
    {
        if (m_replicaHSIds.length > 0) {
            try {
                FragmentTaskMessage replmsg =
                    new FragmentTaskMessage(m_mailbox.getHSId(),
                            m_mailbox.getHSId(), message);
                m_mailbox.send(m_replicaHSIds, replmsg);
            } catch (MessagingException e) {
                hostLog.error("Failed to deliver response from execution site.", e);
            }
        }
        m_scheduler.handleFragmentTaskMessage(message);
    }

    // FragmentResponses come from the local Site and just get forwarded
    // on to either the client interface (for single-part work) or the MPI
    // (for every-partition sysprocs).  Deduping for replicas will change this
    // pattern.
    private void handleFragmentResponseMessage(FragmentResponseMessage message)
    {
        if (message.getDestinationSiteId() != m_mailbox.getHSId()) {
            try {
                m_mailbox.send(message.getDestinationSiteId(), message);
            }
            catch (MessagingException e) {
                hostLog.error("Failed to deliver response from execution site.", e);
            }
        }
    }

    private void handleCompleteTransactionMessage(CompleteTransactionMessage message)
    {
        if (m_replicaHSIds.length > 0) {
            try {
                CompleteTransactionMessage replmsg = message;
                m_mailbox.send(m_replicaHSIds, replmsg);
            } catch (MessagingException e) {
                hostLog.error("Failed to deliver response from execution site.", e);
            }
        }
        m_scheduler.handleCompleteTransactionMessage(message);
    }

    // BorrowTaskMessages just encapsulate a FragmentTaskMessage along with
    // its input dependency tables.
    private void handleBorrowTaskMessage(BorrowTaskMessage message) {
        m_scheduler.handleFragmentTaskMessage(message.getFragmentTaskMessage(),
                                              message.getInputDepMap());
    }
}
