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

import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.MessagingException;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.messaging.BorrowTaskMessage;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class SpInitiatorMessageHandler implements InitiatorMessageHandler
{
    final Scheduler m_scheduler;
    final Mailbox m_mailbox;

    public SpInitiatorMessageHandler(Scheduler scheduler, Mailbox mailbox)
    {
        m_scheduler = scheduler;
        m_mailbox = mailbox;
    }

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

    private void handleIv2InitiateTaskMessage(Iv2InitiateTaskMessage message)
    {
        m_scheduler.handleIv2InitiateTaskMessage(message);
    }

    private void handleInitiateResponseMessage(InitiateResponseMessage message)
    {
        try {
            // the initiatorHSId is the ClientInterface mailbox. Yeah. I know.
            m_mailbox.send(message.getInitiatorHSId(), message);
        }
        catch (MessagingException e) {
            // hostLog.error("Failed to deliver response from execution site.", e);
        }
    }

    private void handleFragmentTaskMessage(FragmentTaskMessage message)
    {
        m_scheduler.handleFragmentTaskMessage(message);
    }

    private void handleFragmentResponseMessage(FragmentResponseMessage message)
    {
        try {
            m_mailbox.send(message.getDestinationSiteId(), message);
        }
        catch (MessagingException e) {
            hostLog.error("Failed to deliver response from execution site.", e);
        }
    }

    private void handleCompleteTransactionMessage(CompleteTransactionMessage message)
    {
        m_scheduler.handleCompleteTransactionMessage(message);
    }

    private void handleBorrowTaskMessage(BorrowTaskMessage message) {
        m_scheduler.handleFragmentTaskMessage(message.getFragmentTaskMessage(),
                                              message.getInputDepMap());
    }
}
