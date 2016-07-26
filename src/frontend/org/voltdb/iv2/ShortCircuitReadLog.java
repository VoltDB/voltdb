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

import java.util.ArrayDeque;
import java.util.Deque;

import org.voltcore.messaging.Mailbox;
import org.voltdb.messaging.InitiateResponseMessage;

public class ShortCircuitReadLog
{
    final Deque<InitiateResponseMessage> m_shortCircuitReadSp;
    Mailbox m_mailbox;

    ShortCircuitReadLog(Mailbox mailbox)
    {
        m_shortCircuitReadSp = new ArrayDeque<InitiateResponseMessage>();

        assert(mailbox != null);
        m_mailbox = mailbox;
    }

    //  SPI offers a new message.
    public void offerSp(InitiateResponseMessage msg, long handle)
    {
        if (msg.getSpHandle() >= handle) {
            m_mailbox.send(msg.getInitiatorHSId(), msg);
        } else {
            m_shortCircuitReadSp.add(msg);
        }
        releaseShortCircuitRead(handle);
    }

    public void releaseShortCircuitRead(long spHandle)
    {
        Deque<InitiateResponseMessage> deq = m_shortCircuitReadSp;
        InitiateResponseMessage msg = null;
        while ((msg = deq.peek()) != null) {
            if (msg.getSpHandle() <= spHandle) {
                m_mailbox.send(msg.getInitiatorHSId(), msg);
                deq.poll();
            } else {
                break;
            }
        }
    }
}
