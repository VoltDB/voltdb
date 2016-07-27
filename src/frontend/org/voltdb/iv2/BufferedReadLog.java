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

public class BufferedReadLog
{
    final Deque<InitiateResponseMessage> m_bufferedReadSp;
    Mailbox m_mailbox;

    BufferedReadLog(Mailbox mailbox)
    {
        m_bufferedReadSp = new ArrayDeque<InitiateResponseMessage>();

        assert(mailbox != null);
        m_mailbox = mailbox;
    }

    //  SPI offers a new message.
    public void offerSp(InitiateResponseMessage msg, long handle)
    {
        if (msg.getSpHandle() <= handle) {
            m_mailbox.send(msg.getInitiatorHSId(), msg);
        } else {
            m_bufferedReadSp.add(msg);
        }
        releaseBufferedRead(handle);
    }

    public void releaseBufferedRead(long spHandle)
    {
        Deque<InitiateResponseMessage> deq = m_bufferedReadSp;
        InitiateResponseMessage msg = null;
        while ((msg = deq.peek()) != null) {
            if (msg.getSpHandle() > spHandle) {
                return;
            }
            // when the sp reads' handle is less equal than truncation handle
            // we know any previous write has been confirmed and it's safe to release.
            m_mailbox.send(msg.getInitiatorHSId(), msg);
            deq.poll();
        }
    }
}
