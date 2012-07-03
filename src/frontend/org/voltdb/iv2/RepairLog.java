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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.voltcore.messaging.VoltMessage;

import org.voltdb.messaging.Iv2InitiateTaskMessage;

/**
 * The repair log stores messages received from a PI in case they need to be
 * shared with less informed RIs should the PI shed its mortal coil.
 */
public class RepairLog
{
    // last seen truncation point.
    long m_truncationPoint;

    // is this a partition leader?
    boolean m_isLeader = false;

    // want voltmessage as payload with message-independent metadata.
    static class Item
    {
        final VoltMessage m_msg;
        final long m_handle;

        Item(VoltMessage msg, long handle)
        {
            m_msg = msg;
            m_handle = handle;
        }

        long getSpHandle()
        {
            return m_handle;
        }

        VoltMessage getMessage()
        {
            return m_msg;
        }
    }

    // log storage.
    final Queue<Item> m_log;

    RepairLog()
    {
        m_log = new LinkedList<Item>();
    }

    // leaders log differently
    void setLeaderState(boolean isLeader)
    {
        m_isLeader = isLeader;
        // If we're the leader, wipe out the old repair log.
        // This call to setLeaderState() to promote us to the leader shouldn't
        // happen until after log repair is complete.
        if (m_isLeader) {
            truncate(Long.MAX_VALUE);
        }
    }

    // Offer a new message to the repair log. This will truncate
    // the repairLog if the message includes a truncation hint.
    public void deliver(VoltMessage msg)
    {
        if (!m_isLeader && msg instanceof Iv2InitiateTaskMessage) {
            final Iv2InitiateTaskMessage m = (Iv2InitiateTaskMessage)msg;
            truncate(m.getTruncationHandle());
            m_log.offer(new Item(m, m.getSpHandle()));
        }
    }

    // trim unnecessary log messages.
    private void truncate(long spHandle)
    {
        // MIN signals no truncation work to do.
        if (spHandle == Long.MIN_VALUE) {
            return;
        }

        m_truncationPoint = spHandle;
        while (!m_log.isEmpty()) {
            if (m_log.peek().m_handle <= m_truncationPoint) {
                m_log.poll();
                continue;
            }
            break;
        }
    }

    // produce the contents of the repair log.
    public List<Item> contents()
    {
        List<Item> response = new LinkedList<Item>();
        Iterator<Item> it = m_log.iterator();
        while (it.hasNext()) {
            response.add(it.next());
        }
        return response;
    }
}

