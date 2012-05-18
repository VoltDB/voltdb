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

import org.voltcore.messaging.VoltMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class MpInitiatorMessageHandler implements InitiatorMessageHandler
{
    final Scheduler m_scheduler;

    public MpInitiatorMessageHandler(Scheduler scheduler)
    {
        m_scheduler = scheduler;
    }

    public void deliver(VoltMessage message)
    {
        if (message instanceof Iv2InitiateTaskMessage) {
            handleIv2InitiateTaskMessage((Iv2InitiateTaskMessage)message);
        }
        else if (message instanceof InitiateResponseMessage) {
            handleInitiateResponseMessage((InitiateResponseMessage)message);
        }
        else if (message instanceof FragmentResponseMessage) {
            handleFragmentResponseMessage((FragmentResponseMessage)message);
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
        m_scheduler.handleInitiateResponseMessage(message);
    }

    private void handleFragmentResponseMessage(FragmentResponseMessage message)
    {
        m_scheduler.handleFragmentResponseMessage(message);
    }
}
