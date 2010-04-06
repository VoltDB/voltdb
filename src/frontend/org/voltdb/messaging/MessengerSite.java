/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.messaging;

import java.util.HashMap;
import java.util.Queue;

class MessengerSite {

    HostMessenger m_hostMessenger;
    final int m_siteId;
    HashMap<Integer, SiteMailbox> m_mailboxes = new HashMap<Integer, SiteMailbox>( 16, (float).1);

    public MessengerSite(HostMessenger hostMessenger, int siteId) {
        m_hostMessenger = hostMessenger;
        m_siteId = siteId;
    }

    public synchronized Mailbox createMailbox(int mailboxId, Queue<VoltMessage> queue) {
        SiteMailbox mbox = getMailbox(mailboxId);
        if (mbox != null) return null;

        SiteMailbox newMbox = new SiteMailbox(m_hostMessenger, m_siteId, mailboxId, queue);
        m_mailboxes.put(mailboxId, newMbox);
        return newMbox;
    }

    SiteMailbox getMailbox(int mailboxId) {
        return m_mailboxes.get(mailboxId);
    }
}
