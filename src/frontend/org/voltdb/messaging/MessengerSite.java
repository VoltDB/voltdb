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

package org.voltdb.messaging;

import java.util.HashMap;

class MessengerSite {

    HostMessenger m_hostMessenger;
    final int m_siteId;
    HashMap<Integer, Mailbox> m_mailboxes = new HashMap<Integer, Mailbox>( 16, (float).1);

    public MessengerSite(HostMessenger hostMessenger, int siteId) {
        m_hostMessenger = hostMessenger;
        m_siteId = siteId;
    }

    public synchronized Mailbox createMailbox(int mailboxId, boolean log) {
        SiteMailbox mbox = (SiteMailbox)getMailbox(mailboxId);
        if (mbox != null) return null;

        SiteMailbox newMbox = new SiteMailbox(m_hostMessenger, m_siteId, mailboxId, log);
        m_mailboxes.put(mailboxId, newMbox);
        return newMbox;
    }

    public synchronized void createMailbox(int mailboxId, Mailbox mailbox) {
        assert(mailbox != null);
        assert(!m_mailboxes.containsKey(mailboxId));
        if (m_mailboxes.containsKey(mailboxId)) {
            throw new IllegalStateException("MessengerSite " + m_siteId + " already contains mailbox " + mailboxId);
        }
        m_mailboxes.put(mailboxId, mailbox);
    }

    Mailbox getMailbox(int mailboxId) {
        return m_mailboxes.get(mailboxId);
    }
}
