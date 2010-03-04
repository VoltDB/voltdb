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

import java.util.Queue;

/**
 * The interface to the global VoltDB messaging system.
 *
 */
public interface Messenger {

    /**
     * Create a new mailbox for the site specified with a specific id and
     * a specified message type.
     *
     * @param siteId The id of the site this new mailbox will be attached
     * to.
     * @param mailboxId The requested id of the new mailbox.
     * @param queue A queue implementation. Can be null in which case the default will be used. Useful in situations where a dummy
     * queue can process the message inside the offer call.
     * @return A new Mailbox instance or null based on success.
     */
    public abstract Mailbox createMailbox(int siteId, int mailboxId, Queue<VoltMessage> queue);
}
