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
     * @param log Enable logging of messages delivered to this mailbox
     * @return A new Mailbox instance or null based on success.
     */
    public abstract Mailbox createMailbox(int siteId, int mailboxId, boolean log);

    /**
     * Create a new mailbox for the site specified with a specific id and
     * a specified message type.
     *
     * @param siteId The id of the site this new mailbox will be attached
     * to.
     * @param mailboxId The requested id of the new mailbox.
     * @param mailbox The mailbox to be used for the siteid/mailboxid
     * @return A new Mailbox instance or null based on success.
     */
    public abstract void createMailbox(int siteId, int mailboxId, Mailbox mailbox);
}
