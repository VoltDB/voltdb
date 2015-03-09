/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltcore.messaging;

/**
 * A <code>Mailbox</code> represents a single destination for
 * messages in the messaging system.
 *
 */
public interface Mailbox {

    /**
     * Send a serializable object to a specific site and mailbox. This just
     * forwards to the Messenger's send method of the instance that generated
     * this mailbox for now.
     *
     * @param hsId The id of the destination site/host.
     * @param message A serializable object to be sent to another mailbox.
     * @throws MessagingException Throws an exception if the destination
     * mailbox cannot be reached (maybe) or if an exception is thrown during
     * serialization.
     */
    public void send(long hsId, VoltMessage message);

    /**
     * Send a serializable object to a specific mailbox at a list of sites.
     * This also just forwards to the Messenger's send method of the instance
     * that generated this mailbox for now. It's assumed that most "to-all"
     * sending will be done to a common mailbox id.
     *
     * @param hsIds The ids of the destination sites/hosts.
     * @param mailboxId The id of the destination mailbox.
     * @param message A serializable object to be sent to other mailboxes.
     * @throws MessagingException Throws an exception if a destination
     * mailbox cannot be reached (maybe) or if an exception is thrown during
     * serialization.
     */
    public void send(long[] hsIds, VoltMessage message);

    /**
     * Allow message delivery to this mailbox.
     * @param message
     */
    public void deliver(VoltMessage message);

    /**
     * Deliver a message to the front of the mailbox queue so it will be processed first. This method is
     * NOT threadsafe. It can only be called by the execution site thread itself.
     * @param message
     */
    void deliverFront(VoltMessage message);

    /**
     * Get the next Object from this messaging queue from the default subjects.
     *
     * @return A message object on success or null if no object is waiting.
     */
    public VoltMessage recv();

    /**
     * Get the next Object from this messaging queue from the default subjects. Blocks if no messages
     * are available.
     *
     * @return A message object on success or null if interrupted
     */
    public VoltMessage recvBlocking();

    /**
     * Get the next Object from this messaging queue from the default subjects. Blocks if no messages
     * are available.
     * @param  timeout Number of milliseconds to wait for work
     * @return A message object on success or null if interrupted
     */
    public VoltMessage recvBlocking(long timeout);

    /**
     * Get the next Object from this messaging queue from the provided subject. The order
     * of the subjects determines the order that subjects are checked for waiting messages.
     *
     * @return A message object on success or null if no object is waiting.
     */
    public VoltMessage recv(Subject s[]);

    /**
     * Get the next Object from this messaging queue from the default subject. Blocks if no messages
     * are available. The order of the subjects determines the order that subjects are checked
     * for waiting messages.
     *
     * @return A message object on success or null if interrupted
     */
    public VoltMessage recvBlocking(Subject s[]);

    /**
     * Get the next Object from this messaging queue from the default subject.. Blocks if no messages
     * are available.The order of the subjects determines the order that subjects are checked
     * for waiting messages.
     * @param s Subjects to check for messages
     * @param  timeout Number of milliseconds to wait for work
     * @return A message object on success or null if interrupted
     */
    public VoltMessage recvBlocking(Subject s[], long timeout);

    public long getHSId();

    public void setHSId(long hsId);
}
