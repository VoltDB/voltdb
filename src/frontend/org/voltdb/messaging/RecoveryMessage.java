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

import java.io.IOException;

import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;

/**
 *  Message containing recovery data for a partition/table pair.
 *
 *  It is kind of obfuscated but the recovery message id is used for picking a class
 *  to deserialize the message. There is also a recovery message type, that is independent of the recovery message id.
 *  There is only one recovery message id but many recovery message types for this different phases and steps of
 *  recovery.
 */
public class RecoveryMessage extends VoltMessage {

    /** Empty constructor for de-serialization */
    public RecoveryMessage() {
        m_subject = Subject.DEFAULT.getId();
        m_recoveryMessagesAvailable = true;
    }

    private static final int sourceSiteIdOffset = HEADER_SIZE + 1;
    private static final int txnIdOffset = sourceSiteIdOffset + 4;

    private static final int addressOffset = txnIdOffset + 8;
    private static final int portOffset = addressOffset + 4;

    /**
     * Reuse this message to indicate that an ack was received and it is a good idea to wake up
     * and do more work. The noarg constructor sets this to true so it can be delivered directly to the mailbox
     * by an IO thread. initFromBuffer sets it to false so that other messages don't get mixed up.
     */
    private boolean m_recoveryMessagesAvailable;

    public boolean recoveryMessagesAvailable() {
        return m_recoveryMessagesAvailable;
    }

    /**
     * Constructor takes a ByteBuffer that already has enough space for the header and
     * sets the source siteId followed by the last committed txnId at the partition before recovery begins.
     * The purpose of this message is to inform the source partition of what txnId the recovering partition
     * decided to stop after (stops after hearing from all initiators)
     * before syncing so that the source partition can decide what txnId the partitions
     * will sync at (will be some txnId >= the one in this message).
     *
     * This constructor is used to generate the recovery message sent by the source partition to
     * the recovering partition in response to the Initiate message received from the recovering partition.
     * The purpose of this message is to inform the recovering partition of what txnId the source partition
     * decided to stop after before syncing so that the recovering partition can start executing stored procedures
     * at the correct txnId.
     */
    public RecoveryMessage(BBContainer container, int sourceSiteId, long txnId, byte address[], int port) {
        m_subject = Subject.DEFAULT.getId();
        m_container = container;
        m_buffer = container.b;
        m_buffer.put(HEADER_SIZE, RECOVERY_ID);
        m_buffer.putInt( sourceSiteIdOffset, sourceSiteId);
        m_buffer.putLong(txnIdOffset, txnId);
        m_buffer.position(addressOffset);
        m_buffer.put(address);
        m_buffer.putInt(portOffset, port);
        m_buffer.limit(portOffset + 4);
        m_recoveryMessagesAvailable = false;
    }

    @Override
    protected void flattenToBuffer(final DBBPool pool) throws IOException {
    }

    public long txnId() {
        return m_buffer.getLong(txnIdOffset);
    }

    public int sourceSite() {
        return m_buffer.getInt(sourceSiteIdOffset);
    }

    public byte[] address() {
        byte address[] = new byte[4];
        m_buffer.position(addressOffset);
        m_buffer.get(address);
        return address;
    }

    public int port() {
        return m_buffer.getInt(portOffset);
    }

    @Override
    protected void initFromBuffer() {
        m_recoveryMessagesAvailable = false;
    }
}
