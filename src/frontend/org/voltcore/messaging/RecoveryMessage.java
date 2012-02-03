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

package org.voltcore.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;

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

    private long m_sourceHSId;
    private long m_txnId;
    private byte m_address[];
    private int m_port;

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
    public RecoveryMessage(long sourceHSId, long txnId, byte address[], int port) {
        m_subject = Subject.DEFAULT.getId();
        m_recoveryMessagesAvailable = false;
        m_sourceHSId = sourceHSId;
        m_txnId = txnId;
        m_address = address;
        m_port = port;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize += 24 + m_address.length;
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(RECOVERY_ID);
        buf.putLong( m_sourceHSId);
        buf.putLong( m_txnId);
        buf.putInt(m_address.length);
        buf.put(m_address);
        buf.putInt( m_port);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    public long txnId() {
        return m_txnId;
    }

    public long sourceSite() {
        return m_sourceHSId;
    }

    public byte[] address() {
        return m_address;
    }

    public int port() {
        return m_port;
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) {
        m_recoveryMessagesAvailable = false;
        m_sourceHSId = buf.getLong();
        m_txnId = buf.getLong();
        m_address = new byte[buf.getInt()];
        buf.get(m_address);
        m_port = buf.getInt();

        assert(buf.capacity() == buf.position());
    }
}
