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
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.voltdb.VoltDB;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;

public abstract class VoltMessage {

    public final static int MAX_DESTINATIONS_PER_HOST = VoltDB.MAX_SITES_PER_HOST;

    // MSG_SIZE + MAILBOX_SIZE + DESTINATION_COUNT * DESTINATION_SIZE * MAX_DEST_COUNT
    public final static int HEADER_SIZE = 4 + 4 + 4 * MAX_DESTINATIONS_PER_HOST;

    // Identify each message
    final public static byte INITIATE_TASK_ID = 1;
    final public static byte INITIATE_RESPONSE_ID = 2;
    final public static byte FRAGMENT_TASK_ID = 3;
    final public static byte FRAGMENT_RESPONSE_ID = 4;
    final public static byte PARTICIPANT_NOTICE_ID = 5;
    final public static byte HEARTBEAT_ID = 6;
    final public static byte HEARTBEAT_RESPONSE_ID = 7;
    final public static byte FAILURE_SITE_UPDATE_ID = 9;
    final public static byte RECOVERY_ID = 10;
    final public static byte COMPLETE_TRANSACTION_ID = 11;
    final public static byte COMPLETE_TRANSACTION_RESPONSE_ID = 12;
    final public static byte AGREEMENT_TASK_ID = 13;
    final public static byte BINARY_PAYLOAD_ID = 14;
    final public static byte COALESCED_HEARTBEAT_ID = 15;

    // place holder for destination site ids when using multi-cast
    final public static int SEND_TO_MANY = -2;

    public int receivedFromSiteId = -1;

    // pluggable handling for messages defined at runtime
    final public static HashMap<Byte, Class<? extends VoltMessage>> externals =
        new HashMap<Byte, Class<? extends VoltMessage>>();

    protected BBContainer m_container = null;
    protected ByteBuffer m_buffer = null;
    protected byte m_subject;

    public static VoltMessage createNewMessage(byte messageType) {
        // instantiate a new message instance according to the type
        VoltMessage message = instantiate(messageType);
        message.initNew();
        return message;
    }

    /*public static VoltMessage createMessageFromBuffer(byte[] buffer) {
        ByteBuffer b = ByteBuffer.allocate(buffer.length + HEADER_SIZE);
        b.position(HEADER_SIZE);
        b.mark();
        b.put(buffer);
        b.reset();
        return createMessageFromBuffer(b, true);
    }*/

    public static VoltMessage createMessageFromBuffer(ByteBuffer buffer, boolean isReleasedToMessage) {

        // ensure the position is the start of the data
        buffer.position(HEADER_SIZE);

        // read the identifier (which message class) and version (ignored for now) from the message
        buffer.mark();
        byte id = buffer.get();
        buffer.reset();

        // get a buffer that the message can own
        ByteBuffer ownedBuffer = buffer;
        if (isReleasedToMessage == false) {
            ownedBuffer = ByteBuffer.allocate(buffer.capacity());
            ownedBuffer.position(HEADER_SIZE);
            ownedBuffer.put(buffer);
        }

        // instantiate a new message instance according to the id
        VoltMessage message = instantiate(id);

        // initialize the message from the byte buffer
        message.m_buffer = ownedBuffer;
        message.m_container = DBBPool.wrapBB(message.m_buffer);
        message.initFromBuffer();

        return message;
    }

    private static VoltMessage instantiate(byte messageType) {
        // instantiate a new message instance according to the id
        VoltMessage message = null;

        switch (messageType) {
        case INITIATE_TASK_ID:
            message = new InitiateTaskMessage();
            break;
        case INITIATE_RESPONSE_ID:
            message = new InitiateResponseMessage();
            break;
        case FRAGMENT_TASK_ID:
            message = new FragmentTaskMessage();
            break;
        case FRAGMENT_RESPONSE_ID:
            message = new FragmentResponseMessage();
            break;
        case PARTICIPANT_NOTICE_ID:
            message = new MultiPartitionParticipantMessage();
            break;
        case HEARTBEAT_ID:
            message = new HeartbeatMessage();
            break;
        case COALESCED_HEARTBEAT_ID:
            message = new CoalescedHeartbeatMessage();
            break;
        case HEARTBEAT_RESPONSE_ID:
            message = new HeartbeatResponseMessage();
            break;
        case FAILURE_SITE_UPDATE_ID:
            message = new FailureSiteUpdateMessage();
            break;
        case RECOVERY_ID:
            message = new RecoveryMessage();
            break;
        case COMPLETE_TRANSACTION_ID:
            message = new CompleteTransactionMessage();
            break;
        case COMPLETE_TRANSACTION_RESPONSE_ID:
            message = new CompleteTransactionResponseMessage();
            break;
        case AGREEMENT_TASK_ID:
            message = new AgreementTaskMessage();
            break;
        case BINARY_PAYLOAD_ID:
            message = new BinaryPayloadMessage();
            break;
        default:
            Class<? extends VoltMessage> cls = externals.get(messageType);
            assert(cls != null);
            try {
                message = cls.newInstance();
            } catch (Exception e) {
                e.printStackTrace();
                assert(false);
            }
        }

        return message;
    }

    protected void setBufferSize(int size, DBBPool pool) {
        int pos = m_buffer.position();
        size += HEADER_SIZE;

        if (m_buffer.capacity() >= size)
            m_buffer.limit(size);
        else {
            m_buffer.rewind();
            BBContainer c = pool.acquire(size * 2);
            c.b.limit(size);
            c.b.put(m_buffer);
            c.b.position(pos);
            if (m_container != null) {
                m_container.discard();
            }
            m_buffer = c.b;
            m_container = c;
        }
    }

    protected abstract void initFromBuffer();
    protected abstract void flattenToBuffer(final DBBPool pool) throws IOException;
    protected void initNew() {
        m_buffer = ByteBuffer.allocate(HEADER_SIZE + 1024);
        m_container = DBBPool.wrapBB(m_buffer);
        m_buffer.position(HEADER_SIZE);
        m_buffer.limit(HEADER_SIZE);
    }

    public BBContainer getBufferForMessaging(final DBBPool pool) throws IOException {
        flattenToBuffer(pool);
        return m_container;
    }

    /**
     * Discard the container if there is one.
     * Used in tests to ensure that Pool memory is returned
     */
    public void discard() {
        if (m_container != null) {
            m_container.discard();
            m_container = null;
            m_buffer = null;
        }
    }

    public byte getSubject() {
        return m_subject;
    }

    public ByteBuffer getBuffer() {
        return m_buffer;
    }
}
