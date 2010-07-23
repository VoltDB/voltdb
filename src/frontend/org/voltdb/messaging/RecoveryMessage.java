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

import java.io.IOException;
import java.nio.ByteBuffer;

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
    RecoveryMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    private static final int sourceSiteOffset = HEADER_SIZE + 1;
    private static final int blockIndexOffset = sourceSiteOffset + 4;
    private static final int typeOffset = blockIndexOffset + 4;
    private static final int tableIdOffset = typeOffset + 1;

    /**
     * Constructor takes a ByteBuffer that already has enough space for the header and
     * already contains the recovery message as serialized by the EE. The limit has been
     * set appropriately.
     */
    public RecoveryMessage(BBContainer container, int siteId, int blockIndex) {
        m_subject = Subject.DEFAULT.getId();
        m_container = container;
        m_buffer = container.b;
        m_buffer.put(HEADER_SIZE, RECOVERY_ID);
        m_buffer.putInt(sourceSiteOffset, siteId);
        m_buffer.putInt(blockIndexOffset, blockIndex);
    }

    /**
     * Constructor for constructing an ack to a recovery message.
     * Reuses the buffer from the message.
     */
    public RecoveryMessage(RecoveryMessage message, int siteId) {
        m_subject = Subject.DEFAULT.getId();
        m_container = message.m_container;
        m_buffer = message.m_container.b;
        m_buffer.putInt(sourceSiteOffset, siteId);
        m_buffer.put(typeOffset, (byte)RecoveryMessageType.Ack.ordinal());
        m_buffer.limit(typeOffset + 1);
    }

    @Override
    protected void flattenToBuffer(final DBBPool pool) throws IOException {
        /*
         * Nothing to do. Everything was already serialized in the source EE
         */
    }

    public int sourceSite() {
        return m_buffer.getInt(sourceSiteOffset);
    }

    public int blockIndex() {
        return m_buffer.getInt(blockIndexOffset);
    }

    public RecoveryMessageType type() {
        return RecoveryMessageType.values()[m_buffer.get(typeOffset)];
    }

    public int tableId() {
        return m_buffer.getInt(tableIdOffset);//tableId is after recovery message type
    }

    public byte[] getMessageData() {
        m_buffer.position(getHeaderLength());
        ByteBuffer slice = m_buffer.slice();
        byte bytes[] = new byte[slice.remaining()];
        slice.get(bytes);
        return bytes;
    }

    /**
     * This is the header that isn't part of the recovery message. It contains the message id
     * used to pick this class for deserializing the incoming message
     */
    public static int getHeaderLength() {
        return typeOffset;//position where the EE will serialize the type of the message
    }

    @Override
    protected void initFromBuffer() {
        /*
         * Nothing to do here either. Everything will be
         * deserialized in the destination EE.
         */
    }
}
