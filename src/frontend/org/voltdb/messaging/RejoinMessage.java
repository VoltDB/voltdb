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

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;

/**
 * Rejoin message used to drive the whole rejoin process. It could be sent from
 * the rejoin coordinator to a local site, or it could be sent from a rejoining
 * site to an existing site.
 */
public class RejoinMessage extends VoltMessage {
    public static enum Type {
        INITIATION, // sent by the coordinator to a local site
        REQUEST, // sent from a rejoining partition to an existing site

        // The following are response types
        REQUEST_RESPONSE,
        FAILURE,
        SNAPSHOT_FINISHED, // sent from a local site to the coordinator
        REPLAY_FINISHED, // sent from a local site to the coordinator
    }

    private Type m_type;
    private long m_txnId = -1; // snapshot txnId
    private List<byte[]> m_addresses = new ArrayList<byte[]>();
    private int m_port = -1;

    /** Empty constructor for de-serialization */
    public RejoinMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public RejoinMessage(long sourceHSId, List<byte[]> addresses, int port) {
        m_type = Type.REQUEST;
        m_sourceHSId = sourceHSId;
        m_subject = Subject.DEFAULT.getId();
        m_addresses = addresses;
        m_port = port;
    }

    public RejoinMessage(long sourceHSId, long txnId) {
        m_type = Type.REQUEST_RESPONSE;
        m_sourceHSId = sourceHSId;
        m_subject = Subject.DEFAULT.getId();
        m_txnId = txnId;
    }

    public RejoinMessage(long sourceHSId, Type type) {
        m_sourceHSId = sourceHSId;
        m_subject = Subject.DEFAULT.getId();
        m_type = type;
    }

    public Type getType() {
        return m_type;
    }

    public long txnId() {
        return m_txnId;
    }

    public List<byte[]> addresses() {
        return m_addresses;
    }

    public int port() {
        return m_port;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize();
        msgsize +=
            8 + // m_sourceHSId
            1 + // m_type
            8 + // m_txnId
            4 + // address count
            (4 * m_addresses.size()) + // 4 bytes per address
            4; // m_port
        for (byte address[] : m_addresses) {
            msgsize += address.length;
        }
        return msgsize;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        m_sourceHSId = buf.getLong();
        m_type = Type.values()[buf.get()];
        m_txnId = buf.getLong();
        int numAddresses = buf.getInt();
        m_addresses = new ArrayList<byte[]>(numAddresses);
        for (int ii = 0; ii < numAddresses; ii++) {
            byte address[] = new byte[buf.getInt()];
            buf.get(address);
            m_addresses.add(address);
        }
        m_port = buf.getInt();

        assert(buf.capacity() == buf.position());
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.REJOIN_RESPONSE_ID);
        buf.putLong(m_sourceHSId);
        buf.put((byte) m_type.ordinal());
        buf.putLong(m_txnId);
        buf.putInt(m_addresses.size());
        for (byte address[] : m_addresses) {
            buf.putInt(address.length);
            buf.put(address);
        }
        buf.putInt(m_port);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }
}
