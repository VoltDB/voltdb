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

import org.voltdb.ClientResponseImpl;
import org.voltdb.utils.DBBPool;

/**
 * Message from an initiator to an execution site, informing the
 * site that it may be requested to do work for a multi-partition
 * transaction, and to reserve a slot in its ordered work queue
 * for this transaction.
 *
 */
public class InitiateResponseMessage extends VoltMessage {

    private long m_txnId;
    private int m_initiatorSiteId;
    private int m_coordinatorSiteId;
    private boolean m_commit;
    private boolean m_recovering;
    private ClientResponseImpl m_response;

    /** Empty constructor for de-serialization */
    InitiateResponseMessage()
    {
        m_initiatorSiteId = -1;
        m_coordinatorSiteId = -1;
        m_subject = Subject.DEFAULT.getId();
    }

    /**
     * Create a response from a request.
     * Note that some private request data is copied to the response.
     * @param task The initiation request object to collect the
     * metadata from.
     */
    public InitiateResponseMessage(InitiateTaskMessage task) {
        m_txnId = task.m_txnId;
        m_initiatorSiteId = task.m_initiatorSiteId;
        m_coordinatorSiteId = task.m_coordinatorSiteId;
        m_subject = Subject.DEFAULT.getId();
    }

    public void setClientHandle(long clientHandle) {
        m_response.setClientHandle(clientHandle);
    }

    public long getTxnId() {
        return m_txnId;
    }

    public int getInitiatorSiteId() {
        return m_initiatorSiteId;
    }

    public int getCoordinatorSiteId() {
        return m_coordinatorSiteId;
    }

    public boolean shouldCommit() {
        return m_commit;
    }

    public boolean isRecovering() {
        return m_recovering;
    }

    public void setRecovering(boolean recovering) {
        m_recovering = recovering;
    }

    public ClientResponseImpl getClientResponseData() {
        return m_response;
    }

    public void setResults(ClientResponseImpl r) {
        setResults( r, null);
    }

    public void setResults(ClientResponseImpl r, InitiateTaskMessage task) {
        m_commit = (r.getStatus() == ClientResponseImpl.SUCCESS);
        m_response = r;
    }

    @Override
    protected void flattenToBuffer(final DBBPool pool) {
        // stupid lame flattening of the client response
        FastSerializer fs = new FastSerializer();
        try {
            fs.writeObject(m_response);
        } catch (IOException e) {
            e.printStackTrace();
            assert(false);
        }
        ByteBuffer responseBytes = fs.getBuffer();

        // I don't know where the two fours that were originally here come from.
        int msgsize = 8 + 4 + 4 + 4 + 4 + 1 + 8 + responseBytes.remaining();

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(INITIATE_RESPONSE_ID);

        m_buffer.putLong(m_txnId);
        m_buffer.putInt(m_initiatorSiteId);
        m_buffer.putInt(m_coordinatorSiteId);
        m_buffer.put((byte) (m_recovering == true ? 1 : 0));
        m_buffer.put(responseBytes);
        m_buffer.limit(m_buffer.position());
    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id
        m_txnId = m_buffer.getLong();
        m_initiatorSiteId = m_buffer.getInt();
        m_coordinatorSiteId = m_buffer.getInt();
        m_recovering = m_buffer.get() == 1;

        FastDeserializer fds = new FastDeserializer(m_buffer);
        try {
            m_response = fds.readObject(ClientResponseImpl.class);
            m_commit = (m_response.getStatus() == ClientResponseImpl.SUCCESS);
        } catch (IOException e) {
            e.printStackTrace();
            assert(false);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("INITITATE_RESPONSE (TO ");
        sb.append(receivedFromSiteId);
        sb.append(") FOR TXN ");
        sb.append(m_txnId);
        sb.append("\n INITIATOR SITE ID: " + m_initiatorSiteId);
        sb.append("\n COORDINATOR SITE ID: " + m_coordinatorSiteId);

        if (m_commit)
            sb.append("\n  COMMIT");
        else
            sb.append("\n  ROLLBACK/ABORT, ");

        // TODO More work here

        return sb.toString();
    }
}
