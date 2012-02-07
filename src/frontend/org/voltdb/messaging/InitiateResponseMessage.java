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

import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.MiscUtils;
import org.voltdb.ClientResponseImpl;

/**
 * Message from an execution site to initiator with the final response for
 * the client
 */
public class InitiateResponseMessage extends VoltMessage {

    private long m_txnId;
    private long m_initiatorHSId;
    private long m_coordinatorHSId;
    private boolean m_commit;
    private boolean m_recovering;
    private ClientResponseImpl m_response;

    /** Empty constructor for de-serialization */
    InitiateResponseMessage()
    {
        m_initiatorHSId = -1;
        m_coordinatorHSId = -1;
        m_subject = Subject.DEFAULT.getId();
    }

    /**
     * Create a response from a request.
     * Note that some private request data is copied to the response.
     * @param task The initiation request object to collect the
     * metadata from.
     */
    public InitiateResponseMessage(InitiateTaskMessage task) {
        m_txnId = task.getTxnId();
        m_initiatorHSId = task.getInitiatorHSId();
        m_coordinatorHSId = task.getCoordinatorHSId();
        m_subject = Subject.DEFAULT.getId();
    }

    public void setClientHandle(long clientHandle) {
        m_response.setClientHandle(clientHandle);
    }

    public long getTxnId() {
        return m_txnId;
    }

    public long getInitiatorHSId() {
        return m_initiatorHSId;
    }

    public long getCoordinatorHSId() {
        return m_coordinatorHSId;
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
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 8 // txnId
            + 8 // initiator HSId
            + 8 // coordinator HSId
            + 1; // node recovering indication

        msgsize += m_response.getSerializedSize();

        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf)
    {
        buf.put(VoltDbMessageFactory.INITIATE_RESPONSE_ID);
        buf.putLong(m_txnId);
        buf.putLong(m_initiatorHSId);
        buf.putLong(m_coordinatorHSId);
        buf.put((byte) (m_recovering == true ? 1 : 0));
        m_response.flattenToBuffer(buf);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        m_txnId = buf.getLong();
        m_initiatorHSId = buf.getLong();
        m_coordinatorHSId = buf.getLong();
        m_recovering = buf.get() == 1;
        m_response = new ClientResponseImpl();
        m_response.initFromBuffer(buf);
        m_commit = (m_response.getStatus() == ClientResponseImpl.SUCCESS);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("INITITATE_RESPONSE FOR TXN ");
        sb.append(m_txnId);
        sb.append("\n INITIATOR HSID: " + MiscUtils.hsIdToString(m_initiatorHSId));
        sb.append("\n COORDINATOR HSID: " + MiscUtils.hsIdToString(m_coordinatorHSId));

        if (m_commit)
            sb.append("\n  COMMIT");
        else
            sb.append("\n  ROLLBACK/ABORT, ");

        // TODO More work here

        return sb.toString();
    }
}
