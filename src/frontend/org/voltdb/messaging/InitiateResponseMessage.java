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

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;

/**
 * Message from an execution site to initiator with the final response for
 * the client
 */
public class InitiateResponseMessage extends VoltMessage {

    private long m_txnId;
    private long m_spHandle;
    private long m_initiatorHSId;
    private long m_coordinatorHSId;
    private long m_clientInterfaceHandle;
    private long m_connectionId;
    private boolean m_commit;
    private boolean m_recovering;
    private boolean m_readOnly;
    private ClientResponseImpl m_response;

    // Mis-partitioned invocation needs to send the invocation back to ClientInterface for restart
    private boolean m_mispartitioned;
    private StoredProcedureInvocation m_invocation;
    private Pair<Long, byte[]> m_currentHashinatorConfig;

    /** Empty constructor for de-serialization */
    public InitiateResponseMessage()
    {
        m_initiatorHSId = -1;
        m_coordinatorHSId = -1;
        m_subject = Subject.DEFAULT.getId();
    }

    /**
     * IV2 constructor
     */
    public InitiateResponseMessage(Iv2InitiateTaskMessage task) {
        m_txnId = task.getTxnId();
        m_spHandle = task.getSpHandle();
        m_initiatorHSId = task.getInitiatorHSId();
        m_coordinatorHSId = task.getCoordinatorHSId();
        m_subject = Subject.DEFAULT.getId();
        m_clientInterfaceHandle = task.getClientInterfaceHandle();
        m_connectionId = task.getConnectionId();
        m_readOnly = task.isReadOnly();
    }

    /**
     * IV2 constructor for sentinel response
     * @param sentinel
     */
    public InitiateResponseMessage(MultiPartitionParticipantMessage sentinel) {
        m_txnId = sentinel.getTxnId();
        m_spHandle = sentinel.getSpHandle();
        m_initiatorHSId = sentinel.getInitiatorHSId();
        m_coordinatorHSId = sentinel.getCoordinatorHSId();
        m_subject = Subject.DEFAULT.getId();
        m_clientInterfaceHandle = sentinel.getClientInterfaceHandle();
        m_connectionId = sentinel.getConnectionId();
    }

    /**
     * Create a response from a request.
     * Note that some private request data is copied to the response.
     * @param task The initiation request object to collect the
     * metadata from.
     */
    public InitiateResponseMessage(InitiateTaskMessage task) {
        m_txnId = task.getTxnId();
        m_spHandle = task.getSpHandle();
        m_initiatorHSId = task.getInitiatorHSId();
        m_coordinatorHSId = task.getCoordinatorHSId();
        m_subject = Subject.DEFAULT.getId();
        m_clientInterfaceHandle = Long.MIN_VALUE;
        m_connectionId = Long.MIN_VALUE;
        m_readOnly = task.isReadOnly();
    }

    public void setClientHandle(long clientHandle) {
        m_response.setClientHandle(clientHandle);
    }

    public long getTxnId() {
        return m_txnId;
    }

    public long getSpHandle() {
        return m_spHandle;
    }

    public long getInitiatorHSId() {
        return m_initiatorHSId;
    }

    public long getCoordinatorHSId() {
        return m_coordinatorHSId;
    }

    public long getClientInterfaceHandle() {
        return m_clientInterfaceHandle;
    }

    public long getClientConnectionId() {
        return m_connectionId;
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

    public boolean isMispartitioned() {
        return m_mispartitioned;
    }

    public StoredProcedureInvocation getInvocation() {
        return m_invocation;
    }

    public Pair<Long, byte[]> getCurrentHashinatorConfig() {
        return m_currentHashinatorConfig;
    }

    public void setMispartitioned(boolean mispartitioned, StoredProcedureInvocation invocation,
                                  Pair<Long, byte[]> currentHashinatorConfig) {
        m_mispartitioned = mispartitioned;
        m_invocation = invocation;
        m_currentHashinatorConfig = currentHashinatorConfig;
        m_commit = false;
        m_response = new ClientResponseImpl(ClientResponse.TXN_RESTART, new VoltTable[]{}, "Mispartitioned");
    }

    public ClientResponseImpl getClientResponseData() {
        return m_response;
    }

    public void setResults(ClientResponseImpl r) {
        m_commit = (r.getStatus() == ClientResponseImpl.SUCCESS);
        m_response = r;
    }

    public boolean isReadOnly() {
        return m_readOnly;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 8 // txnId
            + 8 // m_spHandle
            + 8 // initiator HSId
            + 8 // coordinator HSId
            + 8 // client interface handle
            + 8 // client connection id
            + 1 // read only
            + 1 // node recovering indication
            + 1 // mispartitioned invocation
            + m_response.getSerializedSize();

        if (m_mispartitioned) {
            msgsize += m_invocation.getSerializedSize()
                       + 8 // current hashinator version
                       + 4 // hashinator config length
                       + m_currentHashinatorConfig.getSecond().length; // hashinator config
        }

        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.INITIATE_RESPONSE_ID);
        buf.putLong(m_txnId);
        buf.putLong(m_spHandle);
        buf.putLong(m_initiatorHSId);
        buf.putLong(m_coordinatorHSId);
        buf.putLong(m_clientInterfaceHandle);
        buf.putLong(m_connectionId);
        buf.put((byte) (m_readOnly == true ? 1 : 0));
        buf.put((byte) (m_recovering == true ? 1 : 0));
        buf.put((byte) (m_mispartitioned == true ? 1 : 0));
        m_response.flattenToBuffer(buf);
        if (m_mispartitioned) {
            buf.putLong(m_currentHashinatorConfig.getFirst());
            buf.putInt(m_currentHashinatorConfig.getSecond().length);
            buf.put(m_currentHashinatorConfig.getSecond());
            m_invocation.flattenToBuffer(buf);
        }
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        m_txnId = buf.getLong();
        m_spHandle = buf.getLong();
        m_initiatorHSId = buf.getLong();
        m_coordinatorHSId = buf.getLong();
        m_clientInterfaceHandle = buf.getLong();
        m_connectionId = buf.getLong();
        m_readOnly = buf.get() == 1;
        m_recovering = buf.get() == 1;
        m_mispartitioned = buf.get() == 1;
        m_response = new ClientResponseImpl();
        m_response.initFromBuffer(buf);
        m_commit = (m_response.getStatus() == ClientResponseImpl.SUCCESS);
        if (m_mispartitioned) {
            long hashinatorVersion = buf.getLong();
            byte[] hashinatorBytes = new byte[buf.getInt()];
            buf.get(hashinatorBytes);
            m_currentHashinatorConfig = Pair.of(hashinatorVersion, hashinatorBytes);
            // SPI must be the last to deserialize, it will take the remaining as parameter bytes
            m_invocation = new StoredProcedureInvocation();
            m_invocation.initFromBuffer(buf);
            m_commit = false;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("INITITATE_RESPONSE FOR TXN ");
        sb.append(m_txnId);
        sb.append("\n SP HANDLE: ").append(m_spHandle);
        sb.append("\n INITIATOR HSID: ").append(CoreUtils.hsIdToString(m_initiatorHSId));
        sb.append("\n COORDINATOR HSID: ").append(CoreUtils.hsIdToString(m_coordinatorHSId));
        sb.append("\n CLIENT INTERFACE HANDLE: ").append(m_clientInterfaceHandle);
        sb.append("\n CLIENT CONNECTION ID: ").append(m_connectionId);
        sb.append("\n READ-ONLY: ").append(m_readOnly);
        sb.append("\n RECOVERING: ").append(m_recovering);
        sb.append("\n MISPARTITIONED: ").append(m_mispartitioned);
        if (m_commit)
            sb.append("\n  COMMIT");
        else
            sb.append("\n  ROLLBACK/ABORT, ");
        sb.append("\n CLIENT RESPONSE: \n");
        sb.append(m_response.toJSONString());

        return sb.toString();
    }
}
