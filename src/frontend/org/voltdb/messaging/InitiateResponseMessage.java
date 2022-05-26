/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.iv2.DeterminismHash;
import org.voltdb.iv2.TxnEgo;

/**
 * Message from an execution site to initiator with the final response for
 * the client
 */
public class InitiateResponseMessage extends VoltMessage {

    private static final VoltLogger networkLog = new VoltLogger("NETWORK");

    private long m_txnId;
    private long m_spHandle;
    private long m_initiatorHSId;
    private long m_coordinatorHSId;
    private long m_clientInterfaceHandle;
    private long m_connectionId;
    private boolean m_commit;
    private boolean m_recovering;
    private boolean m_readOnly;
    private boolean m_mpFragmentSent = false;   // Needed to correctly advance the truncation handle
    private ClientResponseImpl m_response;

    // Mis-partitioned invocation needs to send the invocation back to ClientInterface for restart
    private boolean m_mispartitioned;
    private StoredProcedureInvocation m_invocation;
    private Pair<Long, byte[]> m_currentHashinatorConfig;

    //The flag used for MigratePartitionLeader operation, indicating that the task was created
    //when the site was leader partition
    boolean m_executedOnPreviousLeader = false;
    int m_hashMismatchPos = -1;


    // No need to serialize it
    public boolean m_isFromNonRestartableSysproc = false;

    /** Empty constructor for de-serialization */
    public InitiateResponseMessage()
    {
        m_initiatorHSId = -1;
        m_coordinatorHSId = -1;
        m_subject = Subject.DEFAULT.getId();
    }

    public static InitiateResponseMessage messageForNTProcResponse(long clientInterfaceHandle,
                                                                   long connectionId,
                                                                   ClientResponseImpl response)
    {
        InitiateResponseMessage irm = new InitiateResponseMessage();
        irm.m_txnId = -2;
        irm.m_spHandle = -2;
        irm.m_initiatorHSId = -2;
        irm.m_coordinatorHSId = -1;
        irm.m_clientInterfaceHandle = clientInterfaceHandle;
        irm.m_connectionId = connectionId;
        irm.m_commit = true;
        irm.m_recovering = false;
        irm.m_readOnly = false;
        irm.m_response = response;
        irm.m_mispartitioned = false;
        irm.m_invocation = null;
        irm.m_currentHashinatorConfig = null;
        irm.m_subject = Subject.DEFAULT.getId();
        return irm;
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

    public void setConnectionId(long connectionId) {
        m_connectionId = connectionId;
    }

    public boolean isMispartitioned() {
        return m_mispartitioned;
    }

    public boolean isMisrouted() {
        return (m_response != null && m_response.getStatus() == ClientResponse.TXN_MISROUTED);
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

    public void setMisrouted(StoredProcedureInvocation invocation) {
        m_invocation = invocation;
        m_currentHashinatorConfig = TheHashinator.getCurrentVersionedConfig();
        m_commit = false;
        m_response = new ClientResponseImpl(ClientResponse.TXN_MISROUTED, new VoltTable[]{}, "Misrouted");
    }

    public ClientResponseImpl getClientResponseData() {
        return m_response;
    }

    public void setResults(ClientResponseImpl r) {
        m_commit = (r.getStatus() == ClientResponseImpl.SUCCESS);
        m_response = r;
    }

    public void setMismatchPos(int pos) {
        m_hashMismatchPos = pos;
    }


    public boolean isReadOnly() {
        return m_readOnly;
    }

    public void setMpFragmentSent(boolean fragmentSent) {
        m_mpFragmentSent = fragmentSent;
    }

    public boolean haveSentMpFragment() {
        return m_mpFragmentSent;
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
            + 1 // for m_executedOnPreviousLeader
            + 1 // MP fragment was sent to SPIs (used for repair log truncation)
            + m_response.getSerializedSize();

        if (m_mispartitioned || isMisrouted()) {
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
        buf.put((byte) (m_executedOnPreviousLeader == true ? 1 : 0));
        buf.put((byte) (m_mpFragmentSent  == true ? 1 : 0));
        m_response.flattenToBuffer(buf);
        if (m_mispartitioned || isMisrouted()) {
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
        m_executedOnPreviousLeader = buf.get() == 1;
        m_mpFragmentSent = buf.get() == 1;
        m_response = new ClientResponseImpl();
        try {
            m_response.initFromBuffer(buf);
        } catch (IOException e) {
            networkLog.error("Unexpected errors while reading results. " +
                    "Initiator(" + CoreUtils.hsIdToString(m_initiatorHSId) + "), " +
                    "Coordinator(" + CoreUtils.hsIdToString(m_coordinatorHSId) + "), " +
                    "TxnId" + TxnEgo.txnIdToString(m_txnId) + " " + e.getMessage());
            throw e;
        }
        m_commit = (m_response.getStatus() == ClientResponseImpl.SUCCESS);
        if (m_mispartitioned || isMisrouted()) {
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

        sb.append("INITITATE_RESPONSE FOR TXN ").append(TxnEgo.txnIdToString(m_txnId));
        sb.append("\n SP HANDLE: ").append(TxnEgo.txnIdToString(m_spHandle));
        sb.append("\n INITIATOR HSID: ").append(CoreUtils.hsIdToString(m_initiatorHSId));
        sb.append("\n COORDINATOR HSID: ").append(CoreUtils.hsIdToString(m_coordinatorHSId));
        sb.append("\n CLIENT INTERFACE HANDLE: ").append(m_clientInterfaceHandle);
        sb.append("\n CLIENT CONNECTION ID: ").append(m_connectionId);
        sb.append("\n READ-ONLY: ").append(m_readOnly);
        sb.append("\n RECOVERING: ").append(m_recovering);
        sb.append("\n MISPARTITIONED: ").append(m_mispartitioned);
        if (m_commit) {
            sb.append("\n  COMMIT");
        } else {
            sb.append("\n  ROLLBACK/ABORT, ");
        }
        int[] hashes = m_response.getHashes();
        if (hashes != null) {
            sb.append("\n RESPONSE HASH: ").append(DeterminismHash.description(hashes, m_hashMismatchPos));
        }
        sb.append("\n CLIENT RESPONSE: \n");
        if (m_response == null) {
            // This is not going to happen in the real world, but only in the test cases
            // TestSpSchedulerDedupe
            sb.append( "NULL" );
        } else {
            sb.append(m_response.toStatusJSONString());
        }

        return sb.toString();
    }

    public void setExecutedOnPreviousLeader(boolean forOldLeader) {
        m_executedOnPreviousLeader = forOldLeader;
    }

    public boolean isExecutedOnPreviousLeader() {
        return m_executedOnPreviousLeader;
    }

    @Override
    public String getMessageInfo() {
        return "InitiateResponseMessage TxnId:" + TxnEgo.txnIdToString(m_txnId);
    }
}
