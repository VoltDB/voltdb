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
import java.util.HashMap;

import org.voltcore.VoltDB;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;

public abstract class VoltMessage {
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

    public long m_sourceHSId = -1;

    protected byte m_subject;

    public static VoltMessage createMessageFromBuffer(ByteBuffer buffer, long sourceHSId) {
        byte type = buffer.get();

        // instantiate a new message instance according to the id
        VoltMessage message = instantiate(type);
        message.m_sourceHSId = sourceHSId;
        message.initFromBuffer(buffer.slice().asReadOnlyBuffer());
        return message;
    }

    private static VoltMessage instantiate(byte messageType) {
        // instantiate a new message instance according to the id
        VoltMessage message = null;

        switch (messageType) {
//        case INITIATE_TASK_ID:
//            message = new InitiateTaskMessage();
//            break;
//        case INITIATE_RESPONSE_ID:
//            message = new InitiateResponseMessage();
//            break;
//        case FRAGMENT_TASK_ID:
//            message = new FragmentTaskMessage();
//            break;
//        case FRAGMENT_RESPONSE_ID:
//            message = new FragmentResponseMessage();
//            break;
//        case PARTICIPANT_NOTICE_ID:
//            message = new MultiPartitionParticipantMessage();
//            break;
            case HEARTBEAT_ID:
                message = new HeartbeatMessage();
                break;
//        case COALESCED_HEARTBEAT_ID:
//            message = new CoalescedHeartbeatMessage();
//            break;
        case HEARTBEAT_RESPONSE_ID:
            message = new HeartbeatResponseMessage();
            break;
        case FAILURE_SITE_UPDATE_ID:
            message = new FailureSiteUpdateMessage();
            break;
        case RECOVERY_ID:
            message = new RecoveryMessage();
            break;
//        case COMPLETE_TRANSACTION_ID:
//            message = new CompleteTransactionMessage();
//            break;
//        case COMPLETE_TRANSACTION_RESPONSE_ID:
//            message = new CompleteTransactionResponseMessage();
//            break;
        case AGREEMENT_TASK_ID:
            message = new AgreementTaskMessage();
            break;
        case BINARY_PAYLOAD_ID:
            message = new BinaryPayloadMessage();
            break;
        default:
            VoltDB.crashLocalVoltDB("Unrecognized message type " + messageType, true, null);
        }
        return message;
    }

    public int getSerializedSize() {
        return 1;
    }

    protected abstract void initFromBuffer(ByteBuffer buf);
    public abstract void flattenToBuffer(ByteBuffer buf) throws IOException;

    public byte getSubject() {
        return m_subject;
    }
}
