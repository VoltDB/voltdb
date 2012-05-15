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

import org.voltcore.messaging.VoltMessage;
import org.voltcore.messaging.VoltMessageFactory;

public class VoltDbMessageFactory extends VoltMessageFactory
{
    final public static byte INITIATE_TASK_ID = VOLTCORE_MESSAGE_ID_MAX + 1;
    final public static byte INITIATE_RESPONSE_ID = VOLTCORE_MESSAGE_ID_MAX + 2;
    final public static byte FRAGMENT_TASK_ID = VOLTCORE_MESSAGE_ID_MAX + 3;
    final public static byte FRAGMENT_RESPONSE_ID = VOLTCORE_MESSAGE_ID_MAX + 4;
    final public static byte PARTICIPANT_NOTICE_ID = VOLTCORE_MESSAGE_ID_MAX + 5;
    final public static byte COMPLETE_TRANSACTION_ID = VOLTCORE_MESSAGE_ID_MAX + 6;
    final public static byte COMPLETE_TRANSACTION_RESPONSE_ID = VOLTCORE_MESSAGE_ID_MAX + 7;
    final public static byte COALESCED_HEARTBEAT_ID = VOLTCORE_MESSAGE_ID_MAX + 8;
    final public static byte REJOIN_RESPONSE_ID = VOLTCORE_MESSAGE_ID_MAX + 9;

    /**
     * Overridden by subclasses to create message types unknown by voltcore
     * @param messageType
     * @return
     */
    protected VoltMessage instantiate_local(byte messageType)
    {
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
        case COALESCED_HEARTBEAT_ID:
            message = new CoalescedHeartbeatMessage();
            break;
        case COMPLETE_TRANSACTION_ID:
            message = new CompleteTransactionMessage();
            break;
        case COMPLETE_TRANSACTION_RESPONSE_ID:
            message = new CompleteTransactionResponseMessage();
            break;
        case REJOIN_RESPONSE_ID:
            message = new RejoinMessage();
            break;
        default:
            message = null;
        }
        return message;
    }
}
