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

import org.voltcore.messaging.VoltMessage;
import org.voltcore.messaging.VoltMessageFactory;
import org.voltdb.e3.GapFillContinue;
import org.voltdb.e3.GapFillRequest;
import org.voltdb.e3.GapFillResponse;
import org.voltdb.rejoin.RejoinDataAckMessage;
import org.voltdb.rejoin.RejoinDataMessage;

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
    final public static byte IV2_INITIATE_TASK_ID = VOLTCORE_MESSAGE_ID_MAX + 9;
    final public static byte IV2_REPAIR_LOG_REQUEST = VOLTCORE_MESSAGE_ID_MAX + 10;
    final public static byte IV2_REPAIR_LOG_RESPONSE = VOLTCORE_MESSAGE_ID_MAX + 11;
    final public static byte REJOIN_RESPONSE_ID = VOLTCORE_MESSAGE_ID_MAX + 12;
    final public static byte REJOIN_DATA_ID = VOLTCORE_MESSAGE_ID_MAX + 13;
    final public static byte REJOIN_DATA_ACK_ID = VOLTCORE_MESSAGE_ID_MAX + 14;
    final public static byte FRAGMENT_TASK_LOG_ID = VOLTCORE_MESSAGE_ID_MAX + 15;
    final public static byte IV2_LOG_FAULT_ID = VOLTCORE_MESSAGE_ID_MAX + 16;
    final public static byte IV2_EOL_ID = VOLTCORE_MESSAGE_ID_MAX + 17;
    final public static byte DUMP = VOLTCORE_MESSAGE_ID_MAX + 18;
    final public static byte MP_REPLAY_ID = VOLTCORE_MESSAGE_ID_MAX + 19;
    final public static byte MP_REPLAY_ACK_ID = VOLTCORE_MESSAGE_ID_MAX + 20;
    final public static byte SNAPSHOT_CHECK_REQUEST_ID = VOLTCORE_MESSAGE_ID_MAX + 21;
    final public static byte SNAPSHOT_CHECK_RESPONSE_ID = VOLTCORE_MESSAGE_ID_MAX + 22;
    final public static byte IV2_REPAIR_LOG_TRUNCATION = VOLTCORE_MESSAGE_ID_MAX + 23;
    final public static byte DR2_MULTIPART_TASK_ID = VOLTCORE_MESSAGE_ID_MAX + 24;
    final public static byte DR2_MULTIPART_RESPONSE_ID = VOLTCORE_MESSAGE_ID_MAX + 25;
    final public static byte DUMMY_TRANSACTION_TASK_ID = VOLTCORE_MESSAGE_ID_MAX + 26;
    final public static byte DUMMY_TRANSACTION_RESPONSE_ID = VOLTCORE_MESSAGE_ID_MAX + 27;
    final public static byte DUMP_PLAN_ID = VOLTCORE_MESSAGE_ID_MAX + 28;
    final public static byte Migrate_Partition_Leader_MESSAGE_ID = VOLTCORE_MESSAGE_ID_MAX + 29;
    final public static byte FLUSH_RO_TXN_MESSAGE_ID = VOLTCORE_MESSAGE_ID_MAX + 30;
    final public static byte START_TASKS_ID = VOLTCORE_MESSAGE_ID_MAX + 31;
    final public static byte HASH_MISMATCH_MESSAGE_ID = VOLTCORE_MESSAGE_ID_MAX + 32;
    final public static byte E3_GAP_FILL_REQUEST = VOLTCORE_MESSAGE_ID_MAX + 33;
    final public static byte E3_GAP_FILL_RESPONSE = VOLTCORE_MESSAGE_ID_MAX + 34;
    final public static byte E3_GAP_FILL_CONTINUE = VOLTCORE_MESSAGE_ID_MAX + 35;

    /**
     * Overridden by subclasses to create message types unknown by voltcore
     * @param messageType
     * @return
     */
    @Override
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
        case IV2_INITIATE_TASK_ID:
            message = new Iv2InitiateTaskMessage();
            break;
        case IV2_REPAIR_LOG_REQUEST:
            message = new Iv2RepairLogRequestMessage();
            break;
        case IV2_REPAIR_LOG_RESPONSE:
            message = new Iv2RepairLogResponseMessage();
            break;
        case REJOIN_RESPONSE_ID:
            message = new RejoinMessage();
            break;
        case REJOIN_DATA_ID:
            message = new RejoinDataMessage();
            break;
        case REJOIN_DATA_ACK_ID:
            message = new RejoinDataAckMessage();
            break;
        case IV2_LOG_FAULT_ID:
            message = new Iv2LogFaultMessage();
            break;
        case IV2_EOL_ID:
            message = new Iv2EndOfLogMessage();
            break;
        case DUMP:
            message = new DumpMessage();
            break;
        case MP_REPLAY_ID:
            message = new MpReplayMessage();
            break;
        case MP_REPLAY_ACK_ID:
            message = new MpReplayAckMessage();
            break;
        case SNAPSHOT_CHECK_REQUEST_ID:
            message = new SnapshotCheckRequestMessage();
            break;
        case SNAPSHOT_CHECK_RESPONSE_ID:
            message = new SnapshotCheckResponseMessage();
            break;
        case IV2_REPAIR_LOG_TRUNCATION:
            message = new RepairLogTruncationMessage();
            break;
        case DR2_MULTIPART_TASK_ID:
            message = new Dr2MultipartTaskMessage();
            break;
        case DR2_MULTIPART_RESPONSE_ID:
            message = new Dr2MultipartResponseMessage();
            break;
        case DUMMY_TRANSACTION_TASK_ID:
            message = new DummyTransactionTaskMessage();
            break;
        case DUMMY_TRANSACTION_RESPONSE_ID:
            message = new DummyTransactionResponseMessage();
            break;
        case Migrate_Partition_Leader_MESSAGE_ID:
            message = new MigratePartitionLeaderMessage();
            break;
        case DUMP_PLAN_ID:
            message = new DumpPlanThenExitMessage();
            break;
        case FLUSH_RO_TXN_MESSAGE_ID:
            message = new MPBacklogFlushMessage();
            break;
        case START_TASKS_ID:
            message = new EnableTasksOnPartitionsMessage();
            break;
        case HASH_MISMATCH_MESSAGE_ID:
            message = new HashMismatchMessage();
            break;
        case E3_GAP_FILL_REQUEST:
            return new GapFillRequest();
        case E3_GAP_FILL_RESPONSE:
            return new GapFillResponse();
        case E3_GAP_FILL_CONTINUE:
            return new GapFillContinue();
        default:
            message = null;
        }
        return message;
    }
}
