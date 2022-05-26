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

package org.voltcore.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

public class VoltMessageFactory {
    // Identify each message
    final public static byte AGREEMENT_TASK_ID = 1;
    final public static byte BINARY_PAYLOAD_ID = 2;
    final public static byte SITE_FAILURE_UPDATE_ID = 3;
    final public static byte HEARTBEAT_ID = 4;
    final public static byte HEARTBEAT_RESPONSE_ID = 5;
    final public static byte RECOVERY_ID = 6;
    final public static byte SITE_FAILURE_FORWARD_ID = 7;
    final public static byte UNKNOWN_SITE_ID = 8;
    // DON'T JUST ADD A MESSAGE TYPE WITHOUT READING THIS!
    // VoltDbMessageFactory is going to use this to generate non-core message IDs.
    // Update the max value if you add a new message type above, or you
    // will be sad, and I will have no sympathy. --izzy
    final public static byte VOLTCORE_MESSAGE_ID_MAX = 8;

    public VoltMessage createMessageFromBuffer(ByteBuffer buffer, long sourceHSId)
    throws IOException
    {
        byte type = buffer.get();

        // instantiate a new message instance according to the id
        VoltMessage message = instantiate_local(type);
        if (message == null)
        {
            message = instantiate(type);
        }
        message.m_sourceHSId = sourceHSId;
        message.initFromBuffer(this, buffer.slice().asReadOnlyBuffer());
        return message;
    }

    /**
     * Overridden by subclasses to create message types unknown by voltcore
     * @param messageType
     * @return
     */
    protected VoltMessage instantiate_local(byte messageType)
    {
        return null;
    }

    private VoltMessage instantiate(byte messageType) {
        // instantiate a new message instance according to the id
        VoltMessage message = null;

        switch (messageType) {
        case HEARTBEAT_ID:
            message = new HeartbeatMessage();
            break;
        case HEARTBEAT_RESPONSE_ID:
            message = new HeartbeatResponseMessage();
            break;
        case SITE_FAILURE_UPDATE_ID:
            message = new SiteFailureMessage();
            break;
        case RECOVERY_ID:
            message = new RecoveryMessage();
            break;
        case AGREEMENT_TASK_ID:
            message = new AgreementTaskMessage();
            break;
        case BINARY_PAYLOAD_ID:
            message = new BinaryPayloadMessage();
            break;
        case SITE_FAILURE_FORWARD_ID:
            message = new SiteFailureForwardMessage();
            break;
        case UNKNOWN_SITE_ID:
            message = new UnknownSiteId();
            break;
        default:
            org.voltdb.VoltDB.crashLocalVoltDB("Unrecognized message type " + messageType, true, null);
        }
        return message;
    }
}
