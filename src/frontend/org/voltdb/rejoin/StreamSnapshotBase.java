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

package org.voltdb.rejoin;

import org.voltcore.messaging.VoltMessage;
import org.voltdb.exceptions.SerializableException;

/**
 * Base class for reading and writing snapshot streams over the network.
 */
public abstract class StreamSnapshotBase {
    public static final int typeOffset = 0; // 1 byte
    public static final int blockIndexOffset = typeOffset + 1; // 4 bytes
    public static final int tableIdOffset = blockIndexOffset + 4; // 4 bytes
    public static final int contentOffset = tableIdOffset + 4;

    public static interface MessageFactory {
        public VoltMessage makeDataMessage(long targetId, byte[] data);

        public boolean isAckEOS(VoltMessage msg);
        public long getAckTargetId(VoltMessage msg);
        public int getAckBlockIndex(VoltMessage msg);
        public SerializableException getException(VoltMessage msg);
    }

    public static class DefaultMessageFactory implements MessageFactory {
        @Override
        public VoltMessage makeDataMessage(long targetId, byte[] data)
        {
            return new RejoinDataMessage(targetId, data);
        }

        @Override
        public boolean isAckEOS(VoltMessage msg)
        {
            assert msg instanceof RejoinDataAckMessage;
            return ((RejoinDataAckMessage) msg).isEOS();
        }

        @Override
        public long getAckTargetId(VoltMessage msg)
        {
            assert msg instanceof RejoinDataAckMessage;
            return ((RejoinDataAckMessage) msg).getTargetId();
        }

        @Override
        public int getAckBlockIndex(VoltMessage msg)
        {
            assert msg instanceof RejoinDataAckMessage;
            return ((RejoinDataAckMessage) msg).getBlockIndex();
        }

        @Override
        public  SerializableException getException(VoltMessage msg)
        {
            return null;
        }
    }

}
