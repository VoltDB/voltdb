/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.nio.ByteBuffer;

import org.voltcore.utils.CoreUtils;
import org.voltdb.messaging.VoltDbMessageFactory;

public class TruncationHandleMessage extends VoltMessage {
    public long m_truncationHandle;

    public TruncationHandleMessage(long truncationHandle) {
        m_truncationHandle = truncationHandle;
    }

    public TruncationHandleMessage() {
    }

    public long getTruncationHandle() {
        return m_truncationHandle;
    }

    @Override
    public int getSerializedSize() {
        return super.getSerializedSize() + 8; // + 1 long for m_truncationHandle;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) {
        buf.put(VoltDbMessageFactory.TRUNCATION_HANDLE_ID);
        buf.putLong(m_truncationHandle);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) {
        m_truncationHandle = buf.getLong();
    }

    @Override
    public String toString() {
        return super.toString()
                   + " TruncationHandleMessage truncation handle id: "
                   + CoreUtils.hsIdToString(m_truncationHandle);
    }
}
