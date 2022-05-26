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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Sent from SPIs to replicas when transactions fully commits on all replicas.
 */
public class RepairLogTruncationMessage extends VoltMessage {

    protected long m_handle;

    public RepairLogTruncationMessage() {}

    public RepairLogTruncationMessage(long handle)
    {
        this.m_handle = handle;
    }

    public long getHandle()
    {
        return m_handle;
    }

    @Override
    public int getSerializedSize()
    {
        return super.getSerializedSize() + 8;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException
    {
        m_handle = buf.getLong();

        assert(buf.capacity() == buf.position());
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.IV2_REPAIR_LOG_TRUNCATION);
        buf.putLong(m_handle);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }
}
