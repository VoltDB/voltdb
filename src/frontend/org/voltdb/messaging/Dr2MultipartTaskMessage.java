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

package org.voltdb.messaging;

import org.voltcore.messaging.VoltMessage;
import org.voltdb.StoredProcedureInvocation;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Dr2MultipartTaskMessage extends VoltMessage {

    StoredProcedureInvocation m_invocation;

    Dr2MultipartTaskMessage() {
        super();
    }

    public Dr2MultipartTaskMessage(StoredProcedureInvocation invocation) {
        m_invocation = invocation;
    }

    public StoredProcedureInvocation getSpi() {
        return m_invocation;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        if (buf.remaining() > 0) {
            m_invocation = new StoredProcedureInvocation();
            m_invocation.initFromBuffer(buf);
        } else {
            m_invocation = null;
        }
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.DR2_MULTIPART_TASK_ID);

        if (m_invocation != null) {
            m_invocation.flattenToBuffer(buf);
        }

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public int getSerializedSize() {
        int size = super.getSerializedSize();
        if (m_invocation != null) {
            size += m_invocation.getSerializedSize();
        }

        return size;
    }
}
