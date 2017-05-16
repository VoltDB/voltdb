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

import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.common.Constants;

/**
 * Message from a client interface to an initiator, instructing the
 * site to begin executing a stored procedure, coordinating other
 * execution sites if needed.
 *
 */
public class DebugMessage extends VoltMessage
{
    String m_procName;

    /** Empty constructor for de-serialization */
    public DebugMessage() {
        super();
    }

    public DebugMessage(String procName)
    {
        super();
        m_procName = procName;
    }

    public String getProcName() {
        return m_procName;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 4 /*string length */
                + m_procName.length();
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.DEBUG);
        buf.putInt(m_procName.length());
        buf.put(m_procName.getBytes(Constants.UTF8ENCODING));

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        FastDeserializer in = new FastDeserializer(buf);
        m_procName = in.readString().intern();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DEBUG " + m_procName + "(FROM ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        return sb.toString();
    }
}