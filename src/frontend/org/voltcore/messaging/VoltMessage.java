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

public abstract class VoltMessage
{
    // place holder for destination site ids when using multi-cast
    final public static int SEND_TO_MANY = -2;

    public long m_sourceHSId = -1;

    protected byte m_subject;

    public int getSerializedSize() {
        return 1;
    }

    protected void initFromBuffer(VoltMessageFactory factory, ByteBuffer buf) throws IOException {
        initFromBuffer(buf);
    }

    protected abstract void initFromBuffer(ByteBuffer buf) throws IOException;
    public abstract void flattenToBuffer(ByteBuffer buf) throws IOException;

    public static ByteBuffer toBuffer(VoltMessage message) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(message.getSerializedSize());
        message.flattenToBuffer(buf);
        assert(buf.capacity() == buf.position());
        buf.flip();
        return buf;
    }

    public byte getSubject() {
        return m_subject;
    }

    public String getMessageInfo() {
        return getClass().getSimpleName();
    }
}
