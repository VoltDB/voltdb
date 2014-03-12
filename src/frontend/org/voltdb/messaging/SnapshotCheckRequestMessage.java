/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import com.google_voltpatches.common.base.Charsets;
import org.voltcore.messaging.VoltMessage;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SnapshotCheckRequestMessage extends VoltMessage {

    private String m_requestJson;

    /** Empty constructor for de-serialization */
    SnapshotCheckRequestMessage()
    {
        super();
    }

    public SnapshotCheckRequestMessage(String requestJson)
    {
        super();
        m_requestJson = requestJson;
    }

    public String getRequestJson() { return m_requestJson; }

    @Override
    public int getSerializedSize()
    {
        int size = super.getSerializedSize();
        size += 4 + m_requestJson.length();
        return size;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException
    {
        final byte[] strBytes = new byte[buf.getInt()];
        buf.get(strBytes);
        m_requestJson = new String(strBytes, Charsets.UTF_8);
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.SNAPSHOT_CHECK_REQUEST_ID);
        buf.putInt(m_requestJson.length());
        buf.put(m_requestJson.getBytes(Charsets.UTF_8));
    }
}
