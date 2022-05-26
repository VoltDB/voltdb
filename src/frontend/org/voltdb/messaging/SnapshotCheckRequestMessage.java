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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.messaging.VoltMessage;

import com.google_voltpatches.common.base.Charsets;

public class SnapshotCheckRequestMessage extends VoltMessage {

    private byte [] m_requestJson;

    /** Empty constructor for de-serialization */
    SnapshotCheckRequestMessage()
    {
        super();
    }

    public SnapshotCheckRequestMessage(String requestJson)
    {
        super();
        m_requestJson = requestJson.getBytes(Charsets.UTF_8);
    }

    public String getRequestJson() { return new String(m_requestJson,Charsets.UTF_8); }

    @Override
    public int getSerializedSize()
    {
        int size = super.getSerializedSize();
        size += 4 + m_requestJson.length;
        return size;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException
    {
        m_requestJson = new byte[buf.getInt()];
        buf.get(m_requestJson);
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.SNAPSHOT_CHECK_REQUEST_ID);
        buf.putInt(m_requestJson.length);
        buf.put(m_requestJson);
    }
}
