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
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SnapshotCheckResponseMessage extends VoltMessage {

    // for identifying which snapshot the response is for
    private String m_path;
    private String m_nonce;
    private VoltTable m_response;

    /** Empty constructor for de-serialization */
    SnapshotCheckResponseMessage()
    {
        super();
    }

    public SnapshotCheckResponseMessage(String path, String nonce, VoltTable response)
    {
        super();
        m_path = path;
        m_nonce = nonce;
        m_response = response;
        m_response.resetRowPosition();
    }

    public String getPath() { return m_path; }
    public String getNonce() { return m_nonce; }
    public VoltTable getResponse() { return m_response; }

    @Override
    public int getSerializedSize()
    {
        int size = super.getSerializedSize();
        size += 4 + m_path.length()
                + 4 + m_nonce.length()
                + m_response.getSerializedSize();
        return size;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException
    {
        final byte[] pathBytes = new byte[buf.getInt()];
        buf.get(pathBytes);
        m_path = new String(pathBytes, Charsets.UTF_8);
        final byte[] nonceBytes = new byte[buf.getInt()];
        buf.get(nonceBytes);
        m_nonce = new String(nonceBytes, Charsets.UTF_8);
        m_response = PrivateVoltTableFactory.createVoltTableFromSharedBuffer(buf);
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.SNAPSHOT_CHECK_RESPONSE_ID);
        buf.putInt(m_path.length());
        buf.put(m_path.getBytes(Charsets.UTF_8));
        buf.putInt(m_nonce.length());
        buf.put(m_nonce.getBytes(Charsets.UTF_8));
        m_response.flattenToBuffer(buf);
    }
}
