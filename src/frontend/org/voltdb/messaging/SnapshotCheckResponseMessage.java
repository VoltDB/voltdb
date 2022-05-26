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
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;

import com.google_voltpatches.common.base.Charsets;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;

public class SnapshotCheckResponseMessage extends VoltMessage {

    // for identifying which snapshot the response is for
    private byte [] m_path;
    private byte [] m_nonce;
    private VoltTable m_response;
    private byte[] m_stypeBytes;
    private SnapshotPathType m_stype;

    /** Empty constructor for de-serialization */
    SnapshotCheckResponseMessage()
    {
        super();
    }

    public SnapshotCheckResponseMessage(String path, SnapshotPathType stype, String nonce, VoltTable response)
    {
        super();
        m_path = path.getBytes(Charsets.UTF_8);
        m_nonce = nonce.getBytes(Charsets.UTF_8);
        m_response = response;
        m_stype = stype;
        m_stypeBytes = stype.toString().getBytes(Charsets.UTF_8);
        m_response.resetRowPosition();
    }

    public String getPath() { return new String(m_path, Charsets.UTF_8); }
    public String getNonce() { return new String(m_nonce, Charsets.UTF_8); }
    public VoltTable getResponse() { return m_response; }
    public SnapshotPathType getSnapshotPathType() { return m_stype; };

    @Override
    public int getSerializedSize()
    {
        int size = super.getSerializedSize();
        size += 4 + m_path.length
                + 4 + m_stypeBytes.length
                + 4 + m_nonce.length
                + m_response.getSerializedSize();
        return size;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException
    {
        m_path = new byte[buf.getInt()];
        buf.get(m_path);
        m_stypeBytes = new byte[buf.getInt()];
        buf.get(m_stypeBytes);
        m_nonce = new byte[buf.getInt()];
        buf.get(m_nonce);
        m_response = PrivateVoltTableFactory.createVoltTableFromSharedBuffer(buf);
        m_stype = SnapshotPathType.valueOf(new String(m_stypeBytes, Charsets.UTF_8));
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.SNAPSHOT_CHECK_RESPONSE_ID);
        buf.putInt(m_path.length);
        buf.put(m_path);
        buf.putInt(m_stypeBytes.length);
        buf.put(m_stypeBytes);
        buf.putInt(m_nonce.length);
        buf.put(m_nonce);
        m_response.flattenToBuffer(buf);
    }
}
