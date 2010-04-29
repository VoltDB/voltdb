/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.messaging.*;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;

/**
 * Packages up the data to be sent back to the client as a stored
 * procedure response in one FastSerialziable object.
 *
 */
public class ClientResponseImpl implements FastSerializable, ClientResponse {
    private boolean setProperly = false;
    private byte status = 0;
    private VoltTable[] results = new VoltTable[0];
    private String extra = null;
    private int clusterRoundTripTime = 0;
    private int clientRoundTripTime = 0;
    private SerializableException m_exception = null;


    /** opaque data optionally provided by and returned to the client */
    private long clientHandle = -1;

    public ClientResponseImpl() {

    }

    public ClientResponseImpl(byte status, VoltTable[] results, String extra) {
        this(status, results, extra, -1, null);
    }

    public ClientResponseImpl(byte status, VoltTable[] results, String extra, long handle) {
        this(status, results, extra, handle, null);
    }

    public ClientResponseImpl(byte status, VoltTable[] results, String extra, SerializableException e) {
        this(status, results, extra, -1, e);
    }

    ClientResponseImpl(byte status, VoltTable[] results, String extra, long handle, SerializableException e) {
        setResults(status, results, extra, e);
        clientHandle = handle;
    }

    public void setResults(byte status, VoltTable[] results, String extra) {
        assert results != null;
        for (VoltTable result : results) {
            // null values are not permitted in results. If there is one, it will cause an
            // exception in writeExternal. This throws the exception sooner.
            assert result != null;
        }

        this.status = status;
        this.results = results;
        this.extra = extra;
        this.setProperly = true;
    }

    public void setResults(byte status, VoltTable[] results, String extra, SerializableException e) {
        m_exception = e;
        setResults(status, results, extra);
    }

    public byte getStatus() {
        return status;
    }

    public VoltTable[] getResults() {
        return results;
    }

    public String getExtra() {
        return extra;
    }

    public void setClientHandle(long aHandle) {
        clientHandle = aHandle;
    }

    public long getClientHandle() {
        return clientHandle;
    }

    public SerializableException getException() {
        return m_exception;
    }

    @Override
    public void readExternal(FastDeserializer in) throws IOException {
        in.readByte();//Skip version byte
        status = in.readByte();
        clusterRoundTripTime = in.readInt();
        m_exception = SerializableException.deserializeFromBuffer(in.buffer());
        results = (VoltTable[]) in.readArray(VoltTable.class);
        extra = in.readString();
        clientHandle = in.readLong();
        setProperly = true;
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        assert setProperly;
        out.writeByte(0);//version
        out.write(status);
        out.writeInt(clusterRoundTripTime);
        if (m_exception != null) {
            final ByteBuffer b = ByteBuffer.allocate(m_exception.getSerializedSize());
            m_exception.serializeToBuffer(b);
            out.write(b.array());
        } else {
            out.writeInt(0);//Length zero exception
        }
        out.writeArray(results);
        out.writeString(extra);
        out.writeLong(clientHandle);
    }

    @Override
    public int getClusterRoundtrip() {
        return clusterRoundTripTime;
    }

    public void setClusterRoundtrip(int time) {
        clusterRoundTripTime = time;
    }

    @Override
    public int getClientRoundtrip() {
        return clientRoundTripTime;
    }

    public void setClientRoundtrip(int time) {
        clientRoundTripTime = time;
    }

}
