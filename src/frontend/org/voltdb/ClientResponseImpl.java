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
import org.voltdb.client.ProcedureCallback;
import org.voltdb.VoltTable;
import org.voltdb.StoredProcedureInvocation;
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

    /**
     * Time the client queued this transaction
     */
    private long clientQueueTime = -1;

    /**
     * Time the ClientInterface on some host read the transaction
     */
    private long CIAcceptTime = -1;

    /**
     * Time the foreign host that would execute the transaction
     * received the transaction and delivered it into a local priority queue
     */
    private long FHReceiveTime = -1;

    /**
     * Time the foreign host took the transaction out of the local priority queue
     * executed it, and queued a response for writing back to the initiator
     */
    private long FHResponseTime = -1;

    /**
     * Time the initiator received the response for the client from the
     * foreign host and queued it for writing back to the client
     */
    private long initiatorReceiveTime = -1;

    private SerializableException m_exception = null;


    /** opaque data optionally provided by and returned to the client */
    private long clientHandle = -1;

    public long clientQueueTime() { return clientQueueTime; }
    public long CIAcceptTime() { return CIAcceptTime; }
    public long FHReceiveTime() { return FHReceiveTime; }
    public long FHResponseTime() { return FHResponseTime; }
    public long initiatorReceiveTime() { return initiatorReceiveTime; }
    public void setTimingInfo(StoredProcedureInvocation spi,  long FHResponseTime ) {
        this.clientQueueTime = spi.clientQueueTime();
        this.CIAcceptTime = spi.CIAcceptTime();
        this.FHReceiveTime = spi.FHReceiveTime();
        this.FHResponseTime = FHResponseTime;
    }

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
        status = (byte) in.readByte();
        if (ProcedureCallback.measureLatency){
            clientQueueTime = in.readLong();
            CIAcceptTime = in.readLong();
            FHReceiveTime = in.readLong();
            FHResponseTime = in.readLong();
            initiatorReceiveTime = in.readLong();
            if (clientQueueTime != -1) {
                if (initiatorReceiveTime == -1) {
                    initiatorReceiveTime = System.currentTimeMillis();
                }
            }
        }
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
        if (ProcedureCallback.measureLatency) {
            out.writeLong(clientQueueTime);
            out.writeLong(CIAcceptTime);
            out.writeLong(FHReceiveTime);
            out.writeLong(FHResponseTime);
            out.writeLong(initiatorReceiveTime);
        }
        if (m_exception != null) {
            final ByteBuffer b = ByteBuffer.allocate(m_exception.getSerializedSize());
            m_exception.serializeToBuffer(b);
            out.write(b.array());
        } else {
            out.writeShort(0);//Length zero exception
        }
        out.writeArray(results);
        out.writeString(extra);
        out.writeLong(clientHandle);
    }

}
