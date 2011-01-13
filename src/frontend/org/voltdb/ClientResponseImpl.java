/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.client.ClientResponse;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;

/**
 * Packages up the data to be sent back to the client as a stored
 * procedure response in one FastSerialziable object.
 *
 */
public class ClientResponseImpl implements FastSerializable, ClientResponse, JSONString {
    private boolean setProperly = false;
    private byte status = 0;
    private String statusString = null;
    private byte appStatus = Byte.MIN_VALUE;
    private String appStatusString = null;
    private VoltTable[] results = new VoltTable[0];

    private int clusterRoundTripTime = 0;
    private int clientRoundTripTime = 0;
    private SerializableException m_exception = null;

    // JSON KEYS FOR SERIALIZATION
    static final String JSON_STATUS_KEY = "status";
    static final String JSON_STATUSSTRING_KEY = "statusstring";
    static final String JSON_APPSTATUS_KEY = "appstatus";
    static final String JSON_APPSTATUSSTRING_KEY = "appstatusstring";
    static final String JSON_RESULTS_KEY = "results";
    static final String JSON_TYPE_KEY = "type";
    static final String JSON_EXCEPTION_KEY = "exception";

    /** opaque data optionally provided by and returned to the client */
    private long clientHandle = -1;

    public ClientResponseImpl() {}

    /**
     * Used in the successful procedure invocation case.
     */
    public ClientResponseImpl(byte status, byte appStatus, String appStatusString, VoltTable[] results, String statusString) {
        this(status, appStatus, appStatusString, results, statusString, -1, null);
    }

    /**
     * Constructor used for tests and error responses.
     */
    public ClientResponseImpl(byte status, VoltTable[] results, String statusString) {
        this(status, Byte.MIN_VALUE, null, results, statusString, -1, null);
    }

    /**
     * Another constructor for test and error responses
     */
    public ClientResponseImpl(byte status, VoltTable[] results, String statusString, long handle) {
        this(status, Byte.MIN_VALUE, null, results, statusString, handle, null);
    }

    /**
     * And another....
     * @param status
     * @param results
     * @param e
     */
    public ClientResponseImpl(byte status, VoltTable[] results, String statusString, SerializableException e) {
        this(status, Byte.MIN_VALUE, null, results, statusString, -1, e);
    }

    /**
     * Use this when generating an error response in VoltProcedure
     * @param status
     * @param results
     * @param extra
     * @param e
     */
    public ClientResponseImpl(byte status, byte appStatus, String appStatusString, VoltTable results[], String extra, SerializableException e) {
        this(status, appStatus, appStatusString, results, extra, -1, e);
    }

    ClientResponseImpl(byte status, byte appStatus, String appStatusString, VoltTable[] results, String statusString, long handle, SerializableException e) {
        this.appStatus = appStatus;
        this.appStatusString = appStatusString;
        setResults(status, results, statusString, e);
        clientHandle = handle;
    }

    private void setResults(byte status, VoltTable[] results, String statusString) {
        assert results != null;
        for (VoltTable result : results) {
            // null values are not permitted in results. If there is one, it will cause an
            // exception in writeExternal. This throws the exception sooner.
            assert result != null;
        }

        this.status = status;
        this.results = results;
        this.statusString = statusString;
        this.setProperly = true;
    }

    private void setResults(byte status, VoltTable[] results, String extra, SerializableException e) {
        m_exception = e;
        setResults(status, results, extra);
    }

    public byte getStatus() {
        return status;
    }

    public VoltTable[] getResults() {
        return results;
    }

    public String getStatusString() {
        return statusString;
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
        clientHandle = in.readLong();
        byte presentFields = in.readByte();
        status = in.readByte();
        if ((presentFields & (1 << 5)) != 0) {
            statusString = in.readString();
        } else {
            statusString = null;
        }
        appStatus = in.readByte();
        if ((presentFields & (1 << 7)) != 0) {
            appStatusString = in.readString();
        } else {
            appStatusString = null;
        }
        clusterRoundTripTime = in.readInt();
        if ((presentFields & (1 << 6)) != 0) {
            m_exception = SerializableException.deserializeFromBuffer(in.buffer());
        } else {
            m_exception = null;
        }
        results = (VoltTable[]) in.readArray(VoltTable.class);
        setProperly = true;
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        assert setProperly;
        out.writeByte(0);//version
        out.writeLong(clientHandle);
        byte presentFields = 0;
        if (appStatusString != null) {
            presentFields |= 1 << 7;
        }
        if (m_exception != null) {
            presentFields |= 1 << 6;
        }
        if (statusString != null) {
            presentFields |= 1 << 5;
        }
        out.writeByte(presentFields);
        out.write(status);
        if (statusString != null) {
            out.writeString(statusString);
        }
        out.write(appStatus);
        if (appStatusString != null) {
            out.writeString(appStatusString);
        }
        out.writeInt(clusterRoundTripTime);
        if (m_exception != null) {
            final ByteBuffer b = ByteBuffer.allocate(m_exception.getSerializedSize());
            m_exception.serializeToBuffer(b);
            out.write(b.array());
        }
        out.writeArray(results);
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

    @Override
    public byte getAppStatus() {
        return appStatus;
    }

    @Override
    public String getAppStatusString() {
        return appStatusString;
    }

    @Override
    public String toJSONString() {
        JSONStringer js = new JSONStringer();
        try {
            js.object();

            js.key(JSON_STATUS_KEY);
            js.value(status);
            js.key(JSON_APPSTATUS_KEY);
            js.value(appStatus);
            js.key(JSON_STATUSSTRING_KEY);
            js.value(statusString);
            js.key(JSON_APPSTATUSSTRING_KEY);
            js.value(appStatusString);
            js.key(JSON_EXCEPTION_KEY);
            if (m_exception != null) {
                js.value(m_exception);
            }
            else {
                js.value(null);
            }
            js.key(JSON_RESULTS_KEY);
            js.array();
            for (VoltTable o : results) {
                js.value(o);
            }
            js.endArray();

            js.endObject();
        }
        catch (JSONException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to serialized a parameter set to JSON.", e);
        }
        return js.toString();
    }
}
