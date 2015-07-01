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

package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientUtils;
import org.voltdb.common.Constants;
import org.voltdb.utils.SerializationHelper;

/**
 * Packages up the data to be sent back to the client as a stored
 * procedure response in one FastSerialziable object.
 *
 */
public class ClientResponseImpl implements ClientResponse, JSONString {
    private boolean setProperly = false;
    private byte status = 0;
    private String statusString = null;
    private byte encodedStatusString[];
    private byte appStatus = Byte.MIN_VALUE;
    private String appStatusString = null;
    private byte encodedAppStatusString[];
    private VoltTable[] results = new VoltTable[0];
    private Integer m_hash = null;

    private int clusterRoundTripTime = 0;
    private int clientRoundTripTime = 0;
    private long clientRoundTripTimeNanos = 0;

    // JSON KEYS FOR SERIALIZATION
    static final String JSON_STATUS_KEY = "status";
    static final String JSON_STATUSSTRING_KEY = "statusstring";
    static final String JSON_APPSTATUS_KEY = "appstatus";
    static final String JSON_APPSTATUSSTRING_KEY = "appstatusstring";
    static final String JSON_RESULTS_KEY = "results";
    static final String JSON_TYPE_KEY = "type";
    static final String JSON_EXCEPTION_KEY = "exception";

    // Error string returned when a replayed clog transaction is ignored or a replayed DR
    // transaction is a duplicate
    public static final String IGNORED_TRANSACTION = "Ignored replayed transaction";

    /** opaque data optionally provided by and returned to the client */
    private long clientHandle = -1;

    public ClientResponseImpl() {}

    /**
     * Used in the successful procedure invocation case.
     */
    public ClientResponseImpl(byte status, byte appStatus, String appStatusString, VoltTable[] results, String statusString) {
        this(status, appStatus, appStatusString, results, statusString, -1);
    }

    /**
     * Constructor used for tests and error responses.
     */
    public ClientResponseImpl(byte status, VoltTable[] results, String statusString) {
        this(status, ClientResponse.UNINITIALIZED_APP_STATUS_CODE, null, results, statusString, -1);
    }

    /**
     * Another constructor for test and error responses
     */
    public ClientResponseImpl(byte status, VoltTable[] results, String statusString, long handle) {
        this(status, ClientResponse.UNINITIALIZED_APP_STATUS_CODE, null, results, statusString, handle);
    }

    ClientResponseImpl(byte status, byte appStatus, String appStatusString, VoltTable[] results, String statusString, long handle) {
        this.appStatus = appStatus;
        this.appStatusString = appStatusString;
        setResults(status, results, statusString);
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

    public void setHash(Integer hash) {
        m_hash = hash;
    }

    @Override
    public byte getStatus() {
        return status;
    }

    @Override
    public VoltTable[] getResults() {
        return results;
    }

    @Override
    public String getStatusString() {
        return statusString;
    }

    public void setClientHandle(long aHandle) {
        clientHandle = aHandle;
    }

    public long getClientHandle() {
        return clientHandle;
    }

    public Integer getHash() {
        return m_hash;
    }

    public void initFromBuffer(ByteBuffer buf) throws IOException {
        buf.get();//Skip version byte
        clientHandle = buf.getLong();
        byte presentFields = buf.get();
        status = buf.get();
        if ((presentFields & (1 << 5)) != 0) {
            statusString = SerializationHelper.getString(buf);
        } else {
            statusString = null;
        }
        appStatus = buf.get();
        if ((presentFields & (1 << 7)) != 0) {
            appStatusString = SerializationHelper.getString(buf);
        } else {
            appStatusString = null;
        }
        clusterRoundTripTime = buf.getInt();
        if ((presentFields & (1 << 6)) != 0) {
            throw new RuntimeException("Use of deprecated exception in Client Response serialization.");
        }
        if ((presentFields & (1 << 4)) != 0) {
            m_hash = buf.getInt();
        } else {
            m_hash = null;
        }
        int tableCount = buf.getShort();
        results = new VoltTable[tableCount];
        for (int i = 0; i < tableCount; i++) {
            int tableSize = buf.getInt();
            final int originalLimit = buf.limit();
            buf.limit(buf.position() + tableSize);
            final ByteBuffer slice = buf.slice();
            buf.position(buf.position() + tableSize);
            buf.limit(originalLimit);
            results[i] = new VoltTable(slice, false);
        }
        setProperly = true;
    }

    public int getSerializedSize() {
        int msgsize = 1 // version
            + 8 // clientHandle
            + 1 // present fields
            + 1 // status
            + 1 // app status
            + 4 // cluster roundtrip time
            + 2; // number of result tables

        if (appStatusString != null) {
            encodedAppStatusString = appStatusString.getBytes(Constants.UTF8ENCODING);
            msgsize += encodedAppStatusString.length + 4;
        }
        if (statusString != null) {
            encodedStatusString = statusString.getBytes(Constants.UTF8ENCODING);
            msgsize += encodedStatusString.length + 4;
        }
        if (m_hash != null) {
            msgsize += 4;
        }
        for (VoltTable vt : results) {
            msgsize += vt.getSerializedSize();
        }

        return msgsize;
    }

    /**
     * @return buf to allow call chaining.
     */
    public ByteBuffer flattenToBuffer(ByteBuffer buf) {
        assert setProperly;
        buf.put((byte)0); //version
        buf.putLong(clientHandle);
        byte presentFields = 0;
        if (appStatusString != null) {
            presentFields |= 1 << 7;
        }
        if (statusString != null) {
            presentFields |= 1 << 5;
        }
        if (m_hash != null) {
            presentFields |= 1 << 4;
        }
        buf.put(presentFields);
        buf.put(status);
        if (statusString != null) {
            buf.putInt(encodedStatusString.length);
            buf.put(encodedStatusString);
        }
        buf.put(appStatus);
        if (appStatusString != null) {
            buf.putInt(encodedAppStatusString.length);
            buf.put(encodedAppStatusString);
        }
        buf.putInt(clusterRoundTripTime);
        if (m_hash != null) {
            buf.putInt(m_hash.intValue());
        }
        buf.putShort((short)results.length);
        for (VoltTable vt : results)
        {
            vt.flattenToBuffer(buf);
        }
        return buf;
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

    @Override
    public long getClientRoundtripNanos() {
        return clientRoundTripTimeNanos;
    }

    public void setClientRoundtrip(long timeNanos) {
        clientRoundTripTimeNanos = timeNanos;
        clientRoundTripTime = (int)TimeUnit.NANOSECONDS.toMillis(timeNanos);
    }

    @Override
    public byte getAppStatus() {
        return appStatus;
    }

    @Override
    public String getAppStatusString() {
        return appStatusString;
    }

    public boolean isTransactionallySuccessful() {
        return isTransactionallySuccessful(status);
    }

    public static boolean isTransactionallySuccessful(byte status) {
        return (status == SUCCESS) || (status == OPERATIONAL_FAILURE);
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

    /**
     * @return MD5 hash as int of the tables in the result. Only hashes first bits of big results.
     */
    public int getHashOfTableResults() {
        try {
            long cheesyChecksum = 0;
            for (int i = 0; i < results.length; ++i) {
                cheesyChecksum += ClientUtils.cheesyBufferCheckSum(results[i].m_buffer);
            }
            return (int)cheesyChecksum;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * Take the perfectly good results and convert them to a single long value
     * that stores the hash for determinism.
     *
     * This presumes the DR agent has no need for results. This probably saves
     * some small amount of bandwidth. The other proposed idea was using the status
     * string to hold the sql hash. Tossup... this seemed slightly more performant,
     * but also a bit icky.
     */
    public void convertResultsToHashForDeterminism() {
        int hash = m_hash == null ? 0 : m_hash;

        VoltTable t = new VoltTable(new VoltTable.ColumnInfo("", VoltType.INTEGER));
        t.addRow(hash);
        results = new VoltTable[] { t };
    }

    public void dropResultTable() {
        results = new VoltTable[] {};
    }
}
