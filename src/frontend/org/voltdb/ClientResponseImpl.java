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

package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.Pair;
import org.voltdb.client.ClientResponse;
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
    private int[] m_hashes = null;

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

    public ClientResponseImpl(byte status, byte appStatus, String appStatusString, VoltTable[] results, String statusString, long handle) {
        this.appStatus = appStatus;
        this.appStatusString = appStatusString;
        setResults(status, results, statusString);
        clientHandle = handle;
    }

    public Pair<Long, byte[]> getMispartitionedResult() {
        if (results.length != 1 || !results[0].advanceRow()) {
            throw new IllegalArgumentException("No hashinator config in result");
        }
        if (results[0].getColumnCount() != 2 ||
            results[0].getColumnType(0) != VoltType.BIGINT ||
            results[0].getColumnType(1) != VoltType.VARBINARY) {
            throw new IllegalArgumentException("Malformed hashinator result, expecting two columns of types INTEGER and VARBINARY");
        }
        final Pair<Long, byte[]> hashinator = Pair.of(results[0].getLong("HASHINATOR_VERSION"),
                                                      results[0].getVarbinary("HASHINATOR_CONFIG_BYTES"));
        results[0].resetRowPosition();
        return hashinator;
    }

    public void setMispartitionedResult(Pair<Long, byte[]> hashinatorConfig) {
        VoltTable vt = new VoltTable(
                new VoltTable.ColumnInfo("HASHINATOR_VERSION", VoltType.BIGINT),
                new VoltTable.ColumnInfo("HASHINATOR_CONFIG_BYTES", VoltType.VARBINARY));
        vt.addRow(hashinatorConfig.getFirst(), hashinatorConfig.getSecond());
        setResults(ClientResponse.TXN_MISPARTITIONED, new VoltTable[] { vt }, "Transaction mispartitioned");
    }

    private void setResults(byte status, VoltTable[] results, String statusString) {
        setResultTables(results);

        this.status = status;
        this.statusString = statusString;
        this.setProperly = true;
    }

    public void setResultTables(VoltTable[] results) {
        assert results != null;
        for (VoltTable result : results) {
            // null values are not permitted in results. If there is one, it will cause an
            // exception in writeExternal. This throws the exception sooner.
            assert result != null;
        }
        this.results = results;
    }

    public void setHashes(int[] hashes) {
        m_hashes = hashes;
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

    public void setAppStatusString(String appStatusString) {
        this.appStatusString = appStatusString;
    }

    public long getClientHandle() {
        return clientHandle;
    }

    public int[] getHashes() {
        return m_hashes;
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
            int hashArrayLen = buf.getShort();
            m_hashes = new int[hashArrayLen];
            for (int i = 0; i < hashArrayLen; ++i) {
                m_hashes[i] = buf.getInt();
            }
        } else {
            m_hashes = null;
        }
        int tableCount = buf.getShort();
        if (tableCount < 0) {
            throw new IOException("Table count is negative: " + tableCount);
        }
        setProperly = true;
        int count = 0;
        try {
            results = new VoltTable[tableCount];
            for (int i = 0; i < tableCount; i++) {
                int tableSize = buf.getInt();
                final int originalLimit = buf.limit();
                buf.limit(buf.position() + tableSize);
                final ByteBuffer slice = buf.slice();
                buf.position(buf.position() + tableSize);
                buf.limit(originalLimit);
                results[i] = new VoltTable(slice, false);
                count = i;
            }
        } catch (Throwable t) {
            StringBuilder builder = new StringBuilder("Unexpected errors in response. status: ");
            builder.append(getStatus());
            builder.append(statusString == null ? "" : statusString);
            builder.append(" appStatus:");
            builder.append(getAppStatus());
            builder.append(getAppStatusString() == null ? "" : " " + getAppStatusString());
            builder.append(" table count:");
            builder.append(tableCount + "\n");
            while (count > 0) {
                builder.append(results[count--].toFormattedString());
            }
            dropResultTable();
            throw new IOException(builder.toString());
        }
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
        if (m_hashes != null) {
            msgsize += 2; // short array len
            msgsize += m_hashes.length * 4; // array of ints
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
        if (m_hashes != null) {
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
        if (m_hashes != null) {
            assert(m_hashes.length <= Short.MAX_VALUE) : "CRI hash array length overflow";
            buf.putShort((short) m_hashes.length);
            for (int hash : m_hashes) {
                buf.putInt(hash);
            }
        }
        buf.putShort((short) results.length);
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

    public String toStatusJSONString() {
        JSONStringer js = new JSONStringer();
        try {
            js.object();
            js.keySymbolValuePair(JSON_STATUS_KEY, status);
            js.keySymbolValuePair(JSON_APPSTATUS_KEY, appStatus);
            js.keySymbolValuePair(JSON_STATUSSTRING_KEY, statusString);
            js.keySymbolValuePair(JSON_APPSTATUSSTRING_KEY, appStatusString);
            js.endObject();
        }
        catch (JSONException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to serialize a parameter set to JSON.", e);
        }
        return js.toString();
    }


    @Override
    public String toJSONString() {
        JSONStringer js = new JSONStringer();
        try {
            js.object();

            js.keySymbolValuePair(JSON_STATUS_KEY, status);
            js.keySymbolValuePair(JSON_APPSTATUS_KEY, appStatus);
            js.keySymbolValuePair(JSON_STATUSSTRING_KEY, statusString);
            js.keySymbolValuePair(JSON_APPSTATUSSTRING_KEY, appStatusString);
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
            throw new RuntimeException("Failed to serialize a parameter set to JSON.", e);
        }
        return js.toString();
    }

    public void dropResultTable() {
        results = new VoltTable[] {};
    }

    public static boolean aborted(byte status) {
        return status == USER_ABORT || status == COMPOUND_PROC_USER_ABORT;
    }

    public boolean aborted() {
        return aborted(status);
    }

    public static boolean failed(byte status) {
        return status != SUCCESS && !aborted(status);
    }

    public boolean failed() {
        return failed(status);
    }
}
