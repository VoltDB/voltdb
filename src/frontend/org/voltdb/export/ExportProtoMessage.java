/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.export;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;

import com.google.common.base.Charsets;



/**
 * Message exchanged during execution of poll/ack protocol
 */
public class ExportProtoMessage
{
    public static final short kOpen           = 1 << 0;
    public static final short kOpenResponse   = 1 << 1;
    public static final short kPoll           = 1 << 2;
    public static final short kPollResponse   = 1 << 3;
    public static final short kAck            = 1 << 4;
    public static final short kClose          = 1 << 5;
    public static final short kError          = 1 << 6;
    public static final short kSync           = 1 << 7;
    private long m_generation;

    public boolean isOpen()         {return (m_type & kOpen) != 0;}
    public boolean isOpenResponse() {return (m_type & kOpenResponse) != 0;}
    public boolean isPoll()         {return (m_type & kPoll) != 0;}
    public boolean isPollResponse() {return (m_type & kPollResponse) != 0;}
    public boolean isAck()          {return (m_type & kAck) != 0;}
    public boolean isClose()        {return (m_type & kClose) != 0;}
    public boolean isError()        {return (m_type & kError) != 0;}
    public boolean isSync()         {return (m_type & kSync) != 0;}

    /**
     * The Export data source metadata returned in a kOpenResponse message.
     */
    static public class AdvertisedDataSource {
        final public int partitionId;
        final public String signature;
        final public String tableName;
        final public long m_generation;
        final public long systemStartTimestamp;
        final public ArrayList<String> columnNames = new ArrayList<String>();
        final public ArrayList<VoltType> columnTypes = new ArrayList<VoltType>();
        final public List<Integer> columnLengths = new ArrayList<Integer>();

        @Override
        public int hashCode() {
            return (((int)m_generation) + ((int)(m_generation >> 32))) + partitionId + signature.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof AdvertisedDataSource) {
                AdvertisedDataSource other = (AdvertisedDataSource)o;
                if (other.m_generation == m_generation &&
                        other.signature.equals(signature) &&
                        other.partitionId == partitionId) {
                    return true;
                }
            }
            return false;
        }

        public AdvertisedDataSource(int p_id, String t_signature, String t_name,
                                    long systemStartTimestamp,
                                    long generation,
                                    ArrayList<String> names,
                                    ArrayList<VoltType> types,
                                    List<Integer> lengths)
        {
            partitionId = p_id;
            signature = t_signature;
            tableName = t_name;
            m_generation = generation;
            this.systemStartTimestamp = systemStartTimestamp;

            // null checks are for happy-making test time
            if (names != null)
                columnNames.addAll(names);
            if (types != null)
                columnTypes.addAll(types);
            if (lengths != null) {
                columnLengths.addAll(lengths);
            }
        }

        public VoltType columnType(int index) {
            return columnTypes.get(index);
        }

        public String columnName(int index) {
            return columnNames.get(index);
        }

        public Integer columnLength(int index) {
            return columnLengths.get(index);
        }

        @Override
        public String toString() {
            return "Generation: " + m_generation + " Table: " + tableName + " partition " + partitionId + " signature " + signature;
        }
    }

    /**
     * Called to produce an Export protocol message from a FastDeserializer.
     * Note that this expects the length preceding value was already
     * read (probably how the buffer length was originally determined).
     * @param fds
     * @return new ExportProtoMessage from deserializer contents
     * @throws IOException
     */
    public static ExportProtoMessage readExternal(FastDeserializer fds)
    throws IOException
    {
        ExportProtoMessage m = new ExportProtoMessage( 0, 0, null);
        m.m_version = fds.readShort();
        m.m_type = fds.readShort();
        m.m_generation = fds.readLong();
        m.m_partitionId = fds.readInt();
        m.m_signature = fds.readString();
        m.m_offset = fds.readLong();
        // if no data is remaining, m_data will have 0 capacity.
        m.m_data = fds.remainder();
        return m;
    }

    public ExportProtoMessage(long generation, int partitionId, String signature) {
        m_generation = generation;
        m_partitionId = partitionId;
        m_signature = signature;
        if (m_signature == null) {
            m_signatureBytes = new byte[0];
        }
    }

    public String messageTypesAsString() {
        String retval = "|";
        if (isOpen()) retval += "OPEN|";
        if (isOpenResponse()) retval += "OPEN_REPONSE|";
        if (isPoll()) retval += "POLL|";
        if (isPollResponse()) retval += "POLL_RESPONSE|";
        if (isAck()) retval += "ACK|";
        if (isClose()) retval += "CLOSE|";
        if (isError()) retval += "ERROR|";
        if (isSync()) retval += "SYNC|";
        return retval;
    }

    public ByteBuffer toBuffer() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4 + serializableBytes());
        flattenToBuffer(buf);
        buf.flip();
        return buf;
    }

    private byte m_signatureBytes[] = null;

    /**
     * Total bytes that would be used if serialized in its current state.
     * Does not include 4 byte length prefix that flattenToBuffer adds
     * @return byte count.
     */
    public int serializableBytes() {
        if (m_signatureBytes == null) {
            try {
                m_signatureBytes = m_signature.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return FIXED_PAYLOAD_LENGTH + (m_data != null ? m_data.remaining() : 0) + m_signatureBytes.length;
    }

    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        if (m_signatureBytes == null) {
            try {
                m_signatureBytes = m_signature.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        // write the length first. then the payload.
        buf.putInt(buf.capacity() - 4);
        buf.putShort(m_version);
        buf.putShort(m_type);
        buf.putLong(m_generation);
        buf.putInt(m_partitionId);
        FastSerializer.writeString(m_signatureBytes, buf);
        buf.putLong(m_offset);
        if (m_data != null) {
            buf.put(m_data);
            // write advances m_data's position.
            m_data.flip();
        }
    }

    public short version() {
        return m_version;
    }

    public ExportProtoMessage error() {
        m_type |= kError;
        return this;
    }

    public ExportProtoMessage open() {
        m_type |= kOpen;
        return this;
    }

    public ExportProtoMessage openResponse(ByteBuffer bb) {
        m_type |= kOpenResponse;
        m_data = bb;
        return this;
    }

    public ExportProtoMessage poll() {
        m_type |= kPoll;
        return this;
    }

    public ExportProtoMessage pollResponse(long offset, ByteBuffer bb) {
        m_type |= kPollResponse;
        m_data = bb;
        m_offset = offset;
        return this;
    }

    public ExportProtoMessage ack(long ackedOffset) {
        m_type |= kAck;
        m_offset = ackedOffset;
        return this;
    }

    public ExportProtoMessage close() {
        m_type |= kClose;
        return this;
    }

    public ByteBuffer getData() {
        return m_data;
    }

    public int getPartitionId() {
        return m_partitionId;
    }

    public String getSignature() {
        return m_signature;
    }

    public long getAckOffset() {
        return m_offset;
    }

    public long getGeneration() {
        return m_generation;
    }

    /**
     * Provide a simple accessor to read the list of advertised data sources
     * returned as the payload to an open response.
     * @return List of data sources advertised with an open response.
     * @throws IOException
     */
    public Pair<ArrayList<AdvertisedDataSource>,ArrayList<String>> getAdvertisedDataSourcesAndNodes()
    throws IOException
    {
        if (!isOpenResponse()) {
            return null;
        }

        ArrayList<AdvertisedDataSource> sources = new ArrayList<AdvertisedDataSource>();
        ArrayList<String> nodes = new ArrayList<String>();
        Pair<ArrayList<AdvertisedDataSource>,ArrayList<String>> retval =
            new Pair<ArrayList<AdvertisedDataSource>,ArrayList<String>>(sources, nodes);

        byte stringBytes[] = new byte[m_data.remaining()];
        m_data.get(stringBytes);
        try {
            JSONObject jsObj = new JSONObject(new String(stringBytes, Charsets.UTF_8));

            JSONArray sourcesArray = jsObj.getJSONArray("sources");
            for (int i=0; i < sourcesArray.length(); i++) {
                JSONObject source = sourcesArray.getJSONObject(i);
                long version = source.getLong("adVersion");
                if (version != 0) {
                    throw new IOException("Unexpected ad version " + version);
                }
                JSONArray columns = source.getJSONArray("columns");
                ArrayList<VoltType> types = new ArrayList<VoltType>(columns.length());
                ArrayList<String> names = new ArrayList<String>(columns.length());
                ArrayList<Integer> lengths = new ArrayList<Integer>(columns.length());

                long generation = source.getLong("generation");
                int p_id = source.getInt("partitionId");
                String t_signature = source.getString("signature");
                String t_name = source.getString("tableName");
                long sysStartTimestamp = source.getLong("startTime");

                for (int jj = 0; jj < columns.length(); jj++) {
                    JSONObject column = columns.getJSONObject(jj);
                    names.add(column.getString("name"));
                    types.add(VoltType.get((byte)column.getInt("type")));
                    lengths.add(column.getInt("length"));
                }
                sources.add(new AdvertisedDataSource(p_id, t_signature, t_name,
                                                     sysStartTimestamp, generation, names, types, lengths));
            }

            // deserialize the list of running hosts
            JSONArray hostsArray = jsObj.getJSONArray("clusterMetadata");
            for (int i=0; i < hostsArray.length(); i++) {
                String hostname = hostsArray.getString(i);

                nodes.add(hostname);
            }
        } catch (JSONException e) {
            throw new IOException(e);
        }

        return retval;
    }

    @Override
    public String toString() {
        String s = "ExportProtoMessage: type(" + messageTypesAsString() + ") offset(" +
                m_offset + ") partitionId(" + m_partitionId +
                ") signature(" + m_signature +")" + " serializableBytes(" +
                serializableBytes() + ")";
        if (m_data != null) {
            s = s + " payloadBytes(" + m_data.remaining() +")";
        }
        else {
            s = s + " no payload.";
        }
        return s;
    }

    // calculate bytes of fixed payload
    private static int FIXED_PAYLOAD_LENGTH =
        (Short.SIZE/8 * 2) + (Integer.SIZE/8 * 2) + (Long.SIZE/8 * 2);

    // message version.
    short m_version = 2;

    // bitmask of protocol actions in this message.
    short m_type = 0;

    // partition id for this ack or poll
    int m_partitionId = -1;

    // the table id for this ack or poll
    String m_signature = null;

    // if kAck, the offset being acked.
    // if kPollResponse, the offset of the last byte returned.
    long m_offset = -1;

    // poll payload data
    ByteBuffer m_data;
}