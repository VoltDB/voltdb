/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb.export;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.voltdb.VoltType;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;



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
        final private byte m_isReplicated;
        final private int m_partitionId;
        final private long m_tableId;
        final private String m_tableName;

        private ArrayList<String> m_columnNames = new ArrayList<String>();

        private ArrayList<VoltType> m_columnTypes = new ArrayList<VoltType>();

        public AdvertisedDataSource(byte isReplicated,
                                    int p_id, long t_id, String t_name,
                                    ArrayList<String> names,
                                    ArrayList<VoltType> types)
        {
            m_isReplicated = isReplicated;
            m_partitionId = p_id;
            m_tableId = t_id;
            m_tableName = t_name;
            m_columnNames = names;
            m_columnTypes = types;
        }

        public boolean isReplicated()
        {
            return (m_isReplicated != 0);
        }

        public int partitionId() {
            return m_partitionId;
        }

        public long tableId() {
            return m_tableId;
        }

        public VoltType columnType(int index) {
            return m_columnTypes.get(index);
        }

        public ArrayList<VoltType> columnTypes()
        {
            return m_columnTypes;
        }

        public String columnName(int index) {
            return m_columnNames.get(index);
        }

        public ArrayList<String> columnNames()
        {
            return m_columnNames;
        }

        public String tableName() {
            return m_tableName;
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
        ExportProtoMessage m = new ExportProtoMessage(0, 0);
        m.m_version = fds.readShort();
        m.m_type = fds.readShort();
        m.m_partitionId = fds.readInt();
        m.m_tableId = fds.readLong();
        m.m_offset = fds.readLong();
        // if no data is remaining, m_data will have 0 capacity.
        m.m_data = fds.remainder();
        return m;
    }

    public ExportProtoMessage(int partitionId, long tableId) {
        m_partitionId = partitionId;
        m_tableId = tableId;
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
        FastSerializer fs = new FastSerializer();
        writeToFastSerializer(fs);
        return fs.getBuffer();
    }

    /**
     * Total bytes that would be used if serialized in its current state.
     * Does not include the 4 byte length prefix!
     * @return byte count.
     */
    public int serializableBytes() {
        return FIXED_PAYLOAD_LENGTH + (m_data != null ? m_data.remaining() : 0);
    }

    public void writeToFastSerializer(FastSerializer fs) throws IOException
    {
        // write the length first. then the payload.
        fs.writeInt(serializableBytes());
        fs.writeShort(m_version);
        fs.writeShort(m_type);
        fs.writeInt(m_partitionId);
        fs.writeLong(m_tableId);
        fs.writeLong(m_offset);
        if (m_data != null) {
            fs.write(m_data);
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

    public long getTableId() {
        return m_tableId;
    }

    public long getAckOffset() {
        return m_offset;
    }

    /**
     * Provide a simple accessor to read the list of advertised data sources
     * returned as the payload to an open response.
     * @return List of data sources advertised with an open response.
     * @throws IOException
     */
    public ArrayList<AdvertisedDataSource> getAdvertisedDataSources()
    throws IOException
    {
        if (!isOpenResponse()) {
            return null;
        }

        ArrayList<AdvertisedDataSource> result =
            new ArrayList<AdvertisedDataSource>();

        FastDeserializer fds = new FastDeserializer(m_data);

        int count = m_data.getInt();
        for (int i=0; i < count; i++) {
            ArrayList<VoltType> types = new ArrayList<VoltType>();
            ArrayList<String> names = new ArrayList<String>();

            byte is_replicated = fds.readByte();
            int p_id = fds.readInt();
            long t_id = fds.readLong();
            String t_name = fds.readString();
            int colcnt = fds.readInt();
            for (int jj = 0; jj < colcnt; jj++) {
                names.add(fds.readString());
                types.add(VoltType.get((byte)fds.readInt()));
            }
            result.add(new AdvertisedDataSource(is_replicated, p_id, t_id,
                                                t_name, names, types));
        }
        return result;
    }

    @Override
    public String toString() {
        String s = "ExportProtoMessage: type(" + m_type + ") offset(" +
                m_offset + ") partitionId(" + m_partitionId +
                ") tableId(" + m_tableId +")" + " serializableBytes(" +
                serializableBytes() + ")";
        if (m_data != null) {
            s = s + " payloadBytes(" + m_data.remaining() +")";
        }
        else {
            s = s + " no payoad.";
        }
        return s;
    }

    // calculate bytes of fixed payload
    private static int FIXED_PAYLOAD_LENGTH =
        (Short.SIZE/8 * 2) + Integer.SIZE/8 + (Long.SIZE/8 * 2);

    // message version. Currently all messages are version 1.
    short m_version = 1;

    // bitmask of protocol actions in this message.
    short m_type = 0;

    // partition id for this ack or poll
    int m_partitionId = -1;

    // the table id for this ack or poll
    long m_tableId = -1;

    // if kAck, the offset being acked.
    // if kPollResponse, the offset of the last byte returned.
    long m_offset = -1;

    // poll payload data
    ByteBuffer m_data;
}