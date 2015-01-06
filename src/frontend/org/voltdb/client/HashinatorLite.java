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

package org.voltdb.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.InflaterOutputStream;

import org.apache.cassandra_voltpatches.MurmurHash3;
import org.voltcore.utils.Bits;
import org.voltcore.utils.Pair;
import org.voltdb.ParameterConverter;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.common.Constants;

import com.google_voltpatches.common.base.Preconditions;

/**
 * HashinatorLite is a read-only, simplifed version of the Hashinators that can be
 * used by the client without introducing lots of VoltDB dependencies.
 *
 * It currently has a bit more duplicated code than we'd like, but it's nice and
 * standalone.
 *
 */
public class HashinatorLite {

    public static enum HashinatorLiteType {
        LEGACY(0),
        ELASTIC(1);

        public final int typeId;

        private HashinatorLiteType(int typeId) {
            this.typeId = typeId;
        }
    };

    public static byte[] getLegacyConfigureBytes(int catalogPartitionCount) {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(catalogPartitionCount);
        return buf.array();
    }

    //Values for legacy
    private int catalogPartitionCount;

    //Values for Elastic
    /*
     * Pointer to an array of integers containing the tokens and partitions. Even values are tokens and odd values
     * are partition ids.
     */
    private long m_etokens = 0;
    private int m_etokenCount;

    private final HashinatorLiteType m_type;

    public HashinatorLiteType getType() {
        return m_type;
    }

    /**
     * Initialize TheHashinator with the specified implementation class and configuration.
     * The starting version number will be 0.
     */
    public HashinatorLite(HashinatorLiteType type, byte configBytes[], boolean cooked) {
        m_type = type;

        if (type == HashinatorLiteType.ELASTIC) {
            Pair<Long, Integer> p = (cooked ? updateCooked(configBytes) : updateRaw(configBytes));
            m_etokens = p.getFirst();
            m_etokenCount = p.getSecond();
        }
        else {
            catalogPartitionCount = ByteBuffer.wrap(configBytes).getInt();
        }
    }

    public HashinatorLite(int numPartitions) {
        this(HashinatorLiteType.LEGACY, getLegacyConfigureBytes(numPartitions), false);
    }

    @Override
    public void finalize() {
        if (m_etokens != 0) {
            Bits.unsafe.freeMemory(m_etokens);
        }
    }

    /**
     * Update from optimized (cooked) wire format. token-1 token-2 ... partition-1 partition-2 ... tokens are 4 bytes
     *
     * @param compressedData optimized and compressed config data
     * @return token/partition map
     */
    private Pair<Long, Integer> updateCooked(byte[] compressedData) {
        // Uncompress (inflate) the bytes.
        byte[] cookedBytes;
        try {
            cookedBytes = gunzipBytes(compressedData);
        } catch (IOException e) {
            throw new RuntimeException("Unable to decompress elastic hashinator data.");
        }

        int numEntries = (cookedBytes.length >= 4
                ? ByteBuffer.wrap(cookedBytes).getInt()
                : 0);
        int tokensSize = 4 * numEntries;
        int partitionsSize = 4 * numEntries;
        if (numEntries <= 0 || cookedBytes.length != 4 + tokensSize + partitionsSize) {
            throw new RuntimeException("Bad elastic hashinator cooked config size.");
        }
        long tokens = Bits.unsafe.allocateMemory(8 * numEntries);
        ByteBuffer tokenBuf = ByteBuffer.wrap(cookedBytes, 4, tokensSize);
        ByteBuffer partitionBuf = ByteBuffer.wrap(cookedBytes, 4 + tokensSize, partitionsSize);
        int tokensArray[] = new int[numEntries];
        for (int zz = 3; zz >= 0; zz--) {
            for (int ii = 0; ii < numEntries; ii++) {
                int value = tokenBuf.get();
                value = (value << (zz * 8)) & (0xFF << (zz * 8));
                tokensArray[ii] = (tokensArray[ii] | value);
            }
        }

        int lastToken = Integer.MIN_VALUE;
        for (int ii = 0; ii < numEntries; ii++) {
            int token = tokensArray[ii];
            Preconditions.checkArgument(token >= lastToken);
            lastToken = token;
            long ptr = tokens + (ii * 8);
            Bits.unsafe.putInt(ptr, token);
            final int partitionId = partitionBuf.getInt();
            Bits.unsafe.putInt(ptr + 4, partitionId);
        }
        return Pair.of(tokens, numEntries);
    }

    /**
     * Update from raw config bytes. token-1/partition-1 token-2/partition-2 ... tokens are 8 bytes
     *
     * @param configBytes raw config data
     * @return token/partition map
     */
    private Pair<Long, Integer> updateRaw(byte configBytes[]) {
        ByteBuffer buf = ByteBuffer.wrap(configBytes);
        int numEntries = buf.getInt();
        if (numEntries < 0) {
            throw new RuntimeException("Bad elastic hashinator config");
        }
        long tokens = Bits.unsafe.allocateMemory(8 * numEntries);
        int lastToken = Integer.MIN_VALUE;
        for (int ii = 0; ii < numEntries; ii++) {
            long ptr = tokens + (ii * 8);
            final int token = buf.getInt();
            Preconditions.checkArgument(token >= lastToken);
            lastToken = token;
            Bits.unsafe.putInt(ptr, token);
            final int partitionId = buf.getInt();
            Bits.unsafe.putInt(ptr + 4, partitionId);
        }
        return Pair.of(tokens, numEntries);
    }

    /**
     * Given a long value, pick a partition to store the data. It's only called for legacy
     * hashinator, elastic hashinator hashes all types the same way through hashinateBytes().
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    int hashinateLong(long value) {
        if (m_type.equals(HashinatorLiteType.ELASTIC)) {
            //Elastic
            if (value == Long.MIN_VALUE) {
                return 0;
            }

            return partitionForToken(MurmurHash3.hash3_x64_128(value));
        } else {
            // special case this hard to hash value to 0 (in both c++ and java)
            if (value == Long.MIN_VALUE) {
                return 0;
            }

            // hash the same way c++ does
            int index = (int) (value ^ (value >>> 32));
            return java.lang.Math.abs(index % catalogPartitionCount);
        }
    }

    /**
     * For a given a value hash, find the token that corresponds to it. This will be the first token <= the value hash,
     * or if the value hash is < the first token in the ring, it wraps around to the last token in the ring closest to
     * Long.MAX_VALUE
     */
    public int partitionForToken(int hash) {
        long token = getTokenPtr(hash);
        return Bits.unsafe.getInt(token + 4);
    }

    /**
     * Given an byte[] bytes, pick a partition to store the data.
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    int hashinateBytes(byte[] bytes) {
        if (bytes == null) {
            return 0;
        }

        if (m_type.equals(HashinatorLiteType.ELASTIC)) {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            final int hash = MurmurHash3.hash3_x64_128(buf, 0, bytes.length, 0);
            long token = getTokenPtr(hash);
            return Bits.unsafe.getInt(token + 4);
        } else {
            int hashCode = 0;
            int offset = 0;
            for (int ii = 0; ii < bytes.length; ii++) {
                hashCode = 31 * hashCode + bytes[offset++];
            }
            return java.lang.Math.abs(hashCode % catalogPartitionCount);
        }
    }

    private long getTokenPtr(int hash) {
        int min = 0;
        int max = m_etokenCount - 1;

        while (min <= max) {
            int mid = (min + max) >>> 1;
            final long midPtr = m_etokens + (8 * mid);
            int midval = Bits.unsafe.getInt(midPtr);

            if (midval < hash) {
                min = mid + 1;
            } else if (midval > hash) {
                max = mid - 1;
            } else {
                return midPtr;
            }
        }
        return m_etokens + (min - 1) * 8;
    }

    /**
     * Given an object, map it to a partition. DON'T EVER MAKE ME PUBLIC
     */
    int hashToPartition(VoltType type, Object obj) {
        if (m_type.equals(HashinatorLiteType.ELASTIC)) {
            return hashinateBytes(valueToBytes(obj));
        }
        // Annoying, legacy hashes numbers and bytes differently, need to preserve that.
        if (obj == null || VoltType.isNullVoltType(obj)) {
            return 0;
        } else if (obj instanceof Long) {
            long value = ((Long) obj).longValue();
            return hashinateLong(value);
        } else if (obj instanceof Integer) {
            long value = ((Integer) obj).intValue();
            return hashinateLong(value);
        } else if (obj instanceof Short) {
            long value = ((Short) obj).shortValue();
            return hashinateLong(value);
        } else if (obj instanceof Byte) {
            long value = ((Byte) obj).byteValue();
            return hashinateLong(value);
        } else if (obj.getClass() == byte[].class) {
            obj = bytesToValue(type, (byte[]) obj);
            return hashinateBytes(valueToBytes(obj));
        }

        return hashinateBytes(valueToBytes(obj));
    }

    /**
     * Converts the object into bytes for hashing.
     * @param obj
     * @return null if the obj is null or is a Volt null type.
     */
    public static byte[] valueToBytes(Object obj) {
        long value = 0;
        byte[] retval = null;

        if (VoltType.isNullVoltType(obj)) {
            return null;
        } else if (obj instanceof Long) {
            value = ((Long) obj).longValue();
        } else if (obj instanceof String ) {
            retval = ((String) obj).getBytes(Constants.UTF8ENCODING);
        } else if (obj instanceof Integer) {
            value = ((Integer)obj).intValue();
        } else if (obj instanceof Short) {
            value = ((Short)obj).shortValue();
        } else if (obj instanceof Byte) {
            value = ((Byte)obj).byteValue();
        } else if (obj instanceof byte[]) {
            retval = (byte[]) obj;
        }

        if (retval == null) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            buf.putLong(value);
            retval = buf.array();
        }

        return retval;
    }

    /**
     * Converts a byte array with type back to the original partition value.
     * This is the inverse of {@see TheHashinator#valueToBytes(Object)}.
     * @param type VoltType of partition parameter.
     * @param value Byte array representation of partition parameter.
     * @return Java object of the correct type.
     */
    protected static Object bytesToValue(VoltType type, byte[] value) {
        if ((type == VoltType.NULL) || (value == null)) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.wrap(value);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        switch (type) {
        case BIGINT:
            return buf.getLong();
        case STRING:
            return new String(value, Constants.UTF8ENCODING);
        case INTEGER:
            return buf.getInt();
        case SMALLINT:
            return buf.getShort();
        case TINYINT:
            return buf.get();
        case VARBINARY:
            return value;
        default:
            throw new RuntimeException(
                    "TheHashinator#bytesToValue failed to convert a non-partitionable type.");
        }
    }

    /**
     * Given the type of the targeting partition parameter and an object,
     * coerce the object to the correct type and hash it.
     * NOTE NOTE NOTE NOTE! THIS SHOULD BE THE ONLY WAY THAT YOU FIGURE OUT
     * THE PARTITIONING FOR A PARAMETER! THIS IS SHARED BY SERVER AND CLIENT
     * CLIENT USES direct instance method as it initializes its own per connection
     * Hashinator.
     *
     * @return The partition best set up to execute the procedure.
     * @throws VoltTypeException
     */
    public int getHashedPartitionForParameter(int partitionValueType, Object partitionValue)
            throws VoltTypeException {
        final VoltType partitionParamType = VoltType.get((byte) partitionValueType);

        // Special cases:
        // 1) if the user supplied a string for a number column,
        // try to do the conversion. This makes it substantially easier to
        // load CSV data or other untyped inputs that match DDL without
        // requiring the loader to know precise the schema.
        // 2) For legacy hashinators, if we have a numeric column but the param is in a byte
        // array, convert the byte array back to the numeric value
        if (partitionValue != null && partitionParamType.isPartitionableNumber()) {
            if (partitionValue.getClass() == String.class) {
                {
                    Object tempParam = ParameterConverter.stringToLong(
                            partitionValue,
                            partitionParamType.classFromType());
                    // Just in case someone managed to feed us a non integer
                    if (tempParam != null) {
                        partitionValue = tempParam;
                    }
                }
            }
            else if (partitionValue.getClass() == byte[].class) {
                partitionValue = bytesToValue(partitionParamType, (byte[]) partitionValue);
            }
        }

        return hashToPartition(partitionParamType, partitionValue);
    }

    public HashinatorLiteType getConfigurationType() {
        return m_type;
    }

    // copy and pasted code below from the compression service
    // to avoid linking all that jazz into the client code

    public static byte[] gunzipBytes(byte[] compressedBytes) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream((int)(compressedBytes.length * 1.5));
        InflaterOutputStream dos = new InflaterOutputStream(bos);
        dos.write(compressedBytes);
        dos.close();
        return bos.toByteArray();
    }
}
