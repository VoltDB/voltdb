/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;

public class LegacyHashinator extends TheHashinator {
    private final int catalogPartitionCount;
    private final byte m_configBytes[];
    private final String m_configJSON;
    private final long m_signature;
    @SuppressWarnings("unused")
    private static final VoltLogger hostLogger = new VoltLogger("HOST");

    @Override
    public int pHashinateLong(long value) {
        // special case this hard to hash value to 0 (in both c++ and java)
        if (value == Long.MIN_VALUE) return 0;

        // hash the same way c++ does
        int index = (int)(value^(value>>>32));
        return java.lang.Math.abs(index % catalogPartitionCount);
    }

    @Override
    public int pHashinateBytes(byte[] bytes) {
        int hashCode = 0;
        int offset = 0;
        for (int ii = 0; ii < bytes.length; ii++) {
            hashCode = 31 * hashCode + bytes[offset++];
        }
        return java.lang.Math.abs(hashCode % catalogPartitionCount);
    }

    /**
     * Constructor
     * @param configBytes  config data
     * @param cooked       (ignored by legacy)
     */
    public LegacyHashinator(byte configBytes[], boolean cooked) {
        catalogPartitionCount = ByteBuffer.wrap(configBytes).getInt();
        m_configBytes = Arrays.copyOf(configBytes, configBytes.length);
        try {
            m_configJSON = new JSONStringer().
                    array().value(catalogPartitionCount).endArray().toString();
        } catch (JSONException e) {
            throw new RuntimeException("Failed to serialized Hashinator Configuration to JSON.", e);
        }
        m_signature = TheHashinator.computeConfigurationSignature(m_configBytes);
    }

    public static byte[] getConfigureBytes(int catalogPartitionCount) {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(catalogPartitionCount);
        return buf.array();
    }

    @Override
    public HashinatorConfig pGetCurrentConfig() {
        return new HashinatorConfig(HashinatorType.LEGACY, m_configBytes, 0, 0);
    }

    @Override
    public Map<Integer, Integer> pPredecessors(int partition)
    {
        throw new RuntimeException("Legacy hashinator doesn't support predecessors");
    }

    @Override
    public Pair<Integer, Integer> pPredecessor(int partition, int token)
    {
        throw new RuntimeException("Legacy hashinator doesn't support predecessors");
    }

    @Override
    public Map<Integer, Integer> pGetRanges(int partition)
    {
        throw new RuntimeException("Getting ranges is not supported in the legacy hashinator");
    }

    @Override
    public long pGetConfigurationSignature() {
        return m_signature;
    }


    /**
     * Returns straight config bytes (not for serialization).
     * @return config bytes
     */
    @Override
    public byte[] getConfigBytes()
    {
        return m_configBytes;
    }

    /**
     * Returns raw config JSONString.
     * @return config JSONString
     */
    @Override
    public String getConfigJSON()
    {
        return m_configJSON;
    }

    @Override
    public HashinatorType getConfigurationType() {
        return TheHashinator.HashinatorType.LEGACY;
    }

    @Override
    public int pHashToPartition(VoltType type, Object obj) {
        assert(obj != null);
        // Annoying, legacy hashes numbers and bytes differently, need to preserve that.
        if (VoltType.isVoltNullValue(obj)) {
            return 0;
        }
        long value = 0;
        if (obj instanceof Long) {
            value = ((Long) obj).longValue();
        }
        else if (obj instanceof Integer) {
            value = ((Integer) obj).intValue();
        }
        else if (obj instanceof Short) {
            value = ((Short) obj).shortValue();
        }
        else if (obj instanceof Byte) {
            value = ((Byte) obj).byteValue();
        }
        else {
            // The hash formula for a value represented as serialized bytes
            // must still be appropriate for the expected partitioning type,
            // even if this requires a round-trip conversion.
            if (obj.getClass() == byte[].class) {
                obj = type.bytesToValue((byte[]) obj);
            }
            return pHashinateBytes(VoltType.valueToBytes(obj));
        }

        return pHashinateLong(value);
    }

    @Override
    protected Set<Integer> pGetPartitions() {
        Set<Integer> set = new HashSet<Integer>();
        for (int ii = 0; ii < catalogPartitionCount; ii++) {
            set.add(ii);
        }
        return set;
    }

    @Override
    public int getPartitionFromHashedToken(int hashedToken) {
        return java.lang.Math.abs(hashedToken % catalogPartitionCount);
    }
}
