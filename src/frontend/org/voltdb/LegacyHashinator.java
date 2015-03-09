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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import static org.voltdb.TheHashinator.valueToBytes;

public class LegacyHashinator extends TheHashinator {
    private final int catalogPartitionCount;
    private final byte m_configBytes[];
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

    @Override
    public HashinatorType getConfigurationType() {
        return TheHashinator.HashinatorType.LEGACY;
    }

    @Override
    public int pHashToPartition(VoltType type, Object obj) {
        // Annoying, legacy hashes numbers and bytes differently, need to preserve that.
        if (obj == null || VoltType.isNullVoltType(obj)) {
            return 0;
        } else if (obj instanceof Long) {
            long value = ((Long) obj).longValue();
            return pHashinateLong(value);
        } else if (obj instanceof Integer) {
            long value = ((Integer) obj).intValue();
            return pHashinateLong(value);
        } else if (obj instanceof Short) {
            long value = ((Short) obj).shortValue();
            return pHashinateLong(value);
        } else if (obj instanceof Byte) {
            long value = ((Byte) obj).byteValue();
            return pHashinateLong(value);
        } else if (obj.getClass() == byte[].class) {
            obj = bytesToValue(type, (byte[]) obj);
            return pHashinateBytes(valueToBytes(obj));
        }

        return pHashinateBytes(valueToBytes(obj));
    }

    @Override
    protected Set<Integer> pGetPartitions() {
        Set<Integer> set = new HashSet<Integer>();
        for (int ii = 0; ii < catalogPartitionCount; ii++) {
            set.add(ii);
        }
        return set;
    }
}
