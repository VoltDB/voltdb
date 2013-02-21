package org.voltdb;

import java.nio.ByteBuffer;

import org.voltcore.logging.VoltLogger;

import com.google.common.base.Charsets;

public class LegacyHashinator extends TheHashinator {
    private final int catalogPartitionCount;
    private static final VoltLogger hostLogger = new VoltLogger("HOST");

    @Override
    protected int pHashinateLong(long value) {
        // special case this hard to hash value to 0 (in both c++ and java)
        if (value == Long.MIN_VALUE) return 0;

        // hash the same way c++ does
        int index = (int)(value^(value>>>32));
        return java.lang.Math.abs(index % catalogPartitionCount);
    }

    @Override
    protected int pHashinateBytes(byte[] bytes) {
        int hashCode = 0;
        int offset = 0;
        for (int ii = 0; ii < bytes.length; ii++) {
            hashCode = 31 * hashCode + bytes[offset++];
        }
        return java.lang.Math.abs(hashCode % catalogPartitionCount);
    }

    @Override
    protected int pHashinateString(String value) {
        byte bytes[] = value.getBytes(Charsets.UTF_8);
        return hashinateBytes(bytes);
    }

    public LegacyHashinator(byte config[]) {
        catalogPartitionCount = ByteBuffer.wrap(config).getInt();
    }

    public static byte[] getConfigureBytes(int catalogPartitionCount) {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(catalogPartitionCount);
        return buf.array();
    }
}
