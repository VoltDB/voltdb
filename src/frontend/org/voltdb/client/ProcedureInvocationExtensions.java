/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.nio.ByteBuffer;

/**
 * Helper class for managing a defined set of extensions to ProcedureInvocation
 * and StoredProcedureInvocation.
 *
 * Extensions allow additions of flags and extra payloads to the wire protocol
 * without changing the protocol version number. Unknown extensions can be ignored.
 *
 * Examples of possible extensions:
 * - Additional timeout values
 * - Additional tracing information
 * - Additional context needed for better error messages.
 *
 * Each extension has one byte to identify it. This is followed by one byte that
 * stored the size in 2^(N-1) format.
 * examples: If size = 0 => payload = 0B
 *           If size = 1 => payload = 1B
 *           If size = 2 => payload = 2B
 *           If size = 3 => payload = 4B
 *           If size = 5 => payload = 16B
 *           If size = 10 => payload = 512B
 *
 * If the payload only has 300 bytes of data, it is still stored in a 512B chunk of
 * the buffer using padding.
 *
 */
public abstract class ProcedureInvocationExtensions {
    public static final byte BATCH_TIMEOUT = 1;  // batch timeout
    public static final byte ALL_PARTITION = 2; // whether proc is part of run-everywhere

    private static final int INTEGER_SIZE = 4;

    public static byte readNextType(ByteBuffer buf) {
        return buf.get();
    }

    public static void writeBatchTimeoutWithTypeByte(ByteBuffer buf, int timeoutValue) {
        buf.put(BATCH_TIMEOUT);
        writeLength(buf, INTEGER_SIZE);
        buf.putInt(timeoutValue);
    }

    public static int readBatchTimeout(ByteBuffer buf) {
        int len = readLength(buf);
        if (len != INTEGER_SIZE) {
            throw new IllegalStateException(
                    "Batch timeout extension serialization length expected to be 4");
        }
        int timeout = buf.getInt();
        if ((timeout < 0) && (timeout != BatchTimeoutOverrideType.NO_TIMEOUT)) {
            throw new IllegalStateException("Invalid timeout value deserialized: " + timeout);
        }
        return timeout;
    }

    public static void writeAllPartitionWithTypeByte(ByteBuffer buf) {
        buf.put(ALL_PARTITION);
        writeLength(buf, 0);
    }

    public static boolean readAllPartition(ByteBuffer buf) {
        int len = readLength(buf);
        if (len != 0) {
            throw new IllegalStateException(
                    "All-Partition extension serialization length expected to be 0");
        }
        return true;
    }

    public static void skipUnknownExtension(ByteBuffer buf) {
        int len = readLength(buf);
        buf.position(buf.position() + len); // skip ahead
    }

    /**
     * Over-clever way to store lots of lengths in a single byte
     * Length must be a power of two >= 0.
     */
    private static void writeLength(ByteBuffer buf, int length) {
        assert(length >= 0);
        assert(length == (length & (~length + 1))); // check if power of two

        // shockingly fast log_2 uses intrinsics in JDK >= 1.7
        byte log2size = (byte) (32 - Integer.numberOfLeadingZeros(length));

        buf.put(log2size);
    }

    private static int readLength(ByteBuffer buf) {
        byte log2size = buf.get();
        if (log2size == 0) {
            return 0;
        }
        else {
            return 1 << (log2size - 1);
        }
    }
}
