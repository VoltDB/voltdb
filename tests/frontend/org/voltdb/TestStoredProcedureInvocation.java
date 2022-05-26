/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeMap;

import org.voltcore.utils.Pair;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ProcedureInvocation;
import org.voltdb.client.ProcedureInvocationExtensions;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.utils.SerializationHelper;

import junit.framework.TestCase;

/**
 * Tests serialization and deserialization of a cross product of
 * invocations across serialization versions and different code paths.
 *
 */
public class TestStoredProcedureInvocation extends TestCase {

    Pair<?,?>[] procedureNames = {
            new Pair<String, Boolean>("Foo", true),
            new Pair<String, Boolean>("this is spinal tap", true),
            new Pair<String, Boolean>("@snapshot", true),
            new Pair<String, Boolean>("你好", true),
            new Pair<String, Boolean>("1", true),
            new Pair<String, Boolean>("", false),
            new Pair<String, Boolean>(null, false)
    };

    Pair<?,?>[] clientHandles = {
            new Pair<Long, Boolean>(-1L, true),
            new Pair<Long, Boolean>(0L, true),
            new Pair<Long, Boolean>(-96L, true),
            new Pair<Long, Boolean>(96L, true),
            new Pair<Long, Boolean>(6000000000L, true),
            new Pair<Long, Boolean>(Long.MAX_VALUE, true),
            new Pair<Long, Boolean>(Long.MIN_VALUE, true)
    };

    Pair<?,?>[] timeouts = {
            new Pair<Integer, Boolean>(0, true),
            new Pair<Integer, Boolean>(-1, true),
            new Pair<Integer, Boolean>(-2, false),
            new Pair<Integer, Boolean>(1, true),
            new Pair<Integer, Boolean>(Integer.MAX_VALUE, true),
            new Pair<Integer, Boolean>(Integer.MIN_VALUE, false),
            new Pair<Integer, Boolean>(2000000, true),
            new Pair<Integer, Boolean>(BatchTimeoutOverrideType.NO_TIMEOUT, true)
    };

    int[] partitionDestinations = { 5, -1 };

    Pair<?,?>[] params = {
            new Pair<Object[], Boolean>(new Object[0], true),
            new Pair<Object[], Boolean>(new Object[] { 1, 2, 3 }, true),
            new Pair<Object[], Boolean>(new Object[] { null }, true),
            new Pair<Object[], Boolean>(new Object[] { null, null }, true),
            // a very silly parameter indeed
            new Pair<Object[], Boolean>(new Object[] { new TreeMap<Integer,Integer>() }, false)
    };


    void roundTripBuffer(boolean expectSuccess, ByteBuffer buf, String procName, long handle, int timeout,
            int partitionDestination) throws IOException {
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.initFromBuffer(buf);

        if (expectSuccess) {
            assertEquals(handle, spi.getClientHandle());
            assertEquals(timeout, spi.getBatchTimeout());
            assertEquals(partitionDestination, spi.getPartitionDestination());
            assertTrue(procName.equals(spi.getProcName()));
        }
        else {
            if (handle != spi.getClientHandle()) {
                return;
            }
            if (timeout != spi.getBatchTimeout()) {
                return;
            }

            // no all-partition handling for failure because it can't fail

            if (procName != null) {
                if (!procName.equals(spi.getProcName())) {
                    return;
                }
            }
            else {
                if (spi.getProcName() != null) {
                    return;
                }
            }
            fail();
        }
    }

    void roundTripProcedureInvocation(boolean expectSuccess, String procName, long handle, int timeout,
            int partitionDestination, Object[] params) throws IOException {

        // try ProcedureInvocation version
        try {
            ProcedureInvocation pi = new ProcedureInvocation(handle, timeout, partitionDestination, procName, params);

            ByteBuffer buf = ByteBuffer.allocate(pi.getSerializedSize());
            pi.flattenToBuffer(buf);
            buf.flip();

            roundTripBuffer(expectSuccess, buf, procName, handle, timeout, partitionDestination);
        }
        catch (Exception e) {
            if (expectSuccess) {
                e.printStackTrace();
                fail();
            }
        }

        // try StoredProcedureInvocation
        try {
            StoredProcedureInvocation spi = new StoredProcedureInvocation();
            spi.setProcName(procName);
            spi.setClientHandle(handle);
            spi.setBatchTimeout(timeout);
            spi.setParams(params);
            spi.setPartitionDestination(partitionDestination);

            ByteBuffer buf = ByteBuffer.allocate(spi.getSerializedSize());
            spi.flattenToBuffer(buf);
            buf.flip();

            roundTripBuffer(expectSuccess, buf, procName, handle, timeout, partitionDestination);
        }
        catch (Exception e) {
            if (expectSuccess) {
                e.printStackTrace();
                fail();
            }
        }

        // try extra extensions sometimes using latest binary serialization (version 2)
        try {
            ByteBuffer buf = ByteBuffer.allocate(10000); // rando buffer big enough
            buf.put(ProcedureInvocationType.VERSION2.getValue()); //Version
            SerializationHelper.writeString(procName, buf);
            buf.putLong(handle);

            if (timeout == BatchTimeoutOverrideType.NO_TIMEOUT) {
                // write two extensions
                buf.put((byte) 2);
                buf.put((byte) 75); // nonsense type
                buf.put((byte) 0); // zero length
                buf.put((byte) 93); // nonsense type
                buf.put((byte) 4); // eight bytes
                buf.put(new byte[8]); // zero-fill 8 bytes
            }
            else {
                buf.put((byte) 1);
                ProcedureInvocationExtensions.writeBatchTimeoutWithTypeByte(buf, timeout);
            }

            ParameterSet paramSet = (params != null
                    ? ParameterSet.fromArrayWithCopy(params)
                    : ParameterSet.emptyParameterSet());
            paramSet.flattenToBuffer(buf);

            buf.flip();

            // don't bother testing allPartition in binary
            roundTripBuffer(expectSuccess, buf, procName, handle, timeout, -1);
        }
        catch (Exception e) {
            if (expectSuccess) {
                e.printStackTrace();
                fail();
            }
        }

        // try older serialization formats

        // try original if no timeout
        if (timeout == BatchTimeoutOverrideType.NO_TIMEOUT) {
            try {
                StoredProcedureInvocation spi = new StoredProcedureInvocation();
                spi.setProcName(procName);
                spi.setClientHandle(handle);
                spi.setParams(params);

                ByteBuffer buf = ByteBuffer.allocate(spi.getSerializedSizeForOriginalVersion());
                spi.flattenToBufferForOriginalVersion(buf);
                buf.flip();

                roundTripBuffer(expectSuccess, buf, procName, handle, timeout, -1);
            }
            catch (Exception e) {
                if (expectSuccess) {
                    e.printStackTrace();
                    fail();
                }
            }
        }

        // try v1
        try {
            ByteBuffer buf = ByteBuffer.allocate(10000); // rando buffer big enough
            buf.put(ProcedureInvocationType.VERSION1.getValue()); //Version
            if (timeout == BatchTimeoutOverrideType.NO_TIMEOUT) {
                buf.put(BatchTimeoutOverrideType.NO_OVERRIDE_FOR_BATCH_TIMEOUT.getValue());
            }
            else {
                buf.put(BatchTimeoutOverrideType.HAS_OVERRIDE_FOR_BATCH_TIMEOUT.getValue());
                buf.putInt(timeout);
            }

            SerializationHelper.writeString(procName, buf);
            buf.putLong(handle);

            ParameterSet paramSet = (params != null
                    ? ParameterSet.fromArrayWithCopy(params)
                    : ParameterSet.emptyParameterSet());
            paramSet.flattenToBuffer(buf);

            buf.flip();

            // don't bother testing allPartition in older versions
            roundTripBuffer(expectSuccess, buf, procName, handle, timeout, -1);
        }
        catch (Exception e) {
            if (expectSuccess) {
                e.printStackTrace();
                fail();
            }
        }
    }

    public void testTimeoutExtension() throws IOException {
        for (Pair<?,?> procsRaw : procedureNames) {
            String procName = (String) procsRaw.getFirst();
            boolean procShouldWork = (Boolean) procsRaw.getSecond();

            for (Pair<?,?> handlesRaw : clientHandles) {
                long handle = (Long) handlesRaw.getFirst();
                boolean handleShouldWork = (Boolean) handlesRaw.getSecond();

                for (Pair<?,?> timeoutsRaw : timeouts) {
                    int timeout = (Integer) timeoutsRaw.getFirst();
                    boolean timeoutShouldWork = (Boolean) timeoutsRaw.getSecond();

                    for (int partitionDestination : partitionDestinations) {
                        // all allPartition values are valid

                        for (Pair<?,?> paramsRaw : params) {
                            Object[] params = (Object[]) paramsRaw.getFirst();
                            boolean paramsShouldWork = (Boolean) paramsRaw.getSecond();

                            System.out.printf("Trying proc:\"%s\", handle:%d, timeout:%d, allPartition:%s, params:%s\n",
                                    String.valueOf(procName), handle, timeout, partitionDestination,
                                    String.valueOf(params));

                            // try without a timeout
                            boolean shouldWork = procShouldWork && handleShouldWork && paramsShouldWork && timeoutShouldWork;
                            roundTripProcedureInvocation(shouldWork, procName, handle, timeout, partitionDestination,
                                    params);
                        }
                    }
                }
            }
        }
    }
}
