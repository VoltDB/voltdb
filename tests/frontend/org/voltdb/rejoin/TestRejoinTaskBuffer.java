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

package org.voltdb.rejoin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.BeforeClass;
import org.junit.Test;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.NativeLibraryLoader;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class TestRejoinTaskBuffer {
    @BeforeClass
    public static void classInit() throws Exception{
        NativeLibraryLoader.loadVoltDB();
    }

    @Test
    public void testRegularInvocations() throws Exception {
        RejoinTaskBuffer ib = new RejoinTaskBuffer(0, RejoinTaskBuffer.DEFAULT_BUFFER_SIZE);
        TransactionInfoBaseMessage msg = null;
        StoredProcedureInvocation spi = new StoredProcedureInvocation();

        for( int i = 0; i < 10; ++i) {
            spi.setProcName(String.format("YoYo.%04d", i));
            spi.setParams(i,"YoYo",3);
            msg = createTask(spi);
            ib.appendTask(0, msg);
        }
        assertFalse(ib.isReadOnly());
        ib.compile();

        int expectedSize = RejoinTaskBuffer.metadataSize()
                + (msg.getSerializedSize() + RejoinTaskBuffer.taskHeaderSize()) * 10;
        assertEquals(expectedSize, ib.size());

        RejoinTaskBuffer iv = new RejoinTaskBuffer(ib.getContainer());
        int count = 0;
        msg = iv.nextTask();
        while (msg != null) {
            assertTrue(msg instanceof Iv2InitiateTaskMessage);
            StoredProcedureInvocation sp = ((Iv2InitiateTaskMessage)msg).getStoredProcedureInvocation();

            assertNotNull(sp.getProcName());
            assertNotNull(sp.getParams());

            Object [] params = sp.getParams().toArray();
            assertNotNull(params);
            assertEquals(params.length, 3);

            assertEquals(String.format("YoYo.%04d", count), sp.getProcName());
            assertEquals(count, (int)params[0]);
            assertEquals(3, (int)params[2]);
            count++;
            msg = iv.nextTask();
        }
        assertEquals(10, count);
        ib.getContainer().discard();
    }

    @Test
    public void testRegularOverflow() throws Exception {
        StringBuilder sb = new StringBuilder(1024);
        for (int i = 0; i < 64; ++i) {
            sb.append("sixteenairypigs");
        }
        String lottaPigs = sb.toString();

        RejoinTaskBuffer ib = new RejoinTaskBuffer(0, RejoinTaskBuffer.DEFAULT_BUFFER_SIZE);
        int headRoom = RejoinTaskBuffer.DEFAULT_BUFFER_SIZE;
        int appends = 0;

        TransactionInfoBaseMessage msg = null;
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        do {
            spi.setProcName(String.format("YoYo.%04d", appends));
            spi.setParams(appends, lottaPigs, 3);
            msg = createTask(spi);
            headRoom = ib.appendTask(0, msg);
            appends += 1;
        } while (headRoom > 0);

        assertTrue(ib.isReadOnly());
        assertTrue(ib.size() > RejoinTaskBuffer.DEFAULT_BUFFER_SIZE);

        int expectedSize = RejoinTaskBuffer.metadataSize()
                + (msg.getSerializedSize() + RejoinTaskBuffer.taskHeaderSize()) * appends;
        assertEquals(expectedSize, ib.size());

        try {
            spi.setProcName(String.format("YoYo.%04d", appends));
            spi.setParams(appends, lottaPigs, 3);
            msg = createTask(spi);
            headRoom = ib.appendTask(0, msg);
            fail("should have a thrown an illegal state exception");
        } catch (IllegalStateException e) {
        }

        RejoinTaskBuffer iv = new RejoinTaskBuffer(ib.getContainer());
        int count = 0;
        msg = iv.nextTask();
        while (msg != null) {
            assertTrue(msg instanceof Iv2InitiateTaskMessage);
            StoredProcedureInvocation sp = ((Iv2InitiateTaskMessage)msg).getStoredProcedureInvocation();

            assertNotNull(sp.getProcName());
            assertNotNull(sp.getParams());

            Object [] params = sp.getParams().toArray();
            assertNotNull(params);
            assertEquals(params.length, 3);

            assertEquals(String.format("YoYo.%04d", count), sp.getProcName());
            assertEquals(count, (int)params[0]);
            assertEquals(3, (int)params[2]);

            count += 1;
            msg = iv.nextTask();
        }
        assertEquals(appends, count);
        ib.getContainer().discard();
    }

    @Test
    public void testOneBigInvocation() throws Exception {
        StringBuilder sb = new StringBuilder(1048576);
        for (int i = 0; i < 65536; ++i) {
            sb.append("sixteenairypigs");
        }
        String lottaPigs = sb.toString();

        RejoinTaskBuffer ib = new RejoinTaskBuffer(0, RejoinTaskBuffer.DEFAULT_BUFFER_SIZE);
        int appends = 0;

        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setProcName(String.format("YoYo.%04d", appends));
        spi.setParams(appends, lottaPigs, 3);
        TransactionInfoBaseMessage msg = createTask(spi);
        int headRoom = ib.appendTask(0, msg);
        appends += 1;

        assertEquals(0,headRoom);
        assertTrue(ib.isReadOnly());
        assertTrue(ib.size() > RejoinTaskBuffer.DEFAULT_BUFFER_SIZE);

        int expectedSize = RejoinTaskBuffer.metadataSize()
                + (msg.getSerializedSize() + RejoinTaskBuffer.taskHeaderSize()) * appends;
        assertEquals(expectedSize, ib.size());

        try {
            spi.setProcName(String.format("YoYo.%04d", appends));
            spi.setParams(appends, lottaPigs, 3);
            msg = createTask(spi);
            headRoom = ib.appendTask(0, msg);
            fail("should have a thrown an illegal state exception");
        } catch (IllegalStateException e) {
        }

        RejoinTaskBuffer iv = new RejoinTaskBuffer(ib.getContainer());
        int count = 0;
        msg = iv.nextTask();
        while (msg != null) {
            assertTrue(msg instanceof Iv2InitiateTaskMessage);
            StoredProcedureInvocation sp = ((Iv2InitiateTaskMessage)msg).getStoredProcedureInvocation();

            assertNotNull(sp.getProcName());
            assertNotNull(sp.getParams());

            Object [] params = sp.getParams().toArray();
            assertNotNull(params);
            assertEquals(params.length, 3);

            assertEquals(String.format("YoYo.%04d", count), sp.getProcName());
            assertEquals(count, (int)params[0]);
            assertEquals(3, (int)params[2]);

            count += 1;
            msg = iv.nextTask();
        }
        assertEquals(appends, count);
        ib.getContainer().discard();
    }

    private TransactionInfoBaseMessage createTask(StoredProcedureInvocation invocation) {
        return new Iv2InitiateTaskMessage(0, 0, 0, 42, 42, false, true, false, invocation, 0, 0, true);
    }
}
