/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.messaging;

import java.io.IOException;

import junit.framework.TestCase;

public class TestFastSerializer extends TestCase {
    FastSerializer heapOut;
    FastSerializer directOut;
    @Override
    public void setUp() {
        heapOut = new FastSerializer();
        directOut = new FastSerializer(true, true);
    }

    @Override
    public void tearDown() {
        heapOut.discard();
        directOut.discard();
        heapOut = null;
        directOut = null;
        System.gc();
        System.runFinalization();
    }

    public void testHugeMessage() throws IOException {
        testHM(heapOut);
        testHM(directOut);
    }

    private void testHM(FastSerializer fs) throws IOException {
        // 1 MB message
        byte[] huge = new byte[1024*1024];
        huge[huge.length-1] = 42;
        fs.write(huge);
        assertEquals(42, fs.getBytes()[huge.length-1]);
    }

    public void testHugeMessageBigEndian() throws IOException {
        testHMLE(heapOut);
        testHMLE(directOut);
    }

    private void testHMLE(FastSerializer out) throws IOException {
        out = new FastSerializer(false, false);

        // 1 MB message
        byte[] huge = new byte[1024*1024];
        huge[huge.length-1] = 42;
        out.write(huge);
        out.writeInt(0x01020304);

        try {
            byte[] bytes = out.getBBContainer().b().array();
            assertEquals(0x01, bytes[huge.length]);
            assertEquals(0x02, bytes[huge.length+1]);
            assertEquals(0x03, bytes[huge.length+2]);
            assertEquals(0x04, bytes[huge.length+3]);
        } finally {
            out.discard();
        }
    }

    public void testClear() throws IOException {
        testClearP(heapOut);
        testClearP(directOut);
    }

    private void testClearP(FastSerializer out) throws IOException {
        out.writeInt(42);
        out.clear();
        out.writeInt(42);
        byte[] bytes = out.getBytes();
        assertEquals(4, bytes.length);
    }

    public void testDirect() throws IOException {
        assertTrue(directOut.getBBContainer().b().isDirect());
        testHugeMessage();
        // Should still be direct after resizing.
        assertTrue(directOut.getBBContainer().b().isDirect());
    }
}
