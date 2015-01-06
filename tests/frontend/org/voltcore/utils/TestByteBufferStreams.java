/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltcore.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.nio.ByteBuffer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.utils.Digester;

import com.google_voltpatches.common.base.Charsets;

public class TestByteBufferStreams {

    static byte [] hunkOfContent = new byte[1024*1024];
    static final String thirtyTwoBytesPattern = "block %7d repeatable pattern"; // 1 << 5
    static String digestOfHunkOfContent;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        for (int i = 0; i < hunkOfContent.length; i+=1<<5) {
            byte [] bytes = String.format(thirtyTwoBytesPattern, i).getBytes(Charsets.UTF_8);
            System.arraycopy(bytes, 0, hunkOfContent, i, bytes.length);
        }
        digestOfHunkOfContent = Digester.sha1AsBase64(hunkOfContent);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Test
    public void testByteBufferInputStream() throws Exception {
        ByteBufferInputStream bbin = new ByteBufferInputStream(ByteBuffer.wrap(hunkOfContent));
        byte [] bytes = new byte[1<<5];
        for (int i = 0; i < hunkOfContent.length; i+=1<<5) {
            assertEquals(bytes.length, bbin.read(bytes));
            assertEquals(String.format(thirtyTwoBytesPattern, i), new String(bytes,Charsets.UTF_8));
        }
        assertEquals(-1, bbin.read(bytes));
        try {
            bbin.read(bytes);
            fail("expected end of file exception");
        } catch (EOFException expected) {
        }
        bbin.close();
    }

    @Test
    public void testGreedyRead() throws Exception {
        byte [] inbytes = String.format(thirtyTwoBytesPattern, 111).getBytes(Charsets.UTF_8);
        ByteBufferInputStream bbis = new ByteBufferInputStream(ByteBuffer.wrap(inbytes));
        byte [] outbytes = new byte [inbytes.length<<1];
        assertEquals(inbytes.length, bbis.read(outbytes));
        bbis.close();
    }

    @Test
    public void testByteBufferOutputStream() throws Exception {
        ByteBufferOutputStream bbos = new ByteBufferOutputStream();
        for (int i = 0; i < hunkOfContent.length; i+=1<<5) {
            bbos.write(String.format(thirtyTwoBytesPattern, i).getBytes(Charsets.UTF_8));
        }
        assertEquals(digestOfHunkOfContent,Digester.sha1AsBase64(bbos.toByteArray()));
        bbos.close();
    }

}
