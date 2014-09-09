/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
