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

package org.voltdb.utils;

import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;

public class TestEncoder extends TestCase {

    public void testHexEncoderWithString() {
        String someText = "This is some text\nwith a newline.";
        String hexText = Encoder.hexEncode(someText);
        String result = Encoder.hexDecodeToString(hexText);

        assertEquals(someText, result);
    }

    public void testHexEncoderWithBytes() {
        byte[] bytes = new byte[1024];
        new Random().nextBytes(bytes);

        for (int i = 0; i < bytes.length; i++)
            System.out.print(String.valueOf(bytes[i]) + " ");
        System.out.println();

        String hexText = Encoder.hexEncode(bytes);
        byte[] result = Encoder.hexDecode(hexText);

        for (int i = 0; i < result.length; i++)
            System.out.print(String.valueOf(result[i]) + " ");
        System.out.println();

        System.out.println(hexText);

        for (int i = 0; i < bytes.length; i++)
            assertEquals(bytes[i], result[i]);
    }

    public void testCompressionAndBase64EncoderWithString() {
        String someText = "This is some text\nwith a newline.";
        String b64Text = Encoder.compressAndBase64Encode(someText);
        String result = Encoder.decodeBase64AndDecompress(b64Text);

        assertEquals(someText, result);
    }

    public void testB64WithBigness() throws IOException {
        String someText = TPCCProjectBuilder.getTPCCSchemaCatalog().serialize();

        String b64Text = Encoder.compressAndBase64Encode(someText);
        String result = Encoder.decodeBase64AndDecompress(b64Text);

        assertEquals(someText, result);
    }
}
