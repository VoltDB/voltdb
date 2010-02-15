/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.util.Random;

import org.voltdb.utils.HexEncoder;

import junit.framework.TestCase;

public class TestHexEncoder extends TestCase {

    public void testHexEncoderWithString() {
        String someText = "This is some text\nwith a newline.";
        String hexText = HexEncoder.hexEncode(someText);
        String result = HexEncoder.hexDecodeToString(hexText);

        System.out.println(someText);
        System.out.println(hexText);
        System.out.println(result);

        assertEquals(someText, result);
    }

    public void testHexEncoderWithBytes() {
        byte[] bytes = new byte[1024];
        new Random().nextBytes(bytes);

        for (int i = 0; i < bytes.length; i++)
            System.out.print(String.valueOf((int) bytes[i]) + " ");
        System.out.println();

        String hexText = HexEncoder.hexEncode(bytes);
        byte[] result = HexEncoder.hexDecode(hexText);

        for (int i = 0; i < result.length; i++)
            System.out.print(String.valueOf((int) result[i]) + " ");
        System.out.println();

        System.out.println(hexText);

        for (int i = 0; i < bytes.length; i++)
            assertEquals(bytes[i], result[i]);
    }

}
