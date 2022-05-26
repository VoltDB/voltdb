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

package org.voltcore.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.cassandra_voltpatches.MurmurHash3;
import org.junit.Test;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.NativeLibraryLoader;
import org.voltdb.utils.Encoder;

public class TestMurmur3 extends TestCase {

    final long iterations = 300000;
    final int maxLength = 4096;

    @Test
    public void testMatchesNativeLongs() throws Exception {
        final long seed = ByteBuffer.wrap(SecureRandom.getSeed(8)).getInt();
        Random r = new Random(seed);
        System.out.println("Seed is " + seed);
        NativeLibraryLoader.loadVoltDB();

        for (int ii = 0; ii < iterations; ii++) {
            final long nextValue = r.nextLong();

            long nativeHash = DBBPool.getMurmur3128(nextValue);
            long javaHash =  MurmurHash3.hash3_x64_128(nextValue);

            if (nativeHash != javaHash) {
                fail("Failed in iteration " + ii + " native hash " + Long.toHexString(nativeHash) +
                        " java hash " + Long.toHexString(javaHash) +" with value " + nextValue);
            }
        }
    }

    @Test
    public void testMatchesNativeBytes() throws Exception {
        final long seed = ByteBuffer.wrap(SecureRandom.getSeed(8)).getInt();
        Random r = new Random(seed);
        System.out.println("Seed is " + seed);
        NativeLibraryLoader.loadVoltDB();
        BBContainer c = DBBPool.allocateDirect(4096);
        try {
            c.b().order(ByteOrder.LITTLE_ENDIAN);

            for (int ii = 0; ii < iterations; ii++) {
                int bytesToFill = r.nextInt(maxLength + 1);
                byte bytes[] = new byte[bytesToFill];
                r.nextBytes(bytes);
                c.b().clear();
                c.b().put(bytes);
                c.b().flip();

                long nativeHash = DBBPool.getMurmur3128(c.address(), 0, bytesToFill);
                long javaHash =  MurmurHash3.hash3_x64_128(c.b(), 0, bytesToFill, 0);
                if (nativeHash != javaHash) {
                    fail("Failed in iteration " + ii + " native hash " + Long.toHexString(nativeHash) +
                            " java hash " + Long.toHexString(javaHash) +" with bytes " + Encoder.base64Encode(bytes));
                }
            }
        } finally {
            c.discard();
        }
    }
}
