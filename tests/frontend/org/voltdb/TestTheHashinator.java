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

package org.voltdb;

import java.util.Random;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineJNI;
import junit.framework.TestCase;

/**
 * This test verifies that the Java Hashinator behaves
 * identically as the C++ Hashinator.
 *
 */
public class TestTheHashinator extends TestCase {
    Random r = new Random();

    @Override
    public void setUp() {
        VoltDB.instance().readBuildInfo();
    }

    public void testSameLongHash() {
        ExecutionEngine ee = new ExecutionEngineJNI(null, 1, 1, 0, 0, "");

        /**
         *  Run with 100k of random values and make sure C++ and Java hash to
         *  the same value.
         */
        for (int i = 0; i < 100000; i++) {
            int partitionCount = r.nextInt(1000) + 1;
            // this will produce negative values, which is desired here.
            long valueToHash = r.nextLong();

            int eehash = ee.hashinate(valueToHash, partitionCount);
            int javahash = TheHashinator.hashinate(valueToHash, partitionCount);
            assertEquals(eehash, javahash);
            assertTrue(eehash < partitionCount);
            assertTrue(eehash > -1);
        }
    }

    public void testSameStringHash() {
        ExecutionEngine ee = new ExecutionEngineJNI(null, 1, 1, 0, 0, "");

        for (int i = 0; i < 100000; i++) {
            int partitionCount = r.nextInt(1000) + 1;
            String valueToHash = Long.toString(r.nextLong());

            int eehash = ee.hashinate(valueToHash, partitionCount);
            int javahash = TheHashinator.hashinate(valueToHash, partitionCount);
            if (eehash != javahash) {
                partitionCount++;
            }
            assertEquals(eehash, javahash);
            assertTrue(eehash < partitionCount);
        }
    }

    public void testNulls() {
        ExecutionEngine ee = new ExecutionEngineJNI(null, 1, 1, 0, 0, "");

        int jHash = TheHashinator.hashToPartition(new Short(Short.MIN_VALUE), 2);
        int cHash = ee.hashinate(Short.MIN_VALUE, 2);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.hashToPartition(new Integer(Integer.MIN_VALUE), 2);
        cHash = ee.hashinate(Integer.MIN_VALUE, 2);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.hashToPartition(new Long(Long.MIN_VALUE), 2);
        cHash = ee.hashinate(Long.MIN_VALUE, 2);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.hashToPartition(new Byte(Byte.MIN_VALUE), 2);
        cHash = ee.hashinate(Byte.MIN_VALUE, 2);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);
    }
}

