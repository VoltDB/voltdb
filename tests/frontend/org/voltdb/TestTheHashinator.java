/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import junit.framework.TestCase;

import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineJNI;

/**
 * This test verifies that the Java Hashinator behaves
 * identically as the C++ Hashinator.
 *
 */
public class TestTheHashinator extends TestCase {
    Random r = new Random();

    @Override
    public void setUp() {
        VoltDB.instance().readBuildInfo("Test");
    }

    public void testExpectNonZeroHash() {
        ExecutionEngine ee = new ExecutionEngineJNI(null, 1, 1, 0, 0, "", 100);

        int partitionCount = 3;
        long valueToHash = 2;
        int eehash = ee.hashinate(valueToHash, partitionCount);
        int javahash = TheHashinator.hashinate(valueToHash, partitionCount);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertNotSame(0, eehash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        try { ee.release(); } catch (Exception e) {}
    }

    public void testSameLongHash1() {
        ExecutionEngine ee = new ExecutionEngineJNI(null, 1, 1, 0, 0, "", 100);

        int partitionCount = 2;
        long valueToHash = 0;
        int eehash = ee.hashinate(valueToHash, partitionCount);
        int javahash = TheHashinator.hashinate(valueToHash, partitionCount);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        partitionCount = 2;
        valueToHash = 1;
        eehash = ee.hashinate(valueToHash, partitionCount);
        javahash = TheHashinator.hashinate(valueToHash, partitionCount);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        partitionCount = 2;
        valueToHash = 2;
        eehash = ee.hashinate(valueToHash, partitionCount);
        javahash = TheHashinator.hashinate(valueToHash, partitionCount);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        partitionCount = 2;
        valueToHash = 3;
        eehash = ee.hashinate(valueToHash, partitionCount);
        javahash = TheHashinator.hashinate(valueToHash, partitionCount);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        try { ee.release(); } catch (Exception e) {}
    }

    public void testEdgeCases() {
        ExecutionEngine ee = new ExecutionEngineJNI(null, 1, 1, 0, 0, "", 100);

        /**
         *  Run with 100k of random values and make sure C++ and Java hash to
         *  the same value.
         */
        for (int i = 0; i < 5; i++) {
            int partitionCount = r.nextInt(1000) + 1;
            long[] values = new long[] {
                    Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MIN_VALUE + 1
            };
            for (long valueToHash : values) {
                int eehash = ee.hashinate(valueToHash, partitionCount);
                int javahash = TheHashinator.hashinate(valueToHash, partitionCount);
                if (eehash != javahash) {
                    System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
                }
                assertEquals(eehash, javahash);
                assertTrue(eehash < partitionCount);
                assertTrue(eehash >= 0);
            }
        }

        try { ee.release(); } catch (Exception e) {}
    }

    public void testSameLongHash() {
        ExecutionEngine ee = new ExecutionEngineJNI(null, 1, 1, 0, 0, "", 100);

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
            if (eehash != javahash) {
                System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
            }
            assertEquals(eehash, javahash);
            assertTrue(eehash < partitionCount);
            assertTrue(eehash > -1);
        }

        try { ee.release(); } catch (Exception e) {}
    }

    public void testSameStringHash() {
        ExecutionEngine ee = new ExecutionEngineJNI(null, 1, 1, 0, 0, "", 100);

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
            assertTrue(eehash >= 0);
        }

        try { ee.release(); } catch (Exception e) {}
    }

    public void testNulls() {
        ExecutionEngine ee = new ExecutionEngineJNI(null, 1, 1, 0, 0, "", 100);

        int jHash = TheHashinator.hashToPartition(new Byte(VoltType.NULL_TINYINT), 2);
        int cHash = ee.hashinate(VoltType.NULL_TINYINT, 2);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.hashToPartition(new Short(VoltType.NULL_SMALLINT), 2);
        cHash = ee.hashinate(VoltType.NULL_SMALLINT, 2);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.hashToPartition(new Integer(VoltType.NULL_INTEGER), 2);
        cHash = ee.hashinate(VoltType.NULL_INTEGER, 2);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.hashToPartition(new Long(VoltType.NULL_BIGINT), 2);
        cHash = ee.hashinate(VoltType.NULL_BIGINT, 2);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.hashToPartition(VoltType.NULL_STRING_OR_VARBINARY, 2);
        cHash = ee.hashinate(VoltType.NULL_STRING_OR_VARBINARY, 2);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.hashToPartition(null, 2);
        cHash = ee.hashinate(null, 2);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        try { ee.release(); } catch (Exception e) {}
    }
}

