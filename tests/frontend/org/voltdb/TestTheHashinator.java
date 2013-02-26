/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineJNI;

/**
 * This test verifies that the Java Hashinator behaves
 * identically as the C++ Hashinator.
 *
 */
@RunWith(Parameterized.class)
public class TestTheHashinator extends TestCase {
    Random r = new Random();

    @Override
    public void setUp() {
        EELibraryLoader.loadExecutionEngineLibrary(true);
        VoltDB.instance().readBuildInfo("Test");
    }

    @Parameters
    public static Collection<Object[]> startActions() {
        return Arrays.asList(new Object[][] {{HashinatorType.LEGACY},
                                             {HashinatorType.ELASTIC}});
    }

    private final HashinatorType hashinatorType;
    private final int tokensPerPartition = 6;
    public TestTheHashinator(HashinatorType  type) {
        hashinatorType = type;
    }

    public byte[] getConfigBytes(int partitionCount) {
        switch (hashinatorType) {
        case LEGACY:
            return LegacyHashinator.getConfigureBytes(partitionCount);
        case ELASTIC:
            return ElasticHashinator.getConfigureBytes(partitionCount, tokensPerPartition);
        }
        return null;
    }

    public Class<? extends TheHashinator> getHashinatorClass() {
        switch(hashinatorType) {
        case LEGACY:
            return LegacyHashinator.class;
        case ELASTIC:
            return ElasticHashinator.class;
        }
        throw new RuntimeException();
    }

    /*
     * This test validates that not all values hash to 0. Most of the other
     * tests will pass even if everything hashes to a single partition.
     */
    @Test
    public void testExpectNonZeroHash() {
        final byte configBytes[] = getConfigBytes(3);
        ExecutionEngine ee =
                new ExecutionEngineJNI(
                        1,
                        1,
                        0,
                        0,
                        "",
                        100,
                        hashinatorType,
                        configBytes);

        int partitionCount = 3;
        long valueToHash = hashinatorType == HashinatorType.ELASTIC ? 45 : 2;
        TheHashinator.initialize(getHashinatorClass(), configBytes);

        int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        int javahash = TheHashinator.hashinateLong(valueToHash);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertNotSame(0, eehash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        try { ee.release(); } catch (Exception e) {}
    }

    @Test
    public void testSameLongHash1() {
        final byte configBytes[] = getConfigBytes(2);
        ExecutionEngine ee =
                new ExecutionEngineJNI(
                        1,
                        1,
                        0,
                        0,
                        "",
                        100,
                        hashinatorType,
                        configBytes);

        int partitionCount = 2;
        TheHashinator.initialize(getHashinatorClass(), getConfigBytes(partitionCount));

        long valueToHash = 0;
        int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        int javahash = TheHashinator.hashinateLong(valueToHash);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        valueToHash = 1;
        eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        javahash = TheHashinator.hashinateLong(valueToHash);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        valueToHash = 2;
        eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        javahash = TheHashinator.hashinateLong(valueToHash);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        valueToHash = 3;
        eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        javahash = TheHashinator.hashinateLong(valueToHash);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        try { ee.release(); } catch (Exception e) {}
    }

    @Test
    public void testEdgeCases() {
        byte configBytes[] = getConfigBytes(1);
        ExecutionEngine ee =
                new ExecutionEngineJNI(
                        1,
                        1,
                        0,
                        0,
                        "",
                        100,
                        hashinatorType, configBytes);

        /**
         *  Run with 100k of random values and make sure C++ and Java hash to
         *  the same value.
         */
        for (int i = 0; i < 5; i++) {
            int partitionCount = r.nextInt(1000) + 1;
            long[] values = new long[] {
                    Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MIN_VALUE + 1
            };
            configBytes = getConfigBytes(partitionCount);
            TheHashinator.initialize(getHashinatorClass(), configBytes);
            for (long valueToHash : values) {
                int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
                int javahash = TheHashinator.hashinateLong(valueToHash);
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

    @Test
    public void testSameLongHash() {
        byte configBytes[] = getConfigBytes(1);
        ExecutionEngine ee = new ExecutionEngineJNI(1, 1, 0, 0, "", 100, hashinatorType, configBytes);

        /**
         *  Run with 10k of random values and make sure C++ and Java hash to
         *  the same value.
         */
        for (int i = 0; i < 2500; i++) {
            final int partitionCount = r.nextInt(1000) + 1;
            configBytes = getConfigBytes(partitionCount);
            TheHashinator.initialize(getHashinatorClass(), configBytes);
            // this will produce negative values, which is desired here.
            final long valueToHash = r.nextLong();
            final int javahash = TheHashinator.hashinateLong(valueToHash);
            final int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
            if (eehash != javahash) {
                System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
            }
            assertEquals(eehash, javahash);
            assertTrue(eehash < partitionCount);
            assertTrue(eehash > -1);
        }

        try { ee.release(); } catch (Exception e) {}
    }

    @Test
    public void testSameStringHash() {
        byte configBytes[] = getConfigBytes(1);
        ExecutionEngine ee =
                new ExecutionEngineJNI(
                        1,
                        1,
                        0,
                        0,
                        "",
                        100,
                        hashinatorType,
                        configBytes);

        for (int i = 0; i < 2500; i++) {
            int partitionCount = r.nextInt(1000) + 1;
            configBytes = getConfigBytes(partitionCount);
            String valueToHash = Long.toString(r.nextLong());
            TheHashinator.initialize(getHashinatorClass(), configBytes);

            int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
            int javahash = TheHashinator.hashinateString(valueToHash);
            if (eehash != javahash) {
                partitionCount++;
            }
            assertEquals(eehash, javahash);
            assertTrue(eehash < partitionCount);
            assertTrue(eehash >= 0);
        }

        try { ee.release(); } catch (Exception e) {}
    }

    @Test
    public void testNulls() {
        ExecutionEngine ee =
                new ExecutionEngineJNI(
                        1,
                        1,
                        0,
                        0,
                        "",
                        100,
                        hashinatorType,
                        getConfigBytes(2));
        final byte configBytes[] = getConfigBytes(2);
        TheHashinator.initialize(getHashinatorClass(), configBytes);
        int jHash = TheHashinator.hashToPartition(new Byte(VoltType.NULL_TINYINT));
        int cHash = ee.hashinate(VoltType.NULL_TINYINT, hashinatorType, configBytes);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.hashToPartition(new Short(VoltType.NULL_SMALLINT));
        cHash = ee.hashinate(VoltType.NULL_SMALLINT, hashinatorType, configBytes);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.hashToPartition(new Integer(VoltType.NULL_INTEGER));
        cHash = ee.hashinate(
                VoltType.NULL_INTEGER,
                hashinatorType,
                configBytes);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.hashToPartition(new Long(VoltType.NULL_BIGINT));
        cHash = ee.hashinate(
                VoltType.NULL_BIGINT,
                hashinatorType,
                configBytes);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.hashToPartition(VoltType.NULL_STRING_OR_VARBINARY);
        cHash = ee.hashinate(
                VoltType.NULL_STRING_OR_VARBINARY,
                hashinatorType,
                configBytes);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.hashToPartition(null);
        cHash = ee.hashinate(
                null,
                hashinatorType,
                configBytes);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        try { ee.release(); } catch (Exception e) {}
    }

    @Test
    public void testSameBytesHash() {
        ExecutionEngine ee =
                new ExecutionEngineJNI(
                        1,
                        1,
                        0,
                        0,
                        "",
                        100,
                        hashinatorType,
                        getConfigBytes(2));
        for (int i = 0; i < 2500; i++) {
            int partitionCount = r.nextInt(1000) + 1;
            byte[] valueToHash = new byte[r.nextInt(1000)];
            r.nextBytes(valueToHash);
            final byte configBytes[] = getConfigBytes(partitionCount);
            TheHashinator.initialize(getHashinatorClass(), configBytes);
            int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
            int javahash = TheHashinator.hashinateBytes(valueToHash);
            if (eehash != javahash) {
                partitionCount++;
            }
            assertTrue(eehash < partitionCount);
            assertTrue(eehash >= 0);
            assertEquals(eehash, javahash);
        }
        try { ee.release(); } catch (Exception e) {}
    }
}

