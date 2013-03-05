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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.cassandra_voltpatches.MurmurHash3;
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
        long valueToHash = hashinatorType == HashinatorType.ELASTIC ? 41 : 2;
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
                        getConfigBytes(6));
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

    private static class ConfigHolder {
        byte configBytes[];
        NavigableMap<Long, Integer> tokenToPartition = new TreeMap<Long, Integer>();
        Map<Long, Long> tokenToKeys = new HashMap<Long, Long>();
        long keyLessThanFirstToken;
        long keyGreaterThanLastToken;
    }

    /*
     * Generate a config where the key that hashes to the token is known
     * so we can check that the key maps to the partition at the token/key in
     * Java/C++
     */
    public ConfigHolder getConfigForElastic(int partitions) {
        ConfigHolder holder = new ConfigHolder();
        final int tokensPerPartition = 8;
        Random r = new Random();
        ByteBuffer buf = ByteBuffer.allocate(4 + (12 * partitions * tokensPerPartition));
        holder.configBytes = buf.array();
        buf.putInt(partitions * tokensPerPartition);

        for (int ii = 0; ii < partitions; ii++) {
            for (int zz = 0; zz < tokensPerPartition; zz++) {
                while (true) {
                    long candidateKey = r.nextLong();
                    long candidateToken = MurmurHash3.hash3_x64_128(candidateKey);
                    if (holder.tokenToPartition.containsKey(candidateToken)) {
                        continue;
                    }
                    buf.putLong(candidateToken);
                    buf.putInt(ii);
                    holder.tokenToPartition.put(candidateToken, ii);
                    holder.tokenToKeys.put(candidateToken, candidateKey);
                    break;
                }
            }
        }

        /*
         * Now generate a key that hashes to a value < the first token
         */
        final long firstToken = holder.tokenToPartition.firstKey();
        while (true) {
            long candidateKey = r.nextLong();
            ByteBuffer buf2 = ByteBuffer.allocate(8);
            buf2.order(ByteOrder.nativeOrder());
            buf2.putLong(candidateKey);
            long candidateToken = MurmurHash3.hash3_x64_128(buf2, 0, 8, 0);

            if (candidateToken < firstToken) {
                holder.keyLessThanFirstToken = candidateKey;
                break;
            }
        }

        /*
         * Generate a key that hashes to a value > the last token
         */
        final long lastToken = holder.tokenToPartition.lastKey();
        while (true) {
            long candidateKey = r.nextLong();
            ByteBuffer buf2 = ByteBuffer.allocate(8);
            buf2.order(ByteOrder.nativeOrder());
            buf2.putLong(candidateKey);
            long candidateToken = MurmurHash3.hash3_x64_128(buf2, 0, 8, 0);

            if (candidateToken > lastToken) {
                holder.keyGreaterThanLastToken = candidateKey;
                break;
            }
        }
        return holder;
    }
    /*
     * Test that a value that hashes to a token is placed at the partition of that token,
     * and that a value < the first token maps to the last token
     */
    @Test
    public void testHashOfToken() {
        if (hashinatorType == HashinatorType.LEGACY) return;
        ConfigHolder holder = getConfigForElastic(6);
        ExecutionEngine ee = new ExecutionEngineJNI(
                1,
                1,
                0,
                0,
                "",
                100,
                hashinatorType,
                holder.configBytes);

        TheHashinator.initialize(getHashinatorClass(), holder.configBytes);

        /*
         * Check that the first token - 1 hashes to the last token (wraps around)
         */
        final int lastPartition = holder.tokenToPartition.lastEntry().getValue();
        assertEquals(lastPartition, TheHashinator.hashToPartition(holder.keyLessThanFirstToken));
        assertEquals(lastPartition, ee.hashinate(holder.keyLessThanFirstToken, hashinatorType, holder.configBytes));

        /*
         * Check that hashing to the region beyond the last token also works
         */
        assertEquals(lastPartition, TheHashinator.hashToPartition(holder.keyGreaterThanLastToken));
        assertEquals(lastPartition, ee.hashinate(holder.keyGreaterThanLastToken, hashinatorType, holder.configBytes));

        /*
         * Check that keys that fall on tokens map to the token
         */
        for (Map.Entry<Long, Integer> e : holder.tokenToPartition.entrySet()) {
            final int partition = e.getValue();
            final long key = holder.tokenToKeys.get(e.getKey());
            assertEquals(partition, TheHashinator.hashToPartition(key));
            assertEquals(partition, ee.hashinate(key, hashinatorType, holder.configBytes));
        }

        try { ee.release(); } catch (Exception e) {}

    }

    @Test
    public void testElasticHashinatorPartitionMapping() {
        if (hashinatorType == HashinatorType.LEGACY) return;

        ByteBuffer buf = ByteBuffer.allocate(4 + (12 * 3));

        buf.putInt(3);
        buf.putLong(Long.MIN_VALUE);
        buf.putInt(0);
        buf.putLong(0);
        buf.putInt(1);
        buf.putLong(Long.MAX_VALUE);
        buf.putInt(2);

        ElasticHashinator hashinator = new ElasticHashinator(buf.array());

        assertEquals( 0, hashinator.partitionForToken(Long.MIN_VALUE));
        assertEquals( 0, hashinator.partitionForToken(Long.MIN_VALUE + 1));

        assertEquals( 1, hashinator.partitionForToken(0));
        assertEquals( 1, hashinator.partitionForToken(1));

        assertEquals( 2, hashinator.partitionForToken(Long.MAX_VALUE));
        assertEquals( 1, hashinator.partitionForToken(Long.MAX_VALUE - 1));

        buf.clear();
        buf.putInt(3);
        buf.putLong(Long.MIN_VALUE + 1);
        buf.putInt(0);
        buf.putLong(0);
        buf.putInt(1);
        buf.putLong(Long.MAX_VALUE - 1);
        buf.putInt(2);

        hashinator = new ElasticHashinator(buf.array());

        assertEquals( 2, hashinator.partitionForToken(Long.MIN_VALUE));
        assertEquals( 0, hashinator.partitionForToken(Long.MIN_VALUE + 1));

        assertEquals( 1, hashinator.partitionForToken(0));
        assertEquals( 1, hashinator.partitionForToken(1));

        assertEquals( 2, hashinator.partitionForToken(Long.MAX_VALUE));
        assertEquals( 2, hashinator.partitionForToken(Long.MAX_VALUE - 1));
    }

}

