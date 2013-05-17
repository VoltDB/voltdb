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
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.cassandra_voltpatches.MurmurHash3;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.voltcore.utils.Pair;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineJNI;

import static org.junit.Assert.*;

/**
 * This test verifies that the Java Hashinator behaves
 * identically as the C++ Hashinator.
 *
 */
@RunWith(Parameterized.class)
public class TestTheHashinator {
    Random r = new Random();

    @Before
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

    private static Map<Long, Integer> deserializeElasticConfig(byte[] config) {
        Map<Long, Integer> tokens = new HashMap<Long, Integer>();
        ByteBuffer buf = ByteBuffer.wrap(config);
        int count = buf.getInt();

        for (int i = 0; i < count; i++) {
            tokens.put(buf.getLong(), buf.getInt());
        }

        return tokens;
    }

    /** make sure that every range hashes to partition */
    private static void checkRangeBoundaries(ElasticHashinator hashinator,
                                             int partition,
                                             Map<Long, Long> range)
    {
        for (Map.Entry<Long, Long> entry : range.entrySet()) {
            long start = entry.getKey();
            long end = entry.getValue();
            assertEquals(partition, hashinator.partitionForToken(start));
            if (end != Long.MIN_VALUE) {
                assertEquals(partition, hashinator.partitionForToken(end - 1));
            } else {
                assertEquals(partition, hashinator.partitionForToken(Long.MAX_VALUE));
            }
        }
    }

    /** make sure that every token for partition appears in the range map */
    private static void checkTokensInRanges(ElasticHashinator hashinator,
                                            int partition,
                                            Map<Long, Long> range)
    {
        byte[] config = hashinator.pGetCurrentConfig().getSecond();
        Map<Long, Integer> tokens = deserializeElasticConfig(config);
        for (Map.Entry<Long, Integer> entry : tokens.entrySet()) {
            long token = entry.getKey();
            int pid = entry.getValue();

            if (pid == partition) {
                boolean foundRange = false;
                for (Map.Entry<Long, Long> rangeEntry : range.entrySet()) {
                    long start = rangeEntry.getKey();
                    long end = rangeEntry.getValue();
                    if (start <= token && token < end) {
                        foundRange = true;
                        break;
                    } else if (end < start && (start <= token || token < end)) {
                        // Boundary case, [start, end) wraps around the origin
                        foundRange = true;
                        break;
                    }
                }

                assertTrue(foundRange);
            }
        }
    }

    private static void getRangesAndCheck(int partitionCount, int partitionToCheck)
    {
        ElasticHashinator hashinator =
            new ElasticHashinator(ElasticHashinator.getConfigureBytes(partitionCount,
                                                                      ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION));
        Map<Long, Long> range1 = hashinator.pGetRanges(partitionToCheck);
        assertEquals(ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION, range1.size());

        // make sure that the returned map is sorted
        long previous = Long.MIN_VALUE;
        for (long k : range1.keySet()) {
            assertTrue(k >= previous);
            previous = k;
        }

        checkRangeBoundaries(hashinator, partitionToCheck, range1);
        checkTokensInRanges(hashinator, partitionToCheck, range1);

        // non-existing partition should have an empty range
        assertTrue(hashinator.pGetRanges(partitionCount + 1).isEmpty());
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
                        configBytes,
                        null);

        int partitionCount = 3;
        long valueToHash = hashinatorType == HashinatorType.ELASTIC ? 41 : 2;
        TheHashinator.initialize(getHashinatorClass(), configBytes);

        int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        int javahash = TheHashinator.hashToPartition(valueToHash);
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
                        configBytes,
                        null);

        int partitionCount = 2;
        TheHashinator.initialize(getHashinatorClass(), getConfigBytes(partitionCount));

        long valueToHash = 0;
        int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        int javahash = TheHashinator.hashToPartition(valueToHash);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        valueToHash = 1;
        eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        javahash = TheHashinator.hashToPartition(valueToHash);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        valueToHash = 2;
        eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        javahash = TheHashinator.hashToPartition(valueToHash);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        valueToHash = 3;
        eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        javahash = TheHashinator.hashToPartition(valueToHash);
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
                        hashinatorType,
                        configBytes,
                        null);

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
                int javahash = TheHashinator.hashToPartition(valueToHash);
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
        ExecutionEngine ee = new ExecutionEngineJNI(1, 1, 0, 0, "", 100, hashinatorType, configBytes, null);

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
            final int javahash = TheHashinator.hashToPartition(valueToHash);
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
                        configBytes,
                        null);

        for (int i = 0; i < 2500; i++) {
            int partitionCount = r.nextInt(1000) + 1;
            configBytes = getConfigBytes(partitionCount);
            String valueToHash = Long.toString(r.nextLong());
            TheHashinator.initialize(getHashinatorClass(), configBytes);

            int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
            int javahash = TheHashinator.hashToPartition(valueToHash);
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
                        getConfigBytes(2),
                        null);
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
                        getConfigBytes(6),
                        null);
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
                holder.configBytes,
                null);

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

    @Test
    public void testElasticAddPartitions() {
        if (hashinatorType == HashinatorType.LEGACY) return;

        ElasticHashinator hashinator = new ElasticHashinator(ElasticHashinator.getConfigureBytes(3,
                ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION));

        byte[] newConfig = ElasticHashinator.addPartitions(hashinator, Arrays.asList(3, 4, 5),
                ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION);

        Map<Long, Integer> oldTokens = deserializeElasticConfig(hashinator.pGetCurrentConfig().getSecond());
        Map<Long, Integer> newTokens = deserializeElasticConfig(newConfig);

        for (Map.Entry<Long, Integer> entry : oldTokens.entrySet()) {
            assertEquals(entry.getValue(), newTokens.get(entry.getKey()));
        }

        Map<Integer, Integer> newPidCounts = new HashMap<Integer, Integer>();
        for (Map.Entry<Long, Integer> entry : newTokens.entrySet()) {
            switch (entry.getValue()) {
            case 3:
            case 4:
            case 5:
                Integer count = newPidCounts.get(entry.getValue());
                if (count == null) {
                    count = 0;
                }
                newPidCounts.put(entry.getValue(), ++count);
            }
        }

        assertEquals(3, newPidCounts.size());
        assertEquals(ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION, (int) newPidCounts.get(3));
        assertEquals(ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION, (int) newPidCounts.get(4));
        assertEquals(ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION, (int) newPidCounts.get(5));
    }

    @Test
    public void testElasticPredecessors() {
        if (hashinatorType == HashinatorType.LEGACY) return;

        byte[] config = ElasticHashinator.getConfigureBytes(3,
                                                            ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION);
        byte[] newConfig = ElasticHashinator.addPartitions(new ElasticHashinator(config), Arrays.asList(3, 4, 5),
                                                           ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION);
        TheHashinator.initialize(HashinatorType.ELASTIC.hashinatorClass, newConfig);
        Map<Long, Integer> newTokens = deserializeElasticConfig(newConfig);
        Set<Long> tokensForP4 = new HashSet<Long>();
        Map<Long, Integer> tokensToPredecessors = TheHashinator.predecessors(4);
        Set<Integer> predecessors = new HashSet<Integer>(tokensToPredecessors.values());
        // Predecessor set shouldn't contain the partition itself
        assertFalse(predecessors.contains(4));

        for (Map.Entry<Long, Integer> entry : newTokens.entrySet()) {
            if (tokensToPredecessors.containsKey(entry.getKey())) {
                assertEquals(entry.getValue(), tokensToPredecessors.get(entry.getKey()));
            }
            if (entry.getValue() == 4) {
                tokensForP4.add(entry.getKey());
            }
        }
        assertEquals(ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION, tokensForP4.size());

        ElasticHashinator hashinator = new ElasticHashinator(TheHashinator.getCurrentConfig().getSecond());
        for (long token : tokensForP4) {
            int pid;
            if (token != Long.MIN_VALUE) {
                pid = hashinator.partitionForToken(token - 1);
            } else {
                pid = hashinator.partitionForToken(Long.MAX_VALUE);
            }
            predecessors.remove(pid);
        }
        assertEquals(0, predecessors.size());
    }

    @Test
    public void testElasticPredecessor() {
        if (hashinatorType == HashinatorType.LEGACY) return;

        byte[] config = ElasticHashinator.getConfigureBytes(3,
                                                            ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION);
        byte[] newConfig = ElasticHashinator.addPartitions(new ElasticHashinator(config), Arrays.asList(3, 4, 5),
                                                           ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION);
        TheHashinator.initialize(HashinatorType.ELASTIC.hashinatorClass, newConfig);
        Map<Long, Integer> predecessors = TheHashinator.predecessors(4);

        // pick the first predecessor
        Map.Entry<Long, Integer> predecessor = predecessors.entrySet().iterator().next();
        // if token and partition doesn't match, it should throw
        try {
            TheHashinator.predecessor(predecessor.getValue(), predecessor.getKey() - 1);
            fail();
        } catch (Exception e) {}
        Pair<Long, Integer> prevPredecessor = TheHashinator.predecessor(predecessor.getValue(), predecessor.getKey());
        assertNotNull(prevPredecessor);

        ElasticHashinator hashinator = new ElasticHashinator(TheHashinator.getCurrentConfig().getSecond());
        assertEquals(prevPredecessor.getSecond().intValue(), hashinator.partitionForToken(prevPredecessor.getFirst()));
        // check if predecessor's token - 1 belongs to the previous predecessor
        if (predecessor.getKey() != Long.MIN_VALUE) {
            assertEquals(prevPredecessor.getSecond().intValue(), hashinator.partitionForToken(predecessor.getKey() - 1));
        } else {
            assertEquals(prevPredecessor.getSecond().intValue(), hashinator.partitionForToken(Long.MAX_VALUE));
        }
    }

    @Test
    public void testElasticAddPartitionDeterminism() {
        if (hashinatorType == HashinatorType.LEGACY) return;

        ElasticHashinator hashinator = new ElasticHashinator(ElasticHashinator.getConfigureBytes(3,
                ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION));

        // Add 3 partitions in a batch to the original hashinator
        byte[] batchAddConfig = ElasticHashinator.addPartitions(hashinator, Arrays.asList(3, 4, 5),
                                                                ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION);
        Map<Long, Integer> batchAddTokens = deserializeElasticConfig(batchAddConfig);

        // Add the same 3 partitions one at a time to the original hashinator
        byte[] add3Config = ElasticHashinator.addPartitions(hashinator, Arrays.asList(3),
                                                            ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION);
        ElasticHashinator add3Hashinator = new ElasticHashinator(add3Config);
        byte[] add4Config = ElasticHashinator.addPartitions(add3Hashinator, Arrays.asList(4),
                                                            ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION);
        ElasticHashinator add4Hashinator = new ElasticHashinator(add4Config);
        byte[] seqAddConfig = ElasticHashinator.addPartitions(add4Hashinator, Arrays.asList(5),
                                                              ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION);
        Map<Long, Integer> seqAddTokens = deserializeElasticConfig(seqAddConfig);

        // The two approaches should produce the same hash ring
        assertFalse(seqAddTokens.isEmpty());
        assertTrue(seqAddTokens.values().contains(3));
        assertTrue(seqAddTokens.values().contains(4));
        assertTrue(seqAddTokens.values().contains(5));
        assertEquals(batchAddTokens, seqAddTokens);
    }

    @Test
    public void testElasticGetRanges() {
        if (hashinatorType == HashinatorType.LEGACY) return;

        getRangesAndCheck(/* partitionCount = */ 2,  /* partitionToCheck = */ 1);
        getRangesAndCheck(/* partitionCount = */ 24, /* partitionToCheck = */ 15);
    }
}

