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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.voltcore.utils.InstanceId;
import org.voltcore.utils.Pair;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.sysprocs.saverestore.HashinatorSnapshotData;

/**
 * This test verifies that the Java Hashinator behaves
 * identically as the C++ Hashinator.
 *
 */
public class TestTheHashinator {
    Random r = new Random();

    @Before
    public void setUp() {
        ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION = 6;
        EELibraryLoader.loadExecutionEngineLibrary(true);
        VoltDB.instance().readBuildInfo("Test");
    }

    private final HashinatorType hashinatorType = TheHashinator.getConfiguredHashinatorType();
    private final int tokensPerPartition = 6;


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

    private static Map<Long, Long> getRangesAndCheck(int partitionCount, int partitionToCheck)
    {
        ElasticHashinator hashinator =
            new ElasticHashinator(ElasticHashinator.getConfigureBytes(partitionCount,
                                                                      ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION),
                                                                      false);
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

        return range1;
    }

    /**
     * Compare the tokens of the covered partitions before and after adding new partitions to
     * the hashinator. The tokens should match.
     * @param beforePartitionCount    Partition count before adding new partitions
     * @param afterPartitionCount     Partition count after adding new partitions
     */
    private static void checkRangesAfterExpansion(int beforePartitionCount, int afterPartitionCount) {
        for (int i = 0; i < beforePartitionCount; i++) {
            Map<Long, Long> oldrange = getRangesAndCheck(beforePartitionCount, i);
            Map<Long, Long> newrange = getRangesAndCheck(afterPartitionCount, i);

            // Only compare the begin tokens, end tokens are determined by the successors
            assertEquals(oldrange.keySet(), newrange.keySet());
        }
    }

    /*
     * This test validates that not all values hash to 0. Most of the other
     * tests will pass even if everything hashes to a single partition.
     */
    @Test
    public void testExpectNonZeroHash() throws Exception {
        final byte configBytes[] = TheHashinator.getConfigureBytes(3);
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
        long valueToHash = hashinatorType == HashinatorType.ELASTIC ? 39: 2;
        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), configBytes);

        int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        int javahash = TheHashinator.getPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(),
                valueToHash);
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
    public void testSameLongHash1() throws Exception {
        final byte configBytes[] = TheHashinator.getConfigureBytes(2);
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
        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), TheHashinator.getConfigureBytes(partitionCount));

        long valueToHash = 0;
        int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        int javahash = TheHashinator.getPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(),
                valueToHash);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        valueToHash = 1;
        eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        javahash = TheHashinator.getPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(),
                valueToHash);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        valueToHash = 2;
        eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        javahash = TheHashinator.getPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(),
                valueToHash);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        valueToHash = 3;
        eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
        javahash = TheHashinator.getPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(),
                valueToHash);
        if (eehash != javahash) {
            System.out.printf("Hash of %d with %d partitions => EE: %d, Java: %d\n", valueToHash, partitionCount, eehash, javahash);
        }
        assertEquals(eehash, javahash);
        assertTrue(eehash < partitionCount);
        assertTrue(eehash >= 0);

        try { ee.release(); } catch (Exception e) {}
    }

    @Test
    public void testEdgeCases() throws Exception {
        byte configBytes[] = TheHashinator.getConfigureBytes(1);
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

        /**
         *  Run with 100k of random values and make sure C++ and Java hash to
         *  the same value.
         */
        for (int i = 0; i < 5; i++) {
            int partitionCount = r.nextInt(1000) + 1;
            long[] values = new long[] {
                    Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MIN_VALUE + 1
            };
            configBytes = TheHashinator.getConfigureBytes(partitionCount);
            TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), configBytes);
            for (long valueToHash : values) {
                int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
                int javahash = TheHashinator.getPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(),
                        valueToHash);
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
    public void testSameLongHash() throws Exception {
        byte configBytes[] = TheHashinator.getConfigureBytes(1);
        ExecutionEngine ee = new ExecutionEngineJNI(1, 1, 0, 0, "", 100, hashinatorType, configBytes);

        /**
         *  Run with 10k of random values and make sure C++ and Java hash to
         *  the same value.
         */
        for (int i = 0; i < 1500; i++) {
            final int partitionCount = r.nextInt(1000) + 1;
            configBytes = TheHashinator.getConfigureBytes(partitionCount);
            TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), configBytes);
            // this will produce negative values, which is desired here.
            final long valueToHash = r.nextLong();
            final int javahash = TheHashinator.getPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(),
                    valueToHash);
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
    public void testSameStringHash() throws Exception {
        byte configBytes[] = TheHashinator.getConfigureBytes(1);
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

        for (int i = 0; i < 1500; i++) {
            int partitionCount = r.nextInt(1000) + 1;
            configBytes = TheHashinator.getConfigureBytes(partitionCount);
            String valueToHash = Long.toString(r.nextLong());
            TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), configBytes);

            int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
            int javahash = TheHashinator.getPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(),
                    valueToHash);
            if (eehash != javahash) {
                partitionCount++;
            }
            assertEquals(eehash, javahash);
            assertTrue(eehash < partitionCount);
            assertTrue(eehash >= 0);
        }

        try { ee.release(); } catch (Exception e) {}
    }

    // We coerce string representations of numeric types into that numeric type.
    // Verify this works.  We also, for legacy, coerce byte arrays of numeric types
    // into that type.  Verify this at the same time.
    @Test
    public void testNumberCoercionHash() throws Exception {
        System.out.println("=======================");
        System.out.println("NUMBER COERCION");
        for (int i = 0; i < 1500; i++) {
            int partitionCount = r.nextInt(1000) + 1;
            byte[] configBytes = TheHashinator.getConfigureBytes(partitionCount);
            long longToHash = r.nextLong();
            String stringToHash = Long.toString(longToHash);
            byte[] bufToHash;
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            buf.putLong(longToHash);
            bufToHash = buf.array();
            TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), configBytes);
            int longHash =
                TheHashinator.getPartitionForParameter(VoltType.BIGINT.getValue(),
                    longToHash);
            int stringHash =
                TheHashinator.getPartitionForParameter(VoltType.BIGINT.getValue(),
                    stringToHash);
            int bufHash =
                TheHashinator.getPartitionForParameter(VoltType.BIGINT.getValue(),
                    bufToHash);
            assertEquals(longHash, stringHash);
            assertEquals(stringHash, bufHash);
            assertTrue(longHash < partitionCount);
            assertTrue(longHash >= 0);
        }
    }

    @Test
    public void testNulls() throws Exception {
        ExecutionEngine ee =
                new ExecutionEngineJNI(
                        1,
                        1,
                        0,
                        0,
                        "",
                        100,
                        hashinatorType,
                        TheHashinator.getConfigureBytes(2));
        final byte configBytes[] = TheHashinator.getConfigureBytes(2);
        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), configBytes);
        int jHash =
            TheHashinator.getPartitionForParameter(VoltType.TINYINT.getValue(), new Byte(VoltType.NULL_TINYINT));
        int cHash = ee.hashinate(VoltType.NULL_TINYINT, hashinatorType, configBytes);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.getPartitionForParameter(VoltType.SMALLINT.getValue(),
                new Short(VoltType.NULL_SMALLINT));
        cHash = ee.hashinate(VoltType.NULL_SMALLINT, hashinatorType, configBytes);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.getPartitionForParameter(VoltType.INTEGER.getValue(),
                new Integer(VoltType.NULL_INTEGER));
        cHash = ee.hashinate(
                VoltType.NULL_INTEGER,
                hashinatorType,
                configBytes);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.getPartitionForParameter(VoltType.BIGINT.getValue(), new Long(VoltType.NULL_BIGINT));
        cHash = ee.hashinate(
                VoltType.NULL_BIGINT,
                hashinatorType,
                configBytes);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.getPartitionForParameter(VoltType.STRING.getValue(),
                VoltType.NULL_STRING_OR_VARBINARY);
        cHash = ee.hashinate(
                VoltType.NULL_STRING_OR_VARBINARY,
                hashinatorType,
                configBytes);
        assertEquals(0, jHash);
        assertEquals(jHash, cHash);
        System.out.println("jhash " + jHash + " chash " + cHash);

        jHash = TheHashinator.getPartitionForParameter(VoltType.VARBINARY.getValue(), null);
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
    public void testSameBytesHash() throws Exception {
        ExecutionEngine ee =
                new ExecutionEngineJNI(
                        1,
                        1,
                        0,
                        0,
                        "",
                        100,
                        hashinatorType,
                        TheHashinator.getConfigureBytes(6));
        for (int i = 0; i < 2500; i++) {
            int partitionCount = r.nextInt(1000) + 1;
            byte[] valueToHash = new byte[r.nextInt(1000)];
            r.nextBytes(valueToHash);
            final byte configBytes[] = TheHashinator.getConfigureBytes(partitionCount);
            TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), configBytes);
            int eehash = ee.hashinate(valueToHash, hashinatorType, configBytes);
            int javahash = TheHashinator.getPartitionForParameter(VoltType.typeFromClass(byte[].class).getValue(),
                    valueToHash);
            if (eehash != javahash) {
                partitionCount++;
            }
            assertTrue(eehash < partitionCount);
            assertTrue(eehash >= 0);
            assertEquals(eehash, javahash);
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

        ElasticHashinator hashinator = new ElasticHashinator(buf.array(), false);

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

        hashinator = new ElasticHashinator(buf.array(), false);

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
                ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION), false);

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
        byte[] newConfig = ElasticHashinator.addPartitions(new ElasticHashinator(config, false),
                                                           Arrays.asList(3, 4, 5),
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

        ElasticHashinator hashinator = new ElasticHashinator(TheHashinator.getCurrentConfig().getSecond(), false);
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
        byte[] newConfig = ElasticHashinator.addPartitions(new ElasticHashinator(config, false),
                                                           Arrays.asList(3, 4, 5),
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

        ElasticHashinator hashinator = new ElasticHashinator(TheHashinator.getCurrentConfig().getSecond(), false);
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
                ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION), false);

        // Add 3 partitions in a batch to the original hashinator
        byte[] batchAddConfig = ElasticHashinator.addPartitions(hashinator, Arrays.asList(3, 4, 5),
                                                                ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION);
        Map<Long, Integer> batchAddTokens = deserializeElasticConfig(batchAddConfig);

        // Add the same 3 partitions one at a time to the original hashinator
        byte[] add3Config = ElasticHashinator.addPartitions(hashinator, Arrays.asList(3),
                                                            ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION);
        ElasticHashinator add3Hashinator = new ElasticHashinator(add3Config, false);
        byte[] add4Config = ElasticHashinator.addPartitions(add3Hashinator, Arrays.asList(4),
                                                            ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION);
        ElasticHashinator add4Hashinator = new ElasticHashinator(add4Config, false);
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

    @Test
    public void testElasticExpansionDeterminism()
    {
        if (hashinatorType == HashinatorType.LEGACY) return;

        checkRangesAfterExpansion(/* beforePartitionCount = */ 2, /* afterPartitionCount = */ 6);
        checkRangesAfterExpansion(/* beforePartitionCount = */ 21, /* afterPartitionCount = */ 28);
        checkRangesAfterExpansion(/* beforePartitionCount = */ 24, /* afterPartitionCount = */ 48);
    }

    @Test
    public void testGetConfigurationSignature()
    {
        final byte configBytes[] = TheHashinator.getConfigureBytes(2);
        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), configBytes);


        long expected = TheHashinator.computeConfigurationSignature(configBytes);
        assertEquals(expected, TheHashinator.getConfigurationSignature());
    }

    @Test
    public void testSaveRestoreRaw() throws Exception
    {
        if (hashinatorType == HashinatorType.LEGACY) return;

        ElasticHashinator h1 = new ElasticHashinator(
                ElasticHashinator.getConfigureBytes(3,
                ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION), false);
        byte[] bytes = h1.getConfigBytes();
        HashinatorSnapshotData d1 = new HashinatorSnapshotData(bytes, 1234);
        InstanceId iid1 = new InstanceId(111, 222);
        ByteBuffer b1 = d1.saveToBuffer(iid1);
        ByteBuffer b2 = ByteBuffer.wrap(b1.array());
        HashinatorSnapshotData d2 = new HashinatorSnapshotData();
        InstanceId iid2 = d2.restoreFromBuffer(b2);
        assertEquals(iid1, iid2);
        assertTrue(Arrays.equals(d1.m_serData, d2.m_serData));
        ElasticHashinator h2 = new ElasticHashinator(d2.m_serData, false);
        assertEquals(h1.getTokens(), h2.getTokens());
    }

    @Test
    public void testSaveRestoreCooked() throws Exception
    {
        if (hashinatorType == HashinatorType.LEGACY) return;

        ElasticHashinator h1 = new ElasticHashinator(
                ElasticHashinator.getConfigureBytes(3,
                ElasticHashinator.DEFAULT_TOKENS_PER_PARTITION), false);
        byte[] b1 = h1.getCookedBytes();
        ElasticHashinator h2 = new ElasticHashinator(b1, true);
        byte[] b2 = h2.getCookedBytes();
        assertTrue(Arrays.equals(b1, b2));
        assertEquals(h1.getTokens(), h2.getTokens());
    }
}
