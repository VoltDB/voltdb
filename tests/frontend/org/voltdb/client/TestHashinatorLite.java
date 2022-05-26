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

package org.voltdb.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

import junit.framework.TestCase;

import org.junit.Test;
import org.voltdb.ElasticHashinator;
import org.voltdb.TheHashinator;
import org.voltdb.VoltType;

/**
 * This test verifies that the Java Hashinator behaves
 * identically as the C++ Hashinator.
 *
 */
public class TestHashinatorLite extends TestCase {
    Random r = new Random();

    /*
     * This test validates that not all values hash to 0. Most of the other
     * tests will pass even if everything hashes to a single partition.
     */
    @Test
    public void testExpectNonZeroHash() throws Exception {
        int partitionCount = 3;
        byte[] configBytes;
        HashinatorLite h1;
        TheHashinator h2;

        configBytes = ElasticHashinator.getConfigureBytes(partitionCount, ElasticHashinator.DEFAULT_TOTAL_TOKENS);
        h1 = new HashinatorLite(configBytes, false);
        h2 = TheHashinator.getHashinator(ElasticHashinator.class, configBytes, false);
        tandemTestExpectNonZeroHash(h1, h2, partitionCount);
    }

    private void tandemTestExpectNonZeroHash(HashinatorLite h1, TheHashinator h2, int partitionCount) throws Exception {
        long valueToHash = 39;

        int hash1 = h1.getHashedPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(), valueToHash);
        int hash2 = h2.getHashedPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(), valueToHash);

        if (hash1 != hash2) {
            System.out.printf("Hash of %d with %d partitions => Lite: %d, Std: %d\n", valueToHash, partitionCount, hash1, hash2);
        }
        assertEquals(hash1, hash2);
        assertNotSame(0, hash1);
        assertTrue(hash1 < partitionCount);
        assertTrue(hash1 >= 0);
    }

    @Test
    public void testSameLongHash1() throws Exception {
        int partitionCount = 2;
        byte[] configBytes;
        HashinatorLite h1;
        TheHashinator h2;

        configBytes = ElasticHashinator.getConfigureBytes(partitionCount, ElasticHashinator.DEFAULT_TOTAL_TOKENS);
        h1 = new HashinatorLite(configBytes, false);
        h2 = TheHashinator.getHashinator(ElasticHashinator.class, configBytes, false);
        tandemTestSameLongHash1(h1, h2, partitionCount);
    }

    private void tandemTestSameLongHash1(HashinatorLite h1, TheHashinator h2, int partitionCount) throws Exception {
        for (long valueToHash : new long[] {0, 1, 2, 3}) {
            byte type = VoltType.typeFromObject(valueToHash).getValue();
            int hash1 = h1.getHashedPartitionForParameter(type, valueToHash);
            int hash2 = h2.getHashedPartitionForParameter(type, valueToHash);
            if (hash1 != hash2) {
                System.out.printf("Hash of %d with %d partitions => Lite: %d, Std: %d\n",
                        valueToHash, partitionCount, hash1, hash2);
            }
            assertEquals(hash1, hash2);
            assertTrue(hash1 < partitionCount);
            assertTrue(hash1 >= 0);
        }
    }

    @Test
    public void testSizeChanges() {
        int partitionCount;
        byte[] configBytes;
        HashinatorLite h1;
        TheHashinator h2;

        // try with lots of partition counts
        for (partitionCount = 1; partitionCount <= 11; partitionCount++) {
            configBytes = ElasticHashinator.getConfigureBytes(partitionCount, ElasticHashinator.DEFAULT_TOTAL_TOKENS);
            h1 = new HashinatorLite(configBytes, false);
            h2 = TheHashinator.getHashinator(ElasticHashinator.class, configBytes, false);
            tandemTestSizeChanges(h1, h2, partitionCount);
        }
    }

    private void tandemTestSizeChanges(HashinatorLite h1, TheHashinator h2, int partitionCount) {
        int hash1, hash2;

        // use a short value hashed as a long type
        for (short valueToHash = -7; valueToHash <= 7; valueToHash++) {
            byte type = VoltType.typeFromObject(valueToHash).getValue();
            hash1 = h1.getHashedPartitionForParameter(type, valueToHash);
            hash2 = h2.getHashedPartitionForParameter(type, valueToHash);
            if (hash1 != hash2) {
                System.out.printf("Hash of %d with %d partitions => Lite: %d, Std: %d\n", valueToHash, partitionCount, hash1, hash2);
            }
            assertEquals(hash1, hash2);
            assertTrue(hash1 < partitionCount);
            assertTrue(hash1 >= 0);
        }

        // use a long value hashed as a short type
        for (long valueToHash = -7; valueToHash <= 7; valueToHash++) {
            byte type = VoltType.typeFromObject(valueToHash).getValue();
            hash1 = h1.getHashedPartitionForParameter(type, valueToHash);
            hash2 = h2.getHashedPartitionForParameter(type, valueToHash);
            if (hash1 != hash2) {
                System.out.printf("Hash of %d with %d partitions => Lite: %d, Std: %d\n", valueToHash, partitionCount, hash1, hash2);
            }
            assertEquals(hash1, hash2);
            assertTrue(hash1 < partitionCount);
            assertTrue(hash1 >= 0);
        }
    }

    @Test
    public void testEdgeCases() throws Exception {
        int partitionCount;
        byte[] configBytes;
        HashinatorLite h1;
        TheHashinator h2;

        // try with lots of partition counts
        for (int i = 0; i < 50; i++) {
            partitionCount = r.nextInt(1000) + 1;

            configBytes = ElasticHashinator.getConfigureBytes(partitionCount, ElasticHashinator.DEFAULT_TOTAL_TOKENS);
            h1 = new HashinatorLite(configBytes, false);
            h2 = TheHashinator.getHashinator(ElasticHashinator.class, configBytes, false);
            tandemTestEdgeCases(h1, h2, partitionCount);
        }
    }

    private void tandemTestEdgeCases(HashinatorLite h1, TheHashinator h2, int partitionCount) throws Exception {
        //
        //  Run with 100k of random values and make sure C++ and Java hash to
        //  the same value.
        //
        long[] values = new long[] {
                Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MIN_VALUE + 1
        };
        for (long valueToHash : values) {
            int hash1 = h1.getHashedPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(), valueToHash);
            int hash2 = h2.getHashedPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(), valueToHash);
            if (hash1 != hash2) {
                System.out.printf("Hash of %d with %d partitions => Lite: %d, Std: %d\n", valueToHash, partitionCount, hash1, hash2);
            }
            assertEquals(hash1, hash2);
            assertTrue(hash1 < partitionCount);
            assertTrue(hash1 >= 0);
        }
    }

    @Test
    public void testSameLongHash() throws Exception {
        int partitionCount;
        byte[] configBytes;
        HashinatorLite h1;
        TheHashinator h2;

        // try with lots of partition counts
        for (int i = 0; i < 50; i++) {
            partitionCount = r.nextInt(1000) + 1;
            configBytes = ElasticHashinator.getConfigureBytes(partitionCount, ElasticHashinator.DEFAULT_TOTAL_TOKENS);
            h1 = new HashinatorLite(configBytes, false);
            h2 = TheHashinator.getHashinator(ElasticHashinator.class, configBytes, false);
            tandemTestSameLongHash(h1, h2, partitionCount);
        }
    }

    private void tandemTestSameLongHash(HashinatorLite h1, TheHashinator h2, int partitionCount) throws Exception {
        //
        //  Run with 10k of random values and make sure C++ and Java hash to
        //  the same value.
        //

        // this will produce negative values, which is desired here.
        final long valueToHash = r.nextLong();

        int hash1 = h1.getHashedPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(), valueToHash);
        int hash2 = h2.getHashedPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(), valueToHash);
        if (hash1 != hash2) {
            System.out.printf("Hash of %d with %d partitions => Lite: %d, Std: %d\n", valueToHash, partitionCount, hash1, hash2);
        }
        assertEquals(hash1, hash2);
        assertTrue(hash1 < partitionCount);
        assertTrue(hash1 >= 0);
    }

    @Test
    public void testSameStringHash() throws Exception {
        int partitionCount;
        byte[] configBytes;
        HashinatorLite h1;
        TheHashinator h2;

        // try with lots of partition counts
        for (int i = 0; i < 50; i++) {
            partitionCount = r.nextInt(1000) + 1;
            configBytes = ElasticHashinator.getConfigureBytes(partitionCount, ElasticHashinator.DEFAULT_TOTAL_TOKENS);
            h1 = new HashinatorLite(configBytes, false);
            h2 = TheHashinator.getHashinator(ElasticHashinator.class, configBytes, false);
            tandemTestSameStringHash(h1, h2, partitionCount);
        }
    }

    private void tandemTestSameStringHash(HashinatorLite h1, TheHashinator h2, int partitionCount) throws Exception {
        String valueToHash = Long.toString(r.nextLong());

        int hash1 = h1.getHashedPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(), valueToHash);
        int hash2 = h2.getHashedPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(), valueToHash);
        if (hash1 != hash2) {
            System.out.printf("Hash of %d with %d partitions => Lite: %d, Std: %d\n", valueToHash, partitionCount, hash1, hash2);
        }
        assertEquals(hash1, hash2);
        assertTrue(hash1 < partitionCount);
        assertTrue(hash1 >= 0);
    }

    // We coerce string representations of numeric types into that numeric type.
    // Verify this works.  We also, for legacy, coerce byte arrays of numeric types
    // into that type.  Verify this at the same time.
    @Test
    public void testNumberCoercionHash() throws Exception {
        int partitionCount;
        byte[] configBytes;
        HashinatorLite h1;

        System.out.println("=======================");
        System.out.println("NUMBER COERCION");

        // try with lots of partition counts
        for (int i = 0; i < 50; i++) {
            partitionCount = r.nextInt(1000) + 1;

            configBytes = ElasticHashinator.getConfigureBytes(partitionCount, ElasticHashinator.DEFAULT_TOTAL_TOKENS);
            h1 = new HashinatorLite(configBytes, false);
            tandemTestNumberCoercionHash(h1, partitionCount);
        }
    }

    private void tandemTestNumberCoercionHash(HashinatorLite h1, int partitionCount) throws Exception {
        long longToHash = r.nextLong();

        String stringToHash = Long.toString(longToHash);

        byte[] bufToHash;
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(longToHash);
        bufToHash = buf.array();

        int longHash = h1.getHashedPartitionForParameter(VoltType.BIGINT.getValue(), longToHash);
        int stringHash = h1.getHashedPartitionForParameter(VoltType.BIGINT.getValue(), stringToHash);
        int bufHash = h1.getHashedPartitionForParameter(VoltType.BIGINT.getValue(), bufToHash);

        assertEquals(longHash, stringHash);
        assertEquals(stringHash, bufHash);
        assertTrue(longHash < partitionCount);
        assertTrue(longHash >= 0);
    }

    @Test
    public void testNulls() throws Exception {
        int partitionCount = 3;
        byte[] configBytes;
        HashinatorLite h1;
        TheHashinator h2;
        configBytes = ElasticHashinator.getConfigureBytes(partitionCount, ElasticHashinator.DEFAULT_TOTAL_TOKENS);
        h1 = new HashinatorLite(configBytes, false);
        h2 = TheHashinator.getHashinator(ElasticHashinator.class, configBytes, false);
        tandemTestNulls(h1, h2, partitionCount);
    }

    private void tandemTestNulls(HashinatorLite h1, TheHashinator h2, int partitionCount) throws Exception {
        VoltType hashableTypes[] = new VoltType[]{ VoltType.TINYINT, VoltType.SMALLINT,
                VoltType.INTEGER, VoltType.BIGINT, VoltType.STRING, VoltType.VARBINARY};
        for (VoltType type : hashableTypes) {
            Object nullToHash = type.getNullValue();
            int hash1 = h1.getHashedPartitionForParameter(type.getValue(), nullToHash);
            int hash2 = h2.getHashedPartitionForParameter(type.getValue(), nullToHash);
            if (hash1 != 0 || hash2 != 0) {
                System.out.println("Lite " + hash1 + " Std " + hash2);
            }
            assertEquals(0, hash1);
            assertEquals(0, hash2);
        }
    }

    @Test
    public void testSameBytesHash() throws Exception {
        int partitionCount;
        byte[] configBytes;
        HashinatorLite h1;
        TheHashinator h2;

        // try with lots of partition counts
        for (int i = 0; i < 50; i++) {
            partitionCount = r.nextInt(1000) + 1;
            configBytes = ElasticHashinator.getConfigureBytes(partitionCount, ElasticHashinator.DEFAULT_TOTAL_TOKENS);
            h1 = new HashinatorLite(configBytes, false);
            h2 = TheHashinator.getHashinator(ElasticHashinator.class, configBytes, false);
            tandemTestSameBytesHash(h1, h2, partitionCount);
        }
    }

    private void tandemTestSameBytesHash(HashinatorLite h1, TheHashinator h2, int partitionCount) throws Exception {
        byte[] valueToHash = new byte[r.nextInt(1000)];
        r.nextBytes(valueToHash);

        int hash1 = h1.getHashedPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(), valueToHash);
        int hash2 = h2.getHashedPartitionForParameter(VoltType.typeFromObject(valueToHash).getValue(), valueToHash);
        if (hash1 != hash2) {
            System.out.printf("Hash of %d with %d partitions => Lite: %d, Std: %d\n", valueToHash, partitionCount, hash1, hash2);
        }
        assertEquals(hash1, hash2);
        assertTrue(hash1 < partitionCount);
        assertTrue(hash1 >= 0);
    }
}
