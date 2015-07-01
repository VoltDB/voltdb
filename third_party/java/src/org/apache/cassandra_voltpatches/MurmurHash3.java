/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra_voltpatches;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google_voltpatches.common.primitives.UnsignedBytes;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 *
 * hash3_x64_128() is MurmurHash 3.0.
 *
 * <p>
 * The C version of MurmurHash 2.0 found at that site was ported to Java by
 * Andrzej Bialecki (ab at getopt org).
 * </p>
 *
 * <p>
 * This originally came from Cassandra, for Volt we had to modify it to match
 * the native and canonical implementation. The switch statement handling dangling
 * bytes at the end wasn't converting the signed bytes from the ByteBuffer to signed ints,
 * it was casting them straight to long which means that you could end up with negative numbers when
 * the canonical algorithm was working with them unsigned.
 */
public class MurmurHash3
{
    protected static long getblock(ByteBuffer key, int offset, int index)
    {
        int i_8 = index << 3;
        int blockOffset = offset + i_8;
        return ((long) key.get(blockOffset + 0) & 0xFFL) + (((long) key.get(blockOffset + 1) & 0xFFL) << 8) +
               (((long) key.get(blockOffset + 2) & 0xFFL) << 16) + (((long) key.get(blockOffset + 3) & 0xFFL) << 24) +
               (((long) key.get(blockOffset + 4) & 0xFFL) << 32) + (((long) key.get(blockOffset + 5) & 0xFFL) << 40) +
               (((long) key.get(blockOffset + 6) & 0xFFL) << 48) + (((long) key.get(blockOffset + 7) & 0xFFL) << 56);
    }

    protected static long rotl64(long v, int n)
    {
        return ((v << n) | (v >>> (64 - n)));
    }

    protected static long fmix(long k)
    {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;

        return k;
    }

    public static int hash3_x64_128(long value) {
        return hash3_x64_128(value, 0);
    }

    public static int hash3_x64_128(long value, long seed) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(value);
        return hash3_x64_128(buf, 0, 8, seed);
    }

    private final static long MASK = 0xFFFFFFFF00000000L;

    public static int hash3_x64_128(ByteBuffer key, int offset, int length, long seed)
    {
        final int nblocks = length >> 4; // Process as 128-bit blocks.

        long h1 = seed;
        long h2 = seed;

        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        //----------
        // body

        for(int i = 0; i < nblocks; i++)
        {
            long k1 = getblock(key, offset, i*2+0);
            long k2 = getblock(key, offset, i*2+1);

            k1 *= c1; k1 = rotl64(k1,31); k1 *= c2; h1 ^= k1;

            h1 = rotl64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

            k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

            h2 = rotl64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
        }

        //----------
        // tail

        // Advance offset to the unprocessed tail of the data.
        offset += nblocks * 16;

        long k1 = 0;
        long k2 = 0;

        /*
         * For Volt had to add UnsignedBytes.toInt to make the hash output
         * match the native and canonical implementation of MurmurHash3
         */
        switch(length & 15)
        {
            case 15: k2 ^= ((long) UnsignedBytes.toInt(key.get(offset+14))) << 48;
            case 14: k2 ^= ((long) UnsignedBytes.toInt(key.get(offset+13))) << 40;
            case 13: k2 ^= ((long) UnsignedBytes.toInt(key.get(offset+12))) << 32;
            case 12: k2 ^= ((long) UnsignedBytes.toInt(key.get(offset+11))) << 24;
            case 11: k2 ^= ((long) UnsignedBytes.toInt(key.get(offset+10))) << 16;
            case 10: k2 ^= ((long) UnsignedBytes.toInt(key.get(offset+9))) << 8;
            case  9: k2 ^= ((long) UnsignedBytes.toInt(key.get(offset+8))) << 0;
                k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

            case  8: k1 ^= ((long) UnsignedBytes.toInt(key.get(offset+7))) << 56;
            case  7: k1 ^= ((long) UnsignedBytes.toInt(key.get(offset+6))) << 48;
            case  6: k1 ^= ((long) UnsignedBytes.toInt(key.get(offset+5))) << 40;
            case  5: k1 ^= ((long) UnsignedBytes.toInt(key.get(offset+4))) << 32;
            case  4: k1 ^= ((long) UnsignedBytes.toInt(key.get(offset+3))) << 24;
            case  3: k1 ^= ((long) UnsignedBytes.toInt(key.get(offset+2))) << 16;
            case  2: k1 ^= ((long) UnsignedBytes.toInt(key.get(offset+1))) << 8;
            case  1: k1 ^= ((long) UnsignedBytes.toInt(key.get(offset)));
                k1 *= c1; k1  = rotl64(k1,31); k1 *= c2; h1 ^= k1;
        };

        //----------
        // finalization

        h1 ^= length; h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix(h1);
        h2 = fmix(h2);

        h1 += h2;
        h2 += h1;

        //Shift so that we use the higher order bits in case we want to use the lower order ones later
        //Also use the h1 higher order bits because it provided much better performance in voter, consistent too
        return (int)(h1 >>> 32);
    }
}
