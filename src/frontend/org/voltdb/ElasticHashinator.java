/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb;


import java.io.IOException;
import java.lang.Thread.State;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.SortedMapDifference;

import org.apache.cassandra_voltpatches.MurmurHash3;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Bits;
import org.voltcore.utils.Pair;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;
import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.UnmodifiableIterator;

import org.voltdb.utils.CompressionService;

import sun.misc.Cleaner;

/**
 * A hashinator that uses Murmur3_x64_128 to hash values and a consistent hash ring
 * to pick what partition to route a particular value.
 */
public class ElasticHashinator extends TheHashinator {
    private static final AtomicLong m_allocatedHashinatorBytes = new AtomicLong(0);
    private static Thread m_emergencyGCThread;
    public static long HASHINATOR_GC_THRESHHOLD = Long.getLong("HASHINATOR_GC_THRESHHOLD", 128 * 1024 * 1024);

    public static int DEFAULT_TOTAL_TOKENS =
        Integer.parseInt(System.getProperty("ELASTIC_TOTAL_TOKENS", "16384"));

    /**
     * Tokens on the ring. A value hashes to a token if the token is the first value <=
     * the value's hash
     */
    private final Supplier<ImmutableSortedMap<Integer, Integer>> m_tokensMap;

    /*
     * Pointer to an array of integers containing the tokens and partitions. Even values are tokens and odd values
     * are partition ids.
     */
    private final long m_tokens;
    private final int m_tokenCount;
    private final Cleaner m_cleaner;

    private final Supplier<byte[]> m_configBytes;
    private final Supplier<byte[]> m_configBytesSupplier = Suppliers.memoize(new Supplier<byte[]>() {
        @Override
        public byte[] get() {
            return toBytes();
        }
    });
    private final Supplier<byte[]> m_cookedBytes;
    private final Supplier<byte[]> m_cookedBytesSupplier = Suppliers.memoize(new Supplier<byte[]>() {
        @Override
        public byte[] get() {
            return toCookedBytes();
        }
    });
    private final Supplier<Long> m_signature = Suppliers.memoize(new Supplier<Long>() {
        @Override
        public Long get() {
            return TheHashinator.computeConfigurationSignature(m_configBytes.get());
        }
    });

    @Override
    public int pHashToPartition(VoltType type, Object obj) {
        return hashinateBytes(valueToBytes(obj));
    }

    /**
     * The serialization format is big-endian and the first value is the number of tokens
     * Construct the hashinator from a binary description of the ring.
     * followed by the token values where each token value consists of the 8-byte position on the ring
     * and and the 4-byte partition id. All values are signed.
     * @param configBytes  config data
     * @param cooked  compressible wire serialization format if true
     */
    public ElasticHashinator(byte configBytes[], boolean cooked) {
        Pair<Long, Integer> p = (cooked ? updateCooked(configBytes)
                : updateRaw(configBytes));
        m_tokens = p.getFirst();
        m_tokenCount = p.getSecond();
        m_cleaner = Cleaner.create(this, new Deallocator(m_tokens, m_tokenCount * 8));
        m_configBytes = !cooked ? Suppliers.ofInstance(configBytes) : m_configBytesSupplier;
        m_cookedBytes = cooked ? Suppliers.ofInstance(configBytes) : m_cookedBytesSupplier;
        m_tokensMap =  Suppliers.memoize(new Supplier<ImmutableSortedMap<Integer, Integer>>() {

            @Override
            public ImmutableSortedMap<Integer, Integer> get() {
                ImmutableSortedMap.Builder<Integer, Integer> builder = ImmutableSortedMap.naturalOrder();
                for (int ii = 0; ii < m_tokenCount; ii++) {
                    final long ptr = m_tokens + (ii * 8);
                    final int token = Bits.unsafe.getInt(ptr);
                    final int partition = Bits.unsafe.getInt(ptr + 4);
                    builder.put(token, partition);
                }
                return builder.build();
            }
        });
    }

    /**
     * Private constructor to initialize a hashinator with known tokens. Used for adding/removing
     * partitions from existing hashinator.
     * @param tokens
     */
    private ElasticHashinator(SortedMap<Integer, Integer> tokens) {
        m_tokensMap = Suppliers.ofInstance(ImmutableSortedMap.copyOf(tokens));
        Preconditions.checkArgument(m_tokensMap.get().firstEntry().getKey().equals(Integer.MIN_VALUE));
        final int bytes = 8 * tokens.size();
        m_tokens = Bits.unsafe.allocateMemory(bytes);
        trackAllocatedHashinatorBytes(bytes);
        m_cleaner = Cleaner.create(this, new Deallocator(m_tokens, bytes));
        int ii = 0;
        for (Map.Entry<Integer, Integer> e : tokens.entrySet()) {
            final long ptr = m_tokens + (ii * 8);
            Bits.unsafe.putInt(ptr, e.getKey());
            Bits.unsafe.putInt(ptr + 4, e.getValue());
            ii++;
        }
        m_tokenCount = tokens.size();
        m_configBytes = m_configBytesSupplier;
        m_cookedBytes = m_cookedBytesSupplier;
    }

    public static byte[] addPartitions(TheHashinator oldHashinator,
                                       int partitionsToAdd) {
        Preconditions.checkArgument(oldHashinator instanceof ElasticHashinator);
        ElasticHashinator oldElasticHashinator = (ElasticHashinator) oldHashinator;

        Buckets buckets = new Buckets(oldElasticHashinator.m_tokensMap.get());
        buckets.addPartitions(partitionsToAdd);
        return new ElasticHashinator(buckets.getTokens()).getConfigBytes();
    }

    /**
     * Convenience method for generating a deterministic token distribution for the ring based
     * on a given partition count and tokens per partition. Each partition will have N tokens
     * placed randomly on the ring.
     */
    public static byte[] getConfigureBytes(int partitionCount, int tokenCount) {
        Preconditions.checkArgument(partitionCount > 0);
        Preconditions.checkArgument(tokenCount > partitionCount);
        Buckets buckets = new Buckets(partitionCount, tokenCount);
        ElasticHashinator hashinator = new ElasticHashinator(buckets.getTokens());
        return hashinator.getConfigBytes();
    }

    /**
     * Serializes the configuration into bytes, also updates the currently cached m_configBytes.
     * @return The byte[] of the current configuration.
     */
    private byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(4 + (m_tokenCount * 8));
        buf.putInt(m_tokenCount);
        int lastToken = Integer.MIN_VALUE;
        for (int ii = 0; ii < m_tokenCount; ii++) {
            final long ptr = m_tokens + (ii * 8);
            final int token = Bits.unsafe.getInt(ptr);
            Preconditions.checkArgument(token >= lastToken);
            lastToken = token;
            final int pid = Bits.unsafe.getInt(ptr + 4);
            buf.putInt(token);
            buf.putInt(pid);
        }
        return buf.array();
    }

    /**
     * For a given a value hash, find the token that corresponds to it. This will
     * be the first token <= the value hash, or if the value hash is < the first token in the ring,
     * it wraps around to the last token in the ring closest to Long.MAX_VALUE
     */
    public int partitionForToken(int hash) {
        long token = getTokenPtr(hash);
        return Bits.unsafe.getInt(token + 4);
    }

    /**
     * Get all the tokens on the ring.
     */
    public ImmutableSortedMap<Integer, Integer> getTokens()
    {
        return m_tokensMap.get();
    }

    /**
     * Add the given tokens to the ring and generate the new hashinator. The current hashinator is not changed.
     * @param tokensToAdd    Tokens to add as a map of tokens to partitions
     * @return The new hashinator
     */
    public ElasticHashinator addTokens(NavigableMap<Integer, Integer> tokensToAdd)
    {
        // figure out the interval
        long interval = deriveTokenInterval(m_tokensMap.get().keySet());

        Map<Integer, Integer> tokens = Maps.newTreeMap();
        for (Map.Entry<Integer, Integer> e : m_tokensMap.get().entrySet()) {
            if (tokensToAdd.containsKey(e.getKey())) {
                continue;
            }

            // see if we are moving an intermediate token forward
            if (isIntermediateToken(e.getKey(), interval)) {
                Map.Entry<Integer, Integer> floorEntry = tokensToAdd.floorEntry(e.getKey());
                // If the two tokens belong to the same partition and bucket, we are moving the one on the ring
                // forward, so remove it from the ring
                if (floorEntry != null &&
                    floorEntry.getValue().equals(e.getValue()) &&
                    containingBucket(floorEntry.getKey(), interval) == containingBucket(e.getKey(), interval)) {
                    continue;
                }
            }

            tokens.put(e.getKey(), e.getValue());
        }
        tokens.putAll(tokensToAdd);

        return new ElasticHashinator(ImmutableSortedMap.copyOf(tokens));
    }

    @Override
    public int pHashinateLong(long value) {
        if (value == Long.MIN_VALUE) return 0;

        return partitionForToken(MurmurHash3.hash3_x64_128(value));
    }

    @Override
    public int pHashinateBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        final int token = MurmurHash3.hash3_x64_128(buf, 0, bytes.length, 0);
        return partitionForToken(token);
    }

    @Override
    public HashinatorConfig pGetCurrentConfig() {
        return new HashinatorConfig(HashinatorType.ELASTIC, m_configBytes.get(), m_tokens, m_tokenCount) {
            //Store a reference to this hashinator in the config so it doesn't get GCed and release
            //the pointer to the config data that is off heap
            private final ElasticHashinator myHashinator = ElasticHashinator.this;
        };
    }

    /**
     * Find the predecessors of the given partition on the ring. This method runs in linear time,
     * use with caution when the set of partitions is large.
     * @param partition
     * @return The map of tokens to partitions that are the predecessors of the given partition.
     * If the given partition doesn't exist or it's the only partition on the ring, the
     * map will be empty.
     */
    @Override
    public Map<Integer, Integer> pPredecessors(int partition) {
        Map<Integer, Integer> predecessors = new TreeMap<Integer, Integer>();
        UnmodifiableIterator<Map.Entry<Integer,Integer>> iter = m_tokensMap.get().entrySet().iterator();
        Set<Integer> pTokens = new HashSet<Integer>();
        while (iter.hasNext()) {
            Map.Entry<Integer, Integer> next = iter.next();
            if (next.getValue() == partition) {
                pTokens.add(next.getKey());
            }
        }

        for (Integer token : pTokens) {
            Map.Entry<Integer, Integer> predecessor = null;
            if (token != null) {
                predecessor = m_tokensMap.get().headMap(token).lastEntry();
                // If null, it means partition is the first one on the ring, so predecessor
                // should be the last entry on the ring because it wraps around.
                if (predecessor == null) {
                    predecessor = m_tokensMap.get().lastEntry();
                }
            }

            if (predecessor != null && predecessor.getValue() != partition) {
                predecessors.put(predecessor.getKey(), predecessor.getValue());
            }
        }

        return predecessors;
    }

    /**
     * Find the predecessor of the given token on the ring.
     * @param partition    The partition that maps to the given token
     * @param token        The token on the ring
     * @return The predecessor of the given token.
     */
    @Override
    public Pair<Integer, Integer> pPredecessor(int partition, int token) {
        Integer partForToken = m_tokensMap.get().get(token);
        if (partForToken != null && partForToken == partition) {
            Map.Entry<Integer, Integer> predecessor = m_tokensMap.get().headMap(token).lastEntry();

            if (predecessor == null) {
                predecessor = m_tokensMap.get().lastEntry();
            }

            if (predecessor.getKey() != token) {
                return Pair.of(predecessor.getKey(), predecessor.getValue());
            } else {
                // given token is the only one on the ring, umpossible
                throw new RuntimeException("There is only one token on the hash ring");
            }
        } else {
            // given token doesn't map to partition
            throw new IllegalArgumentException("The given token " + token +
                                                   " does not map to partition " + partition);
        }
    }

    /**
     * This runs in linear time with respect to the number of tokens on the ring.
     */
    @Override
    public Map<Integer, Integer> pGetRanges(int partition) {
        Map<Integer, Integer> ranges = new TreeMap<Integer, Integer>();
        Integer first = null; // start of the very first token on the ring
        Integer start = null; // start of a range
        UnmodifiableIterator<Map.Entry<Integer,Integer>> iter = m_tokensMap.get().entrySet().iterator();

        // Iterate through the token map to find the ranges assigned to
        // the given partition
        while (iter.hasNext()) {
            Map.Entry<Integer, Integer> next = iter.next();
            int token = next.getKey();
            int pid = next.getValue();

            if (first == null) {
                first = token;
            }

            // if start is not null, there's an open range, now is
            // the time to close it.
            // else there is no open range, keep on going.
            if (start != null) {
                //Range end is inclusive so do token - 1
                ranges.put(start, token - 1);
                start = null;
            }

            if (pid == partition) {
                // if start is null, there's no open range, start one.
                start = token;
            }
        }

        // if there is an open range when we get here
        // It is the last token which implicity ends at the next max value
        if (start != null) {
            assert first != null;
            ranges.put(start, Integer.MAX_VALUE);
        }

        return ranges;
    }

    /**
     * Returns the configuration signature
     */
    @Override
    public long pGetConfigurationSignature() {
        return m_signature.get();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(" Token       ").append("   Partition\n");
        for (Map.Entry<Integer, Integer> entry : m_tokensMap.get().entrySet()) {
            sb.append(String.format("[%11d => %9d]\n", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }

    /**
     * Returns raw config bytes.
     * @return config bytes
     */
    @Override
    public byte[] getConfigBytes()
    {
        return m_configBytes.get();
    }

    /**
     * Returns compressed config bytes.
     * @return config bytes
     * @throws IOException
     */
    private byte[] toCookedBytes()
    {
        // Allocate for a int pair per token/partition ID entry, plus a size.
        ByteBuffer buf = ByteBuffer.allocate(4 + (m_tokenCount * 8));

        buf.putInt(m_tokenCount);
        // Keep tokens and partition ids separate to aid compression.
        for (int zz = 3; zz >= 0; zz--) {
            int lastToken = Integer.MIN_VALUE;
            for (int ii = 0; ii < m_tokenCount; ii++) {
                int token = Bits.unsafe.getInt(m_tokens + (ii * 8));
                Preconditions.checkArgument(token >= lastToken);
                lastToken = token;
                token = token >>> (zz * 8);
                token = token & 0xFF;
                buf.put((byte)token);
            }
        }
        for (int ii = 0; ii < m_tokenCount; ii++) {
            buf.putInt(Bits.unsafe.getInt(m_tokens + (ii * 8) + 4));
        }

        try {
            return CompressionService.gzipBytes(buf.array());
        } catch (IOException e) {
            throw new RuntimeException("Failed to compress bytes", e);
        }
    }

    /**
     * Update from raw config bytes.
     *      token-1/partition-1
     *      token-2/partition-2
     *      ...
     *      tokens are 8 bytes
     * @param configBytes  raw config data
     * @return  token/partition map
     */
    private Pair<Long, Integer> updateRaw(byte configBytes[]) {
        ByteBuffer buf = ByteBuffer.wrap(configBytes);
        int numEntries = buf.getInt();
        if (numEntries < 0) {
            throw new RuntimeException("Bad elastic hashinator config");
        }
        final int bytes = 8 * numEntries;
        long tokens = Bits.unsafe.allocateMemory(bytes);
        trackAllocatedHashinatorBytes(bytes);

        int lastToken = Integer.MIN_VALUE;
        for (int ii = 0; ii < numEntries; ii++) {
            long ptr = tokens + (ii * 8);
            final int token = buf.getInt();
            Preconditions.checkArgument(token >= lastToken);
            lastToken = token;
            Bits.unsafe.putInt(ptr, token);
            final int partitionId = buf.getInt();
            Bits.unsafe.putInt(ptr + 4, partitionId);
        }
        return Pair.of(tokens, numEntries);
    }

    private long getTokenPtr(int hash) {
        int min = 0;
        int max = m_tokenCount - 1;

        while (min <= max) {
            int mid = (min + max) >>> 1;
            final long midPtr = m_tokens + (8 * mid);
            int midval = Bits.unsafe.getInt(midPtr);

            if (midval < hash) {
                min = mid + 1;
            } else if (midval > hash) {
                max = mid - 1;
            } else {
                return midPtr;
            }
        }
        return m_tokens + (min - 1) * 8;
    }

    /**
     * Update from optimized (cooked) wire format.
     *      token-1 token-2 ...
     *      partition-1 partition-2 ...
     *      tokens are 4 bytes
     * @param compressedData  optimized and compressed config data
     * @return  token/partition map
     */
    private Pair<Long, Integer> updateCooked(byte[] compressedData)
    {
        // Uncompress (inflate) the bytes.
        byte[] cookedBytes;
        try {
            cookedBytes = CompressionService.gunzipBytes(compressedData);
        } catch (IOException e) {
            throw new RuntimeException("Unable to decompress elastic hashinator data.");
        }

        int numEntries = (cookedBytes.length >= 4
                                ? ByteBuffer.wrap(cookedBytes).getInt()
                                : 0);
        int tokensSize = 4 * numEntries;
        int partitionsSize = 4 * numEntries;
        if (numEntries <= 0 || cookedBytes.length != 4 + tokensSize + partitionsSize) {
            throw new RuntimeException("Bad elastic hashinator cooked config size.");
        }
        final long bytes = 8 * numEntries;
        long tokens = Bits.unsafe.allocateMemory(bytes);
        trackAllocatedHashinatorBytes(bytes);
        ByteBuffer tokenBuf = ByteBuffer.wrap(cookedBytes, 4, tokensSize);
        ByteBuffer partitionBuf = ByteBuffer.wrap(cookedBytes, 4 + tokensSize, partitionsSize);
        int tokensArray[] = new int[numEntries];
        for (int zz = 3; zz >= 0; zz--) {
            for (int ii = 0; ii < numEntries; ii++) {
                int value = tokenBuf.get();
                value = (value << (zz * 8)) & (0xFF << (zz * 8));
                tokensArray[ii] = (tokensArray[ii] | value);
            }
        }

        int lastToken = Integer.MIN_VALUE;
        for (int ii = 0; ii < numEntries; ii++) {
            int token = tokensArray[ii];
            Preconditions.checkArgument(token >= lastToken);
            lastToken = token;
            long ptr = tokens + (ii * 8);
            Bits.unsafe.putInt(ptr, token);
            final int partitionId = partitionBuf.getInt();
            Bits.unsafe.putInt(ptr + 4, partitionId);
        }
        return Pair.of(tokens, numEntries);
    }

    //Track allocated bytes and invoke System.gc to encourage reclamation if it is growing large
    private static synchronized void trackAllocatedHashinatorBytes(long bytes) {
        final long allocated = m_allocatedHashinatorBytes.addAndGet(bytes);
        if (allocated > HASHINATOR_GC_THRESHHOLD) {
            hostLogger.warn(allocated + " bytes of hashinator data has been allocated");
            if (m_emergencyGCThread == null || m_emergencyGCThread.getState() == State.TERMINATED) {
                m_emergencyGCThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        hostLogger.warn("Invoking System.gc() to recoup hashinator bytes");
                        System.gc();
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {}
                        hostLogger.info(m_allocatedHashinatorBytes.get() + " bytes of hashinator allocated after GC");
                    }
                }, "Hashinator GC thread");
                m_emergencyGCThread.start();
            }
        }
    }

    /**
     * Return (cooked) bytes optimized for serialization.
     * @return optimized config bytes
     */
    @Override
    public byte[] getCookedBytes()
    {
        return m_cookedBytes.get();
    }

    @Override
    public HashinatorType getConfigurationType() {
        return TheHashinator.HashinatorType.ELASTIC;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ElasticHashinator that = (ElasticHashinator) o;

        if (m_signature.get().equals(that.m_signature.get())) return true;

        SortedMapDifference<Integer,Integer> diff = Maps.difference(m_tokensMap.get(), that.m_tokensMap.get());
        if (!diff.entriesDiffering().isEmpty()) return false;

        // Tolerate tokens that are not on the bucket boundaries. As long as these tokens hash to the same partitions
        // as they are in the other hashinator, it's fine.
        for (Map.Entry<Integer, Integer> leftEntry : diff.entriesOnlyOnLeft().entrySet()) {
            if (that.partitionForToken(leftEntry.getKey()) != leftEntry.getValue()) {
                return false;
            }
        }

        for (Map.Entry<Integer, Integer> rightEntry : diff.entriesOnlyOnRight().entrySet()) {
            if (partitionForToken(rightEntry.getKey()) != rightEntry.getValue()) {
                return false;
            }
        }

        return true;
    }

    /**
     * Figure out the token interval from the first 3 ranges, assuming that there is at most one token that doesn't
     * fall onto the bucket boundary at any given time. The largest range will be the hashinator's bucket size.
     * @return The bucket size, or token interval if you prefer.
     */
    private static long deriveTokenInterval(ImmutableSortedSet<Integer> tokens)
    {
        long interval = 0;
        int count = 4;
        int prevToken = Integer.MIN_VALUE;
        UnmodifiableIterator<Integer> tokenIter = tokens.iterator();
        while (tokenIter.hasNext() && count-- > 0) {
            int nextToken = tokenIter.next();
            interval = Math.max(interval, nextToken - prevToken);
            prevToken = nextToken;
        }
        return interval;
    }

    /**
     * Check if the token doesn't fall onto a bucket boundary given the token interval.
     * @return true if the token doesn't fall onto any bucket boundary.
     */
    private static boolean isIntermediateToken(int token, long interval)
    {
        return (((long) token - Integer.MIN_VALUE)) % interval != 0;
    }

    /**
     * Calculate the boundary of the bucket that countain the given token given the token interval.
     * @return The token of the bucket boundary.
     */
    private static int containingBucket(int token, long interval)
    {
        return (int) ((((long) token - Integer.MIN_VALUE) / interval) * interval + Integer.MIN_VALUE);
    }

    @Override
    protected Set<Integer> pGetPartitions() {
        return new HashSet<Integer>(m_tokensMap.get().values());
    }

    private static class Deallocator implements Runnable {
        private long address;
        private int size;
        public Deallocator(long address, int size) {
            this.address = address;
            this.size = size;
        }

        public void run() {
            if (address == 0) {
                return;
            }
            Bits.unsafe.freeMemory(address);
            address = 0;
            m_allocatedHashinatorBytes.addAndGet(-size);
        }
    }
}
