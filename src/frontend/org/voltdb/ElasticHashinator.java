/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

import org.apache.cassandra_voltpatches.MurmurHash3;
import org.voltcore.utils.Pair;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.UnmodifiableIterator;
import static org.voltdb.TheHashinator.valueToBytes;

/**
 * A hashinator that uses Murmur3_x64_128 to hash values and a consistent hash ring
 * to pick what partition to route a particular value.
 */
public class ElasticHashinator extends TheHashinator {
    public static int DEFAULT_TOKENS_PER_PARTITION =
        Integer.parseInt(System.getProperty("ELASTIC_TOKENS_PER_PARTITION", "256"));

    static final String SECURE_RANDOM_ALGORITHM = "SHA1PRNG";
    static final byte [] SECURE_RANDON_SEED = "Festina Lente".getBytes(Charsets.UTF_8);

    /**
     * Tokens on the ring. A value hashes to a token if the token is the first value <=
     * the value's hash
     */
    private final ImmutableSortedMap<Long, Integer> m_tokens;
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
     * Encapsulates all knowledge of how to compress/uncompress hash tokens.
     */
    private static class TokenCompressor {
        final static long TOKEN_MASK = 0xFFFFFFFF00000000L;
        final static int COMPRESSED_SIZE = 4;

        /**
         * Prepare token for compression.
         * @return cleaned up token
         */
        static long prepare(long token) {
            return token & TOKEN_MASK;
        }

        /**
         * Compress token.
         * @return compressed token
         */
        static int compress(long token) {
            return (int)(token >>> 32);
        }

        /**
         * Uncompress token.
         * @return uncompressed token
         */
        static long uncompress(int token) {
            return (long)token << 32;
        }
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
        m_tokens = (cooked ? updateCooked(configBytes)
                : updateRaw(configBytes));
        m_configBytes = !cooked ? Suppliers.ofInstance(configBytes) : m_configBytesSupplier;
        m_cookedBytes = cooked ? Suppliers.ofInstance(configBytes) : m_cookedBytesSupplier;
    }

    /**
     * Private constructor to initialize a hashinator with known tokens. Used for adding/removing
     * partitions from existing hashinator.
     * @param tokens
     */
    private ElasticHashinator(Map<Long, Integer> tokens) {
        this.m_tokens = ImmutableSortedMap.copyOf(tokens);
        m_configBytes = m_configBytesSupplier;
        m_cookedBytes = m_cookedBytesSupplier;
    }

    /**
     * Given an existing elastic hashinator, add a set of new partitions to the existing hash ring.
     * @param oldHashinator An elastic hashinator
     * @param newPartitions A set of new partitions to add
     * @param tokensPerPartition The number of times a partition appears on the ring
     * @return The config bytes of the new hash ring
     */
    public static byte[] addPartitions(TheHashinator oldHashinator,
                                       Collection<Integer> newPartitions,
                                       int tokensPerPartition) {
        Preconditions.checkArgument(oldHashinator instanceof ElasticHashinator);
        ElasticHashinator oldElasticHashinator = (ElasticHashinator) oldHashinator;

        SecureRandom sr;
        try {
            sr = SecureRandom.getInstance(SECURE_RANDOM_ALGORITHM);
            sr.setSeed(SECURE_RANDON_SEED);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("Unable to initialize secure random generator", ex);
        }

        Map<Long, Integer> newConfig = new HashMap<Long, Integer>(oldElasticHashinator.m_tokens);
        Set<Integer> existingPartitions = new HashSet<Integer>(oldElasticHashinator.m_tokens.values());
        Set<Long> checkSet = new HashSet<Long>(oldElasticHashinator.m_tokens.keySet());

        for (int pid : newPartitions) {
            if (existingPartitions.contains(pid)) {
                throw new RuntimeException("Partition " + pid + " already exists in the " +
                        "hashinator");
            }

            for (int i = 0; i < tokensPerPartition; i++) {
                while (true) {
                    long candidateToken = TokenCompressor.prepare(sr.nextLong());
                    if (!checkSet.add(candidateToken)) {
                        continue;
                    }
                    newConfig.put(candidateToken, pid);
                    break;
                }
            }
        }

        return new ElasticHashinator(newConfig).toBytes();
    }

    /**
     * Convenience method for generating a deterministic token distribution for the ring based
     * on a given partition count and tokens per partition. Each partition will have N tokens
     * placed randomly on the ring.
     */
    public static byte[] getConfigureBytes(int partitionCount, int tokensPerPartition) {
        Preconditions.checkArgument(partitionCount > 0);
        Preconditions.checkArgument(tokensPerPartition > 0);
        ElasticHashinator emptyHashinator = new ElasticHashinator(new HashMap<Long, Integer>());
        Set<Integer> partitions = new TreeSet<Integer>();

        for (int ii = 0; ii < partitionCount; ii++) {
            partitions.add(ii);
        }

        return addPartitions(emptyHashinator, partitions, tokensPerPartition);
    }

    /**
     * Serializes the configuration into bytes, also updates the currently cached m_configBytes.
     * @return The byte[] of the current configuration.
     */
    private byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(4 + (m_tokens.size() * (8 + TokenCompressor.COMPRESSED_SIZE)));
        buf.putInt(m_tokens.size());

        for (Map.Entry<Long, Integer> e : m_tokens.entrySet()) {
            long token = e.getKey();
            int pid = e.getValue();
            buf.putLong(token);
            buf.putInt(pid);
        }

        return buf.array();
    }

    /**
     * For a given a value hash, find the token that corresponds to it. This will
     * be the first token <= the value hash, or if the value hash is < the first token in the ring,
     * it wraps around to the last token in the ring closest to Long.MAX_VALUE
     */
    public int partitionForToken(long hash) {
        Map.Entry<Long, Integer> entry = m_tokens.floorEntry(hash);
        //System.out.println("Finding partition for token " + token);
        /*
         * Because the tokens are randomly distributed it is likely there is a range
         * near Long.MIN_VALUE that isn't covered by a token. Conceptually this is a ring
         * so the correct token is the one near Long.MAX_VALUE.
         */
        if (entry != null) {
            //System.out.println("Floor token was " + entry.getKey());
            return entry.getValue();
        } else {
            //System.out.println("Last entry token " + tokens.lastEntry().getKey());
            return m_tokens.lastEntry().getValue();
        }
    }

    /**
     * Get all the tokens on the ring.
     */
    public ImmutableSortedMap<Long, Integer> getTokens()
    {
        return m_tokens;
    }

    /**
     * Add the given token to the ring and generate the new hashinator. The current hashinator is not changed.
     * @param token        The new token
     * @param partition    The partition associated with the new token
     * @return The new hashinator
     */
    public ElasticHashinator addToken(long token, int partition)
    {
        HashMap<Long, Integer> newTokens = Maps.newHashMap(m_tokens);
        newTokens.put(token, partition);
        return new ElasticHashinator(newTokens);
    }

    @Override
    public int pHashinateLong(long value) {
        if (value == Long.MIN_VALUE) return 0;

        return partitionForToken(MurmurHash3.hash3_x64_128(value));
    }

    @Override
    public int pHashinateBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        final long token = MurmurHash3.hash3_x64_128(buf, 0, bytes.length, 0);
        return partitionForToken(token);
    }

    @Override
    protected Pair<HashinatorType, byte[]> pGetCurrentConfig() {
        return Pair.of(HashinatorType.ELASTIC, m_configBytes.get());
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
    public Map<Long, Integer> pPredecessors(int partition) {
        Map<Long, Integer> predecessors = new TreeMap<Long, Integer>();
        UnmodifiableIterator<Map.Entry<Long,Integer>> iter = m_tokens.entrySet().iterator();
        Set<Long> pTokens = new HashSet<Long>();
        while (iter.hasNext()) {
            Map.Entry<Long, Integer> next = iter.next();
            if (next.getValue() == partition) {
                pTokens.add(next.getKey());
            }
        }

        for (Long token : pTokens) {
            Map.Entry<Long, Integer> predecessor = null;
            if (token != null) {
                predecessor = m_tokens.headMap(token).lastEntry();
                // If null, it means partition is the first one on the ring, so predecessor
                // should be the last entry on the ring because it wraps around.
                if (predecessor == null) {
                    predecessor = m_tokens.lastEntry();
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
    public Pair<Long, Integer> pPredecessor(int partition, long token) {
        Integer partForToken = m_tokens.get(token);
        if (partForToken != null && partForToken == partition) {
            Map.Entry<Long, Integer> predecessor = m_tokens.headMap(token).lastEntry();

            if (predecessor == null) {
                predecessor = m_tokens.lastEntry();
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
    public Map<Long, Long> pGetRanges(int partition) {
        Map<Long, Long> ranges = new TreeMap<Long, Long>();
        Long first = null; // start of the very first token on the ring
        Long start = null; // start of a range
        UnmodifiableIterator<Map.Entry<Long,Integer>> iter = m_tokens.entrySet().iterator();

        // Iterate through the token map to find the ranges assigned to
        // the given partition
        while (iter.hasNext()) {
            Map.Entry<Long, Integer> next = iter.next();
            long token = next.getKey();
            int pid = next.getValue();

            if (first == null) {
                first = token;
            }

            // if start is not null, there's an open range, now is
            // the time to close it.
            // else there is no open range, keep on going.
            if (start != null) {
                ranges.put(start, token);
                start = null;
            }

            if (pid == partition) {
                // if start is null, there's no open range, start one.
                start = token;
            }
        }

        // if there is an open range when we get here, it means that
        // the last token on the ring belongs to the partition, and
        // it wraps around the origin of the ring, so close the range
        // with the the very first token on the ring.
        if (start != null) {
            assert first != null;
            ranges.put(start, first);
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
        sb.append(" Token               ").append("   Partition\n");
        for (Map.Entry<Long, Integer> entry : m_tokens.entrySet()) {
            sb.append(String.format("[%20d => %8d]\n", entry.getKey(), entry.getValue()));
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
        ByteBuffer buf = ByteBuffer.allocate(4 + (m_tokens.size() * (4 + TokenCompressor.COMPRESSED_SIZE)));

        int numEntries = m_tokens.size();
        buf.putInt(numEntries);

        // Keep tokens and partition ids separate to aid compression.
        for (Map.Entry<Long, Integer> e : m_tokens.entrySet()) {
            int optimizedToken = TokenCompressor.compress(e.getKey());
            buf.putInt(optimizedToken);
        }
        for (Map.Entry<Long, Integer> e : m_tokens.entrySet()) {
            int partitionId = e.getValue();
            buf.putInt(partitionId);
        }

        // Compress (deflate) the bytes and cache the results.
        ByteArrayOutputStream bos = new ByteArrayOutputStream(buf.array().length);
        DeflaterOutputStream dos = new DeflaterOutputStream(bos);
        try {
            dos.write(buf.array());
            dos.close();
        }
        catch (IOException e) {
            throw new RuntimeException(String.format(
                    "Failed to deflate cooked bytes: %s", e.toString()));
        }
        return bos.toByteArray();
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
    private ImmutableSortedMap<Long, Integer> updateRaw(byte configBytes[]) {
        ByteBuffer buf = ByteBuffer.wrap(configBytes);
        int numEntries = buf.getInt();
        ImmutableSortedMap.Builder<Long, Integer> builder = ImmutableSortedMap.naturalOrder();
        for (int ii = 0; ii < numEntries; ii++) {
            final long token = buf.getLong();
            final int partitionId = buf.getInt();
            builder.put(token, partitionId);
        }
        return builder.build();
    }

    /**
     * Update from optimized (cooked) wire format.
     *      token-1 token-2 ...
     *      partition-1 partition-2 ...
     *      tokens are 4 bytes
     * @param compressedData  optimized and compressed config data
     * @return  token/partition map
     */
    private ImmutableSortedMap<Long, Integer> updateCooked(byte[] compressedData)
    {
        // Uncompress (inflate) the bytes.
        ByteArrayOutputStream bos = new ByteArrayOutputStream((int)(compressedData.length * 1.5));
        InflaterOutputStream dos = new InflaterOutputStream(bos);
        try {
            dos.write(compressedData);
            dos.close();
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to decompress elastic hashinator data.");
        }
        byte[] cookedBytes = bos.toByteArray();

        int numEntries = (cookedBytes.length >= 4
                                ? ByteBuffer.wrap(cookedBytes).getInt()
                                : 0);
        int tokensSize = TokenCompressor.COMPRESSED_SIZE * numEntries;
        int partitionsSize = 4 * numEntries;
        if (numEntries <= 0 || cookedBytes.length != 4 + tokensSize + partitionsSize) {
            throw new RuntimeException("Bad elastic hashinator cooked config size.");
        }
        ByteBuffer tokenBuf = ByteBuffer.wrap(cookedBytes, 4, tokensSize);
        ByteBuffer partitionBuf = ByteBuffer.wrap(cookedBytes, 4 + tokensSize, partitionsSize);
        ImmutableSortedMap.Builder<Long, Integer> builder = ImmutableSortedMap.naturalOrder();
        for (int ii = 0; ii < numEntries; ii++) {
            final long token = TokenCompressor.uncompress(tokenBuf.getInt());
            final int partitionId = partitionBuf.getInt();
            builder.put( token, partitionId );
        }
        return builder.build();
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
}
