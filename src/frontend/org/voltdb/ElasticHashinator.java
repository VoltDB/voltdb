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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.cassandra_voltpatches.MurmurHash3;
import org.voltcore.utils.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;

/**
 * A hashinator that uses Murmur3_x64_128 to hash values and a consistent hash ring
 * to pick what partition to route a particular value.
 */
public class ElasticHashinator extends TheHashinator {
    public static int DEFAULT_TOKENS_PER_PARTITION = 8;

    /**
     * Tokens on the ring. A value hashes to a token if the token is the first value <=
     * the value's hash
     */
    private final ImmutableSortedMap<Long, Integer> tokens;
    private final byte m_configBytes[];

    /**
     * Initialize the hashinator from a binary description of the ring.
     * The serialization format is big-endian and the first value is the number of tokens
     * followed by the token values where each token value consists of the 8-byte position on the ring
     * and and the 4-byte partition id. All values are signed.
     */
    public ElasticHashinator(byte configureBytes[]) {
        m_configBytes = Arrays.copyOf(configureBytes, configureBytes.length);
        ByteBuffer buf = ByteBuffer.wrap(configureBytes);
        int numEntries = buf.getInt();
        TreeMap<Long, Integer> buildMap = new TreeMap<Long, Integer>();
        for (int ii = 0; ii < numEntries; ii++) {
            final long token = buf.getLong();
            final int partitionId = buf.getInt();
            if (buildMap.containsKey(token)) {
                throw new RuntimeException(
                        "Duplicate token " + token + " partition "
                        + partitionId + " and " + buildMap.get(token));
            }
            buildMap.put( token, partitionId);
        }
        ImmutableSortedMap.Builder<Long, Integer> builder = ImmutableSortedMap.naturalOrder();
        for (Map.Entry<Long, Integer> e : buildMap.entrySet()) {
            builder.put(e.getKey(), e.getValue());
        }
        tokens = builder.build();
    }

    /**
     * Private constructor to initialize a hashinator with known tokens. Used for adding/removing
     * partitions from existing hashinator.
     * @param tokens
     */
    private ElasticHashinator(Map<Long, Integer> tokens) {
        this.tokens = ImmutableSortedMap.copyOf(tokens);
        m_configBytes = toBytes();
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
        Random r = new Random(0);
        Map<Long, Integer> newConfig = new HashMap<Long, Integer>(oldElasticHashinator.tokens);
        Set<Integer> existingPartitions = new HashSet<Integer>(oldElasticHashinator.tokens.values());
        Set<Long> checkSet = new HashSet<Long>(oldElasticHashinator.tokens.keySet());

        for (int pid : newPartitions) {
            if (existingPartitions.contains(pid)) {
                throw new RuntimeException("Partition " + pid + " already exists in the " +
                        "hashinator");
            }

            for (int i = 0; i < tokensPerPartition; i++) {
                while (true) {
                    long candidateToken = MurmurHash3.hash3_x64_128(r.nextLong());
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
        Set<Integer> partitions = new HashSet<Integer>();

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
        ByteBuffer buf = ByteBuffer.allocate(4 + (tokens.size() * 12));//long and an int per
        buf.putInt(tokens.size());

        for (Map.Entry<Long, Integer> e : tokens.entrySet()) {
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
    int partitionForToken(long hash) {
        Map.Entry<Long, Integer> entry = tokens.floorEntry(hash);
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
            return tokens.lastEntry().getValue();
        }
    }

    @Override
    protected int pHashinateLong(long value) {
        if (value == Long.MIN_VALUE) return 0;

        return partitionForToken(MurmurHash3.hash3_x64_128(value));
    }

    @Override
    protected int pHashinateBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        final long token = MurmurHash3.hash3_x64_128(buf, 0, bytes.length, 0);
        return partitionForToken(token);
    }

    @Override
    protected Pair<HashinatorType, byte[]> pGetCurrentConfig() {
        return Pair.of(HashinatorType.ELASTIC, m_configBytes);
    }
}
