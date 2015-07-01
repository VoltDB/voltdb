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

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.collect.TreeMultimap;

import java.util.*;

/**
 * Helper class for distributing a fixed number of buckets over some number of partitions.
 * Can handle adding new partitions by taking buckets from existing partitions and assigning them
 * to new ones
 */
public class Buckets {
    private final List<TreeSet<Integer>> m_partitionTokens = new ArrayList<TreeSet<Integer>>();
    private final int m_tokenCount;
    private final long m_tokenInterval;

    public Buckets(int partitionCount, int tokenCount) {
        Preconditions.checkArgument(partitionCount > 0);
        Preconditions.checkArgument(tokenCount > partitionCount);
        Preconditions.checkArgument(tokenCount % 2 == 0);
        m_tokenCount = tokenCount;
        m_tokenInterval = calculateTokenInterval(tokenCount);
        m_partitionTokens.add(Sets.<Integer>newTreeSet());
        long token = Integer.MIN_VALUE;
        for (int ii = 0; ii < tokenCount; ii++) {
            m_partitionTokens.get(0).add((int)token);
            token += m_tokenInterval;
        }

        addPartitions(partitionCount - 1);

    }

    public void addPartitions(int partitionCount) {
        //Can't have more partitions than tokens
        Preconditions.checkArgument(m_tokenCount > m_partitionTokens.size() + partitionCount);
        TreeSet<LoadPair> loadSet = Sets.newTreeSet();
        for (int ii = 0; ii < m_partitionTokens.size(); ii++) {
            loadSet.add(new LoadPair(ii, m_partitionTokens.get(ii)));
        }

        for (int ii = 0; ii < partitionCount; ii++) {
            TreeSet<Integer> d = Sets.newTreeSet();
            m_partitionTokens.add(d);
            loadSet.add(new LoadPair(m_partitionTokens.size() - 1, d)) ;
            addPartition(loadSet);
        }
    }

    /*
     * Loop and balance data after a partition is added until no
     * more balancing can be done
     */
    private void addPartition(TreeSet<LoadPair> loadSet) {
        while (doNextBalanceOp(loadSet)) {}
    }

    private static long calculateTokenInterval(int bucketCount) {
        return Integer.MAX_VALUE / (bucketCount / 2);
    }

    public Buckets(SortedMap<Integer, Integer> tokens) {
        Preconditions.checkNotNull(tokens);
        Preconditions.checkArgument(tokens.size() > 1);
        Preconditions.checkArgument(tokens.size() % 2 == 0);
        m_tokenCount = tokens.size();
        m_tokenInterval = calculateTokenInterval(m_tokenCount);

        int partitionCount = new HashSet<Integer>(tokens.values()).size();
        for (int partition = 0; partition < partitionCount; partition++) {
            m_partitionTokens.add(Sets.<Integer>newTreeSet());
        }
        for (Map.Entry<Integer, Integer> e : tokens.entrySet()) {
            TreeSet<Integer> buckets = m_partitionTokens.get(e.getValue());
            int token = e.getKey();
            buckets.add(token);
        }
    }

    public SortedMap<Integer, Integer> getTokens() {
        ImmutableSortedMap.Builder<Integer, Integer> b = ImmutableSortedMap.naturalOrder();
        for (int partition = 0; partition < m_partitionTokens.size(); partition++) {
            TreeSet<Integer> tokens = m_partitionTokens.get(partition);
            for (Integer token : tokens) {
                b.put( token, partition);
            }
        }
        return b.build();
    }

    /*
     * Take a token from the most loaded partition and move it to the least loaded partition
     * If no available balancing operation is available return false
     */
    private boolean doNextBalanceOp(TreeSet<LoadPair> loadSet) {
        LoadPair mostLoaded = loadSet.pollLast();
        LoadPair leastLoaded = loadSet.pollFirst();

        try {
            //Perfection
            if (mostLoaded.tokens.size() == leastLoaded.tokens.size()) return false;
            //Can't improve on off by one, just end up off by one again
            if (mostLoaded.tokens.size() == (leastLoaded.tokens.size() + 1)) return false;
            int token = mostLoaded.tokens.pollFirst();
            leastLoaded.tokens.add(token);
        } finally {
            loadSet.add(mostLoaded);
            loadSet.add(leastLoaded);
        }
        return true;
    }

    /*
     * Wrapper that orders and compares on load and then partition id for determinism
     */
    private static class LoadPair implements Comparable<LoadPair> {
        private final Integer partition;
        private final TreeSet<Integer> tokens;

        public LoadPair(Integer partition, TreeSet<Integer> tokens) {
            this.partition = partition;
            this.tokens = tokens;
        }

        @Override
        public int hashCode() {
            return partition.hashCode();
        }

        @Override
        public int compareTo(LoadPair o) {
            Preconditions.checkNotNull(o);
            int comparison = new Integer(tokens.size()).compareTo(o.tokens.size());
            if (comparison == 0) {
                 return partition.compareTo(o.partition);
            } else {
                return comparison;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) return false;
            if (o.getClass() != LoadPair.class) return false;
            LoadPair lp = (LoadPair)o;
            return partition.equals(lp.partition);
        }

        @Override
        public String toString() {
            return "Partition " + partition + " tokens " + tokens.size();
        }
    }

}
