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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

import java.util.*;

public class Buckets {
    private final List<TreeSet<Long>> m_partitionTokens = new ArrayList<TreeSet<Long>>();
    private final int m_tokenCount;
    private final long m_tokenInterval;

    public Buckets(int partitionCount, int tokenCount) {
        Preconditions.checkArgument(partitionCount > 0);
        Preconditions.checkArgument(tokenCount > partitionCount);
        Preconditions.checkArgument(tokenCount % 2 == 0);
        m_tokenCount = tokenCount;
        m_tokenInterval = calculateTokenInterval(tokenCount);
        m_partitionTokens.add(Sets.<Long>newTreeSet());
        long token = Long.MIN_VALUE;
        for (int ii = 0; ii < tokenCount; ii++) {
            m_partitionTokens.get(0).add(token);
            token += m_tokenInterval;
        }

        addPartitions(partitionCount - 1);

    }

    public void addPartitions(int partitionCount) {
        TreeSet<LoadPair> loadSet = Sets.newTreeSet();
        for (int ii = 0; ii < m_partitionTokens.size(); ii++) {
            loadSet.add(new LoadPair(ii, m_partitionTokens.get(ii)));
        }

        for (int ii = 0; ii < partitionCount; ii++) {
            TreeSet<Long> d = Sets.newTreeSet();
            m_partitionTokens.add(d);
            loadSet.add(new LoadPair(m_partitionTokens.size() - 1, d)) ;
            addPartition(loadSet);
        }
    }

    private void addPartition(TreeSet<LoadPair> loadSet) {
        while (doNextBalanceOp(loadSet)) {}
    }

    private static long calculateTokenInterval(int bucketCount) {
        return ElasticHashinator.TokenCompressor.prepare(Long.MAX_VALUE / (bucketCount / 2));
    }

    public Buckets(SortedMap<Long, Integer> tokens) {
        Preconditions.checkNotNull(tokens);
        Preconditions.checkArgument(tokens.size() > 1);
        Preconditions.checkArgument(tokens.size() % 2 == 0);
        m_tokenCount = tokens.size();
        m_tokenInterval = calculateTokenInterval(m_tokenCount);

        int partitionCount = new HashSet<Integer>(tokens.values()).size();
        for (int partition = 0; partition < partitionCount; partition++) {
            m_partitionTokens.add(Sets.<Long>newTreeSet());
        }
        for (Map.Entry<Long, Integer> e : tokens.entrySet()) {
            TreeSet<Long> buckets = m_partitionTokens.get(e.getValue());
            long token = e.getKey();
            buckets.add(token);
        }
    }

    public Map<Long, Integer> getTokens() {
        ImmutableSortedMap.Builder<Long, Integer> b = ImmutableSortedMap.naturalOrder();
        for (int partition = 0; partition < m_partitionTokens.size(); partition++) {
            TreeSet<Long> tokens = m_partitionTokens.get(partition);
            for (Long token : tokens) {
                b.put( token, partition);
            }
        }
        return b.build();
    }

    /*
     * Return the token that will be replaced with a different partition
     */
    private boolean doNextBalanceOp(TreeSet<LoadPair> loadSet) {
        LoadPair mostLoaded = loadSet.pollLast();
        LoadPair leastLoaded = loadSet.pollFirst();

        try {
            //Perfection
            if (mostLoaded.tokens.size() == leastLoaded.tokens.size()) return false;
            //Can't improve on off by one, just end up off by one again
            if (mostLoaded.tokens.size() == (leastLoaded.tokens.size() + 1)) return false;
            long token = mostLoaded.tokens.pollFirst();
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
        private final TreeSet<Long> tokens;

        public LoadPair(Integer partition, TreeSet<Long> tokens) {
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
