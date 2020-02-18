/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
package org.voltdb.utils;

import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.voltcore.utils.Pair;

/**
 * @author rdykiel
 *
 * This class processes a list of Pairs(X, Y) and builds sequences.
 *
 * Each Pair(X, Y) represents a relationship of "X is followed by Y"; the sequencer
 * accepts the pairs in any order and builds the sequences from them.
 *
 * Basic example:
 * Given the pairs:         [<3, 4>, <8, 9>, <6, 3>, <5, 6>, <9, 1>, <7, 8>, <2, 5>, <1, 2>]
 * The sequence found is:   [7, 8, 9, 1, 2, 5, 6, 3, 4]
 *
 * The sequencer is able to handle disjointed sequences:
 *
 * FIXME:
 * - add a verification that ensures the uniqueness of each node
 *
*/
public class PairSequencer<T> {

    private LinkedList<Pair<T,T>> m_pairs = new LinkedList<>();

    public static class CyclicSequenceException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        CyclicSequenceException() { super(); }
        CyclicSequenceException(String s) { super(s); }
    }

    public void add(Pair<T, T> pair) {
        m_pairs.addLast(pair);
    }

    public void addAll(Collection<Pair<T, T>> collection) {
        m_pairs.addAll(collection);
    }

    public Deque<Deque<T>> getSequences() throws CyclicSequenceException {

        Deque<Deque<T>> result = new LinkedList<>();
        if (m_pairs.isEmpty()) {
            return result;
        }

        // Populate result with lists formed by each pair
        for (Pair<T, T> pair : m_pairs) {
            LinkedList<T> list = new LinkedList<>();
            list.add(pair.getFirst());
            list.add(pair.getSecond());
            result.add(list);
        }
        if (result.size() == 1) {
            return result;
        }

        // Now aggregate the lists: we're done when the result is one list
        // or we can't further aggregate any of the list elements.
        int count = 0;
        do {
            Deque<T> candidate = result.removeFirst();
            for (Deque<T> list : result) {
                // try to append the candidate at the front or at the tail
                if (candidate.peekLast().equals(list.peekFirst())) {
                    // Insert candidate in front
                    candidate.removeLast();
                    while(!candidate.isEmpty()) {
                        list.addFirst(candidate.removeLast());
                    }
                    candidate = null;
                    count = 0;
                    break;

                } else if (candidate.peekFirst().equals(list.peekLast())) {
                    // Add candidate to tail
                    candidate.removeFirst();
                    list.addAll(candidate);
                    candidate = null;
                    count = 0;
                    break;
                }
            }

            // If the candidate wasn't appended, count this
            if (candidate != null) {
                result.addLast(candidate);
                count += 1;
            }

        } while (count != result.size());

        validateResult(result);
        return result;
    }

    private void validateResult(Deque<Deque<T>> result) throws CyclicSequenceException {
        Set<T> resultSet = new HashSet<>();
        Set<T> cyclicSet = new HashSet<>();

        try {
            for (Deque<T> sequence : result) {
                for (T item : sequence) {
                    if (resultSet.contains(item)) {
                        if (!cyclicSet.contains(item)) {
                            cyclicSet.add(item);
                        }
                    } else {
                        resultSet.add(item);
                    }
                }
            }
            if (!cyclicSet.isEmpty()) {
                throw new CyclicSequenceException("Cyclic sequence with these elements " + cyclicSet);
            }
        } finally {
            resultSet.clear();
            cyclicSet.clear();
        }
    }
}
