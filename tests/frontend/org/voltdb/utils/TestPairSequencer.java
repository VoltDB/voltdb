/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import org.junit.Test;
import org.voltcore.utils.Pair;
import org.voltdb.utils.PairSequencer.CyclicSequenceException;

/**
 * @author rdykiel
 *
 */
public class TestPairSequencer {

    @Test
    public void testBasicSequencing() {

        List<Integer> origSequence = Arrays.asList(new Integer[] { 7, 8, 9, 1, 2, 5, 6, 3, 4 });
        System.out.println("Original sequence: " + origSequence);

        List<Pair<Integer, Integer>> pairs = makePairs(origSequence);
        System.out.println("Paired sequence: " + pairs);
        System.out.println();

        for (int i = 0; i < 10; i++) {
            Collections.shuffle(pairs);
            System.out.println("Shuffled pairs: " + pairs);

            PairSequencer<Integer> sequencer = new PairSequencer<>();
            sequencer.addAll(pairs);

            Deque<Deque<Integer>> seqs = sequencer.getSequences();
            assertEquals(1, seqs.size());

            Deque<Integer> finalSequence = seqs.getFirst();
            System.out.println("Final sequence: " + finalSequence);
            assertEquals(origSequence, finalSequence);
            System.out.println();
        }
    }

    @Test
    public void testDisjointedSequencing() {

        List<Integer> origSequence1 = Arrays.asList(new Integer[] { 7, 8, 9, 1, 2, 5, 6, 3, 4 });
        System.out.println("Original sequence 1: " + origSequence1);
        List<Pair<Integer, Integer>> pairs1 = makePairs(origSequence1);

        List<Integer> origSequence2 = Arrays.asList(new Integer[] { 17, 18, 19, 11, 12, 15, 16, 13, 44 });
        System.out.println("Original sequence 2: " + origSequence2);
        List<Pair<Integer, Integer>> pairs2 = makePairs(origSequence2);

        pairs1.addAll(pairs2);
        System.out.println("Joint Paired sequence: " + pairs1);
        System.out.println();

        for (int i = 0; i < 10; i++) {
            Collections.shuffle(pairs1);
            System.out.println("Shuffled pairs: " + pairs1);

            PairSequencer<Integer> sequencer = new PairSequencer<>();
            sequencer.addAll(pairs1);

            Deque<Deque<Integer>> seqs = sequencer.getSequences();
            assertEquals(2, seqs.size());

            boolean found1 = false;
            boolean found2 = false;
            for (Deque<Integer> result : seqs) {
                System.out.println("Final sequence: " + result);
                if (origSequence1.equals(result)) {
                    found1 = true;
                }
                if (origSequence2.equals(result)) {
                    found2 = true;
                }
            }
            assertTrue(found1);
            assertTrue(found2);
            System.out.println();
        }
    }

    @Test
    public void testCyclicSequencing() {

        List<Integer> origSequence = Arrays.asList(new Integer[] { 7, 8, 9, 1, 2, 9, 5, 6, 7, 3, 4 });
        System.out.println("Original sequence: " + origSequence);

        List<Pair<Integer, Integer>> pairs = makePairs(origSequence);
        System.out.println("Paired sequence: " + pairs);
        System.out.println();

        for (int i = 0; i < 10; i++) {
            Collections.shuffle(pairs);
            System.out.println("Shuffled pairs: " + pairs);

            PairSequencer<Integer> sequencer = new PairSequencer<>();
            sequencer.addAll(pairs);

            try {
                Deque<Deque<Integer>> seqs = sequencer.getSequences();
                assertFalse(seqs == null);
            }
            catch (CyclicSequenceException e) {
                System.out.println("Got expected error :" + e);
            }
            System.out.println();
        }
    }

    private static List<Pair<Integer, Integer>> makePairs(List<Integer> sequence) {

        ArrayList<Pair<Integer, Integer>> result = new ArrayList<>();

        for (int i = 0; i < sequence.size() - 1; i++) {
            result.add(new Pair<Integer, Integer>(sequence.get(i), sequence.get(i+1)));
        }
        return result;
    }
}
