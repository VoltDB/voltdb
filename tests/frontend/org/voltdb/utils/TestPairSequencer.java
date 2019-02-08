/**
 *
 */
package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.voltcore.utils.Pair;

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

            LinkedList<LinkedList<Integer>> seqs = sequencer.getSequences();
            assertEquals(1, seqs.size());

            List<Integer> finalSequence = seqs.getFirst();
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

            LinkedList<LinkedList<Integer>> seqs = sequencer.getSequences();
            assertEquals(2, seqs.size());

            boolean found1 = false;
            boolean found2 = false;
            for (List<Integer> result : seqs) {
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

    private static List<Pair<Integer, Integer>> makePairs(List<Integer> sequence) {

        ArrayList<Pair<Integer, Integer>> result = new ArrayList<>();

        for (int i = 0; i < sequence.size() - 1; i++) {
            result.add(new Pair<Integer, Integer>(sequence.get(i), sequence.get(i+1)));
        }
        return result;
    }
}
