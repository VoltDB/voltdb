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

package org.voltdb.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to generate permutations of an arbitrary sequence of objects.
 *
 */
public class PermutationGenerator {
    public static <T> List<List<T>> generatePurmutations(List<T> sequence) {
        List<List<T>> purmutations = new ArrayList<List<T>>();
        generatePurmutations(sequence, 0, purmutations);
        return purmutations;
    }

    private static <T> void generatePurmutations(List<T> sequence, int idx, List<List<T>> permutations) {
        if (idx == sequence.size()) {
            // if we are at the end of the array, we have one permutation
            List<T> purmutation = new ArrayList<T>();
            purmutation.addAll(sequence);
            permutations.add(purmutation);
        } else {
            // recursively explore the permutations starting
            //  at index idx going through the last element in the sequence
            generatePurmutations(sequence, idx+1, permutations);
            for (int i = idx + 1; i < sequence.size(); i++) {
                // swap i and idx elements
                swapElements(sequence, i, idx);
                generatePurmutations(sequence, idx+1, permutations);
                // swap them back
                swapElements(sequence, i, idx);
            }

        }
    }

    private static <T> void swapElements(List<T> sequence, int i, int j) {
        assert (i < sequence.size() && j < sequence.size());
        T temp = sequence.get(i);
        sequence.set(i, sequence.get(j));
        sequence.set(j, temp);
    }
}
