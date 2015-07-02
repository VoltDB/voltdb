/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.lib;

import java.util.Comparator;

/**
 * FastQSorts the [l,r] partition (inclusive) of the specfied array of
 * Rows, using the comparator.<p>
 *
 * Searches an ordered array.<p>
 *
 * @author Tony Lai (tony_lai@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class ArraySort {

    /**
     * Returns the index of the lowest element == the given search target,
     * or -1
     * @return index or a negative value if not found
     */
    public static int searchFirst(Object[] array, int start, int limit,
                                  Object value, Comparator c) {

        int low     = start;
        int high    = limit;
        int mid     = start;
        int compare = 0;
        int found   = limit;

        while (low < high) {
            mid     = (low + high) >>> 1;
            compare = c.compare(value, array[mid]);

            if (compare < 0) {
                high = mid;
            } else if (compare > 0) {
                low = mid + 1;
            } else {
                high  = mid;
                found = mid;
            }
        }

        return found == limit ? -low - 1
                              : found;
    }

    public static int deDuplicate(Object[] array, int start, int limit,
                                  Comparator comparator) {

        int baseIndex    = start;
        int currentIndex = start + 1;

        if (array.length == 0) {
            return 0;
        }

        for (; currentIndex < limit; currentIndex++) {
            int compare = comparator.compare(array[baseIndex],
                                             array[currentIndex]);

            if (compare == 0) {
                continue;
            }

            baseIndex++;

            array[baseIndex] = array[currentIndex];
        }

        return baseIndex + 1;
    }

    public static void sort(Object[] array, int start, int limit,
                            Comparator comparator) {

        if (start + 1 >= limit) {
            return;
        }

        quickSort(array, comparator, start, limit - 1);
        insertionSort(array, comparator, start, limit - 1);
    }

    static void quickSort(Object[] array, Comparator comparator, int l,
                          int r) {

        int M = 16;
        int i;
        int j;
        int v;

        if ((r - l) > M) {
            i = (r + l) >>> 1;

            if (comparator.compare(array[i], array[l]) < 0) {
                swap(array, l, i);    // Tri-Median Methode!
            }

            if (comparator.compare(array[r], array[l]) < 0) {
                swap(array, l, r);
            }

            if (comparator.compare(array[r], array[i]) < 0) {
                swap(array, i, r);
            }

            j = r - 1;

            swap(array, i, j);

            i = l;
            v = j;

            for (;;) {
                while (comparator.compare(array[++i], array[v]) < 0) {}

                while (comparator.compare(array[v], array[--j]) < 0) {}

                if (j < i) {
                    break;
                }

                swap(array, i, j);
            }

            swap(array, i, r - 1);
            quickSort(array, comparator, l, j);
            quickSort(array, comparator, i + 1, r);
        }
    }

    public static void insertionSort(Object[] array, Comparator comparator,
                                     int lo0, int hi0) {

        int i;
        int j;

        for (i = lo0 + 1; i <= hi0; i++) {
            j = i;

            while ((j > lo0)
                    && comparator.compare(array[i], array[j - 1]) < 0) {
                j--;
            }

            if (i != j) {
                moveAndInsertRow(array, i, j);
            }
        }
    }

    private static void swap(Object[] array, int i1, int i2) {

        Object val = array[i1];

        array[i1] = array[i2];
        array[i2] = val;
    }

    private static void moveAndInsertRow(Object[] array, int i, int j) {

        Object val = array[i];

        moveRows(array, j, j + 1, i - j);

        array[j] = val;
    }

    private static void moveRows(Object[] array, int fromIndex, int toIndex,
                                 int rows) {
        System.arraycopy(array, fromIndex, array, toIndex, rows);
    }
}
