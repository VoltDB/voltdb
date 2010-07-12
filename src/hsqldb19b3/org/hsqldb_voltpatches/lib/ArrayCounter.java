/* Copyright (c) 2001-2009, The HSQL Development Group
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

/**
 * Collection of routines for counting the distribution of the values
 * in an int[] array.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.7.2
 * @since 1.7.2
 */
public class ArrayCounter {

    /**
     * Returns an int[] array of length segments containing the distribution
     * count of the elements in unsorted int[] array with values between min
     * and max (range). Values outside the min-max range are ignored<p>
     *
     * A usage example is determining the count of people of each age group
     * in a large int[] array containing the age of each person. Called with
     * (array, 16,0,79), it will return an int[16] with the first element
     * the count of people aged 0-4, the second element the count of those
     * aged 5-9, and so on. People above the age of 79 are excluded. If the
     * range is not a multiple of segments, the last segment will be cover a
     * smaller sub-range than the rest.
     *
     */
    public static int[] countSegments(int[] array, int elements,
                                      int segments, int start, int limit) {

        int[] counts   = new int[segments];
        long  interval = calcInterval(segments, start, limit);
        int   index    = 0;
        int   element  = 0;

        if (interval <= 0) {
            return counts;
        }

        for (int i = 0; i < elements; i++) {
            element = array[i];

            if (element < start || element >= limit) {
                continue;
            }

            index = (int) ((element - start) / interval);

            counts[index]++;
        }

        return counts;
    }

    /**
     * With an unsorted int[] array and with target a positive integer in the
     * range (1,array.length), finds the value in the range (start,limit) of the
     * largest element (rank) where the count of all smaller elements in that
     * range is less than or equals target. Parameter margin indicates the
     * margin of error in target<p>
     *
     * In statistics, this can be used to calculate a median or quadrile value.
     * A usage example applied to an array of age values is to determine
     * the maximum age of a given number of people. With the example array
     * given in countSegments, rank(array, c, 6000, 18, 65, 0) will return an age
     * value between 18-64 (inclusive) and the count of all people aged between
     * 18 and the returned value(exclusive) will be less than or equal 6000.
     *
     */
    public static int rank(int[] array, int elements, int target, int start,
                           int limit, int margin) {

        final int segments     = 256;
        int       elementCount = 0;
        int       currentLimit = limit;

        for (;;) {
            long interval = calcInterval(segments, start, currentLimit);
            int[] counts = countSegments(array, elements, segments, start,
                                         currentLimit);

            for (int i = 0; i < counts.length; i++) {
                if (elementCount + counts[i] < target) {
                    elementCount += counts[i];
                    start        += interval;
                } else {
                    break;
                }
            }

            if (elementCount + margin >= target) {
                return start;
            }

            if (interval <= 1) {
                return start;
            }

            currentLimit = start + interval < limit ? (int) (start + interval)
                                                    : limit;
        }
    }

    /**
     * Helper method to calculate the span of the sub-interval. Simply returns
     * the cieling of ((limit - start) / segments) and accounts for invalid
     * start and limit combinations.
     */
    static long calcInterval(int segments, int start, int limit) {

        long range = limit - start;

        if (range < 0) {
            return 0;
        }

        int partSegment = (range % segments) == 0 ? 0
                                                  : 1;

        return (range / segments) + partSegment;
    }
}
