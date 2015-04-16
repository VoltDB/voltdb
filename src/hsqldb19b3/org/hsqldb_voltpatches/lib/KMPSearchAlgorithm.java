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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 * Implements the Knuth-Morris-Pratt string search algorithm for searching
 * streams or arrays of octets or characters. <p>
 *
 * This algorithm is a good choice for searching large, forward-only access
 * streams for repeated search using pre-processed small to medium sized
 * patterns.  <p>
 *
 * This is because in addition to the facts that it:
 *
 * <ul>
 * <li>does not require pre-processing the searched data (only the pattern)
 * <li>scans strictly left-to-right
 * <li>does not need to perform back tracking
 * <li>does not need to employ reverse scan order
 * <li>does not need to perform effectively random access lookups against
 *     the searched data or pattern
 * </ul>
 *
 * it also has:
 *
 * <ul>
 * <li>a very simple, highly predictable behavior
 * <li>an O(n) complexity once the a search pattern is preprocessed
 * <li>an O(m) complexity for preprocessing search patterns
 * <li>a worst case performance characteristic of only 2n
 * <li>a typical performance characteristic that is deemed to be
 *     2-3 times better than the naive search algorithm employed by
 *     {@link String#indexOf(java.lang.String,int)}.
 * </ul>
 *
 * Note that the Boyer-Moore algorithm is generally considered to be the better
 * practical, all-round exact sub-string search algorithm, but due to its
 * reverse pattern scan order, performance considerations dictate that it
 * requires more space and that is somewhat more complex to implement
 * efficiently for searching forward-only access streams. <p>
 *
 * In  particular, its higher average performance is biased toward larger
 * search patterns, due to its ability to skip ahead further and with fewer
 * tests under reverse pattern scan.  But when searching forward-only access
 * streams, overall performance considerations require the use a circular buffer
 * of the same size as the search pattern to hold data from the searched stream
 * as it is being compared in reverse order to the search pattern.  Hence,
 * Boyer-Moore requires at minimum twice the memory required by Knuth-Morris-Pratt
 * to search for the same pattern and that factor has the greatest impact
 * precisely on the same class of patterns (larger) for which it is most
 * outperforms Knuth-Morris-Pratt.
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @version 2.1
 * @since 2.1
 * @see <a href="http://en.wikipedia.org/wiki/Knuth%E2%80%93Morris%E2%80%93Pratt_algorithm">Knuth-Morris-Pratt algorithm</a>
 */
public class KMPSearchAlgorithm {

    /**
     * Searches the given octet stream for the given octet pattern
     * returning the zero-based offset from the initial stream position
     * at which the first match is detected. <p>
     *
     * Note that the signature includes a slot for the table so that
     * searches for a pattern can be performed multiple times without
     * incurring the overhead of computing the table each time.
     *
     * @param inputStream in which to search
     * @param pattern for which to search
     * @param table computed from the pattern that optimizes the search.
     *        If null, automatically computed.
     * @return zero-based offset of first match; -1 if no match found.
     * @throws IOException when an error occurs accessing the input stream.
     */
    public static long search(final InputStream inputStream,
                              final byte[] pattern,
                              int[] table) throws IOException {

        if (inputStream == null || pattern == null || pattern.length == 0) {
            return -1;
        }

        //
        final int patternLength = pattern.length;

        //
        long streamIndex = -1;
        int  currentByte;

        if (patternLength == 1) {
            final int byteToFind = pattern[0];

            while (-1 != (currentByte = inputStream.read())) {
                streamIndex++;

                if (currentByte == byteToFind) {
                    return streamIndex;
                }
            }

            return -1;
        }

        int patternIndex = 0;

        if (table == null) {
            table = computeTable(pattern);
        }

        while (-1 != (currentByte = inputStream.read())) {
            streamIndex++;

            if (currentByte == pattern[patternIndex]) {
                patternIndex++;
            } else if (patternIndex > 0) {
                patternIndex = table[patternIndex];

                patternIndex++;
            }

            if (patternIndex == patternLength) {
                return streamIndex - (patternLength - 1);
            }
        }

        return -1;
    }

    /**
     * Searches the given character stream for the given character pattern
     * returning the zero-based offset from the initial stream position
     * at which the first match is detected. <p>
     *
     * Note that the signature includes a slot for the table so that
     * searches for a pattern can be performed multiple times without
     * incurring the overhead of computing the table each time.
     *
     * @param reader in which to search
     * @param pattern for which to search
     * @param table computed from the pattern that optimizes the search
     *        If null, automatically computed.
     * @return zero-based offset of first match; -1 if no match found.
     * @throws IOException when an error occurs accessing the input stream.
     */
    public static long search(final Reader reader, final char[] pattern,
                              int[] table) throws IOException {

        if (reader == null || pattern == null || pattern.length == 0) {
            return -1;
        }

        //
        final int patternLength = pattern.length;

        //
        long streamIndex = -1;
        int  currentCharacter;

        if (patternLength == 1) {
            final int characterToFind = pattern[0];

            while (-1 != (currentCharacter = reader.read())) {
                streamIndex++;

                if (currentCharacter == characterToFind) {
                    return streamIndex;
                }
            }

            return -1;
        }

        int patternIndex = 0;

        if (table == null) {
            table = computeTable(pattern);
        }

        while (-1 != (currentCharacter = reader.read())) {
            streamIndex++;

            if (currentCharacter == pattern[patternIndex]) {
                patternIndex++;
            } else if (patternIndex > 0) {
                patternIndex = table[patternIndex];

                patternIndex++;
            }

            if (patternIndex == patternLength) {
                return streamIndex - (patternLength - 1);
            }
        }

        return -1;
    }

    /**
     * Searches the given octet string for the given octet pattern
     * returning the zero-based offset from given start position
     * at which the first match is detected. <p>
     *
     * Note that the signature includes a slot for the table so that
     * searches for a pattern can be performed multiple times without
     * incurring the overhead of computing the table each time.
     *
     * @param source array in which to search
     * @param pattern to be matched
     * @param table computed from the pattern that optimizes the search
     *        If null, automatically computed.
     * @param start position in source at which to start the search
     */
    public static int search(final byte[] source, final byte[] pattern,
                             int[] table, final int start) {

        if (source == null || pattern == null || pattern.length == 0) {
            return -1;
        }

        //
        final int sourceLength  = source.length;
        final int patternLength = pattern.length;

        //
        int sourceIndex = start;

        if (patternLength == 1) {
            final int byteToFind = pattern[0];

            for (; sourceIndex < sourceLength; sourceIndex++) {
                if (source[sourceIndex] == byteToFind) {
                    return sourceIndex;
                }
            }

            return -1;
        }

        //
        int matchStart   = start;
        int patternIndex = 0;

        //
        if (table == null) {
            table = computeTable(pattern);
        }

        //
        while ((sourceIndex < sourceLength)
                && (patternIndex < patternLength)) {
            if (source[sourceIndex] == pattern[patternIndex]) {
                patternIndex++;
            } else {
                final int tableVaue = table[patternIndex];

                matchStart += (patternIndex - tableVaue);

                if (patternIndex > 0) {
                    patternIndex = tableVaue;
                }

                patternIndex++;
            }

            sourceIndex = (matchStart + patternIndex);
        }

        if (patternIndex == patternLength) {
            return matchStart;
        } else {
            return -1;
        }
    }

    /**
     * Searches the given character array for the given character pattern
     * returning the zero-based offset from given start position
     * at which the first match is detected.
     *
     * @param source array in which to search
     * @param pattern to be matched
     * @param table computed from the pattern that optimizes the search
     *        If null, automatically computed.
     * @param start position in source at which to start the search
     */
    public static int search(final char[] source, final char[] pattern,
                             int[] table, final int start) {

        if (source == null || pattern == null || pattern.length == 0) {
            return -1;
        }

        final int sourceLength  = source.length;
        final int patternLength = pattern.length;
        int       sourceIndex   = start;

        if (patternLength == 1) {
            final int characterToFind = pattern[0];

            for (; sourceIndex < sourceLength; sourceIndex++) {
                if (source[sourceIndex] == characterToFind) {
                    return sourceIndex;
                }
            }

            return -1;
        }

        //
        int matchStart   = start;
        int patternIndex = 0;

        //
        if (table == null) {
            table = computeTable(pattern);
        }

        //
        while ((sourceIndex < sourceLength)
                && (patternIndex < patternLength)) {
            if (source[sourceIndex] == pattern[patternIndex]) {
                patternIndex++;
            } else {
                final int tableValue = table[patternIndex];

                matchStart += (patternIndex - tableValue);

                if (patternIndex > 0) {
                    patternIndex = tableValue;
                }

                patternIndex++;
            }

            sourceIndex = (matchStart + patternIndex);
        }

        if (patternIndex == patternLength) {
            return matchStart;
        } else {
            return -1;
        }
    }

    /**
     * Searches the given String object for the given character pattern
     * returning the zero-based offset from given start position
     * at which the first match is detected.
     *
     * @param source array to be searched
     * @param pattern to be matched
     * @param table computed from the pattern that optimizes the search
     * @param start position in source at which to start the search
     */
    public static int search(final String source, final String pattern,
                             int[] table, final int start) {

        if (source == null || pattern == null || pattern.length() == 0) {
            return -1;
        }

        final int patternLength = pattern.length();

        //
        if (patternLength == 1) {
            return source.indexOf(pattern, start);
        }

        //
        final int sourceLength = source.length();

        //
        int matchStart   = start;
        int sourceIndex  = start;
        int patternIndex = 0;

        //
        if (table == null) {
            table = computeTable(pattern);
        }

        //
        while ((sourceIndex < sourceLength)
                && (patternIndex < patternLength)) {
            if (source.charAt(sourceIndex) == pattern.charAt(patternIndex)) {
                patternIndex++;
            } else {
                final int tableValue = table[patternIndex];

                matchStart += (patternIndex - tableValue);

                if (patternIndex > 0) {
                    patternIndex = tableValue;
                }

                patternIndex++;
            }

            sourceIndex = matchStart + patternIndex;
        }

        if (patternIndex == patternLength) {
            return matchStart;
        } else {
            return -1;
        }
    }

    /**
     * computes the table used to optimize octet pattern search
     *
     * @param pattern for which to compute the table.
     * @return the table computed from the octet pattern.
     */
    public static int[] computeTable(final byte[] pattern) {

        if (pattern == null) {
            throw new IllegalArgumentException("Pattern must  not be null.");
        } else if (pattern.length < 2) {
            throw new IllegalArgumentException("Pattern length must be > 1.");
        }

        //
        final int[] table = new int[pattern.length];
        int         i     = 2;
        int         j     = 0;

        //
        table[0] = -1;
        table[1] = 0;

        //
        while (i < pattern.length) {
            if (pattern[i - 1] == pattern[j]) {
                table[i] = j + 1;

                j++;
                i++;
            } else if (j > 0) {
                j = table[j];
            } else {
                table[i] = 0;

                i++;

                j = 0;
            }
        }

        //
        return table;
    }

    public static int[] computeTable(final char[] pattern) {

        if (pattern == null) {
            throw new IllegalArgumentException("Pattern must  not be null.");
        } else if (pattern.length < 2) {
            throw new IllegalArgumentException("Pattern length must be > 1.");
        }

        int[] table = new int[pattern.length];
        int   i     = 2;
        int   j     = 0;

        table[0] = -1;
        table[1] = 0;

        while (i < pattern.length) {
            if (pattern[i - 1] == pattern[j]) {
                table[i] = j + 1;

                j++;
                i++;
            } else if (j > 0) {
                j = table[j];
            } else {
                table[i] = 0;

                i++;

                j = 0;
            }
        }

        return table;
    }

    public static int[] computeTable(final String pattern) {

        if (pattern == null) {
            throw new IllegalArgumentException("Pattern must  not be null.");
        } else if (pattern.length() < 2) {
            throw new IllegalArgumentException("Pattern length must be > 1.");
        }

        final int patternLength = pattern.length();

        //
        int[] table = new int[patternLength];
        int   i     = 2;
        int   j     = 0;

        table[0] = -1;
        table[1] = 0;

        while (i < patternLength) {
            if (pattern.charAt(i - 1) == pattern.charAt(j)) {
                table[i] = j + 1;

                j++;
                i++;
            } else if (j > 0) {
                j = table[j];
            } else {
                table[i] = 0;

                i++;

                j = 0;
            }
        }

        return table;
    }
}
