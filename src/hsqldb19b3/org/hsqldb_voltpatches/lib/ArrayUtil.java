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

import java.lang.reflect.Array;

/**
 * Collection of static methods for operations on arrays
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public class ArrayUtil {

    public static final int        CLASS_CODE_BYTE    = 'B';
    public static final int        CLASS_CODE_CHAR    = 'C';
    public static final int        CLASS_CODE_DOUBLE  = 'D';
    public static final int        CLASS_CODE_FLOAT   = 'F';
    public static final int        CLASS_CODE_INT     = 'I';
    public static final int        CLASS_CODE_LONG    = 'J';
    public static final int        CLASS_CODE_OBJECT  = 'L';
    public static final int        CLASS_CODE_SHORT   = 'S';
    public static final int        CLASS_CODE_BOOLEAN = 'Z';
    private static IntValueHashMap classCodeMap       = new IntValueHashMap();

    static {
        classCodeMap.put(byte.class, ArrayUtil.CLASS_CODE_BYTE);
        classCodeMap.put(char.class, ArrayUtil.CLASS_CODE_SHORT);
        classCodeMap.put(short.class, ArrayUtil.CLASS_CODE_SHORT);
        classCodeMap.put(int.class, ArrayUtil.CLASS_CODE_INT);
        classCodeMap.put(long.class, ArrayUtil.CLASS_CODE_LONG);
        classCodeMap.put(float.class, ArrayUtil.CLASS_CODE_FLOAT);
        classCodeMap.put(double.class, ArrayUtil.CLASS_CODE_DOUBLE);
        classCodeMap.put(boolean.class, ArrayUtil.CLASS_CODE_BOOLEAN);
        classCodeMap.put(Object.class, ArrayUtil.CLASS_CODE_OBJECT);
    }

    /**
     * Returns a distinct int code for each primitive type and for all Object types.
     */
    static int getClassCode(Class cla) {

        if (!cla.isPrimitive()) {
            return ArrayUtil.CLASS_CODE_OBJECT;
        }

        return classCodeMap.get(cla, -1);
    }

    /**
     * Clears an area of the given array of the given type.
     */
    public static void clearArray(int type, Object data, int from, int to) {

        switch (type) {

            case ArrayUtil.CLASS_CODE_BYTE : {
                byte[] array = (byte[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_CHAR : {
                byte[] array = (byte[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_SHORT : {
                short[] array = (short[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_INT : {
                int[] array = (int[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_LONG : {
                long[] array = (long[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_FLOAT : {
                float[] array = (float[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_DOUBLE : {
                double[] array = (double[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_BOOLEAN : {
                boolean[] array = (boolean[]) data;

                while (--to >= from) {
                    array[to] = false;
                }

                return;
            }
            default : {
                Object[] array = (Object[]) data;

                while (--to >= from) {
                    array[to] = null;
                }

                return;
            }
        }
    }

    /**
     * Moves the contents of an array to allow both addition and removal of
     * elements. Used arguments must be in range.
     *
     * @param type class type of the array
     * @param array the array
     * @param usedElements count of elements of array in use
     * @param index point at which to add or remove elements
     * @param count number of elements to add or remove
     */
    public static void adjustArray(int type, Object array, int usedElements,
                                   int index, int count) {

        if (index >= usedElements) {
            return;
        }

        int newCount = usedElements + count;
        int source;
        int target;
        int size;

        if (count >= 0) {
            source = index;
            target = index + count;
            size   = usedElements - index;
        } else {
            source = index - count;
            target = index;
            size   = usedElements - index + count;
        }

        if (size > 0) {
            System.arraycopy(array, source, array, target, size);
        }

        if (count < 0) {
            clearArray(type, array, newCount, usedElements);
        }
    }

    /**
     * Basic sort for small arrays of int.
     */
    public static void sortArray(int[] array) {

        boolean swapped;

        do {
            swapped = false;

            for (int i = 0; i < array.length - 1; i++) {
                if (array[i] > array[i + 1]) {
                    int temp = array[i + 1];

                    array[i + 1] = array[i];
                    array[i]     = temp;
                    swapped      = true;
                }
            }
        } while (swapped);
    }

    /**
     *  Basic find for small arrays of Object.
     */
    public static int find(Object[] array, Object object) {

        for (int i = 0; i < array.length; i++) {
            if (array[i] == object) {

                // hadles both nulls
                return i;
            }

            if (object != null && object.equals(array[i])) {
                return i;
            }
        }

        return -1;
    }

    /**
     *  Basic find for small arrays of int.
     */
    public static int find(int[] array, int value) {

        for (int i = 0; i < array.length; i++) {
            if (array[i] == value) {
                return i;
            }
        }

        return -1;
    }

    public static int find(short[] array, int value) {

        for (int i = 0; i < array.length; i++) {
            if (array[i] == value) {
                return i;
            }
        }

        return -1;
    }

    public static int find(short[] array, int value, int offset, int count) {

        for (int i = offset; i < offset + count; i++) {
            if (array[i] == value) {
                return i;
            }
        }

        return -1;
    }

    /**
     *  Finds the first element of the array that is not equal to the given value.
     */
    public static int findNot(int[] array, int value) {

        for (int i = 0; i < array.length; i++) {
            if (array[i] != value) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Returns true if arra and arrb contain the same set of integers, not
     * necessarily in the same order. This implies the arrays are of the same
     * length.
     */
    public static boolean areEqualSets(int[] arra, int[] arrb) {
        return arra.length == arrb.length
               && ArrayUtil.haveEqualSets(arra, arrb, arra.length);
    }

    /**
     * For full == true returns true if arra and arrb are identical (have the
     * same length and contain the same integers in the same sequence).
     *
     * For full == false returns the result
     * of haveEqualArrays(arra,arrb,count)
     *
     * For full == true, the array lengths must be the same as count
     *
     */
    public static boolean areEqual(int[] arra, int[] arrb, int count,
                                   boolean full) {

        if (ArrayUtil.haveEqualArrays(arra, arrb, count)) {
            if (full) {
                return arra.length == arrb.length && count == arra.length;
            }

            return true;
        }

        return false;
    }

    /**
     * Returns true if the first count elements of arra and arrb are identical
     * sets of integers (not necessarily in the same order).
     *
     */
    public static boolean haveEqualSets(int[] arra, int[] arrb, int count) {

        if (ArrayUtil.haveEqualArrays(arra, arrb, count)) {
            return true;
        }

        if (count > arra.length || count > arrb.length) {
            return false;
        }

        if (count == 1) {
            return arra[0] == arrb[0];
        }

        int[] tempa = (int[]) resizeArray(arra, count);
        int[] tempb = (int[]) resizeArray(arrb, count);

        sortArray(tempa);
        sortArray(tempb);

        for (int j = 0; j < count; j++) {
            if (tempa[j] != tempb[j]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns true if the first count elements of arra and arrb are identical
     * subarrays of integers
     *
     */
    public static boolean haveEqualArrays(int[] arra, int[] arrb, int count) {

        if (count > arra.length || count > arrb.length) {
            return false;
        }

        for (int j = 0; j < count; j++) {
            if (arra[j] != arrb[j]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns true if the first count elements of arra and arrb are identical
     * subarrays of Objects
     *
     */
    public static boolean haveEqualArrays(Object[] arra, Object[] arrb,
                                          int count) {

        if (count > arra.length || count > arrb.length) {
            return false;
        }

        for (int j = 0; j < count; j++) {
            if (arra[j] != arrb[j]) {
                if (arra[j] == null || !arra[j].equals(arrb[j])) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Returns true if arra and the first bcount elements of arrb share any
     * element. <p>
     *
     * Used for checks for any overlap between two arrays of column indexes.
     */
    public static boolean haveCommonElement(int[] arra, int[] arrb,
            int bcount) {

        for (int i = 0; i < arra.length; i++) {
            int c = arra[i];

            for (int j = 0; j < bcount; j++) {
                if (c == arrb[j]) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Returns an int[] containing elements shared between the two arrays
     * arra and arrb. The arrays contain sets (no value is repeated).
     *
     * Used to find the overlap between two arrays of column indexes.
     * Ordering of the result arrays will be the same as in array
     * a. The method assumes that each index is only listed
     * once in the two input arrays.
     * <p>
     * e.g.
     * </p>
     * <code>
     * <table width="90%" bgcolor="lightblue">
     * <tr><td colspane="3">The arrays</td></tr>
     * <tr><td>int []arra</td><td>=</td><td>{2,11,5,8}</td></tr>
     * <tr><td>int []arrb</td><td>=</td><td>{20,8,10,11,28,12}</td></tr>
     * <tr><td colspane="3">will result in:</td></tr>
     * <tr><td>int []arrc</td><td>=</td><td>{11,8}</td></tr>
     * </table>
     *
     * @param arra int[]; first column indexes
     * @param arrb int[]; second column indexes
     * @return int[] common indexes or <code>null</code> if there is no overlap.
     */
    public static int[] commonElements(int[] arra, int[] arrb) {

        int[] c = null;
        int   n = countCommonElements(arra, arrb);

        if (n > 0) {
            c = new int[n];

            int k = 0;

            for (int i = 0; i < arra.length; i++) {
                for (int j = 0; j < arrb.length; j++) {
                    if (arra[i] == arrb[j]) {
                        c[k++] = arra[i];
                    }
                }
            }
        }

        return c;
    }

    /**
     * Returns the number of elements shared between the two arrays containing
     * sets.<p>
     *
     * Return the number of elements shared by two column index arrays.
     * This method assumes that each of these arrays contains a set (each
     * element index is listed only once in each index array). Otherwise the
     * returned number will NOT represent the number of unique column indexes
     * shared by both index array.
     *
     * @param arra int[]; first array of column indexes.
     *
     * @param arrb int[]; second array of column indexes
     *
     * @return int; number of elements shared by <code>a</code> and <code>b</code>
     */
    public static int countCommonElements(int[] arra, int[] arrb) {

        int k = 0;

        for (int i = 0; i < arra.length; i++) {
            for (int j = 0; j < arrb.length; j++) {
                if (arra[i] == arrb[j]) {
                    k++;
                }
            }
        }

        return k;
    }

    /**
     * Returns the count of elements in arra from position start that are
     * sequentially equal to the elements of arrb.
     */
    public static int countSameElements(byte[] arra, int start, byte[] arrb) {

        int k     = 0;
        int limit = arra.length - start;

        if (limit > arrb.length) {
            limit = arrb.length;
        }

        for (int i = 0; i < limit; i++) {
            if (arra[i + start] == arrb[i]) {
                k++;
            } else {
                break;
            }
        }

        return k;
    }

    /**
     * Returns the count of elements in arra from position start that are
     * sequentially equal to the elements of arrb.
     */
    public static int countSameElements(char[] arra, int start, char[] arrb) {

        int k     = 0;
        int limit = arra.length - start;

        if (limit > arrb.length) {
            limit = arrb.length;
        }

        for (int i = 0; i < limit; i++) {
            if (arra[i + start] == arrb[i]) {
                k++;
            } else {
                break;
            }
        }

        return k;
    }

    /**
     * Returns the index of the first occurence of arrb in arra. Or -1 if not found.
     */
    public static int find(byte[] arra, int start, int limit, byte[] arrb) {

        int k = start;

        limit = limit - arrb.length + 1;

        int value = arrb[0];

        for (; k < limit; k++) {
            if (arra[k] == value) {
                if (arrb.length == 1) {
                    return k;
                }

                if (containsAt(arra, k, arrb)) {
                    return k;
                }
            }
        }

        return -1;
    }

    /**
     * Returns an index into arra (or -1) where the character is not in the
     * charset byte array.
     */
    public static int findNotIn(byte[] arra, int start, int limit,
                                byte[] charset) {

        int k = 0;

        for (; k < limit; k++) {
            for (int i = 0; i < charset.length; i++) {
                if (arra[k] == charset[i]) {
                    continue;
                }
            }

            return k;
        }

        return -1;
    }

    /**
     * Returns an index into arra (or -1) where the character is in the
     * byteSet byte array.
     */
    public static int findIn(byte[] arra, int start, int limit,
                             byte[] byteSet) {

        int k = 0;

        for (; k < limit; k++) {
            for (int i = 0; i < byteSet.length; i++) {
                if (arra[k] == byteSet[i]) {
                    return k;
                }
            }
        }

        return -1;
    }

    /**
     * Returns the index of b or c in arra. Or -1 if not found.
     */
    public static int find(byte[] arra, int start, int limit, int b, int c) {

        int k = 0;

        for (; k < limit; k++) {
            if (arra[k] == b || arra[k] == c) {
                return k;
            }
        }

        return -1;
    }

    /**
     * Set elements of arrb true if their indexes appear in arrb.
     */
    public static void intIndexesToBooleanArray(int[] arra, boolean[] arrb) {

        for (int i = 0; i < arra.length; i++) {
            if (arra[i] < arrb.length) {
                arrb[arra[i]] = true;
            }
        }
    }

    public static void orBooleanArray(boolean[] source, boolean[] dest) {

        for (int i = 0; i < dest.length; i++) {
            dest[i] |= source[i];
        }
    }

    public static boolean areIntIndexesInBooleanArray(int[] arra,
            boolean[] arrb) {

        for (int i = 0; i < arra.length; i++) {
            if (arrb[arra[i]]) {
                continue;
            }

            return false;
        }

        return true;
    }

    /**
     * Return true if for each true element in arrb, the corresponding
     * element in arra is true
     */
    public static boolean containsAllTrueElements(boolean[] arra,
            boolean[] arrb) {

        for (int i = 0; i < arra.length; i++) {
            if (arrb[i] && !arra[i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Return count of true elements in array
     */
    public static int countTrueElements(boolean[] arra) {

        int count = 0;

        for (int i = 0; i < arra.length; i++) {
            if (arra[i]) {
                count++;
            }
        }

        return count;
    }

    /**
     * Determines if the array has a null column for any of the positions given
     * in the rowColMap array.
     */
    public static boolean hasNull(Object[] array, int[] columnMap) {

        int count = columnMap.length;

        for (int i = 0; i < count; i++) {
            if (array[columnMap[i]] == null) {
                return true;
            }
        }

        return false;
    }

    public static boolean hasAllNull(Object[] array, int[] columnMap) {

        int count = columnMap.length;

        for (int i = 0; i < count; i++) {
            if (array[columnMap[i]] != null) {
                return false;
            }
        }

        return true;
    }


    /**
     * Returns true if arra from position start contains all elements of arrb
     * in sequential order.
     */
    public static boolean containsAt(byte[] arra, int start, byte[] arrb) {
        return countSameElements(arra, start, arrb) == arrb.length;
    }

    /**
     * Returns the count of elements in arra from position start that are
     * among the elements of arrb. Stops at any element not in arrb.
     */
    public static int countStartElementsAt(byte[] arra, int start,
                                           byte[] arrb) {

        int k = 0;

        mainloop:
        for (int i = start; i < arra.length; i++) {
            for (int j = 0; j < arrb.length; j++) {
                if (arra[i] == arrb[j]) {
                    k++;

                    continue mainloop;
                }
            }

            break;
        }

        return k;
    }

    /**
     * Returns true if arra from position start contains all elements of arrb
     * in sequential order.
     */
    public static boolean containsAt(char[] arra, int start, char[] arrb) {
        return countSameElements(arra, start, arrb) == arrb.length;
    }

    /**
     * Returns the count of elements in arra from position start that are not
     * among the elements of arrb.
     *
     */
    public static int countNonStartElementsAt(byte[] arra, int start,
            byte[] arrb) {

        int k = 0;

        mainloop:
        for (int i = start; i < arra.length; i++) {
            for (int j = 0; j < arrb.length; j++) {
                if (arra[i] == arrb[j]) {
                    break mainloop;
                }
            }

            k++;
        }

        return k;
    }

    /**
     * Convenience wrapper for System.arraycopy().
     */
    public static void copyArray(Object source, Object dest, int count) {
        System.arraycopy(source, 0, dest, 0, count);
    }

    /**
     * Returns a range of elements of source from start to end of the array.
     */
    public static int[] arraySlice(int[] source, int start, int count) {

        int[] slice = new int[count];

        System.arraycopy(source, start, slice, 0, count);

        return slice;
    }

    /**
     * Fills the array with a value.
     */
    public static void fillArray(Object[] array, Object value) {

        int to = array.length;

        while (--to >= 0) {
            array[to] = value;
        }
    }

    /**
     * Fills the int array with a value
     */
    public static void fillArray(int[] array, int value) {

        int to = array.length;

        while (--to >= 0) {
            array[to] = value;
        }
    }

    /**
     * Returns a duplicates of an array.
     */
    public static Object duplicateArray(Object source) {

        int size = Array.getLength(source);
        Object newarray =
            Array.newInstance(source.getClass().getComponentType(), size);

        System.arraycopy(source, 0, newarray, 0, size);

        return newarray;
    }

    /**
     * Returns the given array if newsize is the same as existing.
     * Returns a new array of given size, containing as many elements of
     * the original array as it can hold.
     */
    public static Object resizeArrayIfDifferent(Object source, int newsize) {

        int oldsize = Array.getLength(source);

        if (oldsize == newsize) {
            return source;
        }

        Object newarray =
            Array.newInstance(source.getClass().getComponentType(), newsize);

        if (oldsize < newsize) {
            newsize = oldsize;
        }

        System.arraycopy(source, 0, newarray, 0, newsize);

        return newarray;
    }

    /**
     * Returns a new array of given size, containing as many elements of
     * the original array as it can hold. N.B. Always returns a new array
     * even if newsize parameter is the same as the old size.
     */
    public static Object resizeArray(Object source, int newsize) {

        Object newarray =
            Array.newInstance(source.getClass().getComponentType(), newsize);
        int oldsize = Array.getLength(source);

        if (oldsize < newsize) {
            newsize = oldsize;
        }

        System.arraycopy(source, 0, newarray, 0, newsize);

        return newarray;
    }

    /**
     * Returns an array containing the elements of parameter source, with one
     * element removed or added. Parameter adjust {-1, +1} indicates the
     * operation. Parameter colindex indicates the position at which an element
     * is removed or added. Parameter addition is an Object to add when
     * adjust is +1.
     */
    public static Object toAdjustedArray(Object source, Object addition,
                                         int colindex, int adjust) {

        int newsize = Array.getLength(source) + adjust;
        Object newarray =
            Array.newInstance(source.getClass().getComponentType(), newsize);

        copyAdjustArray(source, newarray, addition, colindex, adjust);

        return newarray;
    }

    /**
     *  Copies elements of source to dest. If adjust is -1 the element at
     *  colindex is not copied. If adjust is +1 that element is filled with
     *  the Object addition. All the rest of the elements in source are
     *  shifted left or right accordingly when they are copied. If adjust is 0
     *  the addition is copied over the element at colindex.
     *
     *  No checks are perfomed on array sizes and an exception is thrown
     *  if they are not consistent with the other arguments.
     */
    public static void copyAdjustArray(Object source, Object dest,
                                       Object addition, int colindex,
                                       int adjust) {

        int length = Array.getLength(source);

        if (colindex < 0) {
            System.arraycopy(source, 0, dest, 0, length);

            return;
        }

        System.arraycopy(source, 0, dest, 0, colindex);

        if (adjust == 0) {
            int endcount = length - colindex - 1;

            Array.set(dest, colindex, addition);

            if (endcount > 0) {
                System.arraycopy(source, colindex + 1, dest, colindex + 1,
                                 endcount);
            }
        } else if (adjust < 0) {
            int endcount = length - colindex - 1;

            if (endcount > 0) {
                System.arraycopy(source, colindex + 1, dest, colindex,
                                 endcount);
            }
        } else {
            int endcount = length - colindex;

            Array.set(dest, colindex, addition);

            if (endcount > 0) {
                System.arraycopy(source, colindex, dest, colindex + 1,
                                 endcount);
            }
        }
    }

    /**
     * Returns a new array with the elements in collar adjusted to reflect
     * changes at colindex. <p>
     *
     * Each element in collarr represents an index into another array
     * otherarr. <p>
     *
     * colindex is the index at which an element is added or removed.
     * Each element in the result array represents the new,
     * adjusted index. <p>
     *
     * For each element of collarr that represents an index equal to
     * colindex and adjust is -1, the result will not contain that element
     * and will be shorter than collar by one element.
     *
     * @param  colarr the source array
     * @param  colindex index at which to perform adjustement
     * @param  adjust +1, 0 or -1
     * @return new, adjusted array
     */
    public static int[] toAdjustedColumnArray(int[] colarr, int colindex,
            int adjust) {

        if (colarr == null) {
            return null;
        }

        if (colindex < 0) {
            return colarr;
        }

        int[] intarr = new int[colarr.length];
        int   j      = 0;

        for (int i = 0; i < colarr.length; i++) {
            if (colarr[i] > colindex) {
                intarr[j] = colarr[i] + adjust;

                j++;
            } else if (colarr[i] == colindex) {
                if (adjust < 0) {

                    // skip an element from colarr
                } else {
                    intarr[j] = colarr[i] + adjust;

                    j++;
                }
            } else {
                intarr[j] = colarr[i];

                j++;
            }
        }

        if (colarr.length != j) {
            int[] newarr = new int[j];

            copyArray(intarr, newarr, j);

            return newarr;
        }

        return intarr;
    }

    /**
     * <p>
     *  Copies some elements of row into newRow by using columnMap as
     *  the list of indexes into row. That is, newRow[i] = row[columnMap[i]]
     *  for each i.
     * </p>
     * <p>
     *  columnMap and newRow are of equal length and are normally
     *  shorter than row.
     * </p>
     *
     *  @param row the source array
     *  @param columnMap the list of indexes into row
     *  @param newRow the destination array
     */
    public static void projectRow(Object[] row, int[] columnMap,
                                  Object[] newRow) {

        for (int i = 0; i < columnMap.length; i++) {
            newRow[i] = row[columnMap[i]];
        }
    }

    /**
     * <p>
     *   Copies integers from row to newRow, using columnMap as
     *   a list of indices into the source.  That is,
     *   newRow[i] = row[columnMap[i]] for each i.
     * </p>
     *
     * @param row
     * @param columnMap
     * @param newRow
     */
    public static void projectRow(int[] row, int[] columnMap, int[] newRow) {

        for (int i = 0; i < columnMap.length; i++) {
            newRow[i] = row[columnMap[i]];
        }
    }

    /**
     * <p>
     *   Copies from newRow to row, using the columnMap as a list of indices
     *   into the source.  That is row[columnMap[i]] = newRow[i] for all i.
     * </p>
     *
     *  @param row the target array
     *  @param columnMap the list of indexes into row
     *  @param newRow the source array
     */
    public static void projectRowReverse(Object[] row, int[] columnMap,
                                         Object[] newRow) {

        for (int i = 0; i < columnMap.length; i++) {
            row[columnMap[i]] = newRow[i];
        }
    }

/*
    public static void copyColumnValues(int[] row, int[] colindex,
                                        int[] colobject) {

        for (int i = 0; i < colindex.length; i++) {
            colobject[i] = row[colindex[i]];
        }
    }

    public static void copyColumnValues(boolean[] row, int[] colindex,
                                        boolean[] colobject) {

        for (int i = 0; i < colindex.length; i++) {
            colobject[i] = row[colindex[i]];
        }
    }

    public static void copyColumnValues(byte[] row, int[] colindex,
                                        byte[] colobject) {

        for (int i = 0; i < colindex.length; i++) {
            colobject[i] = row[colindex[i]];
        }
    }
*/
    public static void projectMap(int[] mainMap, int[] subMap,
                                  int[] newSubMap) {

        for (int i = 0; i < subMap.length; i++) {
            for (int j = 0; j < mainMap.length; j++) {
                if (subMap[i] == mainMap[j]) {
                    newSubMap[i] = j;
                    break;
                }
            }
        }
    }

    public static void fillSequence(int[] colindex) {

        for (int i = 0; i < colindex.length; i++) {
            colindex[i] = i;
        }
    }
}
